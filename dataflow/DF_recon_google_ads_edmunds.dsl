parameters{
	Output_recon_File as string ('recon_stats_google_ads_edmunds.csv'),
	Source_recon_File as string,
	Source_recon_Path as string
}
source(output(
		TGT_REC_COUNT as decimal(38,0)
	),
	allowSchemaDrift: true,
	validateSchema: false,
	query: 'SELECT CASE WHEN DATE_TRUNC(\'DAY\',"insert_ts") = current_date THEN record_count  WHEN DATE_TRUNC(\'DAY\',"insert_ts") <> current_date THEN  0   END tgt_rec_count FROM sales.edw_stage.ingestion_record_count  WHERE insert_ts= (SELECT MAX(insert_ts) FROM sales.edw_stage.ingestion_record_count WHERE feed_name=\'google_ads\')',
	format: 'query') ~> SQingestionrecordcount
source(output(
		Record_Count as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	rowUrlColumn: 'currentlyprocessedfilename',
	wildcardPaths:[(concat($Source_recon_Path,'/',$Source_recon_File,'*.txt'))]) ~> SQedlcountfile
SQingestionrecordcount derive(dummy_DB = '1') ~> EXPTRANS
aggregate1 derive(File_record_Count = File_record_Count,
		File_Name = replace(replace(replace(toString(currentlyprocessedfilename),'[',''),']',''),',',';'),
		dummy_FF = '1') ~> EXPTRANS1
EXPTRANS, EXPTRANS1 join(dummy_DB == dummy_FF,
	joinType:'inner',
	matchType:'exact',
	ignoreSpaces: false,
	broadcast: 'auto')~> JNRTRANS
JNRTRANS derive(File_record_Count = File_record_Count,
		TGT_REC_COUNT = TGT_REC_COUNT,
		recon_stat_var = :v_recon_status,
		v_recon_status := iif(File_record_Count == toDecimal(TGT_REC_COUNT), 'SUCCESS', 'FAILURE')) ~> EXPTRANS2
EXPTRANS2 select(mapColumn(
		TGT_CNT = TGT_REC_COUNT,
		SRC_CNT = File_record_Count,
		SRC_FileName = File_Name,
		RECON_STATUS = recon_stat_var
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select1
SQedlcountfile aggregate(File_record_Count = sum(toInteger(Record_Count)),
		currentlyprocessedfilename = collect(currentlyprocessedfilename)) ~> aggregate1
select1 sink(allowSchemaDrift: true,
	validateSchema: false,
	partitionFileNames:[($Output_recon_File)],
	umask: 0022,
	preCommands: [],
	postCommands: [],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	partitionBy('hash', 1)) ~> sink1
select1 sink(validateSchema: false,
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	store: 'cache',
	format: 'inline',
	output: true,
	saveOrder: 1) ~> RECONSTATUS