parameters{
	P_EDW_Database_Table_Name as string,
	P_EDL_Container as string,
	P_EDL_Checkpoint_Path as string
}
source(output(
		edw_database_name as string,
		dbrx_database_name as string,
		edw_table_name as string,
		dbrx_table_name as string,
		edw_database_table_name as string,
		encrypted_flag as string,
		frequency_code as string,
		extract_type_code as string,
		from_run_ts as timestamp,
		to_run_ts as timestamp,
		temp_run_ts as timestamp,
		edl_insert_ts as timestamp,
		edl_update_ts as timestamp,
		edl_update_user as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'delta',
	fileSystem: ($P_EDL_Container),
	folderPath: ($P_EDL_Checkpoint_Path)) ~> PostSQL
filter alterRow(updateIf(edw_database_table_name==$P_EDW_Database_Table_Name)) ~> checktablename3
checktablename3 derive(edl_update_ts = currentTimestamp()) ~> Setupdatetime
PostSQL filter(edw_database_table_name==$P_EDW_Database_Table_Name) ~> filter
Setupdatetime sink(allowSchemaDrift: true,
	validateSchema: false,
	format: 'delta',
	fileSystem: ($P_EDL_Container),
	folderPath: ($P_EDL_Checkpoint_Path),
	mergeSchema: false,
	autoCompact: false,
	optimizedWrite: false,
	vacuum: 0,
	deletable: false,
	insertable: false,
	updateable: true,
	upsertable: false,
	keys:['edw_database_table_name'],
	pruneCondition: ['edw_database_table_name' -> ([$P_EDW_Database_Table_Name])],
	umask: 0022,
	preCommands: [],
	postCommands: [],
	skipDuplicateMapOutputs: true,
	saveOrder: 1,
	mapColumn(
		from_run_ts = to_run_ts,
		to_run_ts = temp_run_ts,
		edl_update_ts,
		edw_database_table_name
	)) ~> PostSQLsink