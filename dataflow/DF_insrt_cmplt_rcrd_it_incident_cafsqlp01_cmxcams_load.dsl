parameters{
	PMWorkflowRunId as string,
	PMWorkflowName as string,
	Path_Directory as string,
	FileName as string
}
source(output(
		machine_name as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	wildcardPaths:[(concat($Path_Directory, '/',$FileName))]) ~> SQitincident
SQitincident derive(app_name_out = 'CAMS_EDW_LOAD_COMPLETE',
		summary_out = 'COMPLETE',
		happened_dt_out = currentTimestamp(),
		machine_name = substring(toString(machine_name), toInteger(1), 64)) ~> EXPTRANS
EXPTRANS sink(allowSchemaDrift: true,
	validateSchema: false,
	umask: 0022,
	preCommands: [],
	postCommands: [],
	saveOrder: 1,
	mapColumn(
		happened_dt = happened_dt_out,
		machine_name,
		app_name = app_name_out,
		summary = summary_out
	)) ~> TGTitincident