source(output(
		customer_id as integer,
		first_name as string,
		last_name as string,
		email as string,
		phone_number as string
	),
	allowSchemaDrift: true,
	validateSchema: false,
	isolationLevel: 'READ_UNCOMMITTED',
	format: 'table',
	partitionBy('hash', 1)) ~> source1
source1 filter(customer_id>=100) ~> filter1
filter1 sink(allowSchemaDrift: true,
	validateSchema: false,
	input(
		ID as string,
		DESC as string
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	partitionBy('roundRobin', 2)) ~> sink1