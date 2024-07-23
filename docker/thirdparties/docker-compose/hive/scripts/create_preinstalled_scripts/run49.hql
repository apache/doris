CREATE TABLE `orc_predicate_table`(
`column_primitive_integer` int,
`column1_struct` struct<field0:bigint,field1:bigint>,
`column_primitive_bigint` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/orc_table/orc_predicate_table';

msck repair table orc_predicate_table;


