create database if not exists multi_catalog;
use multi_catalog;

CREATE TABLE `hive_upper_case_parquet`(
  `id` int,
  `name` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/suites/test_upper_case_column_name/hive_upper_case_parquet'
TBLPROPERTIES (
  'spark.sql.create.version'='3.2.1',
  'spark.sql.sources.schema'='{"type":"struct","fields":[{"name":"ID","type":"integer","nullable":true,"metadata":{}},{"name":"NAME","type":"string","nullable":true,"metadata":{}}]}',
  'transient_lastDdlTime'='1674189051');

CREATE TABLE `hive_upper_case_orc`(
  `id` int,
  `name` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/suites/test_upper_case_column_name/hive_upper_case_orc'
TBLPROPERTIES (
  'spark.sql.create.version'='3.2.1',
  'spark.sql.sources.schema'='{"type":"struct","fields":[{"name":"ID","type":"integer","nullable":true,"metadata":{}},{"name":"NAME","type":"string","nullable":true,"metadata":{}}]}',
  'transient_lastDdlTime'='1674189057');

msck repair table hive_upper_case_parquet;
msck repair table hive_upper_case_orc;

