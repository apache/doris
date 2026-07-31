create database if not exists multi_catalog;
use multi_catalog;

drop table if exists `hive_upper_case_parquet`;

create table `hive_upper_case_parquet`(
  `id` int,
  `name` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/hive_upper_case_parquet'
TBLPROPERTIES (
  'spark.sql.create.version'='3.2.1',
  'spark.sql.sources.schema'='{"type":"struct","fields":[{"name":"ID","type":"integer","nullable":true,"metadata":{}},{"name":"NAME","type":"string","nullable":true,"metadata":{}}]}',
  'transient_lastDdlTime'='1674189051');
