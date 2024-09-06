CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.par_fields_in_file_parquet`(
  `id` int, 
  `name` string, 
  `value` double)
PARTITIONED BY ( 
  `year` int, 
  `month` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/par_fields_in_file_parquet'
TBLPROPERTIES (
  'transient_lastDdlTime'='1692774410');

msck repair table par_fields_in_file_parquet;
