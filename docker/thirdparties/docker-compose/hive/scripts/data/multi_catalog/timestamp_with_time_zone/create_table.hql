CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.timestamp_with_time_zone`(
  `date_col` date, 
  `timestamp_col` timestamp)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/timestamp_with_time_zone'
TBLPROPERTIES (
  'transient_lastDdlTime'='1712113278');

msck repair table timestamp_with_time_zone;
