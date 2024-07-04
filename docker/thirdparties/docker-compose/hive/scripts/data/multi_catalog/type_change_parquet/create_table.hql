CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.type_change_parquet`(
  `numeric_boolean` int, 
  `numeric_tinyint` double, 
  `numeric_smallint` bigint, 
  `numeric_int` int, 
  `numeric_bigint` float, 
  `numeric_float` float, 
  `numeric_double` bigint, 
  `ts_boolean` string, 
  `ts_int` string, 
  `ts_double` string, 
  `ts_decimal` string, 
  `ts_date` string, 
  `ts_timestamp` string, 
  `fs_boolean` boolean, 
  `fs_int` int, 
  `fs_float` float, 
  `fs_decimal` decimal(12,3), 
  `fs_date` date, 
  `fs_timestamp` timestamp, 
  `td_boolean` decimal(5,2), 
  `td_bigint` decimal(11,4), 
  `td_float` decimal(10,3), 
  `td_double` decimal(9,2), 
  `td_decimal` decimal(8,4), 
  `fd_boolean` boolean, 
  `fd_int` int, 
  `fd_float` float, 
  `fd_double` double, 
  `date_timestamp` timestamp, 
  `timestamp_date` date)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/type_change_parquet'
TBLPROPERTIES (
  'transient_lastDdlTime'='1712485017');

msck repair table type_change_parquet;
