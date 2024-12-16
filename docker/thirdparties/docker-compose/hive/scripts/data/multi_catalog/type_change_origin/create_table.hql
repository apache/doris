CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.type_change_origin`(
  `numeric_boolean` boolean, 
  `numeric_tinyint` tinyint, 
  `numeric_smallint` smallint, 
  `numeric_int` int, 
  `numeric_bigint` bigint, 
  `numeric_float` float, 
  `numeric_double` double, 
  `ts_boolean` boolean, 
  `ts_int` int, 
  `ts_double` double, 
  `ts_decimal` decimal(12,4), 
  `ts_date` date, 
  `ts_timestamp` timestamp, 
  `fs_boolean` string, 
  `fs_int` string, 
  `fs_float` string, 
  `fs_decimal` string, 
  `fs_date` string, 
  `fs_timestamp` string, 
  `td_boolean` boolean, 
  `td_bigint` bigint, 
  `td_float` float, 
  `td_double` double, 
  `td_decimal` decimal(8,4), 
  `fd_boolean` decimal(12,3), 
  `fd_int` decimal(10,2), 
  `fd_float` decimal(11,4), 
  `fd_double` decimal(9,4), 
  `date_timestamp` date, 
  `timestamp_date` timestamp)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/type_change_origin'
TBLPROPERTIES (
  'transient_lastDdlTime'='1712485085');

msck repair table type_change_origin;
