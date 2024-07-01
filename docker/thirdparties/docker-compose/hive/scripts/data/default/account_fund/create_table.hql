CREATE DATABASE IF NOT EXISTS default;
USE default;

CREATE TABLE `default.account_fund`(
  `batchno` string, 
  `appsheet_no` string, 
  `filedate` string, 
  `t_no` string, 
  `tano` string, 
  `t_name` string, 
  `chged_no` string, 
  `mob_no2` string, 
  `home_no` string, 
  `off_no` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/default/account_fund'
TBLPROPERTIES (
  'transient_lastDdlTime'='1669712244');

msck repair table account_fund;
