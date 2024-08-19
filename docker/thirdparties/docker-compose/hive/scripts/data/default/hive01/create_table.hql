CREATE DATABASE IF NOT EXISTS default;
USE default;

CREATE TABLE `default.hive01`(
  `first_year` int, 
  `d_disease` varchar(200), 
  `i_day` int, 
  `card_cnt` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/default/hive01'
TBLPROPERTIES (
  'transient_lastDdlTime'='1669712244');

msck repair table hive01;
