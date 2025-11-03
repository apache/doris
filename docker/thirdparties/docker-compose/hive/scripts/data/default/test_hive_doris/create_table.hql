CREATE DATABASE IF NOT EXISTS default;
USE default;

CREATE TABLE `default.test_hive_doris`(
  `id` varchar(100), 
  `age` varchar(100))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/default/test_hive_doris'
TBLPROPERTIES (
  'transient_lastDdlTime'='1669712244');

msck repair table test_hive_doris;
