CREATE DATABASE IF NOT EXISTS default;
USE default;

CREATE TABLE `default.test1`(
  `col_1` int, 
  `col_2` varchar(20), 
  `col_3` int, 
  `col_4` int, 
  `col_5` varchar(20))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/default/test1'
TBLPROPERTIES (
  'transient_lastDdlTime'='1669712243');

msck repair table test1;
