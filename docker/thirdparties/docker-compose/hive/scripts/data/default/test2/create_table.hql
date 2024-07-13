CREATE DATABASE IF NOT EXISTS default;
USE default;

CREATE TABLE `default.test2`(
  `id` int, 
  `name` string, 
  `age` string, 
  `avg_patient_time` double, 
  `dt` date)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/default/test2'
TBLPROPERTIES (
  'transient_lastDdlTime'='1669712244');

msck repair table test2;
