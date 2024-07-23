CREATE DATABASE IF NOT EXISTS default;
USE default;

CREATE TABLE `default.student`(
  `id` varchar(50), 
  `name` varchar(50), 
  `age` int, 
  `gender` varchar(50), 
  `addr` varchar(50), 
  `phone` varchar(50))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/default/student'
TBLPROPERTIES (
  'transient_lastDdlTime'='1669364024');

msck repair table student;
