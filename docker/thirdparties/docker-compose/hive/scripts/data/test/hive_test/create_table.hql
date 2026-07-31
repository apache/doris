CREATE DATABASE IF NOT EXISTS test;
USE test;

drop table if exists `test.hive_test`;

create table `test.hive_test`(
  `a` int, 
  `b` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/test/hive_test'
TBLPROPERTIES (
  'transient_lastDdlTime'='1670291786');
