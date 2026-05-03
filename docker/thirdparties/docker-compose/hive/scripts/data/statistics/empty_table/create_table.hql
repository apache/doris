CREATE DATABASE IF NOT EXISTS statistics;
USE statistics;

drop table if exists `statistics.empty_table`;

create table `statistics.empty_table`(
  `id` int, 
  `name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
  'transient_lastDdlTime'='1702352468');
