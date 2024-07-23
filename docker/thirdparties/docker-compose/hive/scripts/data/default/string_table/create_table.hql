CREATE DATABASE IF NOT EXISTS default;
USE default;

CREATE TABLE `default.string_table`(
  `p_partkey` string, 
  `p_name` string, 
  `p_mfgr` string, 
  `p_brand` string, 
  `p_type` string, 
  `p_size` string, 
  `p_con` string, 
  `p_r_price` string, 
  `p_comment` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/default/string_table'
TBLPROPERTIES (
  'transient_lastDdlTime'='1669712243');

msck repair table string_table;
