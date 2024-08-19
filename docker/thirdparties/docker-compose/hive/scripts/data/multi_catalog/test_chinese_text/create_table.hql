CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.test_chinese_text`(
  `id` int, 
  `col1` varchar(1148))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/test_chinese_text'
TBLPROPERTIES (
  'last_modified_by'='hadoop', 
  'last_modified_time'='1688972116', 
  'transient_lastDdlTime'='1688972116');

msck repair table test_chinese_text;
