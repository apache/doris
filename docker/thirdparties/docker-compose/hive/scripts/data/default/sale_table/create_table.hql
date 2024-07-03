CREATE DATABASE IF NOT EXISTS default;
USE default;

CREATE TABLE `default.sale_table`(
  `bill_code` varchar(500), 
  `dates` varchar(500), 
  `ord_year` varchar(500), 
  `ord_month` varchar(500), 
  `ord_quarter` varchar(500), 
  `on_time` varchar(500))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION '/user/doris/suites/default/sale_table'
TBLPROPERTIES (
  'transient_lastDdlTime'='1669712244');

msck repair table sale_table;
