create database if not exists multi_catalog;

use multi_catalog;

CREATE TABLE `region`(
  `r_regionkey` int, 
  `r_name` char(25))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/region'
TBLPROPERTIES (
  'transient_lastDdlTime'='1670483235');

msck repair table region;
