CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.hive_text_complex_type2`(
  `id` int, 
  `col1` map<int,map<string,int>>, 
  `col2` array<array<map<int,boolean>>>, 
  `col3` struct<field1:int,field2:map<int,string>,field3:struct<sub_field1:boolean,sub_field2:boolean,sub_field3:int>,field4:array<int>>, 
  `col4` map<int,map<int,array<boolean>>>, 
  `col5` map<int,struct<sub_field1:boolean,sub_field2:string>>)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/hive_text_complex_type2'
TBLPROPERTIES (
  'transient_lastDdlTime'='1692719086');

msck repair table hive_text_complex_type2;
