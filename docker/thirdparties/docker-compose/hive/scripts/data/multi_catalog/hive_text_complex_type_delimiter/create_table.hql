CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.hive_text_complex_type_delimiter`(
  `column1` int, 
  `column2` map<int,boolean>, 
  `column3` map<int,tinyint>, 
  `column4` map<string,smallint>, 
  `column5` map<string,int>, 
  `column6` map<string,bigint>, 
  `column7` map<string,float>, 
  `column8` map<string,double>, 
  `column9` map<int,string>, 
  `column10` map<string,timestamp>, 
  `column11` map<string,date>, 
  `column12` struct<field1:boolean,field2:tinyint,field3:smallint,field4:int,field5:bigint,field6:float,field7:double,field8:string,field9:timestamp,field10:date>)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'colelction.delim'='|', 
  'field.delim'=',', 
  'line.delim'='\n', 
  'mapkey.delim'=':', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/hive_text_complex_type_delimiter'
TBLPROPERTIES (
  'transient_lastDdlTime'='1690517298');

msck repair table hive_text_complex_type_delimiter;
