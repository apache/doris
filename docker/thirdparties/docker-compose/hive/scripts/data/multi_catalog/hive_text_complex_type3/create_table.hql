CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.hive_text_complex_type3`(
  `id` int, 
  `column1` map<int,struct<a:int,b:int,c:array<map<string,array<array<array<array<struct<aa:int,bb:string,cc:boolean>>>>>>>>>, 
  `column2` array<struct<a:int,b:array<map<string,map<int,map<string,array<struct<aaa:struct<aa:int,bb:string,cc:boolean>,bbb:boolean,ccc:string,ddd:date>>>>>>,c:int>>, 
  `column3` struct<a:int,b:struct<a:array<map<string,array<map<int,map<boolean,array<struct<aa:int,bb:string,cc:boolean>>>>>>>>,c:map<int,string>,d:array<int>>, 
  `column4` map<int,map<date,map<int,map<double,map<string,map<int,map<string,map<int,map<int,map<int,boolean>>>>>>>>>>, 
  `column5` array<array<array<array<array<array<array<array<array<array<int>>>>>>>>>>, 
  `column6` struct<a:map<int,map<int,map<string,string>>>,b:struct<aa:struct<aaa:struct<aaaa:struct<aaaaa:struct<aaaaaa:struct<aaaaaaa:struct<aaaaaaaa:struct<a1:int,a2:string>,bbbbbbbb:map<int,int>>,bbbbbbb:array<string>>>>,bbbb:map<int,double>>>,bb:date>,c:date>)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'hive.serialization.extend.nesting.levels'='true') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/hive_text_complex_type3'
TBLPROPERTIES (
  'transient_lastDdlTime'='1693389680');

msck repair table hive_text_complex_type3;
