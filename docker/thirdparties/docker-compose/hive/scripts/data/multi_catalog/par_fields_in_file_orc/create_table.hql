CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.par_fields_in_file_orc`(
  `id` int, 
  `name` string, 
  `value` double)
PARTITIONED BY ( 
  `year` int, 
  `month` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/par_fields_in_file_orc'
TBLPROPERTIES (
  'transient_lastDdlTime'='1692774424');

msck repair table par_fields_in_file_orc;
