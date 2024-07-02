CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.test_multi_langs_orc`(
  `id` int, 
  `col1` varchar(1148))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/test_multi_langs_orc'
TBLPROPERTIES (
  'transient_lastDdlTime'='1688971851');

msck repair table test_multi_langs_orc;
