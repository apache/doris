CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.test_chinese_parquet`(
  `id` int, 
  `col1` varchar(1148))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/test_chinese_parquet'
TBLPROPERTIES (
  'last_modified_by'='hadoop', 
  'last_modified_time'='1688972107', 
  'transient_lastDdlTime'='1688972107');

msck repair table test_chinese_parquet;
