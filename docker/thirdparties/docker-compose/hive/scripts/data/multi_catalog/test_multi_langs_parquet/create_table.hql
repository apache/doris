CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

drop table if exists `multi_catalog.test_multi_langs_parquet`;

create table `multi_catalog.test_multi_langs_parquet`(
  `id` int, 
  `col1` varchar(1148))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/test_multi_langs_parquet'
TBLPROPERTIES (
  'transient_lastDdlTime'='1688971869');
