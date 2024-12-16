create database if not exists multi_catalog;

use multi_catalog;

CREATE TABLE `special_character_1_partition`(
  `name` string)
PARTITIONED BY ( 
  `part` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/special_character_1_partition'
TBLPROPERTIES (
  'transient_lastDdlTime'='1689575322');

set hive.msck.path.validation=ignore;

msck repair table special_character_1_partition;
