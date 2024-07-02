CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.partition_location_1`(
  `id` int, 
  `name` string)
PARTITIONED BY ( 
  `part` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/partition_location_1'
TBLPROPERTIES (
  'transient_lastDdlTime'='1682405696');

ALTER TABLE partition_location_1 ADD PARTITION (part='part1') LOCATION '/user/doris/suites/multi_catalog/partition_location_1/part=part1';
ALTER TABLE partition_location_1 ADD PARTITION (part='part2') LOCATION '/user/doris/suites/multi_catalog/partition_location_1/20230425';

msck repair table partition_location_1;
