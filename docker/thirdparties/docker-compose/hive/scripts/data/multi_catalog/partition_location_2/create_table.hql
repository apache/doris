CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.partition_location_2`(
  `id` int, 
  `name` string)
PARTITIONED BY ( 
  `part1` string, 
  `part2` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/partition_location_2'
TBLPROPERTIES (
  'transient_lastDdlTime'='1682406065');

ALTER TABLE partition_location_2 ADD PARTITION (part1='part1_1', part2='part2_1') LOCATION '/user/doris/suites/multi_catalog/partition_location_2/part1=part1_1/part2=part2_1';
ALTER TABLE partition_location_2 ADD PARTITION (part1='part1_2', part2='part2_2') LOCATION '/user/doris/suites/multi_catalog/partition_location_2/20230425';

msck repair table partition_location_2;
