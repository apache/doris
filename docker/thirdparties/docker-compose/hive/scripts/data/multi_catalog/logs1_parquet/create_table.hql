
create database if not exists multi_catalog;
use multi_catalog;

CREATE TABLE IF NOT EXISTS `logs1_parquet`(
  `log_time` timestamp,
  `machine_name` varchar(128),
  `machine_group` varchar(128),
  `cpu_idle` float,
  `cpu_nice` float,
  `cpu_system` float,
  `cpu_user` float,
  `cpu_wio` float,
  `disk_free` float,
  `disk_total` float,
  `part_max_used` float,
  `load_fifteen` float,
  `load_five` float,
  `load_one` float,
  `mem_buffers` float,
  `mem_cached` float,
  `mem_free` float,
  `mem_shared` float,
  `swap_free` float,
  `bytes_in` float,
  `bytes_out` float)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/logs1_parquet';

msck repair table logs1_parquet;