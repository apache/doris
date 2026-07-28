CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

drop table if exists `parquet_partitioned_one_column`;

create table `parquet_partitioned_one_column`(
  `t_float` float,
  `t_string` string,
  `t_timestamp` timestamp)
PARTITIONED BY (
 `t_int` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/parquet_partitioned_one_column';

msck repair table parquet_partitioned_one_column;
