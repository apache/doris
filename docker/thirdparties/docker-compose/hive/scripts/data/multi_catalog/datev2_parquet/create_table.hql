create database if not exists multi_catalog;

use multi_catalog;

CREATE external TABLE `datev2_parquet`(
  `id` int,
  `day` date)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/datev2_parquet'
TBLPROPERTIES (
  'transient_lastDdlTime'='1688118725');

msck repair table datev2_parquet;

