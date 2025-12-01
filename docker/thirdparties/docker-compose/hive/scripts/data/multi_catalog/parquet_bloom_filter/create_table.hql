CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.parquet_bloom_filter`(
  `tinyint_col` tinyint,
  `smallint_col` smallint,
  `int_col` int,
  `bigint_col` bigint,
  `float_col` float,
  `double_col` double,
  `decimal_col` decimal(10,2),
  `name` string,
  `is_active` boolean,
  `created_date` date,
  `last_login` timestamp,
  `numeric_array` array<double>,
  `address_info` struct<city:string,population:int,area:double,is_capital:boolean>,
  `metrics` map<string,string>)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/parquet_bloom_filter'
TBLPROPERTIES (
  'totalSize'='0',
  'numRows'='0',
  'rawDataSize'='0',
  'parquet.dictionary.size.limit'='50',
  'numFiles'='0',
  'transient_lastDdlTime'='1763470218',
  'bucketing_version'='2',
  'parquet.compression'='ZSTD');

msck repair table parquet_bloom_filter;
