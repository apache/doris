create database if not exists multi_catalog;
use multi_catalog;

CREATE TABLE `parquet_lzo_compression`(
  `col_int` int,
  `col_smallint` smallint,
  `col_tinyint` tinyint,
  `col_bigint` bigint,
  `col_float` float,
  `col_double` double,
  `col_boolean` boolean,
  `col_string` string,
  `col_char` char(10),
  `col_varchar` varchar(25),
  `col_date` date,
  `col_timestamp` timestamp,
  `col_decimal` decimal(10,2))
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/parquet_lzo_compression'
TBLPROPERTIES (
  'parquet.compression'='LZO',
  'transient_lastDdlTime'='1701173147');

msck repair table parquet_lzo_compression;

