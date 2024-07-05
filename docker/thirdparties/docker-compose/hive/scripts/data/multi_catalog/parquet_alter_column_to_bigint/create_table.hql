CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.parquet_alter_column_to_bigint`(
  `col_int` bigint, 
  `col_smallint` bigint, 
  `col_tinyint` bigint, 
  `col_bigint` bigint, 
  `col_float` float, 
  `col_double` double, 
  `col_boolean` boolean, 
  `col_string` string, 
  `col_char` char(10), 
  `col_varchar` varchar(255), 
  `col_date` date, 
  `col_timestamp` timestamp, 
  `col_decimal` decimal(10,2))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/parquet_alter_column_to_bigint'
TBLPROPERTIES (
  'last_modified_by'='hadoop', 
  'last_modified_time'='1697217352', 
  'transient_lastDdlTime'='1697217352');

msck repair table parquet_alter_column_to_bigint;
