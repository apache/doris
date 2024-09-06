CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.parquet_alter_column_to_char`(
  `col_int` char(10), 
  `col_smallint` char(10), 
  `col_tinyint` char(10), 
  `col_bigint` char(10), 
  `col_float` char(10), 
  `col_double` char(10), 
  `col_boolean` boolean, 
  `col_string` char(10), 
  `col_char` char(10), 
  `col_varchar` char(10), 
  `col_date` char(10), 
  `col_timestamp` char(10), 
  `col_decimal` char(10))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/parquet_alter_column_to_char'
TBLPROPERTIES (
  'last_modified_by'='hadoop', 
  'last_modified_time'='1697275142', 
  'transient_lastDdlTime'='1697275142');

msck repair table parquet_alter_column_to_char;
