CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.parquet_alter_column_to_string`(
  `col_int` string, 
  `col_smallint` string, 
  `col_tinyint` string, 
  `col_bigint` string, 
  `col_float` string, 
  `col_double` string, 
  `col_boolean` boolean, 
  `col_string` string, 
  `col_char` string, 
  `col_varchar` string, 
  `col_date` string, 
  `col_timestamp` string, 
  `col_decimal` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/parquet_alter_column_to_string'
TBLPROPERTIES (
  'last_modified_by'='hadoop', 
  'last_modified_time'='1697217389', 
  'transient_lastDdlTime'='1697217389');

msck repair table parquet_alter_column_to_string;
