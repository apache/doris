CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.parquet_alter_column_to_varchar`(
  `col_int` varchar(20), 
  `col_smallint` varchar(20), 
  `col_tinyint` varchar(20), 
  `col_bigint` varchar(20), 
  `col_float` varchar(20), 
  `col_double` varchar(20), 
  `col_boolean` boolean, 
  `col_string` varchar(20), 
  `col_char` varchar(20), 
  `col_varchar` varchar(20), 
  `col_date` varchar(20), 
  `col_timestamp` varchar(20), 
  `col_decimal` varchar(20))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/parquet_alter_column_to_varchar'
TBLPROPERTIES (
  'last_modified_by'='hadoop', 
  'last_modified_time'='1697275145', 
  'transient_lastDdlTime'='1697275145');

msck repair table parquet_alter_column_to_varchar;
