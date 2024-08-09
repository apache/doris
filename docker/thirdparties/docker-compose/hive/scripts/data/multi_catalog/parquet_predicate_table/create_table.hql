CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `multi_catalog.parquet_predicate_table`(
  `column_primitive_integer` int, 
  `column1_struct` struct<field0:bigint,field1:bigint>, 
  `column_primitive_bigint` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/parquet_predicate_table'
TBLPROPERTIES (
  'transient_lastDdlTime'='1692368377');

msck repair table parquet_predicate_table;
