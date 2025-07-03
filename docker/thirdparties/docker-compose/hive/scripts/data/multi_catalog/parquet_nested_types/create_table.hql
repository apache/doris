CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `nested_cross_page1_parquet`(
    `id` int,
    `array_col` array<int>,
    `description` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/nested_cross_page1_parquet';

msck repair table nested_cross_page1_parquet;

CREATE TABLE `nested_cross_page2_parquet`(
    id INT,
    nested_array_col ARRAY<ARRAY<INT>>,
    array_struct_col ARRAY<STRUCT<x:INT, y:STRING>>,
    map_array_col MAP<STRING, ARRAY<INT>>,
    complex_struct_col STRUCT<
        a: ARRAY<INT>,
        b: MAP<STRING, ARRAY<INT>>,
        c: STRUCT<
            x: ARRAY<INT>,
            y: STRING
        >
    >,
    description STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/nested_cross_page2_parquet';

msck repair table nested_cross_page2_parquet;

CREATE TABLE `nested_cross_page3_parquet`(
    `id` int,
    `array_col` array<int>,
    `description` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/nested_cross_page3_parquet';

msck repair table nested_cross_page3_parquet;

