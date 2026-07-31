create database if not exists global_lazy_mat_db;
use global_lazy_mat_db;

drop table if exists `orc_topn_lazy_mat_table`;

create table `orc_topn_lazy_mat_table`(
  `id` int, 
  `name` string, 
  `value` double, 
  `active` boolean, 
  `score` double)PARTITIONED BY( 
  `file_id` int) STORED AS ORC LOCATION
        '/user/doris/preinstalled_data/orc_table/orc_global_lazy_mat_table/';

drop table if exists `parquet_topn_lazy_mat_table`;

create table `parquet_topn_lazy_mat_table`(
  `id` int, 
  `name` string, 
  `value` double, 
  `active` boolean, 
  `score` double)PARTITIONED BY( 
  `file_id` int) STORED AS PARQUET LOCATION
        '/user/doris/preinstalled_data/parquet_table/parquet_global_lazy_mat_table/';

msck repair table orc_topn_lazy_mat_table;
msck repair table parquet_topn_lazy_mat_table;



drop table if exists `parquet_topn_lazy_complex_table`;



create table `parquet_topn_lazy_complex_table`(
  id INT,
  col1 STRING,
  col2 STRUCT<a: INT, b: ARRAY<INT>>,
  col3 MAP<INT, ARRAY<STRING>>
) STORED AS PARQUET LOCATION
        '/user/doris/preinstalled_data/parquet_table/parquet_topn_lazy_complex_table/';

drop table if exists `parquet_topn_lazy_complex_table_multi_pages`;

create table `parquet_topn_lazy_complex_table_multi_pages`(
  id INT,
  col1 STRING,
  col2 STRUCT<a: INT, b: ARRAY<INT>>,
  col3 MAP<INT, ARRAY<STRING>>
) STORED AS PARQUET LOCATION
        '/user/doris/preinstalled_data/parquet_table/parquet_topn_lazy_complex_table_multi_pages/';
