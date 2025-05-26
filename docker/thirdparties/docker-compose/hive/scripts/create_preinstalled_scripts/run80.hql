create database if not exists global_lazy_mat_db;
use global_lazy_mat_db;

CREATE TABLE `orc_topn_lazy_mat_table`(
  `id` int, 
  `name` string, 
  `value` double, 
  `active` boolean, 
  `score` double)PARTITIONED BY( 
  `file_id` int) STORED AS ORC LOCATION
        '/user/doris/preinstalled_data/orc_table/orc_global_lazy_mat_table/';

CREATE TABLE `parquet_topn_lazy_mat_table`(
  `id` int, 
  `name` string, 
  `value` double, 
  `active` boolean, 
  `score` double)PARTITIONED BY( 
  `file_id` int) STORED AS PARQUET LOCATION
        '/user/doris/preinstalled_data/parquet_table/parquet_global_lazy_mat_table/';

msck repair table orc_topn_lazy_mat_table;
msck repair table parquet_topn_lazy_mat_table;
