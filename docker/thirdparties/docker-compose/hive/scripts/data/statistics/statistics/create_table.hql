CREATE DATABASE IF NOT EXISTS statistics;
USE statistics;

CREATE TABLE `statistics.statistics`(
  `lo_orderkey` int, 
  `lo_linenumber` int, 
  `lo_custkey` int, 
  `lo_partkey` int, 
  `lo_suppkey` int, 
  `lo_orderdate` int, 
  `lo_orderpriority` string, 
  `lo_shippriority` int, 
  `lo_quantity` int, 
  `lo_extendedprice` int, 
  `lo_ordtotalprice` int, 
  `lo_discount` int, 
  `lo_revenue` int, 
  `lo_supplycost` int, 
  `lo_tax` int, 
  `lo_commitdate` int, 
  `lo_shipmode` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/user/doris/suites/statistics/statistics'
TBLPROPERTIES (
  'transient_lastDdlTime'='1688892552');

msck repair table statistics;
