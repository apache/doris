create database if not exists tpch_1000_parquet;
use tpch_1000_parquet;


CREATE TABLE IF NOT EXISTS `part`(
  `p_partkey` int,
  `p_name` varchar(55),
  `p_mfgr` char(25),
  `p_brand` char(10),
  `p_type` varchar(25),
  `p_size` int,
  `p_container` char(10),
  `p_retailprice` decimal(15,2),
  `p_comment` varchar(23))
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/suites/tpch_1000_parquet/part';

msck repair table part;