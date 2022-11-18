use default;

CREATE TABLE `customer`(
  `c_custkey` int,
  `c_name` string,
  `c_address` string,
  `c_nationkey` int,
  `c_phone` string,
  `c_acctbal` decimal(12,2),
  `c_mktsegment` string,
  `c_comment` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/tpch1.db/customer/'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

CREATE TABLE `lineitem`(
  `l_orderkey` int,
  `l_partkey` int,
  `l_suppkey` int,
  `l_linenumber` int,
  `l_quantity` decimal(12,2),
  `l_extendedprice` decimal(12,2),
  `l_discount` decimal(12,2),
  `l_tax` decimal(12,2),
  `l_returnflag` string,
  `l_linestatus` string,
  `l_shipdate` date,
  `l_commitdate` date,
  `l_receiptdate` date,
  `l_shipinstruct` string,
  `l_shipmode` string,
  `l_comment` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/tpch1.db/lineitem'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

CREATE TABLE `nation`(
  `n_nationkey` int,
  `n_name` string,
  `n_regionkey` int,
  `n_comment` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/tpch1.db/nation'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

CREATE TABLE `orders`(
  `o_orderkey` int,
  `o_custkey` int,
  `o_orderstatus` string,
  `o_totalprice` decimal(12,2),
  `o_orderdate` date,
  `o_orderpriority` string,
  `o_clerk` string,
  `o_shippriority` int,
  `o_comment` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/tpch1.db/orders'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

CREATE TABLE `part`(
  `p_partkey` int,
  `p_name` string,
  `p_mfgr` string,
  `p_brand` string,
  `p_type` string,
  `p_size` int,
  `p_container` string,
  `p_retailprice` decimal(12,2),
  `p_comment` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/tpch1.db/part'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

CREATE TABLE `partsupp`(
  `ps_partkey` int,
  `ps_suppkey` int,
  `ps_availqty` int,
  `ps_supplycost` decimal(12,2),
  `ps_comment` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/tpch1.db/partsupp'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

CREATE TABLE `region`(
  `r_regionkey` int,
  `r_name` string,
  `r_comment` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/tpch1.db/region'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

CREATE TABLE `supplier`(
  `s_suppkey` int,
  `s_name` string,
  `s_address` string,
  `s_nationkey` int,
  `s_phone` string,
  `s_acctbal` decimal(12,2),
  `s_comment` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/tpch1.db/supplier'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

CREATE TABLE `partition_table`(
  `l_orderkey` int,
  `l_partkey` int,
  `l_suppkey` int,
  `l_linenumber` int,
  `l_quantity` decimal(12,2),
  `l_extendedprice` decimal(12,2),
  `l_discount` decimal(12,2),
  `l_tax` decimal(12,2),
  `l_returnflag` string,
  `l_linestatus` string,
  `l_shipdate` date,
  `l_commitdate` date,
  `l_receiptdate` date,
  `l_shipinstruct` string,
  `l_shipmode` string,
  `l_comment` string)
partitioned by (nation string, city string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/parquet/partition_table'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table partition_table;

CREATE EXTERNAL TABLE IF NOT EXISTS `orc_all_types`(
  `tinyint_col` tinyint,
  `smallint_col` smallint,
  `int_col` int,
  `bigint_col` bigint,
  `boolean_col` boolean,
  `float_col` float,
  `double_col` double,
  `string_col` string,
  `binary_col` binary,
  `timestamp_col` timestamp,
  `decimal_col` decimal(12,4),
  `char_col` char(50),
  `varchar_col` varchar(50),
  `date_col` date,
  `list_double_col` array<double>,
  `list_string_col` array<string>)
PARTITIONED BY (
  `p1_col` string,
  `p2_col` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/orc/orc_all_types';

msck repair table orc_all_types;

show tables;

