use default;

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
  '/user/doris/preinstalled_data/parquet/partition_table'
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
  '/user/doris/preinstalled_data/orc/orc_all_types';

msck repair table orc_all_types;

CREATE TABLE `student` (
  id varchar(50),
  name varchar(50),
  age int,
  gender varchar(50),
  addr varchar(50),
  phone varchar(50)
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
LOCATION '/user/doris/preinstalled_data/data_case/student'
TBLPROPERTIES ('transient_lastDdlTime'='1658816839');

CREATE TABLE `lineorder` (
  `lo_orderkey` int, 
  `lo_linenumber` int,
  `lo_custkey` int, 
  `lo_partkey` int, 
  `lo_suppkey` int, 
  `lo_orderdate` int, 
  `lo_orderpriority` varchar(16),
  `lo_shippriority` int, 
  `lo_quantity` int, 
  `lo_extendedprice` int, 
  `lo_ordtotalprice` int, 
  `lo_discount` int,
  `lo_revenue` int,
  `lo_supplycost` int, 
  `lo_tax` int, 
  `lo_commitdate` int, 
  `lo_shipmode` varchar(11) 
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
LOCATION '/user/doris/preinstalled_data/data_case/lineorder'
TBLPROPERTIES ('transient_lastDdlTime'='1658816839');

CREATE TABLE `test1` (
  col_1 int,
  col_2 varchar(20),
  col_3 int,
  col_4 int,
  col_5 varchar(20)
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
LOCATION '/user/doris/preinstalled_data/data_case/test1'
TBLPROPERTIES ('transient_lastDdlTime'='1658816839');

CREATE TABLE `string_table` (
  p_partkey string,
  p_name string,
  p_mfgr string,
  p_brand string,
  p_type string,
  p_size string,
  p_con string,
  p_r_price string,
  p_comment string
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
LOCATION '/user/doris/preinstalled_data/data_case/string_table'
TBLPROPERTIES ('transient_lastDdlTime'='1658816839');

CREATE TABLE `account_fund` (
  `batchno` string,
  `appsheet_no` string,
  `filedate` string,
  `t_no` string,
  `tano` string,
  `t_name` string,
  `chged_no` string,
  `mob_no2` string,
  `home_no` string,
  `off_no` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/data_case/account_fund'
TBLPROPERTIES ('transient_lastDdlTime'='1658816839');

create table sale_table (
  `bill_code` varchar(500),
  `dates` varchar(500),
  `ord_year` varchar(500),
  `ord_month` varchar(500),
  `ord_quarter` varchar(500),
  `on_time` varchar(500)
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/data_case/sale_table'
TBLPROPERTIES ('transient_lastDdlTime'='1658816839');

create table t_hive (
  `k1` int,
  `k2` char(10),
  `k3` date,
  `k5` varchar(20),
  `k6` double
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/data_case/t_hive'
TBLPROPERTIES ('transient_lastDdlTime'='1658816839');

create table hive01 (
  first_year int,
  d_disease varchar(200),
  i_day int,
  card_cnt bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
LOCATION
  '/user/doris/preinstalled_data/data_case/hive01'
TBLPROPERTIES ('transient_lastDdlTime'='1658816839');

CREATE TABLE test2 (
id int,
name string ,
age string ,
avg_patient_time double,
dt date
)
row format delimited fields terminated by ','
stored as textfile
LOCATION '/user/doris/preinstalled_data/data_case/test2'
TBLPROPERTIES ('transient_lastDdlTime'='1658816839');

create table test_hive_doris(
id varchar(100),
age varchar(100)
)
row format delimited fields terminated by ','
stored as textfile
LOCATION '/user/doris/preinstalled_data/data_case/test_hive_doris'
TBLPROPERTIES ('transient_lastDdlTime'='1658816839');

CREATE external TABLE `table_with_vertical_line`(
  `k1` string COMMENT 'k1',
  `k2` string COMMENT 'k2',
  `k3` string COMMENT 'k3',
  `k4` string COMMENT 'k4',
  `k5` string COMMENT 'k5',
  `k6` string COMMENT 'k6',
  `k7` string COMMENT 'k7',
  `k8` string COMMENT 'k8',
  `k9` string COMMENT 'k9',
  `k10` string COMMENT 'k10',
  `k11` string COMMENT 'k11',
  `k12` string COMMENT 'k12',
  `k13` string COMMENT 'k13',
  `k14` string COMMENT 'k14',
  `k15` string COMMENT 'k15')
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='|',
  'serialization.format'='|')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/csv_partition_table/table_with_vertical_line/'
TBLPROPERTIES (
  'transient_lastDdlTime'='1669304897');

msck repair table table_with_vertical_line;

CREATE TABLE `table_with_x01`(
  `k1` string COMMENT 'k1',
  `k2` string COMMENT 'k2',
  `k3` string COMMENT 'k3',
  `k4` string COMMENT 'k4',
  `k5` string COMMENT 'k5',
  `k6` string COMMENT 'k6',
  `k7` string COMMENT 'k7',
  `k8` string COMMENT 'k8',
  `k9` string COMMENT 'k9',
  `k10` string COMMENT 'k10',
  `k11` string COMMENT 'k11',
  `k12` string COMMENT 'k12',
  `k13` string COMMENT 'k13',
  `k14` string COMMENT 'k14',
  `k15` string COMMENT 'k15')
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/csv_partition_table/table_with_x01/'
TBLPROPERTIES (
  'transient_lastDdlTime'='1669360080');

msck repair table table_with_x01;

show tables;
