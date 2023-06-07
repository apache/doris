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


CREATE TABLE `delta_byte_array`(
  `c_salutation` string,
  `c_first_name` string,
  `c_last_name` string,
  `c_preferred_cust_flag` string,
  `c_birth_country` string,
  `c_login` string,
  `c_email_address` string,
  `c_last_review_date` string,
  `c_customer_id` string
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/delta_byte_array'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');


CREATE TABLE `delta_length_byte_array`(
  `FRUIT` string
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/delta_length_byte_array'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table delta_length_byte_array;

CREATE EXTERNAL TABLE `delta_binary_packed`(
  bitwidth0 bigint,
  bitwidth1 bigint,
  bitwidth2 bigint,
  bitwidth3 bigint,
  bitwidth4 bigint,
  bitwidth5 bigint,
  bitwidth6 bigint,
  bitwidth7 bigint,
  bitwidth8 bigint,
  bitwidth9 bigint,
  bitwidth10 bigint,
  bitwidth11 bigint,
  bitwidth12 bigint,
  bitwidth13 bigint,
  bitwidth14 bigint,
  bitwidth15 bigint,
  bitwidth16 bigint,
  bitwidth17 bigint,
  bitwidth18 bigint,
  bitwidth19 bigint,
  bitwidth20 bigint,
  bitwidth21 bigint,
  bitwidth22 bigint,
  bitwidth23 bigint,
  bitwidth24 bigint,
  bitwidth25 bigint,
  bitwidth26 bigint,
  bitwidth27 bigint,
  bitwidth28 bigint,
  bitwidth29 bigint,
  bitwidth30 bigint,
  bitwidth31 bigint,
  bitwidth32 bigint,
  bitwidth33 bigint,
  bitwidth34 bigint,
  bitwidth35 bigint,
  bitwidth36 bigint,
  bitwidth37 bigint,
  bitwidth38 bigint,
  bitwidth39 bigint,
  bitwidth40 bigint,
  bitwidth41 bigint,
  bitwidth42 bigint,
  bitwidth43 bigint,
  bitwidth44 bigint,
  bitwidth45 bigint,
  bitwidth46 bigint,
  bitwidth47 bigint,
  bitwidth48 bigint,
  bitwidth49 bigint,
  bitwidth50 bigint,
  bitwidth51 bigint,
  bitwidth52 bigint,
  bitwidth53 bigint,
  bitwidth54 bigint,
  bitwidth55 bigint,
  bitwidth56 bigint,
  bitwidth57 bigint,
  bitwidth58 bigint,
  bitwidth59 bigint,
  bitwidth60 bigint,
  bitwidth61 bigint,
  bitwidth62 bigint,
  bitwidth63 bigint,
  bitwidth64 bigint,
  int_value  int
  )
STORED AS parquet
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/delta_binary_packed'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table delta_binary_packed;


CREATE TABLE `delta_encoding_required_column`(
   c_customer_sk int,
    c_current_cdemo_sk int,
   c_current_hdemo_sk int,
   c_current_addr_sk int,
   c_first_shipto_date_sk int,
   c_first_sales_date_sk  int,
   c_birth_day  int,
   c_birth_month  int,
     c_birth_year int,
     c_customer_id string,
     c_salutation string,
     c_first_name string,
     c_last_name string,
     c_preferred_cust_flag string,
     c_birth_country string,
     c_email_address string,
     c_last_review_date  string
  )
STORED AS parquet;

load data inpath '/user/doris/preinstalled_data/different_types_parquet/delta_encoding_required_column/delta_encoding_required_column.parquet' into table default.delta_encoding_required_column;

msck repair table delta_encoding_required_column;


CREATE EXTERNAL TABLE `delta_encoding_optional_column`(
    c_customer_sk int,
    c_current_cdemo_sk int,
    c_current_hdemo_sk int,
    c_current_addr_sk int,
    c_first_shipto_date_sk int,
    c_first_sales_date_sk  int,
     c_birth_year int,
     c_customer_id string,
     c_salutation string,
     c_first_name string,
     c_last_name string,
     c_preferred_cust_flag string,
     c_birth_country string
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/delta_encoding_optional_column'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table delta_encoding_optional_column;


CREATE TABLE `datapage_v1_snappy_compressed_checksum`(
  `a` int,
  `b` int
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/datapage_v1-snappy-compressed-checksum'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table datapage_v1_snappy_compressed_checksum;


CREATE TABLE `overflow_i16_page_cnt`(
  `inc` boolean
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/overflow_i16_page_cnt'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table overflow_i16_page_cnt;


CREATE TABLE `alltypes_tiny_pages`(
  bool_col boolean,
  tinyint_col int,
  smallint_col  int,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  id int,
  date_string_col string,
  string_col string,
  timestamp_col timestamp,
  year int,
  month int
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/alltypes_tiny_pages'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table alltypes_tiny_pages;


CREATE TABLE `alltypes_tiny_pages_plain`(
  bool_col boolean,
  tinyint_col int,
  smallint_col  int,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  id int,
  date_string_col string,
  string_col string,
  timestamp_col timestamp,
  year int,
  month int
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/alltypes_tiny_pages_plain'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table alltypes_tiny_pages_plain;

CREATE TABLE `example_string`(
  `strings` string
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\t',
  'serialization.format'='\t')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/example_string.parquet'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table example_string;


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

CREATE external TABLE `table_with_pars`(
  `id` int COMMENT 'id',
  `data` string COMMENT 'data')
PARTITIONED BY (
  `dt_par` date,
  `time_par` timestamp,
  `decimal_par1` decimal(8, 4),
  `decimal_par2` decimal(18, 6),
  `decimal_par3` decimal(38, 12))
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
  '/user/doris/preinstalled_data/csv_partition_table/table_with_pars/';

set hive.msck.path.validation=ignore;
msck repair table table_with_pars;

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

CREATE TABLE `unsupported_type_table`(
  k1 int,
  k2 string,
  k3 double,
  k4 map<string,int>,
  k5 STRUCT<
            houseno:    STRING
           ,streetname: STRING
           >,
  k6 int
);

CREATE TABLE `schema_evo_test_text`(
  id int,
  name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ',';
insert into `schema_evo_test_text` select 1, "kaka";
alter table `schema_evo_test_text` ADD COLUMNS (`ts` timestamp);
insert into `schema_evo_test_text` select 2, "messi", from_unixtime(to_unix_timestamp('20230101 13:01:03','yyyyMMdd HH:mm:ss'));

CREATE TABLE `schema_evo_test_parquet`(
  id int,
  name string
)
stored as parquet;
insert into `schema_evo_test_parquet` select 1, "kaka";
alter table `schema_evo_test_parquet` ADD COLUMNS (`ts` timestamp);
insert into `schema_evo_test_parquet` select 2, "messi", from_unixtime(to_unix_timestamp('20230101 13:01:03','yyyyMMdd HH:mm:ss'));

CREATE TABLE `schema_evo_test_orc`(
  id int,
  name string
)
stored as orc;
insert into `schema_evo_test_orc` select 1, "kaka";
alter table `schema_evo_test_orc` ADD COLUMNS (`ts` timestamp);
insert into `schema_evo_test_orc` select 2, "messi", from_unixtime(to_unix_timestamp('20230101 13:01:03','yyyyMMdd HH:mm:ss'));

show tables;
