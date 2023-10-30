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
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/delta_encoding_required_column/'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

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

-- Currently docker is hive 2.x version. Hive 2.x versioned full-acid tables need to run major compaction.
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table orc_full_acid (id INT, value STRING)
CLUSTERED BY (id) INTO 3 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional' = 'true');

insert into orc_full_acid values
(1, 'A'),
(2, 'B'),
(3, 'C');

update orc_full_acid set value = 'CC' where id = 3;

alter table orc_full_acid compact 'major';

create table orc_full_acid_par (id INT, value STRING)
PARTITIONED BY (part_col INT)
CLUSTERED BY (id) INTO 3 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional' = 'true');

insert into orc_full_acid_par PARTITION(part_col=20230101) values
(1, 'A'),
(2, 'B'),
(3, 'C');

insert into orc_full_acid_par PARTITION(part_col=20230102) values
(4, 'D'),
(5, 'E'),
(6, 'F');

update orc_full_acid_par set value = 'BB' where id = 2;

alter table orc_full_acid_par PARTITION(part_col=20230101) compact 'major';
alter table orc_full_acid_par PARTITION(part_col=20230102) compact 'major';

CREATE TABLE `test_different_column_orders_orc`(
  `name` string,
  `id` int,
  `city` string,
  `age` int,
  `sex` string)
STORED AS ORC
LOCATION
  '/user/doris/preinstalled_data/test_different_column_orders/orc';

CREATE TABLE `test_different_column_orders_parquet`(
  `name` string,
  `id` int,
  `city` string,
  `age` int,
  `sex` string)
STORED AS PARQUET
LOCATION
  '/user/doris/preinstalled_data/test_different_column_orders/parquet';

CREATE TABLE `parquet_partition_table`(
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
  '/user/doris/preinstalled_data/parquet_table/parquet_partition_table'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table parquet_partition_table;


CREATE EXTERNAL TABLE `parquet_delta_binary_packed`(
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
  '/user/doris/preinstalled_data/parquet_table/parquet_delta_binary_packed'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table parquet_delta_binary_packed;

CREATE TABLE `parquet_alltypes_tiny_pages`(
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
  '/user/doris/preinstalled_data/parquet_table/parquet_alltypes_tiny_pages'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table parquet_alltypes_tiny_pages;


CREATE EXTERNAL TABLE IF NOT EXISTS `orc_all_types_partition`(
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
  '/user/doris/preinstalled_data/orc_table/orc_all_types_partition';

msck repair table orc_all_types_partition;

CREATE external TABLE `csv_partition_table`(
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
  '/user/doris/preinstalled_data/csv/csv_partition_table/'
TBLPROPERTIES (
  'transient_lastDdlTime'='1669304897');

msck repair table csv_partition_table;

CREATE TABLE `parquet_all_types`(
    `t_null_string` string,
    `t_null_varchar` varchar(65535),
    `t_null_char` char(10),
    `t_null_decimal_precision_2` decimal(2,1),
    `t_null_decimal_precision_4` decimal(4,2),
    `t_null_decimal_precision_8` decimal(8,4),
    `t_null_decimal_precision_17` decimal(17,8),
    `t_null_decimal_precision_18` decimal(18,8),
    `t_null_decimal_precision_38` decimal(38,16),
    `t_empty_string` string,
    `t_string` string,
    `t_empty_varchar` varchar(65535),
    `t_varchar` varchar(65535),
    `t_varchar_max_length` varchar(65535),
    `t_char` char(10),
    `t_int` int,
    `t_bigint` bigint,
    `t_float` float,
    `t_double` double,
    `t_boolean_true` boolean,
    `t_boolean_false` boolean,
    `t_decimal_precision_2` decimal(2,1),
    `t_decimal_precision_4` decimal(4,2),
    `t_decimal_precision_8` decimal(8,4),
    `t_decimal_precision_17` decimal(17,8),
    `t_decimal_precision_18` decimal(18,8),
    `t_decimal_precision_38` decimal(38,16),
    `t_binary` binary,
    `t_map_string` map<string,string>,
    `t_map_varchar` map<varchar(65535),varchar(65535)>,
    `t_map_char` map<char(10),char(10)>,
    `t_map_int` map<int,int>,
    `t_map_bigint` map<bigint,bigint>,
    `t_map_float` map<float,float>,
    `t_map_double` map<double,double>,
    `t_map_boolean` map<boolean,boolean>,
    `t_map_decimal_precision_2` map<decimal(2,1),decimal(2,1)>,
    `t_map_decimal_precision_4` map<decimal(4,2),decimal(4,2)>,
    `t_map_decimal_precision_8` map<decimal(8,4),decimal(8,4)>,
    `t_map_decimal_precision_17` map<decimal(17,8),decimal(17,8)>,
    `t_map_decimal_precision_18` map<decimal(18,8),decimal(18,8)>,
    `t_map_decimal_precision_38` map<decimal(38,16),decimal(38,16)>,
    `t_array_string` array<string>,
    `t_array_int` array<int>,
    `t_array_bigint` array<bigint>,
    `t_array_float` array<float>,
    `t_array_double` array<double>,
    `t_array_boolean` array<boolean>,
    `t_array_varchar` array<varchar(65535)>,
    `t_array_char` array<char(10)>,
    `t_array_decimal_precision_2` array<decimal(2,1)>,
    `t_array_decimal_precision_4` array<decimal(4,2)>,
    `t_array_decimal_precision_8` array<decimal(8,4)>,
    `t_array_decimal_precision_17` array<decimal(17,8)>,
    `t_array_decimal_precision_18` array<decimal(18,8)>,
    `t_array_decimal_precision_38` array<decimal(38,16)>,
    `t_struct_bigint` struct<s_bigint:bigint>,
    `t_complex` map<string,array<struct<s_int:int>>>,
    `t_struct_nested` struct<struct_field:array<string>>,
    `t_struct_null` struct<struct_field_null:string,struct_field_null2:string>,
    `t_struct_non_nulls_after_nulls` struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>,
    `t_nested_struct_non_nulls_after_nulls` struct<struct_field1:int,struct_field2:string,strict_field3:struct<nested_struct_field1:int,nested_struct_field2:string>>,
    `t_map_null_value` map<string,string>,
    `t_array_string_starting_with_nulls` array<string>,
    `t_array_string_with_nulls_in_between` array<string>,
    `t_array_string_ending_with_nulls` array<string>,
    `t_array_string_all_nulls` array<string>
) ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/parquet_table/parquet_all_types'
TBLPROPERTIES (
  'transient_lastDdlTime'='1681213018');

msck repair table parquet_all_types;

CREATE TABLE IF NOT EXISTS `avro_all_types`(
`t_null_string` string,
`t_null_varchar` varchar(65535),
`t_null_char` char(10),
`t_null_array_int` array<int>,
`t_null_decimal_precision_2` decimal(2,1),
`t_null_decimal_precision_4` decimal(4,2),
`t_null_decimal_precision_8` decimal(8,4),
`t_null_decimal_precision_17` decimal(17,8),
`t_null_decimal_precision_18` decimal(18,8),
`t_null_decimal_precision_38` decimal(38,16),
`t_empty_string` string,
`t_string` string,
`t_empty_varchar` varchar(65535),
`t_varchar` varchar(65535),
`t_varchar_max_length` varchar(65535),
`t_char` char(10),
`t_int` int,
`t_bigint` bigint,
`t_float` float,
`t_double` double,
`t_boolean_true` boolean,
`t_boolean_false` boolean,
`t_date` date,
`t_timestamp` timestamp,
`t_decimal_precision_2` decimal(2,1),
`t_decimal_precision_4` decimal(4,2),
`t_decimal_precision_8` decimal(8,4),
`t_decimal_precision_17` decimal(17,8),
`t_decimal_precision_18` decimal(18,8),
`t_decimal_precision_38` decimal(38,16),
`t_binary` binary,
`t_map_string` map<string,string>,
`t_array_empty` array<string>,
`t_array_string` array<string>,
`t_array_int` array<int>,
`t_array_bigint` array<bigint>,
`t_array_float` array<float>,
`t_array_double` array<double>,
`t_array_boolean` array<boolean>,
`t_array_varchar` array<varchar(65535)>,
`t_array_char` array<char(10)>,
`t_array_date` array<date>,
`t_array_timestamp` array<timestamp>,
`t_array_decimal_precision_2` array<decimal(2,1)>,
`t_array_decimal_precision_4` array<decimal(4,2)>,
`t_array_decimal_precision_8` array<decimal(8,4)>,
`t_array_decimal_precision_17` array<decimal(17,8)>,
`t_array_decimal_precision_18` array<decimal(18,8)>,
`t_array_decimal_precision_38` array<decimal(38,16)>,
`t_struct_bigint` struct<s_bigint:bigint>,
`t_complex` map<string,array<struct<s_int:int>>>,
`t_struct_nested` struct<struct_field:array<string>>,
`t_struct_null` struct<struct_field_null:string,struct_field_null2:string>,
`t_struct_non_nulls_after_nulls` struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>,
`t_array_string_starting_with_nulls` array<string>,
`t_array_string_with_nulls_in_between` array<string>,
`t_array_string_ending_with_nulls` array<string>,
`t_array_string_all_nulls` array<string>
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/avro/avro_all_types';

msck repair table avro_all_types;


CREATE TABLE IF NOT EXISTS `orc_all_types_t`(
`t_null_string` string,
`t_null_varchar` varchar(65535),
`t_null_char` char(10),
`t_null_array_int` array<int>,
`t_null_decimal_precision_2` decimal(2,1),
`t_null_decimal_precision_4` decimal(4,2),
`t_null_decimal_precision_8` decimal(8,4),
`t_null_decimal_precision_17` decimal(17,8),
`t_null_decimal_precision_18` decimal(18,8),
`t_null_decimal_precision_38` decimal(38,16),
`t_empty_string` string,
`t_string` string,
`t_empty_varchar` varchar(65535),
`t_varchar` varchar(65535),
`t_varchar_max_length` varchar(65535),
`t_char` char(10),
`t_tinyint` tinyint,
`t_smallint` smallint,
`t_int` int,
`t_bigint` bigint,
`t_float` float,
`t_double` double,
`t_boolean_true` boolean,
`t_boolean_false` boolean,
`t_date` date,
`t_timestamp` timestamp,
`t_decimal_precision_2` decimal(2,1),
`t_decimal_precision_4` decimal(4,2),
`t_decimal_precision_8` decimal(8,4),
`t_decimal_precision_17` decimal(17,8),
`t_decimal_precision_18` decimal(18,8),
`t_decimal_precision_38` decimal(38,16),
`t_binary` binary,
`t_map_string` map<string,string>,
`t_map_tinyint` map<tinyint,tinyint>,
`t_map_varchar` map<varchar(65535),varchar(65535)>,
`t_map_char` map<char(10),char(10)>,
`t_map_smallint` map<smallint,smallint>,
`t_map_int` map<int,int>,
`t_map_bigint` map<bigint,bigint>,
`t_map_float` map<float,float>,
`t_map_double` map<double,double>,
`t_map_boolean` map<boolean,boolean>,
`t_map_date` map<date,date>,
`t_map_timestamp` map<timestamp,timestamp>,
`t_map_decimal_precision_2` map<decimal(2,1),decimal(2,1)>,
`t_map_decimal_precision_4` map<decimal(4,2),decimal(4,2)>,
`t_map_decimal_precision_8` map<decimal(8,4),decimal(8,4)>,
`t_map_decimal_precision_17` map<decimal(17,8),decimal(17,8)>,
`t_map_decimal_precision_18` map<decimal(18,8),decimal(18,8)>,
`t_map_decimal_precision_38` map<decimal(38,16),decimal(38,16)>,
`t_array_empty` array<string>,
`t_array_string` array<string>,
`t_array_tinyint` array<tinyint>,
`t_array_smallint` array<smallint>,
`t_array_int` array<int>,
`t_array_bigint` array<bigint>,
`t_array_float` array<float>,
`t_array_double` array<double>,
`t_array_boolean` array<boolean>,
`t_array_varchar` array<varchar(65535)>,
`t_array_char` array<char(10)>,
`t_array_date` array<date>,
`t_array_timestamp` array<timestamp>,
`t_array_decimal_precision_2` array<decimal(2,1)>,
`t_array_decimal_precision_4` array<decimal(4,2)>,
`t_array_decimal_precision_8` array<decimal(8,4)>,
`t_array_decimal_precision_17` array<decimal(17,8)>,
`t_array_decimal_precision_18` array<decimal(18,8)>,
`t_array_decimal_precision_38` array<decimal(38,16)>,
`t_struct_bigint` struct<s_bigint:bigint>,
`t_complex` map<string,array<struct<s_int:int>>>,
`t_struct_nested` struct<struct_field:array<string>>,
`t_struct_null` struct<struct_field_null:string,struct_field_null2:string>,
`t_struct_non_nulls_after_nulls` struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>,
`t_nested_struct_non_nulls_after_nulls` struct<struct_field1:int,struct_field2:string,strict_field3:struct<nested_struct_field1:int,nested_struct_field2:string>>,
`t_map_null_value` map<string,string>,
`t_array_string_starting_with_nulls` array<string>,
`t_array_string_with_nulls_in_between` array<string>,
`t_array_string_ending_with_nulls` array<string>,
`t_array_string_all_nulls` array<string>
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/orc_table/orc_all_types';

msck repair table orc_all_types_t;

CREATE TABLE IF NOT EXISTS `json_all_types`(
`t_null_string` string,
`t_null_varchar` varchar(65535),
`t_null_char` char(10),
`t_null_decimal_precision_2` decimal(2,1),
`t_null_decimal_precision_4` decimal(4,2),
`t_null_decimal_precision_8` decimal(8,4),
`t_null_decimal_precision_17` decimal(17,8),
`t_null_decimal_precision_18` decimal(18,8),
`t_null_decimal_precision_38` decimal(38,16),
`t_empty_string` string,
`t_string` string,
`t_empty_varchar` varchar(65535),
`t_varchar` varchar(65535),
`t_varchar_max_length` varchar(65535),
`t_char` char(10),
`t_tinyint` tinyint,
`t_smallint` smallint,
`t_int` int,
`t_bigint` bigint,
`t_float` float,
`t_double` double,
`t_boolean_true` boolean,
`t_boolean_false` boolean,
`t_date` date,
`t_timestamp` timestamp,
`t_decimal_precision_2` decimal(2,1),
`t_decimal_precision_4` decimal(4,2),
`t_decimal_precision_8` decimal(8,4),
`t_decimal_precision_17` decimal(17,8),
`t_decimal_precision_18` decimal(18,8)
)
ROW FORMAT SERDE
  'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION
  '/user/doris/preinstalled_data/json/json_all_types';

msck repair table json_all_types;


CREATE TABLE IF NOT EXISTS `csv_all_types`(
`t_empty_string` string,
`t_string` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION
  '/user/doris/preinstalled_data/csv/csv_all_types';

msck repair table csv_all_types;

CREATE TABLE IF NOT EXISTS `text_all_types`(
`t_null_string` string,
`t_null_varchar` varchar(65535),
`t_null_char` char(10),
`t_null_decimal_precision_2` decimal(2,1),
`t_null_decimal_precision_4` decimal(4,2),
`t_null_decimal_precision_8` decimal(8,4),
`t_null_decimal_precision_17` decimal(17,8),
`t_null_decimal_precision_18` decimal(18,8),
`t_null_decimal_precision_38` decimal(38,16),
`t_empty_string` string,
`t_string` string,
`t_empty_varchar` varchar(65535),
`t_varchar` varchar(65535),
`t_varchar_max_length` varchar(65535),
`t_char` char(10),
`t_tinyint` tinyint,
`t_smallint` smallint,
`t_int` int,
`t_bigint` bigint,
`t_float` float,
`t_double` double,
`t_boolean_true` boolean,
`t_boolean_false` boolean,
`t_date` date,
`t_timestamp` timestamp,
`t_decimal_precision_2` decimal(2,1),
`t_decimal_precision_4` decimal(4,2),
`t_decimal_precision_8` decimal(8,4),
`t_decimal_precision_17` decimal(17,8),
`t_decimal_precision_18` decimal(18,8),
`t_decimal_precision_38` decimal(38,16),
`t_binary` binary
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION
  '/user/doris/preinstalled_data/text/text_all_types';

msck repair table text_all_types;


CREATE TABLE IF NOT EXISTS `sequence_all_types`(
`t_null_string` string,
`t_null_varchar` varchar(65535),
`t_null_char` char(10),
`t_null_array_int` array<int>,
`t_null_decimal_precision_2` decimal(2,1),
`t_null_decimal_precision_4` decimal(4,2),
`t_null_decimal_precision_8` decimal(8,4),
`t_null_decimal_precision_17` decimal(17,8),
`t_null_decimal_precision_18` decimal(18,8),
`t_null_decimal_precision_38` decimal(38,16),
`t_empty_string` string,
`t_string` string,
`t_empty_varchar` varchar(65535),
`t_varchar` varchar(65535),
`t_varchar_max_length` varchar(65535),
`t_char` char(10),
`t_tinyint` tinyint,
`t_smallint` smallint,
`t_int` int,
`t_bigint` bigint,
`t_float` float,
`t_double` double,
`t_boolean_true` boolean,
`t_boolean_false` boolean,
`t_date` date,
`t_timestamp` timestamp,
`t_decimal_precision_2` decimal(2,1),
`t_decimal_precision_4` decimal(4,2),
`t_decimal_precision_8` decimal(8,4),
`t_decimal_precision_17` decimal(17,8),
`t_decimal_precision_18` decimal(18,8),
`t_decimal_precision_38` decimal(38,16),
`t_binary` binary,
`t_map_string` map<string,string>,
`t_map_tinyint` map<tinyint,tinyint>,
`t_map_varchar` map<varchar(65535),varchar(65535)>,
`t_map_char` map<char(10),char(10)>,
`t_map_smallint` map<smallint,smallint>,
`t_map_int` map<int,int>,
`t_map_bigint` map<bigint,bigint>,
`t_map_float` map<float,float>,
`t_map_double` map<double,double>,
`t_map_boolean` map<boolean,boolean>,
`t_map_date` map<date,date>,
`t_map_timestamp` map<timestamp,timestamp>,
`t_map_decimal_precision_2` map<decimal(2,1),decimal(2,1)>,
`t_map_decimal_precision_4` map<decimal(4,2),decimal(4,2)>,
`t_map_decimal_precision_8` map<decimal(8,4),decimal(8,4)>,
`t_map_decimal_precision_17` map<decimal(17,8),decimal(17,8)>,
`t_map_decimal_precision_18` map<decimal(18,8),decimal(18,8)>,
`t_map_decimal_precision_38` map<decimal(38,16),decimal(38,16)>,
`t_array_empty` array<string>,
`t_array_string` array<string>,
`t_array_tinyint` array<tinyint>,
`t_array_smallint` array<smallint>,
`t_array_int` array<int>,
`t_array_bigint` array<bigint>,
`t_array_float` array<float>,
`t_array_double` array<double>,
`t_array_boolean` array<boolean>,
`t_array_varchar` array<varchar(65535)>,
`t_array_char` array<char(10)>,
`t_array_date` array<date>,
`t_array_timestamp` array<timestamp>,
`t_array_decimal_precision_2` array<decimal(2,1)>,
`t_array_decimal_precision_4` array<decimal(4,2)>,
`t_array_decimal_precision_8` array<decimal(8,4)>,
`t_array_decimal_precision_17` array<decimal(17,8)>,
`t_array_decimal_precision_18` array<decimal(18,8)>,
`t_array_decimal_precision_38` array<decimal(38,16)>,
`t_struct_bigint` struct<s_bigint:bigint>,
`t_complex` map<string,array<struct<s_int:int>>>,
`t_struct_nested` struct<struct_field:array<string>>,
`t_struct_null` struct<struct_field_null:string,struct_field_null2:string>,
`t_struct_non_nulls_after_nulls` struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>,
`t_nested_struct_non_nulls_after_nulls` struct<struct_field1:int,struct_field2:string,strict_field3:struct<nested_struct_field1:int,nested_struct_field2:string>>,
`t_map_null_value` map<string,string>,
`t_array_string_starting_with_nulls` array<string>,
`t_array_string_with_nulls_in_between` array<string>,
`t_array_string_ending_with_nulls` array<string>,
`t_array_string_all_nulls` array<string>
)
STORED AS SEQUENCEFILE
LOCATION
  '/user/doris/preinstalled_data/sequence/sequence_all_types';

msck repair table sequence_all_types;

CREATE TABLE `parquet_gzip_all_types`(
`t_null_string` string,
`t_null_varchar` varchar(65535),
`t_null_char` char(10),
`t_null_decimal_precision_2` decimal(2,1),
`t_null_decimal_precision_4` decimal(4,2),
`t_null_decimal_precision_8` decimal(8,4),
`t_null_decimal_precision_17` decimal(17,8),
`t_null_decimal_precision_18` decimal(18,8),
`t_null_decimal_precision_38` decimal(38,16),
`t_empty_string` string,
`t_string` string,
`t_empty_varchar` varchar(65535),
`t_varchar` varchar(65535),
`t_varchar_max_length` varchar(65535),
`t_char` char(10),
`t_int` int,
`t_bigint` bigint,
`t_float` float,
`t_double` double,
`t_boolean_true` boolean,
`t_boolean_false` boolean,
`t_decimal_precision_2` decimal(2,1),
`t_decimal_precision_4` decimal(4,2),
`t_decimal_precision_8` decimal(8,4),
`t_decimal_precision_17` decimal(17,8),
`t_decimal_precision_18` decimal(18,8),
`t_decimal_precision_38` decimal(38,16),
`t_binary` binary,
`t_map_string` map<string,string>,
`t_map_varchar` map<varchar(65535),varchar(65535)>,
`t_map_char` map<char(10),char(10)>,
`t_map_int` map<int,int>,
`t_map_bigint` map<bigint,bigint>,
`t_map_float` map<float,float>,
`t_map_double` map<double,double>,
`t_map_boolean` map<boolean,boolean>,
`t_map_decimal_precision_2` map<decimal(2,1),decimal(2,1)>,
`t_map_decimal_precision_4` map<decimal(4,2),decimal(4,2)>,
`t_map_decimal_precision_8` map<decimal(8,4),decimal(8,4)>,
`t_map_decimal_precision_17` map<decimal(17,8),decimal(17,8)>,
`t_map_decimal_precision_18` map<decimal(18,8),decimal(18,8)>,
`t_map_decimal_precision_38` map<decimal(38,16),decimal(38,16)>,
`t_array_string` array<string>,
`t_array_int` array<int>,
`t_array_bigint` array<bigint>,
`t_array_float` array<float>,
`t_array_double` array<double>,
`t_array_boolean` array<boolean>,
`t_array_varchar` array<varchar(65535)>,
`t_array_char` array<char(10)>,
`t_array_decimal_precision_2` array<decimal(2,1)>,
`t_array_decimal_precision_4` array<decimal(4,2)>,
`t_array_decimal_precision_8` array<decimal(8,4)>,
`t_array_decimal_precision_17` array<decimal(17,8)>,
`t_array_decimal_precision_18` array<decimal(18,8)>,
`t_array_decimal_precision_38` array<decimal(38,16)>,
`t_struct_bigint` struct<s_bigint:bigint>,
`t_complex` map<string,array<struct<s_int:int>>>,
`t_struct_nested` struct<struct_field:array<string>>,
`t_struct_null` struct<struct_field_null:string,struct_field_null2:string>,
`t_struct_non_nulls_after_nulls` struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>,
`t_nested_struct_non_nulls_after_nulls` struct<struct_field1:int,struct_field2:string,strict_field3:struct<nested_struct_field1:int,nested_struct_field2:string>>,
`t_map_null_value` map<string,string>,
`t_array_string_starting_with_nulls` array<string>,
`t_array_string_with_nulls_in_between` array<string>,
`t_array_string_ending_with_nulls` array<string>,
`t_array_string_all_nulls` array<string>
) ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/parquet_table/parquet_gzip_all_types'
TBLPROPERTIES (
  'transient_lastDdlTime'='1681213018',
  "parquet.compression"="GZIP");

msck repair table parquet_gzip_all_types;

CREATE TABLE `rcbinary_all_types`(
`t_null_string` string,
`t_null_varchar` varchar(65535),
`t_null_char` char(10),
`t_null_array_int` array<int>,
`t_null_decimal_precision_2` decimal(2,1),
`t_null_decimal_precision_4` decimal(4,2),
`t_null_decimal_precision_8` decimal(8,4),
`t_null_decimal_precision_17` decimal(17,8),
`t_null_decimal_precision_18` decimal(18,8),
`t_null_decimal_precision_38` decimal(38,16),
`t_empty_string` string,
`t_string` string,
`t_varchar` varchar(65535),
`t_varchar_max_length` varchar(65535),
`t_char` char(10),
`t_tinyint` tinyint,
`t_smallint` smallint,
`t_int` int,
`t_bigint` bigint,
`t_float` float,
`t_double` double,
`t_boolean_true` boolean,
`t_boolean_false` boolean,
`t_date` date,
`t_timestamp` timestamp,
`t_decimal_precision_2` decimal(2,1),
`t_decimal_precision_4` decimal(4,2),
`t_decimal_precision_8` decimal(8,4),
`t_decimal_precision_17` decimal(17,8),
`t_decimal_precision_18` decimal(18,8),
`t_decimal_precision_38` decimal(38,16),
`t_binary` binary,
`t_map_string` map<string,string>,
`t_map_tinyint` map<tinyint,tinyint>,
`t_map_varchar` map<varchar(65535),varchar(65535)>,
`t_map_char` map<char(10),char(10)>,
`t_map_smallint` map<smallint,smallint>,
`t_map_int` map<int,int>,
`t_map_bigint` map<bigint,bigint>,
`t_map_float` map<float,float>,
`t_map_double` map<double,double>,
`t_map_boolean` map<boolean,boolean>,
`t_map_date` map<date,date>,
`t_map_timestamp` map<timestamp,timestamp>,
`t_map_decimal_precision_2` map<decimal(2,1),decimal(2,1)>,
`t_map_decimal_precision_4` map<decimal(4,2),decimal(4,2)>,
`t_map_decimal_precision_8` map<decimal(8,4),decimal(8,4)>,
`t_map_decimal_precision_17` map<decimal(17,8),decimal(17,8)>,
`t_map_decimal_precision_18` map<decimal(18,8),decimal(18,8)>,
`t_map_decimal_precision_38` map<decimal(38,16),decimal(38,16)>,
`t_array_empty` array<string>,
`t_array_string` array<string>,
`t_array_tinyint` array<tinyint>,
`t_array_smallint` array<smallint>,
`t_array_int` array<int>,
`t_array_bigint` array<bigint>,
`t_array_float` array<float>,
`t_array_double` array<double>,
`t_array_boolean` array<boolean>,
`t_array_varchar` array<varchar(65535)>,
`t_array_char` array<char(10)>,
`t_array_date` array<date>,
`t_array_timestamp` array<timestamp>,
`t_array_decimal_precision_2` array<decimal(2,1)>,
`t_array_decimal_precision_4` array<decimal(4,2)>,
`t_array_decimal_precision_8` array<decimal(8,4)>,
`t_array_decimal_precision_17` array<decimal(17,8)>,
`t_array_decimal_precision_18` array<decimal(18,8)>,
`t_array_decimal_precision_38` array<decimal(38,16)>,
`t_struct_bigint` struct<s_bigint:bigint>,
`t_complex` map<string,array<struct<s_int:int>>>,
`t_struct_nested` struct<struct_field:array<string>>,
`t_struct_null` struct<struct_field_null:string,struct_field_null2:string>,
`t_struct_non_nulls_after_nulls` struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>,
`t_nested_struct_non_nulls_after_nulls` struct<struct_field1:int,struct_field2:string,strict_field3:struct<nested_struct_field1:int,nested_struct_field2:string>>,
`t_map_null_value` map<string,string>,
`t_array_string_starting_with_nulls` array<string>,
`t_array_string_with_nulls_in_between` array<string>,
`t_array_string_ending_with_nulls` array<string>,
`t_array_string_all_nulls` array<string>
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'
STORED AS RCFILE
LOCATION
  '/user/doris/preinstalled_data/rcbinary/rcbinary_all_types';

msck repair table rcbinary_all_types;

CREATE TABLE `bloom_parquet_table`(
`t_null_string` string,
`t_null_varchar` varchar(65535),
`t_null_char` char(10),
`t_null_decimal_precision_2` decimal(2,1),
`t_null_decimal_precision_4` decimal(4,2),
`t_null_decimal_precision_8` decimal(8,4),
`t_null_decimal_precision_17` decimal(17,8),
`t_null_decimal_precision_18` decimal(18,8),
`t_null_decimal_precision_38` decimal(38,16),
`t_empty_string` string,
`t_string` string,
`t_empty_varchar` varchar(65535),
`t_varchar` varchar(65535),
`t_varchar_max_length` varchar(65535),
`t_char` char(10),
`t_int` int,
`t_bigint` bigint,
`t_float` float,
`t_double` double,
`t_boolean_true` boolean,
`t_boolean_false` boolean,
`t_decimal_precision_2` decimal(2,1),
`t_decimal_precision_4` decimal(4,2),
`t_decimal_precision_8` decimal(8,4),
`t_decimal_precision_17` decimal(17,8),
`t_decimal_precision_18` decimal(18,8),
`t_decimal_precision_38` decimal(38,16),
`t_binary` binary,
`t_array_string` array<string>,
`t_array_int` array<int>,
`t_array_bigint` array<bigint>,
`t_array_float` array<float>,
`t_array_double` array<double>,
`t_array_boolean` array<boolean>,
`t_array_varchar` array<varchar(65535)>,
`t_array_char` array<char(10)>,
`t_array_decimal_precision_2` array<decimal(2,1)>,
`t_array_decimal_precision_4` array<decimal(4,2)>,
`t_array_decimal_precision_8` array<decimal(8,4)>,
`t_array_decimal_precision_17` array<decimal(17,8)>,
`t_array_decimal_precision_18` array<decimal(18,8)>,
`t_array_decimal_precision_38` array<decimal(38,16)>,
`t_struct_bigint` struct<s_bigint:bigint>,
`t_struct_nested` struct<struct_field:array<string>>,
`t_struct_null` struct<struct_field_null:string,struct_field_null2:string>,
`t_struct_non_nulls_after_nulls` struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>,
`t_nested_struct_non_nulls_after_nulls` struct<struct_field1:int,struct_field2:string,strict_field3:struct<nested_struct_field1:int,nested_struct_field2:string>>,
`t_array_string_starting_with_nulls` array<string>,
`t_array_string_with_nulls_in_between` array<string>,
`t_array_string_ending_with_nulls` array<string>,
`t_array_string_all_nulls` array<string>
) ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/parquet_table/bloom_parquet_table'
TBLPROPERTIES (
  'transient_lastDdlTime'='1681213018',
  'parquet.bloom.filter.columns'='t_int',
  'parquet.bloom.filter.fpp'='0.05');

msck repair table bloom_parquet_table;


CREATE TABLE `bloom_orc_table`(
`t_null_string` string,
`t_null_varchar` varchar(65535),
`t_null_char` char(10),
`t_null_array_int` array<int>,
`t_null_decimal_precision_2` decimal(2,1),
`t_null_decimal_precision_4` decimal(4,2),
`t_null_decimal_precision_8` decimal(8,4),
`t_null_decimal_precision_17` decimal(17,8),
`t_null_decimal_precision_18` decimal(18,8),
`t_null_decimal_precision_38` decimal(38,16),
`t_empty_string` string,
`t_string` string,
`t_empty_varchar` varchar(65535),
`t_varchar` varchar(65535),
`t_varchar_max_length` varchar(65535),
`t_char` char(10),
`t_tinyint` tinyint,
`t_smallint` smallint,
`t_int` int,
`t_bigint` bigint,
`t_float` float,
`t_double` double,
`t_boolean_true` boolean,
`t_boolean_false` boolean,
`t_date` date,
`t_timestamp` timestamp,
`t_decimal_precision_2` decimal(2,1),
`t_decimal_precision_4` decimal(4,2),
`t_decimal_precision_8` decimal(8,4),
`t_decimal_precision_17` decimal(17,8),
`t_decimal_precision_18` decimal(18,8),
`t_decimal_precision_38` decimal(38,16),
`t_binary` binary,
`t_map_string` map<string,string>,
`t_map_tinyint` map<tinyint,tinyint>,
`t_map_varchar` map<varchar(65535),varchar(65535)>,
`t_map_char` map<char(10),char(10)>,
`t_map_smallint` map<smallint,smallint>,
`t_map_int` map<int,int>,
`t_map_bigint` map<bigint,bigint>,
`t_map_float` map<float,float>,
`t_map_double` map<double,double>,
`t_map_boolean` map<boolean,boolean>,
`t_map_date` map<date,date>,
`t_map_timestamp` map<timestamp,timestamp>,
`t_map_decimal_precision_2` map<decimal(2,1),decimal(2,1)>,
`t_map_decimal_precision_4` map<decimal(4,2),decimal(4,2)>,
`t_map_decimal_precision_8` map<decimal(8,4),decimal(8,4)>,
`t_map_decimal_precision_17` map<decimal(17,8),decimal(17,8)>,
`t_map_decimal_precision_18` map<decimal(18,8),decimal(18,8)>,
`t_map_decimal_precision_38` map<decimal(38,16),decimal(38,16)>,
`t_array_empty` array<string>,
`t_array_string` array<string>,
`t_array_tinyint` array<tinyint>,
`t_array_smallint` array<smallint>,
`t_array_int` array<int>,
`t_array_bigint` array<bigint>,
`t_array_float` array<float>,
`t_array_double` array<double>,
`t_array_boolean` array<boolean>,
`t_array_varchar` array<varchar(65535)>,
`t_array_char` array<char(10)>,
`t_array_date` array<date>,
`t_array_timestamp` array<timestamp>,
`t_array_decimal_precision_2` array<decimal(2,1)>,
`t_array_decimal_precision_4` array<decimal(4,2)>,
`t_array_decimal_precision_8` array<decimal(8,4)>,
`t_array_decimal_precision_17` array<decimal(17,8)>,
`t_array_decimal_precision_18` array<decimal(18,8)>,
`t_array_decimal_precision_38` array<decimal(38,16)>,
`t_struct_bigint` struct<s_bigint:bigint>,
`t_complex` map<string,array<struct<s_int:int>>>,
`t_struct_nested` struct<struct_field:array<string>>,
`t_struct_null` struct<struct_field_null:string,struct_field_null2:string>,
`t_struct_non_nulls_after_nulls` struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>,
`t_nested_struct_non_nulls_after_nulls` struct<struct_field1:int,struct_field2:string,strict_field3:struct<nested_struct_field1:int,nested_struct_field2:string>>,
`t_map_null_value` map<string,string>,
`t_array_string_starting_with_nulls` array<string>,
`t_array_string_with_nulls_in_between` array<string>,
`t_array_string_ending_with_nulls` array<string>,
`t_array_string_all_nulls` array<string>
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/orc_table/bloom_orc_table'
TBLPROPERTIES (
  'transient_lastDdlTime'='1681213018',
  'orc.bloom.filter.columns'='t_int',
  'orc.bloom.filter.fpp'='0.05');

msck repair table bloom_orc_table;


CREATE TABLE `orc_predicate_table`(
`column_primitive_integer` int,
`column1_struct` struct<field0:bigint,field1:bigint>,
`column_primitive_bigint` bigint
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/orc_table/orc_predicate_table';

msck repair table orc_predicate_table;


CREATE TABLE `parquet_predicate_table`(
`column_primitive_integer` int,
`column1_struct` struct<field0:bigint,field1:bigint>,
`column_primitive_bigint` bigint
) ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/parquet_table/parquet_predicate_table';

msck repair table parquet_predicate_table;

CREATE TABLE `only_null`(
`x` int
) ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/parquet_table/only_null';

msck repair table only_null;


CREATE TABLE `parquet_timestamp_millis`(
test timestamp
) ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/parquet_table/parquet_timestamp_millis';

msck repair table parquet_timestamp_millis;


CREATE TABLE `parquet_timestamp_micros`(
test timestamp
) ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/parquet_table/parquet_timestamp_micros';

msck repair table parquet_timestamp_micros;

CREATE TABLE `parquet_timestamp_nanos`(
test timestamp
) ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/parquet_table/parquet_timestamp_nanos';

msck repair table parquet_timestamp_nanos;

CREATE TABLE `orc_decimal_table`(
    id INT,
    decimal_col1 DECIMAL(8, 4),
    decimal_col2 DECIMAL(18, 6),
    decimal_col3 DECIMAL(38, 12),
    decimal_col4 DECIMAL(9, 0),
    decimal_col5 DECIMAL(27, 9),
    decimal_col6 DECIMAL(9, 0))
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/orc_table/orc_decimal_table';

msck repair table orc_decimal_table;


show tables;
