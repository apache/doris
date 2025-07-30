use paimon;
create database if not exists test_paimon_spark;
use test_paimon_spark;

drop table if exists paimon_incremental_mv_test_agg_table;
create table paimon_incremental_mv_test_agg_table (
  date bigint,
  k1 bigint,
  k2 bigint,
  a1 double,
  a2 double,
  a3 string)
USING paimon
PARTITIONED BY (date)
TBLPROPERTIES (
  'bucket' = '1',
  'bucket-key' = 'k1,k2',
  'fields.a1.aggregate-function' = 'sum',
  'fields.a2.aggregate-function' = 'max',
  'fields.a3.aggregate-function' = 'first_value',
  'merge-engine' = 'aggregation',
  'primary-key' = 'date,k1,k2');

INSERT INTO paimon_incremental_mv_test_agg_table values(20250101, 1, 1, 1.0, 1.0, '1');
INSERT INTO paimon_incremental_mv_test_agg_table values(20250102, 2, 2, 2.0, 2.0, '2');
INSERT INTO paimon_incremental_mv_test_agg_table values(20250103, 3, 3, 3.0, 3.0, '3');
