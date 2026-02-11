use paimon;
create database if not exists test_paimon_spark;
use test_paimon_spark;

drop table if exists data_sys_table_append;
create table data_sys_table_append (
    id int,
    name string
) TBLPROPERTIES (
    'bucket' = '2',
    'bucket-key' = 'id',
    'file.format' = 'parquet'
);

insert into data_sys_table_append values
(10, 'delta'),
(20, 'epsilon'),
(30, 'zeta');

drop table if exists data_sys_table;
create table data_sys_table (
    id int,
    name string
) TBLPROPERTIES (
    'primary-key' = 'id',
    'bucket' = '1',
    'file.format' = 'parquet',
    'changelog-producer' = 'input'
);

insert into data_sys_table values
(1, 'alpha'),
(2, 'beta'),
(3, 'gamma');

update data_sys_table
set name = 'beta_v2'
where id = 2;

delete from data_sys_table
where id = 3;

insert into data_sys_table values
(4, 'delta');

drop table if exists data_sys_table_native;
create table data_sys_table_native (
    id int,
    name string
) TBLPROPERTIES (
    'primary-key' = 'id',
    'bucket' = '1',
    'file.format' = 'parquet',
    'changelog-producer' = 'input'
);

insert into data_sys_table_native values
(1, 'alpha'),
(2, 'beta'),
(3, 'gamma'),
(4, 'delta');

CALL sys.compact(table => 'data_sys_table_native', compact_strategy => 'full');
