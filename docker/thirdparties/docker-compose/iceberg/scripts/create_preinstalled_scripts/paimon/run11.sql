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

drop table if exists data_sys_table_seq_on;
create table data_sys_table_seq_on (
    id int,
    name string
) TBLPROPERTIES (
    'primary-key' = 'id',
    'bucket' = '1',
    'file.format' = 'parquet',
    'changelog-producer' = 'input',
    'table-read.sequence-number.enabled' = 'true'
);

insert into data_sys_table_seq_on values
(1, 'alpha'),
(2, 'beta'),
(3, 'gamma');

update data_sys_table_seq_on
set name = 'beta_v2'
where id = 2;

delete from data_sys_table_seq_on
where id = 3;

insert into data_sys_table_seq_on values
(4, 'delta');

drop table if exists data_sys_table_seq_off;
create table data_sys_table_seq_off (
    id int,
    name string
) TBLPROPERTIES (
    'primary-key' = 'id',
    'bucket' = '1',
    'file.format' = 'parquet',
    'changelog-producer' = 'input'
);

insert into data_sys_table_seq_off values
(1, 'alpha'),
(2, 'beta'),
(3, 'gamma');

update data_sys_table_seq_off
set name = 'beta_v2'
where id = 2;

delete from data_sys_table_seq_off
where id = 3;

insert into data_sys_table_seq_off values
(4, 'delta');
