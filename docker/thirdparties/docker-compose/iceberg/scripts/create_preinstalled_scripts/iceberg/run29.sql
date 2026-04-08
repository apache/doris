create database if not exists demo.test_v2_to_v3_doris_spark_compare_db;
use demo.test_v2_to_v3_doris_spark_compare_db;

drop table if exists v2v3_row_lineage_null_after_upgrade;
drop table if exists v2v3_spark_ops_reference;
drop table if exists v2v3_doris_ops_target;

create table v2v3_row_lineage_null_after_upgrade (
    id int,
    tag string,
    score int,
    dt date
) using iceberg
partitioned by (days(dt))
tblproperties (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into v2v3_row_lineage_null_after_upgrade values
(1, 'base', 100, date '2024-01-01'),
(2, 'base', 200, date '2024-01-02');

insert into v2v3_row_lineage_null_after_upgrade values
(3, 'base', 300, date '2024-01-03');

update v2v3_row_lineage_null_after_upgrade
set tag = 'base_u', score = score + 10
where id = 1;

alter table v2v3_row_lineage_null_after_upgrade
set tblproperties ('format-version' = '3');

create table v2v3_spark_ops_reference (
    id int,
    tag string,
    score int,
    dt date
) using iceberg
partitioned by (days(dt))
tblproperties (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into v2v3_spark_ops_reference values
(1, 'base', 100, date '2024-02-01'),
(2, 'base', 200, date '2024-02-02');

insert into v2v3_spark_ops_reference values
(3, 'base', 300, date '2024-02-03');

update v2v3_spark_ops_reference
set tag = 'base_u', score = score + 10
where id = 1;

alter table v2v3_spark_ops_reference
set tblproperties ('format-version' = '3');

update v2v3_spark_ops_reference
set tag = 'post_v3_u', score = score + 20
where id = 2;

insert into v2v3_spark_ops_reference values
(4, 'post_v3_i', 400, date '2024-02-04');

call demo.system.rewrite_data_files(
    table => 'demo.test_v2_to_v3_doris_spark_compare_db.v2v3_spark_ops_reference',
    options => map('target-file-size-bytes', '10485760', 'min-input-files', '1')
);

create table v2v3_doris_ops_target (
    id int,
    tag string,
    score int,
    dt date
) using iceberg
partitioned by (days(dt))
tblproperties (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into v2v3_doris_ops_target values
(1, 'base', 100, date '2024-02-01'),
(2, 'base', 200, date '2024-02-02');

insert into v2v3_doris_ops_target values
(3, 'base', 300, date '2024-02-03');

update v2v3_doris_ops_target
set tag = 'base_u', score = score + 10
where id = 1;

alter table v2v3_doris_ops_target
set tblproperties ('format-version' = '3');


drop table if exists v2v3_doris_upd_case1;
drop table if exists v2v3_doris_upd_case2;
drop table if exists v2v3_doris_upd_case3;
drop table if exists v2v3_doris_upd_case4;


create table v2v3_doris_upd_case1 (
    id int,
    tag string,
    score int,
    dt date
) using iceberg
partitioned by (days(dt))
tblproperties (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into v2v3_doris_upd_case1 values (1, 'base', 100, date '2024-01-01'),(2, 'base', 200, date '2024-01-02');
insert into v2v3_doris_upd_case1 values (3, 'base', 300, date '2024-01-03');
update      v2v3_doris_upd_case1 set tag = 'base_u', score = score + 10 where id = 1;
delete from v2v3_doris_upd_case1 where id = 2;
alter table v2v3_doris_upd_case1 set tblproperties ('format-version' = '3');


create table v2v3_doris_upd_case2 (
    id int,
    tag string,
    score int,
    dt date
) using iceberg
partitioned by (days(dt))
tblproperties (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into v2v3_doris_upd_case2 values (1, 'base', 100, date '2024-01-01'),(2, 'base', 200, date '2024-01-02');
insert into v2v3_doris_upd_case2 values (3, 'base', 300, date '2024-01-03');
update      v2v3_doris_upd_case2 set tag = 'base_u', score = score + 10 where id = 1;
delete from v2v3_doris_upd_case2 where id = 2;
alter table v2v3_doris_upd_case2 set tblproperties ('format-version' = '3');


create table v2v3_doris_upd_case3 (
    id int,
    tag string,
    score int,
    dt date
) using iceberg
partitioned by (days(dt))
tblproperties (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into v2v3_doris_upd_case3 values (1, 'base', 100, date '2024-01-01'),(2, 'base', 200, date '2024-01-02');
insert into v2v3_doris_upd_case3 values (3, 'base', 300, date '2024-01-03');
update      v2v3_doris_upd_case3 set tag = 'base_u', score = score + 10 where id = 1;
delete from v2v3_doris_upd_case3 where id = 2;
alter table v2v3_doris_upd_case3 set tblproperties ('format-version' = '3');


create table v2v3_doris_upd_case4 (
    id int,
    tag string,
    score int,
    dt date
) using iceberg
partitioned by (days(dt))
tblproperties (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into v2v3_doris_upd_case4 values (1, 'base', 100, date '2024-01-01'),(2, 'base', 200, date '2024-01-02');
insert into v2v3_doris_upd_case4 values (3, 'base', 300, date '2024-01-03');
update      v2v3_doris_upd_case4 set tag = 'base_u', score = score + 10 where id = 1;
delete from v2v3_doris_upd_case4 where id = 2;
alter table v2v3_doris_upd_case4 set tblproperties ('format-version' = '3');


drop table if exists v2v3_row_lineage_null_after_upgrade_orc;
drop table if exists v2v3_spark_ops_reference_orc;
drop table if exists v2v3_doris_ops_target_orc;
drop table if exists v2v3_doris_upd_case1_orc;
drop table if exists v2v3_doris_upd_case2_orc;
drop table if exists v2v3_doris_upd_case3_orc;
drop table if exists v2v3_doris_upd_case4_orc;

create table v2v3_row_lineage_null_after_upgrade_orc (
    id int,
    tag string,
    score int,
    dt date
) using iceberg
partitioned by (days(dt))
tblproperties (
    'format-version' = '2',
    'write.format.default' = 'orc',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into v2v3_row_lineage_null_after_upgrade_orc values
(1, 'base', 100, date '2024-01-01'),
(2, 'base', 200, date '2024-01-02');

insert into v2v3_row_lineage_null_after_upgrade_orc values
(3, 'base', 300, date '2024-01-03');

update v2v3_row_lineage_null_after_upgrade_orc
set tag = 'base_u', score = score + 10
where id = 1;

alter table v2v3_row_lineage_null_after_upgrade_orc
set tblproperties ('format-version' = '3');

create table v2v3_spark_ops_reference_orc (
    id int,
    tag string,
    score int,
    dt date
) using iceberg
partitioned by (days(dt))
tblproperties (
    'format-version' = '2',
    'write.format.default' = 'orc',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into v2v3_spark_ops_reference_orc values
(1, 'base', 100, date '2024-02-01'),
(2, 'base', 200, date '2024-02-02');

insert into v2v3_spark_ops_reference_orc values
(3, 'base', 300, date '2024-02-03');

update v2v3_spark_ops_reference_orc
set tag = 'base_u', score = score + 10
where id = 1;

alter table v2v3_spark_ops_reference_orc
set tblproperties ('format-version' = '3');

update v2v3_spark_ops_reference_orc
set tag = 'post_v3_u', score = score + 20
where id = 2;

insert into v2v3_spark_ops_reference_orc values
(4, 'post_v3_i', 400, date '2024-02-04');

call demo.system.rewrite_data_files(
    table => 'demo.test_v2_to_v3_doris_spark_compare_db.v2v3_spark_ops_reference_orc',
    options => map('target-file-size-bytes', '10485760', 'min-input-files', '1')
);

create table v2v3_doris_ops_target_orc (
    id int,
    tag string,
    score int,
    dt date
) using iceberg
partitioned by (days(dt))
tblproperties (
    'format-version' = '2',
    'write.format.default' = 'orc',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into v2v3_doris_ops_target_orc values
(1, 'base', 100, date '2024-02-01'),
(2, 'base', 200, date '2024-02-02');

insert into v2v3_doris_ops_target_orc values
(3, 'base', 300, date '2024-02-03');

update v2v3_doris_ops_target_orc
set tag = 'base_u', score = score + 10
where id = 1;

alter table v2v3_doris_ops_target_orc
set tblproperties ('format-version' = '3');

create table v2v3_doris_upd_case1_orc (
    id int,
    tag string,
    score int,
    dt date
) using iceberg
partitioned by (days(dt))
tblproperties (
    'format-version' = '2',
    'write.format.default' = 'orc',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into v2v3_doris_upd_case1_orc values (1, 'base', 100, date '2024-01-01'),(2, 'base', 200, date '2024-01-02');
insert into v2v3_doris_upd_case1_orc values (3, 'base', 300, date '2024-01-03');
update      v2v3_doris_upd_case1_orc set tag = 'base_u', score = score + 10 where id = 1;
delete from v2v3_doris_upd_case1_orc where id = 2;
alter table v2v3_doris_upd_case1_orc set tblproperties ('format-version' = '3');

create table v2v3_doris_upd_case2_orc (
    id int,
    tag string,
    score int,
    dt date
) using iceberg
partitioned by (days(dt))
tblproperties (
    'format-version' = '2',
    'write.format.default' = 'orc',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into v2v3_doris_upd_case2_orc values (1, 'base', 100, date '2024-01-01'),(2, 'base', 200, date '2024-01-02');
insert into v2v3_doris_upd_case2_orc values (3, 'base', 300, date '2024-01-03');
update      v2v3_doris_upd_case2_orc set tag = 'base_u', score = score + 10 where id = 1;
delete from v2v3_doris_upd_case2_orc where id = 2;
alter table v2v3_doris_upd_case2_orc set tblproperties ('format-version' = '3');

create table v2v3_doris_upd_case3_orc (
    id int,
    tag string,
    score int,
    dt date
) using iceberg
partitioned by (days(dt))
tblproperties (
    'format-version' = '2',
    'write.format.default' = 'orc',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into v2v3_doris_upd_case3_orc values (1, 'base', 100, date '2024-01-01'),(2, 'base', 200, date '2024-01-02');
insert into v2v3_doris_upd_case3_orc values (3, 'base', 300, date '2024-01-03');
update      v2v3_doris_upd_case3_orc set tag = 'base_u', score = score + 10 where id = 1;
delete from v2v3_doris_upd_case3_orc where id = 2;
alter table v2v3_doris_upd_case3_orc set tblproperties ('format-version' = '3');

create table v2v3_doris_upd_case4_orc (
    id int,
    tag string,
    score int,
    dt date
) using iceberg
partitioned by (days(dt))
tblproperties (
    'format-version' = '2',
    'write.format.default' = 'orc',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into v2v3_doris_upd_case4_orc values (1, 'base', 100, date '2024-01-01'),(2, 'base', 200, date '2024-01-02');
insert into v2v3_doris_upd_case4_orc values (3, 'base', 300, date '2024-01-03');
update      v2v3_doris_upd_case4_orc set tag = 'base_u', score = score + 10 where id = 1;
delete from v2v3_doris_upd_case4_orc where id = 2;
alter table v2v3_doris_upd_case4_orc set tblproperties ('format-version' = '3');
