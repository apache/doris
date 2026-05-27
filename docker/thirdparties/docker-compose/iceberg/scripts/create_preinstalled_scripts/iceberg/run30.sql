create database if not exists demo.test_v2_to_v3_doris_spark_compare_db;
use demo.test_v2_to_v3_doris_spark_compare_db;

drop table if exists v2v3_doris_upd_case5;
drop table if exists v2v3_doris_upd_case5_orc;

create table v2v3_doris_upd_case5 (
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

insert into v2v3_doris_upd_case5 values
(1, 'base', 100, date '2024-01-01'),
(2, 'base', 200, date '2024-01-02');

insert into v2v3_doris_upd_case5 values
(3, 'base', 300, date '2024-01-03');

update v2v3_doris_upd_case5
set tag = 'base_u', score = score + 10
where id = 1;

delete from v2v3_doris_upd_case5
where id = 2;

alter table v2v3_doris_upd_case5
set tblproperties ('format-version' = '3');

create table v2v3_doris_upd_case5_orc (
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

insert into v2v3_doris_upd_case5_orc values
(1, 'base', 100, date '2024-01-01'),
(2, 'base', 200, date '2024-01-02');

insert into v2v3_doris_upd_case5_orc values
(3, 'base', 300, date '2024-01-03');

update v2v3_doris_upd_case5_orc
set tag = 'base_u', score = score + 10
where id = 1;

delete from v2v3_doris_upd_case5_orc
where id = 2;

alter table v2v3_doris_upd_case5_orc
set tblproperties ('format-version' = '3');
