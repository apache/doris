create database if not exists format_v3;
use format_v3;

-- Keep deletion-vector test data in the aggregated spark-sql bootstrap flow
-- so the iceberg container does not need an additional spark-shell session.
drop table if exists dv_test;
create table dv_test (
    id int,
    batch int,
    data string
)
using iceberg
tblproperties (
    'format-version' = '3',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into dv_test values
    (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c'), (4, 1, 'd'),
    (5, 1, 'e'), (6, 1, 'f'), (7, 1, 'g'), (8, 1, 'h');

delete from dv_test
where batch = 1 and id in (3, 4, 5);

insert into dv_test values
    (9, 2, 'i'), (10, 2, 'j'), (11, 2, 'k'), (12, 2, 'l'),
    (13, 2, 'm'), (14, 2, 'n'), (15, 2, 'o'), (16, 2, 'p');

delete from dv_test
where batch = 2 and id >= 14;

delete from dv_test
where id % 2 = 1;

drop table if exists dv_test_v2;
create table dv_test_v2 (
    id int,
    batch int,
    data string
)
using iceberg
tblproperties (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

insert into dv_test_v2 values
    (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c'), (4, 1, 'd'),
    (5, 1, 'e'), (6, 1, 'f'), (7, 1, 'g'), (8, 1, 'h');

delete from dv_test_v2
where batch = 1 and id in (3, 4, 5);

alter table dv_test_v2
set tblproperties ('format-version' = '3');

delete from dv_test_v2
where id % 2 = 1;

drop table if exists dv_test_1w;
create table dv_test_1w (
    id bigint,
    grp int,
    value int,
    ts timestamp
)
using iceberg
tblproperties (
    'format-version' = '3',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.parquet.row-group-size-bytes' = '10240'
);

insert into dv_test_1w
select /*+ REPARTITION(10) */
    id,
    cast(id % 100 as int) as grp,
    cast(rand(20260324) * 1000 as int) as value,
    timestamp '2025-01-01 00:00:00' as ts
from range(0, 100000);

set spark.sql.shuffle.partitions = 1;
set spark.sql.adaptive.enabled = false;

delete from dv_test_1w
where id % 2 = 1;

delete from dv_test_1w
where id % 3 = 1;

reset spark.sql.shuffle.partitions;
reset spark.sql.adaptive.enabled;
