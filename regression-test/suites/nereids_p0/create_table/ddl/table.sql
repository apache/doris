drop table if exists test_all_types;
create table test_all_types (
    `id` int null,
    `kbool` boolean null,
    `ktint` tinyint(4) null,
    `ksint` smallint(6) null,
    `kint` int(11) null,
    `kbint` bigint(20) null,
    `klint` largeint(40) null,
    `kfloat` float null,
    `kdbl` double null,
    `kdcml` decimal(9, 3) null,
    `kchr` char(10) null,
    `kvchr` varchar(10) null,
    `kstr` string null,
    `kdt` date null,
    `kdtv2` datev2 null,
    `kdtm` datetime null,
    `kdtmv2` datetimev2(0) null,
    `kdcml32v3` decimalv3(7, 3) null,
    `kdcml64v3` decimalv3(10, 5) null,
    `kdcml128v3` decimalv3(20, 8) null,
    `kaint` array<int> null,
    `kaaint` array<array<int>> null
) engine=OLAP
duplicate key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1"
);

drop table if exists test_agg_key;
create table test_agg_key (
    `id` int null,
    `kbool` boolean replace null,
    `ktint` tinyint(4) replace_if_not_null null,
    `ksint` smallint(6) max null,
    `kint` int(11) max null,
    `kbint` bigint(20) min null,
    `klint` largeint(40) sum null
) engine=OLAP
aggregate key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1"
);

drop table if exists test_uni_key;
create table test_uni_key (
    `id` int null,
    `kbool` boolean null,
    `ktint` tinyint(4) null,
    `ksint` smallint(6) null,
    `kint` int(11) null,
    `kbint` bigint(20) null,
    `klint` largeint(40) null
) engine=OLAP
unique key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1"
);

drop table if exists test_uni_key_mow;
create table test_uni_key_mow (
    `id` int null,
    `kbool` boolean null,
    `ktint` tinyint(4) null,
    `ksint` smallint(6) null,
    `kint` int(11) null,
    `kbint` bigint(20) null,
    `klint` largeint(40) null
) engine=OLAP
unique key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1",
   "enable_unique_key_merge_on_write"="true"
);

drop table if exists test_not_null;
create table test_not_null (
    `id` int not null,
    `kbool` boolean not null,
    `ktint` tinyint(4) not null,
    `ksint` smallint(6) not null,
    `kint` int(11) not null,
    `kbint` bigint(20) not null,
    `klint` largeint(40) not null
) engine=OLAP
duplicate key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1"
);

drop table if exists test_random;
create table test_random (
    `id` int not null,
    `kbool` boolean not null,
    `ktint` tinyint(4) not null,
    `ksint` smallint(6) not null,
    `kint` int(11) not null,
    `kbint` bigint(20) not null,
    `klint` largeint(40) not null
) engine=OLAP
duplicate key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1"
);

drop table if exists test_random_auto;
create table test_random_auto (
    `id` int not null,
    `kbool` boolean not null,
    `ktint` tinyint(4) not null,
    `ksint` smallint(6) not null,
    `kint` int(11) not null,
    `kbint` bigint(20) not null,
    `klint` largeint(40) not null
) engine=OLAP
duplicate key(id)
distributed by random buckets auto
properties (
   "replication_num"="1"
);

drop table if exists test_less_than_partition;
create table test_less_than_partition (
    `id` int not null,
    `kbool` boolean not null,
    `ktint` tinyint(4) not null,
    `ksint` smallint(6) not null,
    `kint` int(11) not null,
    `kbint` bigint(20) not null,
    `klint` largeint(40) not null
) engine=OLAP
duplicate key(id)
partition by range(id) (
    partition p1 values less than (5),
    partition p2 values less than (10),
    partition p3 values less than (maxvalue)
)
distributed by random buckets auto
properties (
   "replication_num"="1"
);

drop table if exists test_range_partition;
create table test_range_partition (
    `id` int not null,
    `kbool` boolean not null,
    `ktint` tinyint(4) not null,
    `ksint` smallint(6) not null,
    `kint` int(11) not null,
    `kbint` bigint(20) not null,
    `klint` largeint(40) not null
) engine=OLAP
duplicate key(id)
partition by range(id) (
    partition p1 values [(10), (maxvalue)),
    partition p2 values [(5), (10)),
    partition p3 values [(0), (5))
)
distributed by random buckets auto
properties (
   "replication_num"="1"
);

drop table if exists test_step_partition;
create table test_step_partition (
    `id` int not null,
    `kbool` boolean not null,
    `ktint` tinyint(4) not null,
    `ksint` smallint(6) not null,
    `kint` int(11) not null,
    `kbint` bigint(20) not null,
    `klint` largeint(40) not null
) engine=OLAP
duplicate key(id)
partition by range(id) (
    from (1) to (15) interval 1
)
distributed by random buckets auto
properties (
   "replication_num"="1"
);

drop table if exists test_date_step_partition;
CREATE TABLE test_date_step_partition (
    k1 DATE,
    k2 INT,
    id VARCHAR(20)
)
PARTITION BY RANGE (k1) (
    FROM ("2000-11-14") TO ("2021-11-14") INTERVAL 1 YEAR,
    FROM ("2021-11-14") TO ("2022-11-14") INTERVAL 1 MONTH,
    FROM ("2022-11-14") TO ("2023-01-03") INTERVAL 1 WEEK,
    FROM ("2023-01-03") TO ("2023-01-14") INTERVAL 1 DAY
)
DISTRIBUTED BY HASH(k2) BUCKETS 1
PROPERTIES(
    "replication_num" = "1"
);

drop table if exists test_list_partition;
create table test_list_partition (
    `id` int not null,
    `kbool` boolean not null,
    `ktint` tinyint(4) not null,
    `ksint` smallint(6) not null,
    `kint` int(11) not null,
    `kbint` bigint(20) not null,
    `klint` largeint(40) not null
) engine=OLAP
duplicate key(id)
partition by list(id) (
    partition p1 values in (1, 2, 3),
    partition p2 values in (4, 5, 10),
    partition p3 values in (13, 15, 20)
)
distributed by random buckets auto
properties (
   "replication_num"="1"
);

drop table if exists test_rollup;
create table test_rollup (
    `id` int not null,
    `kbool` boolean not null,
    `ktint` tinyint(4) not null,
    `ksint` smallint(6) not null,
    `kint` int(11) not null,
    `kbint` bigint(20) not null,
    `klint` largeint(40) not null
) engine=OLAP
duplicate key(id, kbool, ktint)
distributed by random buckets auto
rollup (
    r1 (id, ktint, kbool, kbint) duplicate key(id)
)
properties (
   "replication_num"="1"
);

drop table if exists test_default_value;
create table test_default_value (
    `id` int not null,
    `kbool` boolean not null,
    `ktint` tinyint(4) default null,
    `kdtmv2` datetimev2(0) default current_timestamp
)
distributed by hash(id)
properties (
   "replication_num"="1"
);