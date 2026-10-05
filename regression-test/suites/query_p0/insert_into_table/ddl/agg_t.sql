create table agg_t (
    `id` int null,
    `kbool` boolean replace null,
    `ktint` tinyint(4) max null,
    `ksint` smallint(6) max null,
    `kint` int(11) max null,
    `kbint` bigint(20) max null,
    `klint` largeint(40) max null,
    `kfloat` float max null,
    `kdbl` double max null,
    `kdcml` decimal(12, 6) replace null,
    `kchr` char(10) replace null,
    `kvchr` varchar(10) replace null,
    `kstr` string replace null,
    `kdt` date replace null,
    `kdtv2` datev2 replace null,
    `kdtm` datetime replace null,
    `kdtmv2` datetimev2(0) replace null,
    `kdcml32v3` decimalv3(7, 3) replace null,
    `kdcml64v3` decimalv3(10, 5) replace null,
    `kdcml128v3` decimalv3(20, 8) replace null
) engine=OLAP
aggregate key(id)
partition by range(id) (
    partition p1 values less than ("3"),
    partition p2 values less than ("5"),
    partition p3 values less than ("7"),
    partition p4 values less than ("15")
)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1"
);


create table agg_light_sc_t (
    `id` int null,
    `kbool` boolean replace null,
    `ktint` tinyint(4) max null,
    `ksint` smallint(6) max null,
    `kint` int(11) max null,
    `kbint` bigint(20) max null,
    `klint` largeint(40) max null,
    `kfloat` float max null,
    `kdbl` double max null,
    `kdcml` decimal(12, 6) replace null,
    `kchr` char(10) replace null,
    `kvchr` varchar(10) replace null,
    `kstr` string replace null,
    `kdt` date replace null,
    `kdtv2` datev2 replace null,
    `kdtm` datetime replace null,
    `kdtmv2` datetimev2(0) replace null,
    `kdcml32v3` decimalv3(7, 3) replace null,
    `kdcml64v3` decimalv3(10, 5) replace null,
    `kdcml128v3` decimalv3(20, 8) replace null
) engine=OLAP
aggregate key(id)
partition by range(id) (
    partition p1 values less than ("3"),
    partition p2 values less than ("5"),
    partition p3 values less than ("7"),
    partition p4 values less than ("15")
)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1",
   "light_schema_change"="true"
);


create table agg_not_null_t (
    `id` int not null,
    `kbool` boolean replace not null,
    `ktint` tinyint(4) max not null,
    `ksint` smallint(6) max not null,
    `kint` int(11) max not null,
    `kbint` bigint(20) max not null,
    `klint` largeint(40) max not null,
    `kfloat` float max not null,
    `kdbl` double max not null,
    `kdcml` decimal(12, 6) replace not null,
    `kchr` char(10) replace not null,
    `kvchr` varchar(10) replace not null,
    `kstr` string replace not null,
    `kdt` date replace not null,
    `kdtv2` datev2 replace not null,
    `kdtm` datetime replace not null,
    `kdtmv2` datetimev2(0) replace not null,
    `kdcml32v3` decimalv3(7, 3) replace not null,
    `kdcml64v3` decimalv3(10, 5) replace not null,
    `kdcml128v3` decimalv3(20, 8) replace not null
) engine=OLAP
aggregate key(id)
partition by range(id) (
    partition p1 values less than ("3"),
    partition p2 values less than ("5"),
    partition p3 values less than ("7"),
    partition p4 values less than ("15")
)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1"
);


create table agg_light_sc_not_null_t (
    `id` int not null,
    `kbool` boolean replace not null,
    `ktint` tinyint(4) max not null,
    `ksint` smallint(6) max not null,
    `kint` int(11) max not null,
    `kbint` bigint(20) max not null,
    `klint` largeint(40) max not null,
    `kfloat` float max not null,
    `kdbl` double max not null,
    `kdcml` decimal(12, 6) replace not null,
    `kchr` char(10) replace not null,
    `kvchr` varchar(10) replace not null,
    `kstr` string replace not null,
    `kdt` date replace not null,
    `kdtv2` datev2 replace not null,
    `kdtm` datetime replace not null,
    `kdtmv2` datetimev2(0) replace not null,
    `kdcml32v3` decimalv3(7, 3) replace not null,
    `kdcml64v3` decimalv3(10, 5) replace not null,
    `kdcml128v3` decimalv3(20, 8) replace not null
) engine=OLAP
aggregate key(id)
partition by range(id) (
    partition p1 values less than ("3"),
    partition p2 values less than ("5"),
    partition p3 values less than ("7"),
    partition p4 values less than ("15")
)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1",
   "light_schema_change"="true"
);