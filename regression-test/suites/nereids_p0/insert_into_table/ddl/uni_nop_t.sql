create table uni_nop_t (
    `id` int null,
    `kbool` boolean null,
    `ktint` tinyint(4) null,
    `ksint` smallint(6) null,
    `kint` int(11) null,
    `kbint` bigint(20) null,
    `klint` largeint(40) null,
    `kfloat` float null,
    `kdbl` double null,
    `kdcml` decimal(12, 6) null,
    `kchr` char(10) null,
    `kvchr` varchar(10) null,
    `kstr` string null,
    `kdt` date null,
    `kdtv2` datev2 null,
    `kdtm` datetime null,
    `kdtmv2` datetimev2(0) null,
    `kdcml32v3` decimalv3(7, 3) null,
    `kdcml64v3` decimalv3(10, 5) null,
    `kdcml128v3` decimalv3(20, 8) null
) engine=OLAP
unique key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1"
);


create table uni_light_sc_nop_t (
    `id` int null,
    `kbool` boolean null,
    `ktint` tinyint(4) null,
    `ksint` smallint(6) null,
    `kint` int(11) null,
    `kbint` bigint(20) null,
    `klint` largeint(40) null,
    `kfloat` float null,
    `kdbl` double null,
    `kdcml` decimal(12, 6) null,
    `kchr` char(10) null,
    `kvchr` varchar(10) null,
    `kstr` string null,
    `kdt` date null,
    `kdtv2` datev2 null,
    `kdtm` datetime null,
    `kdtmv2` datetimev2(0) null,
    `kdcml32v3` decimalv3(7, 3) null,
    `kdcml64v3` decimalv3(10, 5) null,
    `kdcml128v3` decimalv3(20, 8) null
) engine=OLAP
unique key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1",
   "light_schema_change"="true"
);


create table uni_mow_nop_t (
    `id` int null,
    `kbool` boolean null,
    `ktint` tinyint(4) null,
    `ksint` smallint(6) null,
    `kint` int(11) null,
    `kbint` bigint(20) null,
    `klint` largeint(40) null,
    `kfloat` float null,
    `kdbl` double null,
    `kdcml` decimal(12, 6) null,
    `kchr` char(10) null,
    `kvchr` varchar(10) null,
    `kstr` string null,
    `kdt` date null,
    `kdtv2` datev2 null,
    `kdtm` datetime null,
    `kdtmv2` datetimev2(0) null,
    `kdcml32v3` decimalv3(7, 3) null,
    `kdcml64v3` decimalv3(10, 5) null,
    `kdcml128v3` decimalv3(20, 8) null
) engine=OLAP
unique key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1"
);


create table uni_light_sc_mow_nop_t (
    `id` int null,
    `kbool` boolean null,
    `ktint` tinyint(4) null,
    `ksint` smallint(6) null,
    `kint` int(11) null,
    `kbint` bigint(20) null,
    `klint` largeint(40) null,
    `kfloat` float null,
    `kdbl` double null,
    `kdcml` decimal(12, 6) null,
    `kchr` char(10) null,
    `kvchr` varchar(10) null,
    `kstr` string null,
    `kdt` date null,
    `kdtv2` datev2 null,
    `kdtm` datetime null,
    `kdtmv2` datetimev2(0) null,
    `kdcml32v3` decimalv3(7, 3) null,
    `kdcml64v3` decimalv3(10, 5) null,
    `kdcml128v3` decimalv3(20, 8) null
) engine=OLAP
unique key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1",
   "light_schema_change"="true"
);


create table uni_not_null_nop_t (
    `id` int not null,
    `kbool` boolean not null,
    `ktint` tinyint(4) not null,
    `ksint` smallint(6) not null,
    `kint` int(11) not null,
    `kbint` bigint(20) not null,
    `klint` largeint(40) not null,
    `kfloat` float not null,
    `kdbl` double not null,
    `kdcml` decimal(12, 6) not null,
    `kchr` char(10) not null,
    `kvchr` varchar(10) not null,
    `kstr` string not null,
    `kdt` date not null,
    `kdtv2` datev2 not null,
    `kdtm` datetime not null,
    `kdtmv2` datetimev2(0) not null,
    `kdcml32v3` decimalv3(7, 3) not null,
    `kdcml64v3` decimalv3(10, 5) not null,
    `kdcml128v3` decimalv3(20, 8) not null
) engine=OLAP
unique key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1"
);


create table uni_light_sc_not_null_nop_t (
    `id` int not null,
    `kbool` boolean not null,
    `ktint` tinyint(4) not null,
    `ksint` smallint(6) not null,
    `kint` int(11) not null,
    `kbint` bigint(20) not null,
    `klint` largeint(40) not null,
    `kfloat` float not null,
    `kdbl` double not null,
    `kdcml` decimal(12, 6) not null,
    `kchr` char(10) not null,
    `kvchr` varchar(10) not null,
    `kstr` string not null,
    `kdt` date not null,
    `kdtv2` datev2 not null,
    `kdtm` datetime not null,
    `kdtmv2` datetimev2(0) not null,
    `kdcml32v3` decimalv3(7, 3) not null,
    `kdcml64v3` decimalv3(10, 5) not null,
    `kdcml128v3` decimalv3(20, 8) not null
) engine=OLAP
unique key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1",
   "light_schema_change"="true"
);


create table uni_mow_not_null_nop_t (
    `id` int not null,
    `kbool` boolean not null,
    `ktint` tinyint(4) not null,
    `ksint` smallint(6) not null,
    `kint` int(11) not null,
    `kbint` bigint(20) not null,
    `klint` largeint(40) not null,
    `kfloat` float not null,
    `kdbl` double not null,
    `kdcml` decimal(12, 6) not null,
    `kchr` char(10) not null,
    `kvchr` varchar(10) not null,
    `kstr` string not null,
    `kdt` date not null,
    `kdtv2` datev2 not null,
    `kdtm` datetime not null,
    `kdtmv2` datetimev2(0) not null,
    `kdcml32v3` decimalv3(7, 3) not null,
    `kdcml64v3` decimalv3(10, 5) not null,
    `kdcml128v3` decimalv3(20, 8) not null
) engine=OLAP
unique key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1"
);


create table uni_light_sc_mow_not_null_nop_t (
    `id` int not null,
    `kbool` boolean not null,
    `ktint` tinyint(4) not null,
    `ksint` smallint(6) not null,
    `kint` int(11) not null,
    `kbint` bigint(20) not null,
    `klint` largeint(40) not null,
    `kfloat` float not null,
    `kdbl` double not null,
    `kdcml` decimal(12, 6) not null,
    `kchr` char(10) not null,
    `kvchr` varchar(10) not null,
    `kstr` string not null,
    `kdt` date not null,
    `kdtv2` datev2 not null,
    `kdtm` datetime not null,
    `kdtmv2` datetimev2(0) not null,
    `kdcml32v3` decimalv3(7, 3) not null,
    `kdcml64v3` decimalv3(10, 5) not null,
    `kdcml128v3` decimalv3(20, 8) not null
) engine=OLAP
unique key(id)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1",
   "light_schema_change"="true"
);