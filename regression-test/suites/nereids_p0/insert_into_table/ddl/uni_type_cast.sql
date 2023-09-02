create table uni_t_type_cast (
    `id` int null,
    `kint` int(11) null,
    `kdbl` double null,
    `kdcml` decimal(20, 6) null,
    `kvchr` varchar(20) null,
    `kdt` date null,
    `kdtmv2` datetimev2(0) null,
    `kdcml32v3` decimalv3(7, 3) null
) engine=OLAP
unique key(id)
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


create table uni_light_sc_t_type_cast (
    `id` int null,
    `kint` int(11) null,
    `kdbl` double null,
    `kdcml` decimal(20, 6) null,
    `kvchr` varchar(20) null,
    `kdt` date null,
    `kdtmv2` datetimev2(0) null,
    `kdcml32v3` decimalv3(7, 3) null
) engine=OLAP
unique key(id)
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


create table uni_mow_t_type_cast (
    `id` int null,
    `kint` int(11) null,
    `kdbl` double null,
    `kdcml` decimal(20, 6) null,
    `kvchr` varchar(20) null,
    `kdt` date null,
    `kdtmv2` datetimev2(0) null,
    `kdcml32v3` decimalv3(7, 3) null
) engine=OLAP
unique key(id)
partition by range(id) (
    partition p1 values less than ("3"),
    partition p2 values less than ("5"),
    partition p3 values less than ("7"),
    partition p4 values less than ("15")
)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1",
   "enable_unique_key_merge_on_write"="true"
);


create table uni_light_sc_mow_t_type_cast (
    `id` int null,
    `kint` int(11) null,
    `kdbl` double null,
    `kdcml` decimal(20, 6) null,
    `kvchr` varchar(20) null,
    `kdt` date null,
    `kdtmv2` datetimev2(0) null,
    `kdcml32v3` decimalv3(7, 3) null
) engine=OLAP
unique key(id)
partition by range(id) (
    partition p1 values less than ("3"),
    partition p2 values less than ("5"),
    partition p3 values less than ("7"),
    partition p4 values less than ("15")
)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1",
   "light_schema_change"="true",
   "enable_unique_key_merge_on_write"="true"
);


create table uni_not_null_t_type_cast (
    `id` int not null,
    `kint` int(11) not null,
    `kdbl` double not null,
    `kdcml` decimal(20, 6) not null,
    `kvchr` varchar(20) not null,
    `kdt` date not null,
    `kdtmv2` datetimev2(0) not null,
    `kdcml32v3` decimalv3(7, 3) not null
) engine=OLAP
unique key(id)
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


create table uni_light_sc_not_null_t_type_cast (
    `id` int not null,
    `kint` int(11) not null,
    `kdbl` double not null,
    `kdcml` decimal(20, 6) not null,
    `kvchr` varchar(20) not null,
    `kdt` date not null,
    `kdtmv2` datetimev2(0) not null,
    `kdcml32v3` decimalv3(7, 3) not null
) engine=OLAP
unique key(id)
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


create table uni_mow_not_null_t_type_cast (
    `id` int not null,
    `kint` int(11) not null,
    `kdbl` double not null,
    `kdcml` decimal(20, 6) not null,
    `kvchr` varchar(20) not null,
    `kdt` date not null,
    `kdtmv2` datetimev2(0) not null,
    `kdcml32v3` decimalv3(7, 3) not null
) engine=OLAP
unique key(id)
partition by range(id) (
    partition p1 values less than ("3"),
    partition p2 values less than ("5"),
    partition p3 values less than ("7"),
    partition p4 values less than ("15")
)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1",
   "enable_unique_key_merge_on_write"="true"
);


create table uni_light_sc_mow_not_null_t_type_cast (
    `id` int not null,
    `kint` int(11) not null,
    `kdbl` double not null,
    `kdcml` decimal(20, 6) not null,
    `kvchr` varchar(20) not null,
    `kdt` date not null,
    `kdtmv2` datetimev2(0) not null,
    `kdcml32v3` decimalv3(7, 3) not null
) engine=OLAP
unique key(id)
partition by range(id) (
    partition p1 values less than ("3"),
    partition p2 values less than ("5"),
    partition p3 values less than ("7"),
    partition p4 values less than ("15")
)
distributed by hash(id) buckets 4
properties (
   "replication_num"="1",
   "light_schema_change"="true",
   "enable_unique_key_merge_on_write"="true"
);