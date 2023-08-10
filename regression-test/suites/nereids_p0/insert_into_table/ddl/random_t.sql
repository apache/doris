create table dup_t_type_cast_rd (
    `id` int null,
    `kint` int(11) max null,
    `kdbl` double max null,
    `kdcml` decimal(20, 6) replace null,
    `kvchr` varchar(20) replace null,
    `kdt` date replace null,
    `kdtmv2` datetimev2(0) replace null,
    `kdcml32v3` decimalv3(7, 3) replace null
) engine=OLAP
duplicate key(id)
partition by range(id) (
    partition p1 values less than ("3"),
    partition p2 values less than ("5"),
    partition p3 values less than ("7"),
    partition p4 values less than ("15")
)
distributed by random buckets 4
properties (
   "replication_num"="1"
);

create table agg_t_type_cast_rd (
    `id` int null,
    `kint` int(11) max null,
    `kdbl` double max null,
    `kdcml` decimal(20, 6) replace null,
    `kvchr` varchar(20) replace null,
    `kdt` date replace null,
    `kdtmv2` datetimev2(0) replace null,
    `kdcml32v3` decimalv3(7, 3) replace null
) engine=OLAP
aggregate key(id)
partition by range(id) (
    partition p1 values less than ("3"),
    partition p2 values less than ("5"),
    partition p3 values less than ("7"),
    partition p4 values less than ("15")
)
distributed by random buckets 4
properties (
   "replication_num"="1"
);

create table uni_t_type_cast (
    `id` int null,
    `kint` int(11) max null,
    `kdbl` double max null,
    `kdcml` decimal(20, 6) replace null,
    `kvchr` varchar(20) replace null,
    `kdt` date replace null,
    `kdtmv2` datetimev2(0) replace null,
    `kdcml32v3` decimalv3(7, 3) replace null
) engine=OLAP
unique key(id)
partition by range(id) (
    partition p1 values less than ("3"),
    partition p2 values less than ("5"),
    partition p3 values less than ("7"),
    partition p4 values less than ("15")
)
distributed by random buckets 4
properties (
   "replication_num"="1"
);

