create table dup_t_type_cast_rd (
    `id` int null,
    `kint` int(11) null,
    `kdbl` double null,
    `kdcml` decimal(20, 6) null,
    `kvchr` varchar(20) null,
    `kdt` date null,
    `kdtmv2` datetimev2(0) null,
    `kdcml32v3` decimalv3(7, 3) null
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
