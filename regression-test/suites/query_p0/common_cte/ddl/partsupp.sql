CREATE TABLE IF NOT EXISTS partsupp (
    partkey bigint,
    suppkey bigint,
    availqty integer,
    supplycost double,
    comment varchar(199)
) DUPLICATE KEY(partkey, suppkey) DISTRIBUTED BY HASH(partkey) BUCKETS 3 PROPERTIES ("replication_num" = "1")
