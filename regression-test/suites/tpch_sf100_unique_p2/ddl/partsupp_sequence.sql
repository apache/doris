CREATE TABLE IF NOT EXISTS partsupp (
    ps_partkey     int NOT NULL,
    ps_suppkey     int NOT NULL,
    ps_availqty    int NOT NULL,
    ps_supplycost  decimal(15, 2)  NOT NULL,
    ps_comment     VARCHAR(199) NOT NULL
)ENGINE=OLAP
UNIQUE KEY(`ps_partkey`,`ps_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
PROPERTIES (
    "enable_mow_light_delete" = "true",
    "function_column.sequence_type" = 'int',
    "replication_num" = "3"
)

