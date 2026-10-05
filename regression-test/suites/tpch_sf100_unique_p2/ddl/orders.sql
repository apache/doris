CREATE TABLE IF NOT EXISTS orders  (
    o_orderkey       bigint NOT NULL,
    o_orderdate      DATE NOT NULL,
    o_custkey        int NOT NULL,
    o_orderstatus    VARCHAR(1) NOT NULL,
    o_totalprice     decimal(15, 2) NOT NULL,
    o_orderpriority  VARCHAR(15) NOT NULL,
    o_clerk          VARCHAR(15) NOT NULL,
    o_shippriority   int NOT NULL,
    o_comment        VARCHAR(79) NOT NULL
)ENGINE=OLAP
UNIQUE KEY(`o_orderkey`, `o_orderdate`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
PROPERTIES (
    "enable_mow_light_delete" = "true",
    "replication_num" = "3"
)
