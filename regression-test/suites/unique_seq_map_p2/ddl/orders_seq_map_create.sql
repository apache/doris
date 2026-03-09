CREATE TABLE IF NOT EXISTS orders_seq_map  (
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
    "enable_unique_key_merge_on_write" = "false",
    "light_schema_change"="true",
    "replication_num" = "1",
    "sequence_mapping.o_shippriority" = "o_custkey,o_orderstatus,o_totalprice,o_orderpriority,o_clerk,o_comment"
)
