CREATE TABLE IF NOT EXISTS tpch_tiny_region (
    r_regionkey  INTEGER NOT NULL,
    r_name       CHAR(25) NOT NULL,
    r_comment    VARCHAR(152)
)
DUPLICATE KEY(r_regionkey)
DISTRIBUTED BY HASH(r_regionkey) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
)