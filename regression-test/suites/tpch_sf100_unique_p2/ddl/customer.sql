CREATE TABLE IF NOT EXISTS customer (
    c_custkey     int NOT NULL,
    c_name        VARCHAR(25) NOT NULL,
    c_address     VARCHAR(40) NOT NULL,
    c_nationkey   int NOT NULL,
    c_phone       VARCHAR(15) NOT NULL,
    c_acctbal     decimal(15, 2)   NOT NULL,
    c_mktsegment  VARCHAR(10) NOT NULL,
    c_comment     VARCHAR(117) NOT NULL
)ENGINE=OLAP
UNIQUE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 24
PROPERTIES (
    "enable_mow_light_delete" = "true",
    "replication_num" = "3"
)

