CREATE TABLE IF NOT EXISTS region (
    r_regionkey  int NOT NULL,
    r_name       VARCHAR(25) NOT NULL,
    r_comment    VARCHAR(152)
)ENGINE=OLAP
UNIQUE KEY(`r_regionkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1
PROPERTIES (
    "function_column.sequence_type" = 'int',
    "replication_num" = "3"
)

