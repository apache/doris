CREATE TABLE IF NOT EXISTS parquet_s3_case7 (
    p_partkey          int NOT NULL DEFAULT "1",
    p_name        VARCHAR(55) NOT NULL DEFAULT "2",
    p_mfgr        VARCHAR(25) NOT NULL DEFAULT "3",
    col4       VARCHAR(10) NOT NULL DEFAULT "4"
)ENGINE=OLAP
DUPLICATE KEY(`p_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
