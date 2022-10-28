CREATE TABLE IF NOT EXISTS parquet_s3_case1 (
    p_partkey          int NOT NULL DEFAULT "1",
    p_name        VARCHAR(55) NOT NULL DEFAULT "2",
    p_mfgr        VARCHAR(25) NOT NULL DEFAULT "3",
    p_brand       VARCHAR(10) NOT NULL DEFAULT "4",
    p_type        VARCHAR(25) NOT NULL DEFAULT "5",
    p_size        int NOT NULL DEFAULT "6",
    p_container   VARCHAR(10) NOT NULL DEFAULT "7",
    p_retailprice decimal(15, 2) NOT NULL DEFAULT "8",
    p_comment     VARCHAR(23) NOT NULL DEFAULT "9",
    col1 int not null default "10"
)ENGINE=OLAP
DUPLICATE KEY(`p_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);
