CREATE TABLE IF NOT EXISTS path_column (
    p_partkey          int NOT NULL,
    p_name        VARCHAR(55) NOT NULL,
    p_mfgr        VARCHAR(25) NOT NULL,
    p_brand       VARCHAR(10) NOT NULL,
    p_type        VARCHAR(25) NOT NULL,
    p_size        int NOT NULL,
    p_container   VARCHAR(10) NOT NULL,
    p_retailprice decimal(15, 2) NOT NULL,
    p_comment     VARCHAR(23) NOT NULL,
    city varchar(10) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`p_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1"
);
