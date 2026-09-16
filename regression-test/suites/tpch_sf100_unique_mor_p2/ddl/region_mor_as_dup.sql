CREATE TABLE IF NOT EXISTS region_mor_as_dup (
    r_regionkey  int NOT NULL,
    r_name       VARCHAR(25) NOT NULL,
    r_comment    VARCHAR(152)
)ENGINE=OLAP
UNIQUE KEY(`r_regionkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1
PROPERTIES (
    "enable_unique_key_merge_on_write" = "false",
    "disable_auto_compaction" = "true",
    "replication_num" = "3"
)
