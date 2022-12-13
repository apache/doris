CREATE TABLE IF NOT EXISTS test_join (
  `k1` int NOT NULL COMMENT ""
) ENGINE=OLAP
UNIQUE KEY(`k1`)
COMMENT "olap"
DISTRIBUTED BY HASH(`k1`) BUCKETS 10
PROPERTIES (
"in_memory" = "false",
"storage_format" = "V2",
"replication_num" = "1"
);