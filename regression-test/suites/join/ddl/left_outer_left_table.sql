CREATE TABLE left_outer_left_table (
  `k1` varchar(200) NULL COMMENT ""
) ENGINE=OLAP
UNIQUE KEY(`k1`)
COMMENT "olap"
DISTRIBUTED BY HASH(`k1`) BUCKETS 20
PROPERTIES (
"in_memory" = "false",
"storage_format" = "V2",
"replication_num" = "1"
);
