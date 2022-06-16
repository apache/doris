CREATE TABLE left_outer_right_table (
  `k2` varchar(100) NULL COMMENT ""
) ENGINE=OLAP
UNIQUE KEY(`k2`)
COMMENT "olap"
DISTRIBUTED BY HASH(`k2`) BUCKETS 20
PROPERTIES (
"in_memory" = "false",
"storage_format" = "V2",
"replication_num" = "1"
);
