CREATE TABLE IF NOT EXISTS `full_join_table` (
  `x` varchar(150) NULL COMMENT ""
) ENGINE=OLAP
UNIQUE KEY(`x`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`x`) BUCKETS 3
  PROPERTIES (
  "replication_allocation" = "tag.location.default: 1",
  "in_memory" = "false",
  "storage_format" = "V2"
)
