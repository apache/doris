CREATE TABLE `util_date_type` (
    `date_type` varchar(100) NULL COMMENT ''
) ENGINE=OLAP
DUPLICATE KEY(`date_type`)
COMMENT ''
DISTRIBUTED BY HASH(`date_type`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
);
