CREATE TABLE IF NOT EXISTS `test_bucket_shuffle_join` (
  `id` int(11) NOT NULL COMMENT "",
  `rectime` datetime NOT NULL COMMENT ""
) ENGINE=OLAP
UNIQUE KEY(`id`, `rectime`)
COMMENT "olap"
PARTITION BY RANGE(`rectime`)
(
PARTITION p202111 VALUES [('2021-11-01 00:00:00'), ('2021-12-01 00:00:00')))
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
)

