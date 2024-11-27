CREATE TABLE IF NOT EXISTS `customer` (
  `c_custkey` int(11) NOT NULL COMMENT "",
  `c_name` varchar(26) NOT NULL COMMENT "",
  `c_address` varchar(41) NOT NULL COMMENT "",
  `c_city` varchar(11) NOT NULL COMMENT "",
  `c_nation` varchar(16) NOT NULL COMMENT "",
  `c_region` varchar(13) NOT NULL COMMENT "",
  `c_phone` varchar(16) NOT NULL COMMENT "",
  `c_mktsegment` varchar(11) NOT NULL COMMENT ""
)
UNIQUE KEY (`c_custkey`)
CLUSTER BY (`c_name`, `c_phone`, `c_city`)
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10
PROPERTIES (
"compression"="zstd",
"replication_num" = "1",
"disable_auto_compaction" = "true",
"enable_unique_key_merge_on_write" = "true",
"enable_mow_light_delete" = "true"
);
