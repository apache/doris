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
CLUSTER BY (`c_region`, `c_address`, `c_city`, `c_name`)
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10
PROPERTIES (
"compression"="zstd",
"replication_num" = "1",
"disable_auto_compaction" = "true",
"enable_unique_key_merge_on_write" = "true"
);
