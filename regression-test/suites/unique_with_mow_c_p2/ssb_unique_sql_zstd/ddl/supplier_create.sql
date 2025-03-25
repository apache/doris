CREATE TABLE IF NOT EXISTS `supplier` (
  `s_suppkey` int(11) NOT NULL COMMENT "",
  `s_name` varchar(26) NOT NULL COMMENT "",
  `s_address` varchar(26) NOT NULL COMMENT "",
  `s_city` varchar(11) NOT NULL COMMENT "",
  `s_nation` varchar(16) NOT NULL COMMENT "",
  `s_region` varchar(13) NOT NULL COMMENT "",
  `s_phone` varchar(16) NOT NULL COMMENT ""
)
UNIQUE KEY (`s_suppkey`)
CLUSTER BY (`s_address`, `s_name`)
DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10
PROPERTIES (
"enable_mow_light_delete" = "true",
"compression"="zstd",
"replication_num" = "1",
"enable_unique_key_merge_on_write" = "true"
);
