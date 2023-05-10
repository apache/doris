CREATE TABLE IF NOT EXISTS `supplier` (
  `s_suppkey` int(11) NOT NULL COMMENT "",
  `s_name` varchar(26) NOT NULL COMMENT "",
  `s_address` varchar(26) NOT NULL COMMENT "",
  `s_city` varchar(11) NOT NULL COMMENT "",
  `s_nation` varchar(16) NOT NULL COMMENT "",
  `s_region` varchar(13) NOT NULL COMMENT "",
  `s_phone` varchar(16) NOT NULL COMMENT "",
  INDEX s_suppkey_idx(s_suppkey) USING INVERTED COMMENT 's_suppkey index',
  INDEX s_name_idx(s_name) USING INVERTED COMMENT 's_name index',
  INDEX s_address_idx(s_address) USING INVERTED COMMENT 's_address index'
)
UNIQUE KEY (`s_suppkey`)
DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10
PROPERTIES (
"compression"="zstd",
"replication_num" = "1",
"enable_unique_key_merge_on_write" = "true"
);
