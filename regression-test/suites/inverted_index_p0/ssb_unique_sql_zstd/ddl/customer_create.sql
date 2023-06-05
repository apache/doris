CREATE TABLE IF NOT EXISTS `customer` (
  `c_custkey` int(11) NOT NULL COMMENT "",
  `c_name` varchar(26) NOT NULL COMMENT "",
  `c_address` varchar(41) NOT NULL COMMENT "",
  `c_city` varchar(11) NOT NULL COMMENT "",
  `c_nation` varchar(16) NOT NULL COMMENT "",
  `c_region` varchar(13) NOT NULL COMMENT "",
  `c_phone` varchar(16) NOT NULL COMMENT "",
  `c_mktsegment` varchar(11) NOT NULL COMMENT "",
  INDEX c_custkey_idx(c_custkey) USING INVERTED COMMENT 'c_custkey index',
  INDEX c_name_idx(c_name) USING INVERTED COMMENT 'c_name index',
  INDEX c_address_idx(c_address) USING INVERTED COMMENT 'c_address index',
  INDEX c_city_idx(c_city) USING INVERTED COMMENT 'c_city index',
  INDEX c_nation_idx(c_nation) USING INVERTED COMMENT 'c_nation index',
  INDEX c_region_idx(c_region) USING INVERTED COMMENT 'c_region index',
  INDEX c_phone_idx(c_phone) USING INVERTED COMMENT 'c_phone index',
  INDEX c_mktsegment_idx(c_mktsegment) USING INVERTED COMMENT 'c_mktsegment index'
)
UNIQUE KEY (`c_custkey`)
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10
PROPERTIES (
"compression"="zstd",
"replication_num" = "1",
"enable_unique_key_merge_on_write" = "true"
);
