CREATE TABLE IF NOT EXISTS `part` (
  `p_partkey` int(11) NOT NULL COMMENT "",
  `p_name` varchar(23) NOT NULL COMMENT "",
  `p_mfgr` varchar(7) NOT NULL COMMENT "",
  `p_category` varchar(8) NOT NULL COMMENT "",
  `p_brand` varchar(10) NOT NULL COMMENT "",
  `p_color` varchar(12) NOT NULL COMMENT "",
  `p_type` varchar(26) NOT NULL COMMENT "",
  `p_size` int(11) NOT NULL COMMENT "",
  `p_container` varchar(11) NOT NULL COMMENT ""
)
UNIQUE KEY (`p_partkey`)
CLUSTER BY (`p_size`)
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 10
PROPERTIES (
"function_column.sequence_type" = 'int',
"compression"="zstd",
"replication_num" = "1",
"disable_auto_compaction" = "true",
"enable_unique_key_merge_on_write" = "true"
);
