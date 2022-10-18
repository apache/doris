CREATE TABLE `table_3` (
  `id` bigint(20) NOT NULL,
  `plat_id` int(11) NOT NULL,
  `company_id` varchar(180) NOT NULL,
  `user_name` varchar(150) NOT NULL COMMENT '名称',
  `user_email` varchar(150) NOT NULL COMMENT '邮箱',
  `group_id` int(11) NULL COMMENT '组id',
  `is_delete` int(11) NOT NULL,
  `ctime` int(11) NOT NULL DEFAULT "0",
  `mtime` int(11) NOT NULL DEFAULT "0",
  `op_type` varchar(6) NOT NULL DEFAULT "insert"
) ENGINE=OLAP
UNIQUE KEY(`id`, `plat_id`, `company_id`, `user_name`, `user_email`)
DISTRIBUTED BY HASH(`id`, `plat_id`, `company_id`, `user_name`, `user_email`) BUCKETS 4
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"bloom_filter_columns" = "plat_id",
"compression" = "ZSTD"
)
