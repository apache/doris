CREATE TABLE `ad_order_data` (
  `pin_id` bigint(20) NOT NULL,
  `date_time` datetime NOT NULL COMMENT '点击时间',
  `order_day` datetime NOT NULL COMMENT '下单时间',
  `rptcnt` bigint(20) SUM NULL DEFAULT "0",
  `rptgmv` bigint(20) SUM NULL DEFAULT "0"
) ENGINE=OLAP
AGGREGATE KEY(`pin_id`, `date_time`, `order_day`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`pin_id`) BUCKETS 16
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
);