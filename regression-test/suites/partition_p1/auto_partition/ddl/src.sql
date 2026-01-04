CREATE TABLE `auto_inc_src` (
  `dt` date NOT NULL COMMENT "统计日期",
  `_id` varchar(255) NULL COMMENT "表ID",
  `transaction_id` varchar(255) NULL COMMENT "交易ID",
  `insert_time` datetime NULL COMMENT "数仓数据更新时间"
)
DUPLICATE KEY(`dt`)
DISTRIBUTED BY HASH(`transaction_id`) BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);