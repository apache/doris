CREATE TABLE `auto_inc_target` (
  `close_account_month` date NOT NULL COMMENT "关账月份",
  `auto_increment_id` bigint NOT NULL AUTO_INCREMENT(1) COMMENT "自增ID",
  `close_account_status` int NULL COMMENT "关账状态(1关账/0未关账)",
  `_id` varchar(255) NULL COMMENT "表ID",
  `transaction_id` varchar(255) NULL COMMENT "交易ID",
  `insert_time` datetime NULL COMMENT "数仓数据更新时间"
)
DUPLICATE KEY(`close_account_month`, `auto_increment_id`)
AUTO PARTITION BY RANGE (date_trunc(`close_account_month`, 'month'))
()
DISTRIBUTED BY HASH(`transaction_id`) BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);