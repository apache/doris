CREATE TABLE `flink_connector`
(
    `order_id`     int NULL,
    `order_amount` varchar(50) NULL,
    `order_status` int NULL
) ENGINE=OLAP
DUPLICATE KEY(`order_id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`order_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "1"
);
