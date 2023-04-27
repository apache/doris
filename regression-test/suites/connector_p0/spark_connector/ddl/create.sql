CREATE TABLE `spark_connector`
(
    `order_id`     varchar(30) NULL,
    `order_amount` int(11) NULL,
    `order_status` varchar(30) NULL
) ENGINE=OLAP
DUPLICATE KEY(`order_id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`order_id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "1"
);
