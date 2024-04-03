CREATE TABLE `stress_source` (
  `create_date` DATE NOT NULL,
  `parent_org_id` VARCHAR(96) NULL,
  `org_id` VARCHAR(100) NULL,
  `org_name` VARCHAR(192) NULL,
  `create_month` VARCHAR(11) NOT NULL,
  `org_type` VARCHAR(192) NULL,
  `sms_total` INT NULL,
  `success_sms_total` INT NULL,
  `sms_price_total` DOUBLE NULL,
  `sms_total_sum` INT NULL,
  `has_sub` INT NULL,
  `order_num` INT NULL
) ENGINE=OLAP
UNIQUE KEY(`create_date`, `parent_org_id`, `org_id`)
DISTRIBUTED BY HASH(`create_date`, `org_id`) BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
