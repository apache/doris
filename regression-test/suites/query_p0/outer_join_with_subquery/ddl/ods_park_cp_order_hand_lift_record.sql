CREATE TABLE `ods_park_cp_order_hand_lift_record` (
   `pk_lift_id` varchar(96) NOT NULL,
   `order_id` varchar(96) NULL COMMENT '',
   `local_order_id` varchar(108) NULL COMMENT '',
   `tenant_id` varchar(96) NULL COMMENT '',
   `park_id` varchar(96) NULL COMMENT '',
   `plate` varchar(48) NULL COMMENT '',
   `direction` char(6) NULL COMMENT '',
   `channel_id` varchar(96) NULL COMMENT '',
   `lift_time` datetime NULL COMMENT '',
   `lift_cause` char(60) NULL COMMENT '',
   `lift_type` char(6) NULL COMMENT '',
   `operater` varchar(96) NULL COMMENT '',
   `operater_id` varchar(96) NULL COMMENT '',
   `img` varchar(765) NULL COMMENT '',
   `review_status` char(6) NULL COMMENT '',
   `reviewer` varchar(96) NULL COMMENT '',
   `review_time` datetime NULL COMMENT '',
   `review_description` varchar(765) NULL COMMENT '',
   `create_time` datetime NULL COMMENT '',
   `update_time` datetime NULL COMMENT '',
   `deleted` int(11) NULL COMMENT '',
   `version` int(11) NULL COMMENT '',
   `create_by` varchar(192) NULL COMMENT '',
   `update_by` varchar(192) NULL COMMENT '',
   `description` varchar(765) NULL COMMENT '',
   `record_id` varchar(192) NULL COMMENT '',
   `cdc_create_time` bigint(20) NOT NULL COMMENT '',
   `cdc_type` varchar(10) NOT NULL COMMENT ''
) ENGINE=OLAP
UNIQUE KEY(`pk_lift_id`)
COMMENT 'ods_park_cp_order_hand_lift_record'
DISTRIBUTED BY HASH(`pk_lift_id`) BUCKETS 64
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"bloom_filter_columns" = "park_id, lift_time",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
);
