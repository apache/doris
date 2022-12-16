CREATE TABLE `ods_park_cp_base_channel` (
  `pk_channel_id` varchar(96) NOT NULL COMMENT '',
  `channel_code` varchar(108) NULL COMMENT '',
  `space_id` varchar(96) NULL COMMENT '',
  `name` varchar(192) NULL COMMENT '',
  `park_id` varchar(96) NULL COMMENT '',
  `area_id` varchar(96) NULL COMMENT '',
  `channel_type` char(6) NULL COMMENT '',
  `from_area_id` varchar(96) NULL COMMENT '',
  `to_area_id` varchar(96) NULL COMMENT '',
  `terminal_id` varchar(96) NULL COMMENT '',
  `major_camera_id` varchar(96) NULL COMMENT '',
  `secondary_camera_id` varchar(96) NULL COMMENT '',
  `display_id` varchar(96) NULL COMMENT '',
  `road_gate_id` varchar(96) NULL COMMENT '',
  `extend_device_id` varchar(96) NULL COMMENT '',
  `status` char(6) NULL COMMENT '',
  `pay_url` varchar(765) NULL COMMENT '',
  `no_plate_enter_url` text NULL COMMENT '',
  `no_plate_exit_url` text NULL COMMENT '',
  `tenent_id` varchar(96) NULL COMMENT '',
  `create_time` datetime NULL COMMENT '',
  `update_time` datetime NULL COMMENT '',
  `deleted` int(11) NULL COMMENT '',
  `version` int(11) NULL COMMENT '',
  `create_by` varchar(192) NULL COMMENT '',
  `update_by` varchar(192) NULL COMMENT '',
  `no_plate_enter_url_status` varchar(6) NULL COMMENT '',
  `no_plate_exit_url_status` varchar(6) NULL COMMENT '',
  `pay_url_status` varchar(6) NULL COMMENT '',
  `dim_rule` char(6) NULL COMMENT '',
  `cdc_create_time` bigint(20) NOT NULL COMMENT '',
  `cdc_type` varchar(10) NOT NULL COMMENT ''
) ENGINE=OLAP
UNIQUE KEY(`pk_channel_id`)
COMMENT ''
DISTRIBUTED BY HASH(`pk_channel_id`) BUCKETS 4
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false"
);
