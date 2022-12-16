CREATE TABLE `ods_park_cp_sys_enum` (
   `id` varchar(192) NOT NULL,
   `parent_id` varchar(192) NULL COMMENT '',
   `sequence` int(11) NULL COMMENT '',
   `code` varchar(192) NULL COMMENT '',
   `en_name` varchar(192) NULL COMMENT '',
   `cn_name` varchar(192) NULL COMMENT '',
   `type` varchar(6) NULL COMMENT '',
   `status` varchar(6) NULL COMMENT '',
   `amount_ascription` char(6) NULL COMMENT '',
   `describes` varchar(765) NULL COMMENT '',
   `create_time` datetime NULL COMMENT '',
   `update_time` datetime NULL COMMENT '',
   `deleted` int(11) NULL COMMENT '',
   `version` int(11) NULL COMMENT '',
   `create_by` varchar(96) NULL COMMENT '',
   `update_by` varchar(96) NULL COMMENT '',
   `cdc_create_time` bigint(20) NOT NULL COMMENT '',
   `cdc_type` varchar(10) NOT NULL COMMENT ''
) ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT ''
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
);
