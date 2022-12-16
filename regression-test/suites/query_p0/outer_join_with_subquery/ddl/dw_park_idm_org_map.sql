CREATE TABLE `dw_park_idm_org_map` (
   `org_id` varchar(100) NULL COMMENT '',
   `org_name` varchar(500) NULL COMMENT '',
   `org_type` varchar(100) NULL COMMENT '',
   `parent_id` varchar(100) NULL COMMENT '',
   `parent_name` varchar(500) NULL COMMENT '',
   `child_id` varchar(100) NULL COMMENT '',
   `child_name` varchar(500) NULL COMMENT '',
   `park_id` varchar(500) NULL COMMENT '',
   `park_name` varchar(500) NULL COMMENT '',
   `park_project_id` varchar(100) NULL COMMENT '',
   `id_project` varchar(500) NULL COMMENT '',
   `name_project` varchar(500) NULL COMMENT '',
   `id_slice` varchar(500) NULL COMMENT '',
   `name_slice` varchar(500) NULL COMMENT '',
   `id_area` varchar(500) NULL COMMENT '',
   `name_area` varchar(500) NULL COMMENT '',
   `id_head` varchar(500) NULL COMMENT '',
   `name_head` varchar(500) NULL COMMENT ''
) ENGINE=OLAP
DUPLICATE KEY(`org_id`)
COMMENT ''
DISTRIBUTED BY HASH(`org_id`) BUCKETS 2
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
);
