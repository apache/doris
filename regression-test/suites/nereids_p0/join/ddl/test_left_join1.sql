CREATE TABLE `test_left_join1` (
   `f_key` bigint(10) NOT NULL DEFAULT "0" COMMENT "key",
   `f_value` bigint(10) REPLACE_IF_NOT_NULL NULL COMMENT "value"
) ENGINE=OLAP
AGGREGATE KEY(`f_key`)
COMMENT "向量引擎bug展示"
DISTRIBUTED BY HASH(`f_key`) BUCKETS 2
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
);
