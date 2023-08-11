-- https://github.com/apache/incubator-doris/issues/5144
DROP DATABASE IF EXISTS issue_5144;
CREATE DATABASE issue_5144;
USE issue_5144

CREATE TABLE dynamic_partition2 (k1 bigint(20) NULL COMMENT "",k2 int(11) NULL COMMENT "",k3 smallint(6) NULL COMMENT "") ENGINE=OLAP DUPLICATE KEY(k1, k2, k3) COMMENT "OLAP" PARTITION BY RANGE(k1)(PARTITION p20201230 VALUES [("20201230"), ("20201231")),PARTITION p20201231 VALUES [("20201231"), ("20210101")))DISTRIBUTED BY HASH(k2) BUCKETS 32 PROPERTIES ("replication_num" = "1","colocate_with" = "group1","dynamic_partition.enable" = "true","dynamic_partition.time_unit" = "DAY","dynamic_partition.time_zone" = "Asia/Shanghai","dynamic_partition.start" = "-6","dynamic_partition.end" = "6","dynamic_partition.prefix" = "p","dynamic_partition.replication_num" = "1","dynamic_partition.buckets" = "32","in_memory" = "false","storage_format" = "V2");

CREATE TABLE dynamic_partition_bucket2 (k1 bigint(20) NULL COMMENT "",k2 int(11) NULL COMMENT "",k3 smallint(6) NULL COMMENT "") ENGINE=OLAP DUPLICATE KEY(k1, k2, k3) COMMENT "OLAP" PARTITION BY RANGE(k1)(PARTITION p20201230 VALUES [("20201230"), ("20201231")),PARTITION p20201231 VALUES [("20201231"), ("20210101")))DISTRIBUTED BY HASH(k2, k3) BUCKETS 32 PROPERTIES ("replication_num" = "1","colocate_with" = "group3","dynamic_partition.enable" = "true","dynamic_partition.time_unit" = "DAY","dynamic_partition.time_zone" = "Asia/Shanghai","dynamic_partition.start" = "-6","dynamic_partition.end" = "6","dynamic_partition.prefix" = "p","dynamic_partition.replication_num" = "1","dynamic_partition.buckets" = "32","in_memory" = "false","storage_format" = "V2");

set enable_bucket_shuffle_join=true;

select * from dynamic_partition2 a join dynamic_partition_bucket2 b on a.k2 = b.k2 where a.k1=20201218;

DROP DATABASE IF EXISTS issue_5144;
