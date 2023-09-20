-- https://github.com/apache/incubator-doris/issues/5430
DROP DATABASE IF EXISTS issue_5430;
CREATE DATABASE issue_5430;
USE issue_5430

CREATE TABLE `t1` (`id` bigint(20) NULL COMMENT "", `id1` bigint(20) NULL COMMENT "") ENGINE=OLAP AGGREGATE KEY(`id`, `id1`) COMMENT "OLAP" DISTRIBUTED BY HASH(`id1`) BUCKETS 2 PROPERTIES ("replication_num" = "1", "in_memory" = "false", "storage_format" = "V2");
insert into t1 values (1, 2);

CREATE TABLE `t2` (`id` bigint(20) NULL COMMENT "", name  varchar(10) NULL COMMENT "") ENGINE=OLAP AGGREGATE KEY(`id`, name) COMMENT "OLAP" DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ("replication_num" = "1", "in_memory" = "false", "storage_format" = "V2");
select grouping_id(t1.id), t1.id, max(name) from t1 left join t2 on t1.id1 = t2.id group by grouping sets ((id), ());

DROP DATABASE IF EXISTS issue_5430;
