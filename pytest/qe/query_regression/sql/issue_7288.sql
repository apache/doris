-- https://github.com/apache/incubator-doris/issues/7288
DROP DATABASE IF EXISTS issue_7288;
CREATE DATABASE issue_7288;
use issue_7288
CREATE TABLE left_table (k1 int(11) NULL COMMENT "", no varchar(50) NOT NULL COMMENT "") ENGINE=OLAP DUPLICATE KEY(k1) COMMENT "OLAP" DISTRIBUTED BY HASH(k1) BUCKETS 2 PROPERTIES ("replication_allocation" = "tag.location.default: 1", "in_memory" = "false", "storage_format" = "V2");
CREATE TABLE right_table (k1 int(11) NULL COMMENT "", no varchar(50) NOT NULL COMMENT "") ENGINE=OLAP DUPLICATE KEY(k1) COMMENT "OLAP" DISTRIBUTED BY HASH(k1) BUCKETS 2 PROPERTIES ("replication_allocation" = "tag.location.default: 1", "in_memory" = "false", "storage_format" = "V2");
insert into left_table values(1, "test");
select l.k1, group_concat(r.no) from left_table l left join right_table r on l.k1=r.k1 group by l.k1;
DROP DATABASE issue_7288;
