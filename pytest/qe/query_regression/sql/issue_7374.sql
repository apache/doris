-- https://github.com/apache/incubator-doris/issues/7374
set enable_sql_cache=true;
DROP DATABASE IF EXISTS issue_7374;
CREATE DATABASE issue_7374;
use issue_7374
CREATE TABLE `tbl1` (`dt` int(11) NULL, `org_id` int(11) NULL ) ENGINE=OLAP AGGREGATE KEY(`dt`, `org_id`) PARTITION BY RANGE(`dt`) (PARTITION p20211209 VALUES [("20211208"), ("20211209"))) DISTRIBUTED BY HASH(`org_id`) BUCKETS 10 PROPERTIES ("replication_num" = "1", "in_memory" = "false", "storage_format" = "V2");
CREATE VIEW `view1` AS SELECT `org_id` AS `org_id`, `dt` AS `dt` FROM `tbl1`;
SELECT origin.org_id AS ord_id FROM (SELECT view1.org_id FROM view1 view1 WHERE view1.dt = 20211208 AND view1.org_id IN (2132) LIMIT 0, 10000) origin;
insert into tbl1 values(20211208, 2132);
SELECT origin.org_id AS ord_id FROM (SELECT view1.org_id FROM view1 view1 WHERE view1.dt = 20211208 AND view1.org_id IN (2132) LIMIT 0, 10000) origin;
DROP DATABASE issue_7374;

