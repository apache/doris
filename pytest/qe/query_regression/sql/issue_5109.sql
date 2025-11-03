-- https://github.com/apache/incubator-doris/issues/5109
DROP DATABASE IF EXISTS issue_5109;
CREATE DATABASE issue_5109;
USE issue_5109

CREATE TABLE `per` ( `k1` bigint(20) NULL COMMENT "") ENGINE=OLAP DUPLICATE KEY(`k1`) COMMENT "OLAP" DISTRIBUTED BY HASH(`k1`) BUCKETS 5 PROPERTIES ( "replication_num" = "1", "in_memory" = "false", "storage_format" = "V2");

insert into per values(65536);

DELETE FROM per WHERE k1 is null;

SELECT * FROM per

DROP DATABASE IF EXISTS issue_5109;
