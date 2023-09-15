-- https://github.com/apache/incubator-doris/issues/5171
DROP DATABASE IF EXISTS issue_5171;
CREATE DATABASE issue_5171;
USE issue_5171

CREATE TABLE `per` ( `A` bigint(20) NULL COMMENT "") ENGINE=OLAP DUPLICATE KEY(`A`) COMMENT "OLAP" DISTRIBUTED BY HASH(`A`) BUCKETS 5 PROPERTIES ( "replication_num" = "1", "in_memory" = "false", "storage_format" = "V2");

insert into per values(65536);

set parallel_fragment_exec_instance_num = 15;

SELECT PERCENTILE_APPROX(A, 0.5) from per;

DROP DATABASE IF EXISTS issue_5171;
