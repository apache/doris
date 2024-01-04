-- https://github.com/apache/incubator-doris/issues/4546
DROP DATABASE IF EXISTS issue_4546;
CREATE DATABASE issue_4546;
USE issue_4546

CREATE TABLE `baseall` ( `k1` tinyint(4) NULL COMMENT "",  `k2` smallint(6) NULL COMMENT "",  `k3` int(11) NULL COMMENT "",  `k4` bigint(20) NULL COMMENT "",  `k5` decimal(9, 3) NULL COMMENT "",  `k6` char(5) NULL COMMENT "",  `k10` date NULL COMMENT "",  `k11` datetime NULL COMMENT "",  `k7` varchar(20) NULL COMMENT "",  `k8` double MAX NULL COMMENT "", `k9` float SUM NULL COMMENT "") ENGINE=OLAP AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`) COMMENT "OLAP" DISTRIBUTED BY HASH(`k1`) BUCKETS 5 PROPERTIES ("replication_num" = "1");

select * from baseall b1, baseall b2 where b1.k1 = b2.k1;

DROP DATABASE IF EXISTS issue_4546;
