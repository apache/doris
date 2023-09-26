-- https://github.com/apache/incubator-doris/issues/3854
drop database if exists issue_3854

create database issue_3854
use issue_3854
-- create table
DROP TABLE IF EXISTS `t1`

CREATE TABLE `t1` (`a` int(11) NULL COMMENT "", `b` int(11) NULL COMMENT "", `c` int(11) NULL COMMENT "", `d` int(11) NULL COMMENT "", `e` int(11) NULL COMMENT "") ENGINE=OLAP DUPLICATE KEY(`a`) COMMENT "OLAP" DISTRIBUTED BY HASH(`a`) BUCKETS 10 PROPERTIES ("replication_num" = "1", "in_memory" = "false", "storage_format" = "DEFAULT")

INSERT INTO t1(b,e,d,a,c) VALUES(223,221,222,220,224)

INSERT INTO t1(d,e,b,a,c) VALUES(226,227,228,229,225)

INSERT INTO t1(a,c,b,e,d) VALUES(234,231,232,230,233)

INSERT INTO t1(e,b,a,c,d) VALUES(237,236,239,235,238)

INSERT INTO t1(e,c,b,a,d) VALUES(242,244,240,243,241)

INSERT INTO t1(e,d,c,b,a) VALUES(246,248,247,249,245)

SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1 ORDER BY 1

SELECT c, d - e , CASE a + 1 WHEN b THEN 111 WHEN c THEN 222 WHEN d THEN 333 WHEN e THEN 444 ELSE 555 END , a + b * 2 + c * 3 + d * 4 , e FROM t1 WHERE d NOT BETWEEN 110 AND 150 OR c BETWEEN b - 2 AND d + 2 OR e > c OR e < d ORDER BY 1, 5, 3, 2, 4

drop database issue_3854
