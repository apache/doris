-- https://github.com/apache/incubator-doris/issues/4278
drop database if exists issue_4278

create database issue_4278
use issue_4278
-- create table
DROP TABLE IF EXISTS `t1`
DROP TABLE IF EXISTS `t2`

CREATE TABLE `t1` (`v1` int(11) NULL COMMENT "",`v2` int(11) NULL COMMENT "", `v3` int(11) NULL COMMENT "") ENGINE=OLAP DUPLICATE KEY(`v1`, `v2`, `v3`) COMMENT "OLAP" DISTRIBUTED BY HASH(`v1`) BUCKETS 1 PROPERTIES ("replication_num" = "1","in_memory" = "false","storage_format" = "V2")

CREATE TABLE `t2` (`v1` int(11) NULL COMMENT "",`v2` int(11) NULL COMMENT "", `v3` int(11) NULL COMMENT "") ENGINE=OLAP DUPLICATE KEY(`v1`, `v2`, `v3`) COMMENT "OLAP" DISTRIBUTED BY HASH(`v1`) BUCKETS 1 PROPERTIES ("replication_num" = "1","in_memory" = "false","storage_format" = "V2")

insert into t1 values(1,2,3)
insert into t1 values(4,5,6)
insert into t2 values(1,2,3)

select t1.v1,b.v2 from t1 left join (select v1, v2 is null as v2 from t2) as b on t1.v1=b.v1

drop database issue_4278
