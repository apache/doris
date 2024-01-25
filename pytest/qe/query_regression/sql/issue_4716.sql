-- https://github.com/apache/incubator-doris/issues/4772
drop database if exists issue_4772

create database issue_4772
use issue_4772
-- create table
DROP TABLE IF EXISTS `test`
CREATE TABLE `test` (`k1` int(11) NULL COMMENT "",`k2` bigint(20) NULL COMMENT "",`k3` decimal(9, 0) NULL COMMENT "") ENGINE=OLAP DUPLICATE KEY(`k1`, `k2`) COMMENT "OLAP" DISTRIBUTED BY HASH(`k1`) BUCKETS 10 PROPERTIES ("replication_num" = "1","in_memory" = "false","storage_format" = "DEFAULT")

insert into test values(2,2,2)
select a from (select 'c1' a , k1 from test) a

insert into test values(2,2,2)
select a from (select 'c1' a , k1 from test) a

drop database issue_4772
