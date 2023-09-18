-- https://github.com/apache/incubator-doris/issues/4218
drop database if exists issue_4218

create database issue_4218
use issue_4218
-- create table
DROP TABLE IF EXISTS `t3`

CREATE TABLE `t3` (`c0` varchar(1)) ENGINE=OLAP DUPLICATE KEY(`c0`) COMMENT "OLAP" DISTRIBUTED BY HASH(`c0`) BUCKETS 10 PROPERTIES ("replication_num" = "1", "in_memory" = "false", "storage_format" = "DEFAULT")
insert into t3 values(''),(NULL),('N'),('w')

select *FROM t3 WHERE (CAST(t3.c0 AS BOOLEAN)) is NULL
select * from t3
select CAST('' AS BOOLEAN)
select CAST('w' AS BOOLEAN)
select CAST('n' AS BOOLEAN)
select CAST('1' AS BOOLEAN)
select CAST('0' AS BOOLEAN)

drop database issue_4218
