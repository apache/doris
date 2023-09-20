-- https://github.com/apache/incubator-doris/issues/7410
DROP DATABASE IF EXISTS issue_7410;
CREATE DATABASE issue_7410;
use issue_7410
CREATE TABLE `table1` (`siteid` int(11) NULL DEFAULT "10" COMMENT "", `citycode` int(11) NULL COMMENT "", `username` varchar(32) NULL DEFAULT "" COMMENT "", `pv` bigint(20) NULL DEFAULT "0" COMMENT "") ENGINE=OLAP DUPLICATE KEY(`siteid`, `citycode`) COMMENT "OLAP" DISTRIBUTED BY HASH(`siteid`) BUCKETS 1 PROPERTIES ("replication_num" = "3", "in_memory" = "false", "storage_format" = "V2");
create view view_test1 as select siteid, citycode, concat(username, pv) as concat from table1;
insert into table1 values(0, 2, 'chengdu', 5), (0, 4, 'wuhan', 4);
select siteid, citycode, concat from view_test1 group by rollup(siteid, citycode, concat) order by 1, 2, 3;
select siteid, citycode, username from table1 group by rollup(siteid, citycode, username) order by 1, 2, 3;
DROP DATABASE issue_7410;

