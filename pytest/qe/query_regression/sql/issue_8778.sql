-- https://github.com/apache/incubator-doris/issues/8778
DROP DATABASE IF EXISTS issue_8778;
CREATE DATABASE issue_8778;
use issue_8778
CREATE TABLE `t1` (`tc1` int(11) NULL COMMENT "", `tc2` int(11) NULL COMMENT "", `tc3` int(11) NULL COMMENT "") ENGINE=OLAP DUPLICATE KEY(`tc1`, `tc2`, `tc3`) COMMENT "OLAP" DISTRIBUTED BY HASH(`tc1`) BUCKETS 10 PROPERTIES ("replication_allocation" = "tag.location.default: 1", "in_memory" = "false", "storage_format" = "V2");
set parallel_fragment_exec_instance_num=2;
set disable_colocate_plan=false;
insert into t1 values(1,2,1),(1,3,1),(2,1,1),(3,1,1);
select t1.tc1,t1.tc2,sum(t1.tc3) as total from t1 join[shuffle] t1 t2 on t1.tc1=t2.tc1 group by rollup(tc1,tc2) order by t1.tc1,t1.tc2,total;
DROP DATABASE issue_8778;
