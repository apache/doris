--https://github.com/apache/incubator-doris/issues/6095
drop database if exists issue_6095
create database issue_6095
use issue_6095
drop table if exists `test_issue_6095`
create table test_issue_6095 (k1 int,k2 int) distributed by hash(k1) buckets 8 properties("replication_num"="1")
insert into test_issue_6095 values (1,10),(2,11),(3,12),(4,13),(5,14),(6,15)
create view test_view_6095 as select * from test_issue_6095 where 11 > 12
select * from test_view_6095
create view test_view_6095_2 as select * from test_issue_6095 where 11 < 12
select * from test_view_6095_2 order by k1
drop database issue_6095
