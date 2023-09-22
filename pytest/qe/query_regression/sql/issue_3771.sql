-- https://github.com/apache/incubator-doris/issues/3771
drop database if exists issue_3771

create database issue_3771
use issue_3771

create table unique_table(k1 int, k2 int) UNIQUE KEY(k1) distributed by hash(k1) properties("replication_num" = "1")
insert into unique_table values(1,1)
select * from unique_table

drop database issue_3771
