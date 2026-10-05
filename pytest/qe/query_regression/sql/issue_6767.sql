--https://github.com/apache/incubator-doris/issues/6767
drop database if exists issue_6767
create database issue_6767
use issue_6767
create table table1 (city_id bigint, tag_id bigint, dt bigint, num bigint) duplicate key(city_id, tag_id, dt) distributed by hash(city_id) buckets 8
insert into table1 values(1100000000000000005,1,20200102,1),(1100000000000000006,1,20200102,1),(1100000000000000007,1,20200102,1),(1100000000000000008,1,20200102,1)
select * from table1 where city_id in ("1100000000000000005")
drop database issue_6767
