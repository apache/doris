-- https://github.com/apache/incubator-doris/issues/5483
drop database if exists issue_5483;
create database issue_5483;
use issue_5483
create view test as select split_part(current_user(), "'", 2) as k1;
select * from test;
drop database issue_5483;
