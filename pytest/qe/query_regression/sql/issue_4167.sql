-- https://github.com/apache/incubator-doris/issues/4167
drop database if exists issue_4167

create database issue_4167
use issue_4167
-- create table
DROP TABLE IF EXISTS `duplicate_table_with_default`
CREATE TABLE duplicate_table_with_default (k1 date default "1970-01-01",k2 datetime default "1970-01-01 00:00:00",k3 char(20) default "",k4 varchar(20) default "",k5 boolean,k6 tinyint default "0",k7 smallint default "0",k8 int default "0",k9 bigint default "0",k10 largeint default "12345678901234567890",k11 float default "-1",k12 double default "0",k13 decimal(27,9) default "0") ENGINE=OLAP DUPLICATE KEY(k1, k2, k3, k4, k5) COMMENT "OLAP" DISTRIBUTED BY HASH(k1, k2, k3, k4, k5) BUCKETS 3 PROPERTIES ("replication_num" = "1","storage_format" = "v2")

insert into duplicate_table_with_default values ('2020-06-25', '2020-06-25 00:16:23', 'beijing', 'haidian', 0, 87, -31785, -2147482665, -9223372036854774825, default, 25.2, 2.69, -2.18)

select * from duplicate_table_with_default

drop database issue_4167
