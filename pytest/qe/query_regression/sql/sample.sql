-- create db -> create table -> load -> select -> drop database
drop database if exists sample1
create database sample1
use sample1
--comment
create table baseall(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=olap distributed by hash(k1) buckets 5 properties("storage_type"="column")
--LOAD LABEL sample1.label_1 (DATA INFILE("hdfs://yf-palo-test13.yf01.baidu.com:54310/user/palo/test/data/qe/baseall.txt") INTO TABLE `baseall` COLUMNS TERMINATED BY "\t") WITH BROKER "hdfs" ("username"="root", "password"="3trqDWfl")
SELECT * FROM baseall
select * from baseall #IGNORE CHECK

create table t(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=olap distributed by hash(k1) buckets 5 properties("storage_type"="column")
--insert into t select * from baseall
select * from t

create table t1(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=olap distributed by hash(k1) buckets 5 properties("storage_type"="column")
curl --location-trusted -u root: -T ./data/baseall.txt http://{FE_HOST}:{HTTP_PORT}/api/sample1/t1/_load?label=label12
select * from t1
--drop database sample1
