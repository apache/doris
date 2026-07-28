create database if not exists stats_test;
use stats_test;
drop table if exists stats_test1;
create table stats_test1 (id INT, value STRING) STORED AS ORC;
drop table if exists stats_test2;
create table stats_test2 (id INT, value STRING) STORED AS PARQUET;
drop table if exists stats_test3;
create table stats_test3 (id INT, value STRING) STORED AS PARQUET;

insert into stats_test1 values (1, 'name1'), (2, 'name2'), (3, 'name3');
INSERT INTO stats_test2 VALUES (1, ';'), (2, '\*');

drop table if exists employee_gz;

create table employee_gz(name string,salary string)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties 
('quoteChar'='\"'
,'separatorChar'=',');

insert into employee_gz values ('a', '1.1'), ('b', '2.2');

