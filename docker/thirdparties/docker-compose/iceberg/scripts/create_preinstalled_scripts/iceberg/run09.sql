create database if not exists demo.test_db;
use demo.test_db;

create table schema_change_with_time_travel (c1 int);
insert into schema_change_with_time_travel values (1);

alter table schema_change_with_time_travel add column c2 int;
insert into schema_change_with_time_travel values (2,3);

alter table schema_change_with_time_travel add column c3 int; 
insert into schema_change_with_time_travel values (4,5,6);

alter table schema_change_with_time_travel drop column c2;
insert into schema_change_with_time_travel values (7,8);

alter table schema_change_with_time_travel add column c2 int;
insert into schema_change_with_time_travel values (9,10,11);

alter table schema_change_with_time_travel add column c4 int;


create table schema_change_with_time_travel_orc (c1 int) tblproperties ("write.format.default"="orc");
insert into schema_change_with_time_travel_orc values (1);

alter table schema_change_with_time_travel_orc add column c2 int;
insert into schema_change_with_time_travel_orc values (2,3);

alter table schema_change_with_time_travel_orc add column c3 int; 
insert into schema_change_with_time_travel_orc values (4,5,6);

alter table schema_change_with_time_travel_orc drop column c2;
insert into schema_change_with_time_travel_orc values (7,8);

alter table schema_change_with_time_travel_orc add column c2 int;
insert into schema_change_with_time_travel_orc values (9,10,11);

alter table schema_change_with_time_travel_orc add column c4 int;

