create database if not exists demo.test_db;
use demo.test_db;

drop table if exists tag_branch_table;
create table tag_branch_table (c1 int);
insert into tag_branch_table values (1);

alter table demo.test_db.tag_branch_table create branch b1;
alter table demo.test_db.tag_branch_table create tag t1;

insert into tag_branch_table values (2);

alter table demo.test_db.tag_branch_table create branch b2;
alter table demo.test_db.tag_branch_table create tag t2;

alter table demo.test_db.tag_branch_table add column c2 int;

insert into tag_branch_table values (3, 4);

alter table demo.test_db.tag_branch_table create branch b3;
alter table demo.test_db.tag_branch_table create tag t3;

alter table demo.test_db.tag_branch_table add column c3 int;
