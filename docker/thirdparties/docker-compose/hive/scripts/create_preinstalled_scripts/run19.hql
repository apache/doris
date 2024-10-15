set hive.stats.column.autogather=false;

CREATE TABLE `test_hive_orc_add_column`(
  id int,
  col1 int
)
stored as orc;
insert into `test_hive_orc_add_column` values(1,2),(3,4),(4,6);
alter table `test_hive_orc_add_column` ADD COLUMNS (col2 int);
insert into `test_hive_orc_add_column` values(7,8,9),(10,11,null),(12,13,null),(14,15,16);
alter table `test_hive_orc_add_column` ADD COLUMNS (col3 int,col4 string);
insert into `test_hive_orc_add_column` values(17,18,19,20,"hello world"),(21,22,23,24,"cywcywcyw"),(25,26,null,null,null),(27,28,29,null,null),(30,31,32,33,null);

CREATE TABLE `test_hive_parquet_add_column`(
  id int,
  col1 int
)
stored as parquet;
insert into `test_hive_parquet_add_column` values(1,2),(3,4),(4,6);
alter table `test_hive_parquet_add_column` ADD COLUMNS (col2 int);
insert into `test_hive_parquet_add_column` values(7,8,9),(10,11,null),(12,13,null),(14,15,16);
alter table `test_hive_parquet_add_column` ADD COLUMNS (col3 int,col4 string);
insert into `test_hive_parquet_add_column` values(17,18,19,20,"hello world"),(21,22,23,24,"cywcywcyw"),(25,26,null,null,null),(27,28,29,null,null),(30,31,32,33,null);

CREATE TABLE `schema_evo_test_text`(
  id int,
  name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ',';
insert into `schema_evo_test_text` select 1, "kaka";
alter table `schema_evo_test_text` ADD COLUMNS (`ts` timestamp);
insert into `schema_evo_test_text` select 2, "messi", from_unixtime(to_unix_timestamp('20230101 13:01:03','yyyyMMdd HH:mm:ss'));

CREATE TABLE `schema_evo_test_parquet`(
  id int,
  name string
)
stored as parquet;
insert into `schema_evo_test_parquet` select 1, "kaka";
alter table `schema_evo_test_parquet` ADD COLUMNS (`ts` timestamp);
insert into `schema_evo_test_parquet` select 2, "messi", from_unixtime(to_unix_timestamp('20230101 13:01:03','yyyyMMdd HH:mm:ss'));

CREATE TABLE `schema_evo_test_orc`(
  id int,
  name string
)
stored as orc;
insert into `schema_evo_test_orc` select 1, "kaka";
alter table `schema_evo_test_orc` ADD COLUMNS (`ts` timestamp);
insert into `schema_evo_test_orc` select 2, "messi", from_unixtime(to_unix_timestamp('20230101 13:01:03','yyyyMMdd HH:mm:ss'));

set hive.stats.column.autogather=true;

