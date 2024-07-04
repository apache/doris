CREATE TABLE `test_hive_parquet_add_column`(
  id int,
  col1 int
)
stored as parquet;
insert into  `test_hive_parquet_add_column` values(1,2);
insert into  `test_hive_parquet_add_column` values(3,4),(4,6);
alter table `test_hive_parquet_add_column` ADD COLUMNS (col2 int);
insert into  `test_hive_parquet_add_column` values(7,8,9);
insert into  `test_hive_parquet_add_column` values(10,11,null);
insert into  `test_hive_parquet_add_column` values(12,13,null);
insert into  `test_hive_parquet_add_column` values(14,15,16);
alter table `test_hive_parquet_add_column` ADD COLUMNS (col3 int,col4 string);
insert into  `test_hive_parquet_add_column` values(17,18,19,20,"hello world");
insert into  `test_hive_parquet_add_column` values(21,22,23,24,"cywcywcyw");
insert into  `test_hive_parquet_add_column` values(25,26,null,null,null);
insert into  `test_hive_parquet_add_column` values(27,28,29,null,null);
insert into  `test_hive_parquet_add_column` values(30,31,32,33,null);

