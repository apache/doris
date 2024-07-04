CREATE TABLE `schema_evo_test_parquet`(
  id int,
  name string
)
stored as parquet;
insert into `schema_evo_test_parquet` select 1, "kaka";
alter table `schema_evo_test_parquet` ADD COLUMNS (`ts` timestamp);
insert into `schema_evo_test_parquet` select 2, "messi", from_unixtime(to_unix_timestamp('20230101 13:01:03','yyyyMMdd HH:mm:ss'));

