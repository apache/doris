CREATE TABLE `schema_evo_test_text`(
  id int,
  name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ',';
insert into `schema_evo_test_text` select 1, "kaka";
alter table `schema_evo_test_text` ADD COLUMNS (`ts` timestamp);
insert into `schema_evo_test_text` select 2, "messi", from_unixtime(to_unix_timestamp('20230101 13:01:03','yyyyMMdd HH:mm:ss'));

