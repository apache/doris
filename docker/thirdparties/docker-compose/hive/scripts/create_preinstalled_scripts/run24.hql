CREATE TABLE `schema_evo_test_orc`(
  id int,
  name string
)
stored as orc;
insert into `schema_evo_test_orc` select 1, "kaka";
alter table `schema_evo_test_orc` ADD COLUMNS (`ts` timestamp);
insert into `schema_evo_test_orc` select 2, "messi", from_unixtime(to_unix_timestamp('20230101 13:01:03','yyyyMMdd HH:mm:ss'));

set hive.stats.column.autogather=true;

-- Currently docker is hive 2.x version. Hive 2.x versioned full-acid tables need to run major compaction.
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

