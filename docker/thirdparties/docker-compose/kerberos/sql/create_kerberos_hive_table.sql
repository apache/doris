CREATE DATABASE IF NOT EXISTS `test_krb_hive_db`;
CREATE TABLE IF NOT EXISTS `test_krb_hive_db`.`test_krb_hive_tbl`(
  `id_key` int,
  `string_key` string,
  `rate_val` double,
  `comment` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';

INSERT INTO test_krb_hive_db.test_krb_hive_tbl values(1, 'a', 3.16, 'cc0');
INSERT INTO test_krb_hive_db.test_krb_hive_tbl values(2, 'b', 41.2, 'cc1');
INSERT INTO test_krb_hive_db.test_krb_hive_tbl values(3, 'c', 6.2, 'cc2');
INSERT INTO test_krb_hive_db.test_krb_hive_tbl values(4, 'd', 1.4, 'cc3');
