CREATE DATABASE IF NOT EXISTS regression;
USE regression;

CREATE TABLE `multi_delimit_test`(
  `k1` int,
  `k2` int,
  `name` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='|+|',
  'mapkey.delim'='@',
  'collection.delim'=':',
  'serialization.format'='1',
  'serialization.encoding'='UTF-8')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/regression/multi_delimit_test'
TBLPROPERTIES (
  'transient_lastDdlTime'='1692719456');

CREATE TABLE `multi_delimit_test2`(
  `id` int,
  `value` double,
  `description` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='||',
  'serialization.format'='1')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/regression/multi_delimit_test2'
TBLPROPERTIES (
  'transient_lastDdlTime'='1692719456');

INSERT INTO multi_delimit_test VALUES
  (1, 100, 'test1'),
  (2, 200, 'test2'),
  (3, 300, 'test3');

INSERT INTO multi_delimit_test2 VALUES
  (1, 1.5, 'description1'),
  (2, 2.5, 'description2'),
  (3, 3.5, 'description3');