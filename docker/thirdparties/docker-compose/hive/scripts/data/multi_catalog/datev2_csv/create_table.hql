create database if not exists multi_catalog;

use multi_catalog;

CREATE external TABLE `datev2_csv`(
  `id` int,
  `day` date)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/datev2_csv'
TBLPROPERTIES (
  'transient_lastDdlTime'='1688118691');

msck repair table datev2_csv;

