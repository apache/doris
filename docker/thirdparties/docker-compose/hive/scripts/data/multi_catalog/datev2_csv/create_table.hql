create database if not exists multi_catalog;

use multi_catalog;

drop table if exists `datev2_csv`;

create external table `datev2_csv`(
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
