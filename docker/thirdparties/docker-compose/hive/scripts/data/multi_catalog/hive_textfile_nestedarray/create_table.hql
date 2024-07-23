create database if not exists multi_catalog;
use multi_catalog;

CREATE TABLE IF NOT EXISTS `hive_textfile_nestedarray`(
  `col1` int,
  `col2` array<array<array<int>>>)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/hive_textfile_nestedarray';

msck repair table hive_textfile_nestedarray;