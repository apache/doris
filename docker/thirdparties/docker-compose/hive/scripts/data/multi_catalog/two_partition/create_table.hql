create database if not exists multi_catalog;
use multi_catalog;


CREATE TABLE IF NOT EXISTS `two_partition`(
  `id` int)
PARTITIONED BY (
  `part1` int,
  `part2` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='|',
  'serialization.format'='|')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/two_partition';



msck repair table two_partition;

