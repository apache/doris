create database if not exists multi_catalog;
use multi_catalog;


drop table if exists `one_partition`;


create table `one_partition`(
  `id` int)
PARTITIONED BY (
  `part1` int)
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
  '/user/doris/suites/multi_catalog/one_partition';


msck repair table one_partition;