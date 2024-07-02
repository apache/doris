create database if not exists multi_catalog;
use multi_catalog;



CREATE TABLE IF NOT EXISTS `test_date_string_partition`(
  `k1` int)
PARTITIONED BY (
  `day1` string,
  `day2` date)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/test_date_string_partition';


msck repair table test_date_string_partition;

