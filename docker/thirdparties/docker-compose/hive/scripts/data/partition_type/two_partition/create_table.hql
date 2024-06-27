CREATE DATABASE IF NOT EXISTS partition_type;
USE partition_type;

CREATE TABLE `partition_type.two_partition`(
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
LOCATION '/user/doris/suites/partition_type/two_partition'
TBLPROPERTIES (
  'transient_lastDdlTime'='1697101231');

msck repair table two_partition;
