CREATE DATABASE IF NOT EXISTS partition_type;
USE partition_type;

drop table if exists `partition_type.string_partition`;

create table `partition_type.string_partition`(
  `id` int)
PARTITIONED BY ( 
  `string_part` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/partition_type/string_partition'
TBLPROPERTIES (
  'transient_lastDdlTime'='1697100288');

msck repair table string_partition;
