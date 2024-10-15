CREATE DATABASE IF NOT EXISTS partition_type;
USE partition_type;

CREATE TABLE `partition_type.smallint_partition`(
  `id` int)
PARTITIONED BY ( 
  `smallint_part` smallint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='|', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/partition_type/smallint_partition'
TBLPROPERTIES (
  'transient_lastDdlTime'='1697099288');

msck repair table smallint_partition;
