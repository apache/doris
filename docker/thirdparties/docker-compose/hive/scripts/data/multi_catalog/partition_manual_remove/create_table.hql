create database if not exists multi_catalog;

use multi_catalog;

CREATE EXTERNAL TABLE `partition_manual_remove`(
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
    '/user/doris/suites/multi_catalog/partition_manual_remove'
TBLPROPERTIES (
    'transient_lastDdlTime'='1684941779');

msck repair table partition_manual_remove;

