create database if not exists default;
use default;

CREATE TABLE `hive_recursive_directories_table`(
  `id` int,
  `name` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/suites/default/hive_recursive_directories_table';


CREATE TABLE `hive_ignore_absent_partitions_table`(
  `id` int,
  `name` string)
PARTITIONED BY (country STRING, city STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/suites/default/hive_ignore_absent_partitions_table';

ALTER TABLE hive_ignore_absent_partitions_table ADD PARTITION (country='USA', city='NewYork');
ALTER TABLE hive_ignore_absent_partitions_table ADD PARTITION (country='India', city='Delhi');
