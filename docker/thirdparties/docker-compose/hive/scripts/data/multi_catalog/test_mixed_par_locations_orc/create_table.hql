CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `test_mixed_par_locations_orc`(
  `id` int,
  `name` string,
  `age` int,
  `city` string,
  `sex` string)
PARTITIONED BY (
 `par` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = '1')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/test_mixed_par_locations_orc';

msck repair table test_mixed_par_locations_orc;
