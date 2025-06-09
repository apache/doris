CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `orc_top_level_column_has_present_stream`(
    `id` int, 
    `name` string, 
    `age` int, 
    `salary` float, 
    `is_active` boolean)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/orc_top_level_column_has_present_stream';

msck repair table orc_top_level_column_has_present_stream;
