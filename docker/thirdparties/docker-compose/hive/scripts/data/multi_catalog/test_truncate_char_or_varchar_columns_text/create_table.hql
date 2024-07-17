CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `test_truncate_char_or_varchar_columns_text`(
  `id` int,
  `city` varchar(3),
  `country` char(3))
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/doris/suites/multi_catalog/test_truncate_char_or_varchar_columns_text';

msck repair table test_truncate_char_or_varchar_columns_text;
