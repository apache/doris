create database if not exists multi_catalog;
use multi_catalog;

CREATE TABLE IF NOT EXISTS `hive_textfile_array_all_types`(
  `col1` array<tinyint>,
  `col2` array<smallint>,
  `col3` array<int>,
  `col4` array<bigint>,
  `col5` array<boolean>,
  `col6` array<float>,
  `col7` array<double>,
  `col8` array<string>,
  `col9` array<timestamp>,
  `col10` array<date>,
  `col11` array<decimal(10,3)>,
  `col12` array<char(1)>,
  `col13` array<varchar(10)>)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/hive_textfile_array_all_types';

msck repair table hive_textfile_array_all_types;