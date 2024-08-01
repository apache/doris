CREATE TABLE `string_col_dict_plain_mixed_orc`(
  `col0` int,
  `col1` string,
  `col2` double,
  `col3` boolean,
  `col4` string,
  `col5` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/orc_table/string_col_dict_plain_mixed_orc'
TBLPROPERTIES (
  'orc.compress'='ZLIB');

msck repair table string_col_dict_plain_mixed_orc;

