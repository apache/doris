CREATE EXTERNAL TABLE IF NOT EXISTS `orc_all_types_partition`(
  `tinyint_col` tinyint,
  `smallint_col` smallint,
  `int_col` int,
  `bigint_col` bigint,
  `boolean_col` boolean,
  `float_col` float,
  `double_col` double,
  `string_col` string,
  `binary_col` binary,
  `timestamp_col` timestamp,
  `decimal_col` decimal(12,4),
  `char_col` char(50),
  `varchar_col` varchar(50),
  `date_col` date,
  `list_double_col` array<double>,
  `list_string_col` array<string>)
PARTITIONED BY (
  `p1_col` string,
  `p2_col` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/orc_table/orc_all_types_partition';

msck repair table orc_all_types_partition;

