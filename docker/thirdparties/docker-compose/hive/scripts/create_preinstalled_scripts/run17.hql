CREATE external TABLE `table_with_pars`(
  `id` int COMMENT 'id',
  `data` string COMMENT 'data')
PARTITIONED BY (
  `dt_par` date,
  `time_par` timestamp,
  `decimal_par1` decimal(8, 4),
  `decimal_par2` decimal(18, 6),
  `decimal_par3` decimal(38, 12))
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
  '/user/doris/preinstalled_data/csv_partition_table/table_with_pars/';

set hive.msck.path.validation=ignore;
msck repair table table_with_pars;

