CREATE external TABLE `table_with_vertical_line`(
  `k1` string COMMENT 'k1',
  `k2` string COMMENT 'k2',
  `k3` string COMMENT 'k3',
  `k4` string COMMENT 'k4',
  `k5` string COMMENT 'k5',
  `k6` string COMMENT 'k6',
  `k7` string COMMENT 'k7',
  `k8` string COMMENT 'k8',
  `k9` string COMMENT 'k9',
  `k10` string COMMENT 'k10',
  `k11` string COMMENT 'k11',
  `k12` string COMMENT 'k12',
  `k13` string COMMENT 'k13',
  `k14` string COMMENT 'k14',
  `k15` string COMMENT 'k15')
PARTITIONED BY (
  `dt` string)
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
  '/user/doris/preinstalled_data/csv_partition_table/table_with_vertical_line/'
TBLPROPERTIES (
  'transient_lastDdlTime'='1669304897');

msck repair table table_with_vertical_line;

