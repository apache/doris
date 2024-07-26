create table t_hive (
  `k1` int,
  `k2` char(10),
  `k3` date,
  `k5` varchar(20),
  `k6` double
)
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/data_case/t_hive'
TBLPROPERTIES ('transient_lastDdlTime'='1658816839');

