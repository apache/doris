CREATE TABLE `overflow_i16_page_cnt`(
  `inc` boolean
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/overflow_i16_page_cnt'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table overflow_i16_page_cnt;


