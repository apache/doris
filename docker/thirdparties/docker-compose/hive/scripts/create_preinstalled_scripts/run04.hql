CREATE TABLE `delta_length_byte_array`(
  `FRUIT` string
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/delta_length_byte_array'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table delta_length_byte_array;

