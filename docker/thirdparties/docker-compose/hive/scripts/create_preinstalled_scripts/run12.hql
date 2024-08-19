CREATE TABLE `example_string`(
  `strings` string
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\t',
  'serialization.format'='\t')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/example_string.parquet'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table example_string;


