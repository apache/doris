CREATE TABLE `fixed_length_byte_array_decimal_table`(
  `decimal_col1` decimal(7,2),
  `decimal_col2` decimal(7,2),
  `decimal_col3` decimal(7,2),
  `decimal_col4` decimal(7,2),
  `decimal_col5` decimal(7,2))
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/parquet_table/fixed_length_byte_array_decimal_table'
TBLPROPERTIES (
  'parquet.compress'='SNAPPY');

msck repair table fixed_length_byte_array_decimal_table;

