CREATE TABLE `datapage_v1_snappy_compressed_checksum`(
  `a` int,
  `b` int
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/datapage_v1-snappy-compressed-checksum'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table datapage_v1_snappy_compressed_checksum;


