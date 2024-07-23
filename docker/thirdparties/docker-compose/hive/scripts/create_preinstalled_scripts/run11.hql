CREATE TABLE `alltypes_tiny_pages_plain`(
  bool_col boolean,
  tinyint_col int,
  smallint_col  int,
  int_col int,
  bigint_col bigint,
  float_col float,
  double_col double,
  id int,
  date_string_col string,
  string_col string,
  timestamp_col timestamp,
  year int,
  month int
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/alltypes_tiny_pages_plain'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table alltypes_tiny_pages_plain;

