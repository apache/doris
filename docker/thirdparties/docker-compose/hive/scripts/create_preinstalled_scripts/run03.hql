CREATE TABLE `delta_byte_array`(
  `c_salutation` string,
  `c_first_name` string,
  `c_last_name` string,
  `c_preferred_cust_flag` string,
  `c_birth_country` string,
  `c_login` string,
  `c_email_address` string,
  `c_last_review_date` string,
  `c_customer_id` string
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/delta_byte_array'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');


