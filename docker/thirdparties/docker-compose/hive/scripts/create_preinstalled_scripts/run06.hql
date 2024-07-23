CREATE TABLE `delta_encoding_required_column`(
   c_customer_sk int,
    c_current_cdemo_sk int,
   c_current_hdemo_sk int,
   c_current_addr_sk int,
   c_first_shipto_date_sk int,
   c_first_sales_date_sk  int,
   c_birth_day  int,
   c_birth_month  int,
     c_birth_year int,
     c_customer_id string,
     c_salutation string,
     c_first_name string,
     c_last_name string,
     c_preferred_cust_flag string,
     c_birth_country string,
     c_email_address string,
     c_last_review_date  string
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/different_types_parquet/delta_encoding_required_column/'
TBLPROPERTIES (
  'transient_lastDdlTime'='1661955829');

msck repair table delta_encoding_required_column;


