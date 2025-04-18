create database if not exists multi_catalog;
use multi_catalog;

CREATE TABLE text_table_normal_skip_header (
  id INT,
  name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/doris/preinstalled_data/text/text_table_normal_skip_header'
TBLPROPERTIES ("skip.header.line.count"="2");

CREATE TABLE text_table_compressed_skip_header (
  id INT,
  name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/doris/preinstalled_data/text/text_table_compressed_skip_header'
TBLPROPERTIES ("skip.header.line.count"="5");

CREATE TABLE csv_json_table(
  trace_id string, 
  loan_account_id string, 
  name string, 
  mobile_number string, 
  identity_number string, 
  status_desc string, 
  status string, 
  status_json string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/user/doris/preinstalled_data/csv/csv_json_table';
