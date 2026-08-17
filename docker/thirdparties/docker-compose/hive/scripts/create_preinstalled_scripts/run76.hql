create database if not exists multi_catalog;
use multi_catalog;

drop table if exists text_table_normal_skip_header;

create table text_table_normal_skip_header (
  id INT,
  name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/doris/preinstalled_data/text/text_table_normal_skip_header'
TBLPROPERTIES ("skip.header.line.count"="2");

drop table if exists text_table_compressed_skip_header;

create table text_table_compressed_skip_header (
  id INT,
  name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/doris/preinstalled_data/text/text_table_compressed_skip_header'
TBLPROPERTIES ("skip.header.line.count"="5");

drop table if exists csv_json_table_simple;

create table csv_json_table_simple (
  id STRING,
  status_json STRING
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/user/doris/preinstalled_data/csv/csv_json_table_simple';

drop table if exists open_csv_table_null_format;

create table open_csv_table_null_format (
  id INT,
  name STRING
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/user/doris/preinstalled_data/csv/open_csv_table_null_format';

drop table if exists open_csv_complex_type;

create table open_csv_complex_type (
  id INT,
  arr_col ARRAY<INT>,
  map_col MAP<STRING, INT>,
  struct_col STRUCT<name:STRING, age:INT>
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\"",
  "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/user/doris/preinstalled_data/csv/open_csv_complex_type';

create database if not exists openx_json;
use openx_json;

drop table if exists json_table;

create table json_table (
    id INT,
    name STRING,
    numbers ARRAY<INT>,
    scores MAP<STRING, INT>,
    details STRUCT<a:INT, b:STRING, c:BIGINT>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/user/doris/preinstalled_data/json/openx_json/json_table';


drop table if exists json_table_ignore_malformed;


create table json_table_ignore_malformed (
    id INT,
    name STRING,
    numbers ARRAY<INT>,
    scores MAP<STRING, INT>,
    details STRUCT<a:INT, b:STRING, c:BIGINT>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ("ignore.malformed.json" = "true" )
LOCATION '/user/doris/preinstalled_data/json/openx_json/json_table';


drop table if exists json_data_arrays_tb;


create table json_data_arrays_tb (
    name string, age int)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/user/doris/preinstalled_data/json/openx_json/json_data_arrays_tb';


drop table if exists scalar_to_array_tb;


create table scalar_to_array_tb(
    id INT,
    name STRING,
    tags ARRAY<STRING>
)ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/user/doris/preinstalled_data/json/openx_json/scalar_to_array_tb';


drop table if exists json_one_column_table;


create table json_one_column_table (
    name STRING,    
    id INT,
    numbers ARRAY<INT>,
    scores MAP<STRING, INT>,
    details STRUCT<a:INT, b:STRING, c:BIGINT>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/user/doris/preinstalled_data/json/openx_json/json_one_column_table';
