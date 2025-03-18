create database if not exists openx_json;
use openx_json;


CREATE TABLE IF NOT EXISTS json_table (
    id INT,
    name STRING,
    numbers ARRAY<INT>,
    scores MAP<STRING, INT>,
    details STRUCT<a:INT, b:STRING, c:BIGINT>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/user/doris/preinstalled_data/json/openx_json/json_table';


CREATE TABLE IF NOT EXISTS json_table_ignore_malformed (
    id INT,
    name STRING,
    numbers ARRAY<INT>,
    scores MAP<STRING, INT>,
    details STRUCT<a:INT, b:STRING, c:BIGINT>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ("ignore.malformed.json" = "true" )
LOCATION '/user/doris/preinstalled_data/json/openx_json/json_table';


CREATE TABLE json_data_arrays_tb (
    name string, age int)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/user/doris/preinstalled_data/json/openx_json/json_data_arrays_tb';


CREATE TABLE IF NOT EXISTS scalar_to_array_tb(
    id INT,
    name STRING,
    tags ARRAY<STRING>
)ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/user/doris/preinstalled_data/json/openx_json/scalar_to_array_tb';


msck repair table json_table;
msck repair table json_table_ignore_malformed;
msck repair table json_data_arrays_tb;
msck repair table scalar_to_array_tb;