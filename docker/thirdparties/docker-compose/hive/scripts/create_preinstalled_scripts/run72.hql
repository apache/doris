CREATE TABLE invalid_utf8_data (
    id INT,
    corrupted_data STRING,
    string_data1 STRING,
    string_data2 STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
location '/user/doris/preinstalled_data/text/utf8_check';


CREATE TABLE invalid_utf8_data2 (
    id INT,
    corrupted_data STRING,
    string_data1 STRING,
    string_data2 STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
location '/user/doris/preinstalled_data/text/utf8_check';



msck repair table invalid_utf8_data;
msck repair table invalid_utf8_data2;

