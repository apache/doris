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