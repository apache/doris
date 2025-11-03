CREATE TABLE `orc_decimal_table`(
    id INT,
    decimal_col1 DECIMAL(8, 4),
    decimal_col2 DECIMAL(18, 6),
    decimal_col3 DECIMAL(38, 12),
    decimal_col4 DECIMAL(9, 0),
    decimal_col5 DECIMAL(27, 9),
    decimal_col6 DECIMAL(9, 0))
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/orc_table/orc_decimal_table';

msck repair table orc_decimal_table;

