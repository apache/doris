CREATE TABLE IF NOT EXISTS `csv_all_types`(
`t_empty_string` string,
`t_string` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION
  '/user/doris/preinstalled_data/csv/csv_all_types';

msck repair table csv_all_types;

