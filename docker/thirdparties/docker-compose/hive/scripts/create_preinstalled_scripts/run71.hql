use `default`;


CREATE TABLE json_load_data_table (
    `id` int,
    `col1` int,
    `col2` struct< col2a:int, col2b:string>,
    `col3` map<int,string>
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION
  '/user/doris/preinstalled_data/json/json_load_data_table';

msck repair table json_load_data_table;
