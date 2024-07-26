CREATE TABLE IF NOT EXISTS `json_all_types`(
`t_null_string` string,
`t_null_varchar` varchar(65535),
`t_null_char` char(10),
`t_null_decimal_precision_2` decimal(2,1),
`t_null_decimal_precision_4` decimal(4,2),
`t_null_decimal_precision_8` decimal(8,4),
`t_null_decimal_precision_17` decimal(17,8),
`t_null_decimal_precision_18` decimal(18,8),
`t_null_decimal_precision_38` decimal(38,16),
`t_empty_string` string,
`t_string` string,
`t_empty_varchar` varchar(65535),
`t_varchar` varchar(65535),
`t_varchar_max_length` varchar(65535),
`t_char` char(10),
`t_tinyint` tinyint,
`t_smallint` smallint,
`t_int` int,
`t_bigint` bigint,
`t_float` float,
`t_double` double,
`t_boolean_true` boolean,
`t_boolean_false` boolean,
`t_date` date,
`t_timestamp` timestamp,
`t_decimal_precision_2` decimal(2,1),
`t_decimal_precision_4` decimal(4,2),
`t_decimal_precision_8` decimal(8,4),
`t_decimal_precision_17` decimal(17,8),
`t_decimal_precision_18` decimal(18,8)
)
ROW FORMAT SERDE
  'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION
  '/user/doris/preinstalled_data/json/json_all_types';

msck repair table json_all_types;


