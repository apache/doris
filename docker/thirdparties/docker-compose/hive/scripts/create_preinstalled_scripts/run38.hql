CREATE TABLE IF NOT EXISTS `avro_all_types`(
`t_null_string` string,
`t_null_varchar` varchar(65535),
`t_null_char` char(10),
`t_null_array_int` array<int>,
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
`t_decimal_precision_18` decimal(18,8),
`t_decimal_precision_38` decimal(38,16),
`t_binary` binary,
`t_map_string` map<string,string>,
`t_array_empty` array<string>,
`t_array_string` array<string>,
`t_array_int` array<int>,
`t_array_bigint` array<bigint>,
`t_array_float` array<float>,
`t_array_double` array<double>,
`t_array_boolean` array<boolean>,
`t_array_varchar` array<varchar(65535)>,
`t_array_char` array<char(10)>,
`t_array_date` array<date>,
`t_array_timestamp` array<timestamp>,
`t_array_decimal_precision_2` array<decimal(2,1)>,
`t_array_decimal_precision_4` array<decimal(4,2)>,
`t_array_decimal_precision_8` array<decimal(8,4)>,
`t_array_decimal_precision_17` array<decimal(17,8)>,
`t_array_decimal_precision_18` array<decimal(18,8)>,
`t_array_decimal_precision_38` array<decimal(38,16)>,
`t_struct_bigint` struct<s_bigint:bigint>,
`t_complex` map<string,array<struct<s_int:int>>>,
`t_struct_nested` struct<struct_field:array<string>>,
`t_struct_null` struct<struct_field_null:string,struct_field_null2:string>,
`t_struct_non_nulls_after_nulls` struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>,
`t_array_string_starting_with_nulls` array<string>,
`t_array_string_with_nulls_in_between` array<string>,
`t_array_string_ending_with_nulls` array<string>,
`t_array_string_all_nulls` array<string>
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  '/user/doris/preinstalled_data/avro/avro_all_types';

msck repair table avro_all_types;


