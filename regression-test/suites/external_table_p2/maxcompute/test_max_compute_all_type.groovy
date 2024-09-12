// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/*

drop table mc_all_types;

CREATE TABLE `mc_all_types`(
  `id` int,
  `boolean_col` boolean, 
  `tinyint_col` tinyint, 
  `smallint_col` smallint, 
  `int_col` int, 
  `bigint_col` bigint, 
  `float_col` float, 
  `double_col` double, 
  `decimal_col1` decimal(9,0), 
  `decimal_col2` decimal(8,4), 
  `decimal_col3` decimal(18,6), 
  `decimal_col4` decimal(38,12), 
  `string_col` string, 
  `date_col` date, 
  `datetime_col` datetime, 
  `timestamp_ntz_col2` timestamp_ntz, 
  `timestamp_ntz_col3` timestamp_ntz, 
  `char_col1` char(50), 
  `char_col2` char(100), 
  `char_col3` char(255), 
  `varchar_col1` varchar(50), 
  `varchar_col2` varchar(100), 
  `varchar_col3` varchar(255), 
  `t_map_string` map<string,string>, 
  `t_map_varchar` map<varchar(65535),varchar(65535)>, 
  `t_map_char` map<char(10),char(10)>, 
  `t_map_int` map<int,int>, 
  `t_map_bigint` map<bigint,bigint>, 
  `t_map_float` map<float,float>, 
  `t_map_double` map<double,double>, 
  `t_map_boolean` map<boolean,boolean>, 
  `t_array_string` array<string>, 
  `t_array_int` array<int>, 
  `t_array_bigint` array<bigint>, 
  `t_array_float` array<float>, 
  `t_array_double` array<double>, 
  `t_array_boolean` array<boolean>, 
  `t_array_varchar` array<varchar(65535)>, 
  `t_array_char` array<char(10)>, 
  `t_array_decimal_precision_2` array<decimal(2,1)>, 
  `t_array_decimal_precision_4` array<decimal(4,2)>, 
  `t_array_decimal_precision_8` array<decimal(8,4)>, 
  `t_array_decimal_precision_17` array<decimal(17,8)>, 
  `t_array_decimal_precision_18` array<decimal(18,8)>, 
  `t_array_decimal_precision_38` array<decimal(38,16)>, 
  `t_struct_bigint` struct<s_bigint:bigint>, 
  `t_complex` map<string,array<struct<s_int:int>>>, 
  `t_struct_nested` struct<struct_field:array<string>>, 
  `t_struct_non_nulls_after_nulls` struct<struct_non_nulls_after_nulls1:int,struct_non_nulls_after_nulls2:string>, 
  `t_nested_struct_non_nulls_after_nulls` struct<struct_field1:int,struct_field2:string,strict_field3:struct<nested_struct_field1:int,nested_struct_field2:string>>, 
  `t_array_string_starting_with_nulls` array<string>, 
  `t_array_string_with_nulls_in_between` array<string>, 
  `t_array_string_ending_with_nulls` array<string>
);



insert into mc_all_types
VALUES (
  1,
    cast(0 as boolean), -- boolean_col
    cast(-7 as tinyint), -- tinyint_col
    cast(-15 as smallint), -- smallint_col
    16, -- int_col
    cast(-9223372036854775807 as bigint), -- bigint_col
    CAST(-123.45 AS FLOAT), -- float_col
    CAST(-123456.789 AS DOUBLE), -- double_col
    CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
    CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
    CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
    CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
    'str', -- string_col
    cast('2024-03-25' as date), -- date_col
    cast('2024-03-25 12:00:00' as datetime),
    cast('2024-03-25 12:00:00.123456' as timestamp_ntz), -- timestamp_ntz_col2
    cast('2024-03-25 12:00:00.123456789' as timestamp_ntz), -- timestamp_ntz_col3
    'char_value11111', -- char_col1
    'char_value22222', -- char_col2
    'char_value33333', -- char_col3
    'varchar_value11111', -- varchar_col1
    'varchar_value22222', -- varchar_col2
    'varchar_value33333', -- varchar_col3
    MAP('key7', 'value1'), -- t_map_string
    MAP('key7', 'value1'), -- t_map_varchar
    MAP('x', 'y'), -- t_map_char
    MAP(3, 20), -- t_map_int
    MAP(3, 200000000000), -- t_map_bigint
    MAP(CAST(3.2 AS FLOAT), CAST(20.2 AS FLOAT)), -- t_map_float
    MAP(CAST(3.2 AS DOUBLE), CAST(20.2 AS DOUBLE)), -- t_map_double
    MAP(false, true), -- t_map_boolean
    ARRAY('string1', 'string2'), -- t_array_string
    ARRAY(4, 5, 6), -- t_array_int
    ARRAY(300000000000, 400000000000), -- t_array_bigint
    ARRAY(CAST(3.3 AS FLOAT), CAST(4.4 AS FLOAT)), -- t_array_float
    ARRAY(CAST(3.123456789 AS DOUBLE), CAST(4.123456789 AS DOUBLE)), -- t_array_double
    ARRAY(false, true), -- t_array_boolean
    ARRAY('varchar1', 'varchar2'), -- t_array_varchar
    ARRAY('char1', 'char2'), -- t_array_char
    ARRAY(CAST(3.3 AS DECIMAL(2,1)), CAST(4.4 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
    ARRAY(CAST(3.45 AS DECIMAL(4,2)), CAST(4.56 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
    ARRAY(CAST(9.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
    ARRAY(CAST(6.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
    ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
    ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
    NAMED_STRUCT('s_bigint', -1234567890), -- t_struct_bigint
    MAP('key', ARRAY(NAMED_STRUCT('s_int', -123))), -- t_complex
    NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
    NAMED_STRUCT('struct_non_nulls_after_nulls1', -123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
    NAMED_STRUCT('struct_field1', -123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', -123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
    ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
    ARRAY('value1', null, 'value2'), -- t_array_string_with_nulls_in_between
    ARRAY('value11', 'value2', null) -- t_array_string_ending_with_nulls
);



INSERT into  mc_all_types
VALUES (
  2,
  CAST (1 as boolean), -- boolean_col
  CAST (127 as tinyint), -- tinyint_col
  cast(32767 as smallint), -- smallint_col
  2147483647, -- int_col
  cast(9223372036854775807 as bigint), -- bigint_col
  cast(123.45 as float), -- float_col
  123456.789, -- double_col
  CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
  CAST(1234.5678 AS DECIMAL(8,4)), -- decimal_col2
  CAST(123456.789012 AS DECIMAL(18,6)), -- decimal_col3
  CAST(123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
  'string_value', -- string_col
  cast('2024-03-20' as date), -- date_col
  cast('2024-03-20 12:00:00' as datetime ),
  cast('2024-03-20 12:00:00.123456' as TIMESTAMP_ntz ), -- timestamp_ntz_col2
  cast('2024-03-20 12:00:00.123456789' as TIMESTAMP_ntz ), -- timestamp_ntz_col3
  'char_value1', -- char_col1
  'char_value2', -- char_col2
  'char_value3', -- char_col3
  'varchar_value1', -- varchar_col1
  'varchar_value2', -- varchar_col2
  'varchar_value3', -- varchar_col3
  MAP('key1', 'value1'), -- t_map_string
  MAP('key1', 'value1'), -- t_map_varchar
  MAP('a', 'b'), -- t_map_char
  MAP(1, 10), -- t_map_int
  MAP(1, 100000000000), -- t_map_bigint
  MAP(CAST(1.1 AS FLOAT), CAST(10.1 AS FLOAT)), -- t_map_float
  MAP(CAST(1.1 AS DOUBLE), CAST(10.1 AS DOUBLE)), -- t_map_double
  MAP(TRUE, FALSE), -- t_map_boolean
  ARRAY('string1', 'string2'), -- t_array_string
  ARRAY(1, 2, 3), -- t_array_int
  ARRAY(100000000000, 200000000000), -- t_array_bigint
  ARRAY(CAST(1.1 AS FLOAT), CAST(2.2 AS FLOAT)), -- t_array_float
  ARRAY(CAST(1.123456789 AS DOUBLE), CAST(2.123456789 AS DOUBLE)), -- t_array_double
  ARRAY(TRUE, FALSE), -- t_array_boolean
  ARRAY('varchar1', 'varchar2'), -- t_array_varchar
  ARRAY('char1', 'char2'), -- t_array_char
  ARRAY(CAST(1.1 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
  ARRAY(CAST(1.23 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
  ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
  ARRAY(CAST(1.23456789 AS DECIMAL(17,8)), CAST(2.34567891 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
  ARRAY(1.23456789, 2.34567891), -- t_array_decimal_precision_18
  ARRAY(1.234567890123456789, 2.345678901234567890), -- t_array_decimal_precision_38
  NAMED_STRUCT('s_bigint', 1234567890), -- t_struct_bigint
  MAP('key', ARRAY(NAMED_STRUCT('s_int', 123))), -- t_complex
  NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
  NAMED_STRUCT('struct_non_nulls_after_nulls1', 123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
  NAMED_STRUCT('struct_field1', 123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', 123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
  ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
  ARRAY('value1', null, 'value2'), -- t_array_string_with_nulls_in_between
  ARRAY('value1', 'value2', null) -- t_array_string_ending_with_nulls
);

INSERT into  mc_all_types
VALUES 
(
  3,
  cast(0 as boolean), -- boolean_col
  cast(-7 as tinyint), -- tinyint_col
  cast(-15 as smallint), -- smallint_col
  16, -- int_col
  cast(-9223372036854775807 as bigint), -- bigint_col
  CAST(-123.45 AS FLOAT), -- float_col
  CAST(-123456.789 AS DOUBLE), -- double_col
  CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
  CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
  CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
  CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
  'str', -- string_col
  cast('2024-03-25' as date), -- date_col
  cast('2024-03-25 12:00:00' as datetime),
  cast('2024-03-25 12:00:00.123456' as timestamp_ntz), -- timestamp_ntz_col2
  cast('2024-03-25 12:00:00.123456789' as timestamp_ntz), -- timestamp_ntz_col3
  'char_value11111', -- char_col1
  'char_value22222', -- char_col2
  'char_value33333', -- char_col3
  'varchar_value11111', -- varchar_col1
  'varchar_value22222', -- varchar_col2
  'varchar_value33333', -- varchar_col3
  MAP('key7', 'value1'), -- t_map_string
  MAP('key7', 'value1'), -- t_map_varchar
  MAP('x', 'y'), -- t_map_char
  MAP(3, 20), -- t_map_int
  MAP(3, 200000000000), -- t_map_bigint
  MAP(CAST(3.2 AS FLOAT), CAST(20.2 AS FLOAT)), -- t_map_float
  MAP(CAST(3.2 AS DOUBLE), CAST(20.2 AS DOUBLE)), -- t_map_double
  MAP(false, true), -- t_map_boolean
  ARRAY('string1', 'string2'), -- t_array_string
  ARRAY(4, 5, 6), -- t_array_int
  ARRAY(300000000000, 400000000000), -- t_array_bigint
  ARRAY(CAST(3.3 AS FLOAT), CAST(4.4 AS FLOAT)), -- t_array_float
  ARRAY(CAST(3.123456789 AS DOUBLE), CAST(4.123456789 AS DOUBLE)), -- t_array_double
  ARRAY(false, true), -- t_array_boolean
  ARRAY('varchar1', 'varchar2'), -- t_array_varchar
  ARRAY('char1', 'char2'), -- t_array_char
  ARRAY(CAST(3.3 AS DECIMAL(2,1)), CAST(4.4 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
  ARRAY(CAST(3.45 AS DECIMAL(4,2)), CAST(4.56 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
  ARRAY(CAST(9.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
  ARRAY(CAST(6.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
  ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
  ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
  NAMED_STRUCT('s_bigint', -1234567890), -- t_struct_bigint
  MAP('key', ARRAY(NAMED_STRUCT('s_int', -123))), -- t_complex
  NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
  NAMED_STRUCT('struct_non_nulls_after_nulls1', -123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
  NAMED_STRUCT('struct_field1', -123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', -123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
  ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
  ARRAY('value1', null, 'value2'), -- t_array_string_with_nulls_in_between
  ARRAY('value11', 'value2', null) -- t_array_string_ending_with_nulls
);
INSERT into  mc_all_types
VALUES 
(
  4,
  cast(0 as boolean), -- boolean_col
  cast(-128 as tinyint), -- tinyint_col
  cast(-32768 as smallint), -- smallint_col
  -2147483647, -- int_col
  cast(-9223372036854775807 as bigint), -- bigint_col
  CAST(-123.45 AS FLOAT), -- float_col
  CAST(-123456.789 AS DOUBLE), -- double_col
  CAST(-123456789 AS DECIMAL(9,0)), -- decimal_col1
  CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
  CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
  CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
  'string_value', -- string_col
  cast('2024-03-21' as date), -- date_col
  cast('2024-03-21 12:00:00' as datetime),
  cast('2024-03-21 12:00:00.123456' as timestamp_ntz), -- timestamp_ntz_col2
  cast('2024-03-21 12:00:00.123456789' as timestamp_ntz), -- timestamp_ntz_col3
  'char_value1', -- char_col1
  'char_value2', -- char_col2
  'char_value3', -- char_col3
  'varchar_value1', -- varchar_col1
  'varchar_value2', -- varchar_col2
  'varchar_value3', -- varchar_col3
  MAP('key1', 'value1'), -- t_map_string
  MAP('key1', 'value1'), -- t_map_varchar
  MAP('x', 'y'), -- t_map_char
  MAP(2, 20), -- t_map_int
  MAP(2, 200000000000), -- t_map_bigint
  MAP(CAST(2.2 AS FLOAT), CAST(20.2 AS FLOAT)), -- t_map_float
  MAP(CAST(2.2 AS DOUBLE), CAST(20.2 AS DOUBLE)), -- t_map_double
  MAP(false, true), -- t_map_boolean
  ARRAY('string1', 'string2'), -- t_array_string
  ARRAY(4, 5, 6), -- t_array_int
  ARRAY(300000000000, 400000000000), -- t_array_bigint
  ARRAY(CAST(3.3 AS FLOAT), CAST(4.4 AS FLOAT)), -- t_array_float
  ARRAY(CAST(3.123456789 AS DOUBLE), CAST(4.123456789 AS DOUBLE)), -- t_array_double
  ARRAY(false, true), -- t_array_boolean
  ARRAY('varchar1', 'varchar2'), -- t_array_varchar
  ARRAY('char1', 'char2'), -- t_array_char
  ARRAY(CAST(3.3 AS DECIMAL(2,1)), CAST(4.4 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
  ARRAY(CAST(3.45 AS DECIMAL(4,2)), CAST(4.56 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
  ARRAY(CAST(3.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
  ARRAY(CAST(3.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
  ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
  ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
  NAMED_STRUCT('s_bigint', -1234567890), -- t_struct_bigint
  MAP('key', ARRAY(NAMED_STRUCT('s_int', -123))), -- t_complex
  NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
  NAMED_STRUCT('struct_non_nulls_after_nulls1', -123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
  NAMED_STRUCT('struct_field1', -123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', -123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
  ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
  ARRAY('value1', null, 'value2'), -- t_array_string_with_nulls_in_between
  ARRAY('value1', 'value2', null) -- t_array_string_ending_with_nulls
);

ALTER TABLE mc_all_types MERGE SMALLFILES;

select * from mc_all_types;
 */
suite("test_max_compute_all_type", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ak = context.config.otherConfigs.get("aliYunAk")
        String sk = context.config.otherConfigs.get("aliYunSk")
        String mc_catalog_name = "test_max_compute_all_type"
        sql """drop catalog if exists ${mc_catalog_name} """
        
        String defaultProject = "jz_datalake" 
        sql """
        CREATE CATALOG IF NOT EXISTS ${mc_catalog_name} PROPERTIES (
                "type" = "max_compute",
                "mc.default.project" = "${defaultProject}",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
                "mc.quota" = "pay-as-you-go"
        );
        """

        logger.info("catalog " + mc_catalog_name + " created")
        sql """switch ${mc_catalog_name};"""
        logger.info("switched to catalog " + mc_catalog_name)
        sql """ show databases; """
        sql """ use ${defaultProject} """

        String table_name = "mc_all_types"
        
        
        qt_desc """ desc ${table_name} """ 
        qt_test_1 """ select * from ${table_name} order by id; """ 
        qt_test_2 """ select * from ${table_name} where id = 1 ; """ 
        
        qt_test_3 """ select id,boolean_col from ${table_name}  order by id """
        qt_test_4 """ select id,boolean_col from ${table_name} where boolean_col is false order by  id; """ 
        
        qt_test_5 """ select id,tinyint_col from ${table_name}  order by id """
        qt_test_5 """ select id,tinyint_col from ${table_name} where  tinyint_col in (-127,-128) order by id """ 
        
        qt_test_7 """ select id,smallint_col from ${table_name}  order by id """
        qt_test_8 """ select id,smallint_col from ${table_name} where smallint_col > 16 order by id """ 
        
        qt_test_9 """ select id,int_col from ${table_name}  order by id """
        qt_test_10 """ select id,int_col from ${table_name} where int_col = 16 order by id """ 
        qt_test_11 """ select id,int_col from ${table_name} where int_col != 16 order by id """ 
        
        qt_test_12 """ select id,bigint_col from ${table_name}   order by id """
        qt_test_13 """ select id,bigint_col from ${table_name} where bigint_col > -9223372036854775807 order by id """ 
        qt_test_14 """ select id,bigint_col from ${table_name} where bigint_col >= -9223372036854775807 order by id """ 
        

        qt_test_15 """ select id,float_col from ${table_name}   order by id """
        qt_test_16 """ select id,float_col from ${table_name} where round(float_col,2) > -123.45 order by id """
        qt_test_17 """ select id,float_col from ${table_name} where round(float_col,2) < -123.45 order by id """
        qt_test_18 """ select id,float_col from ${table_name} where round(float_col,2) = -123.45 order by id """
        qt_test_19 """ select id,float_col from ${table_name} where round(float_col,2) >= -123.45 order by id """
        qt_test_20 """ select id,float_col from ${table_name} where round(float_col,2) <= -123.45 order by id """ 
        qt_test_21 """ select id,float_col from ${table_name} where round(float_col,2) in (-123.45,0.123) order by id """
        qt_test_22 """ select id,float_col from ${table_name} where round(float_col,2) not in (-123.45, 0.1) order by id """
        

        qt_test_23 """ select id,double_col from ${table_name}   order by id """
        qt_test_24 """ select id,double_col from ${table_name} where double_col in ( -123456.789 ,0.123) order by id """
        qt_test_25 """ select id,double_col from ${table_name} where double_col !=  -123456.789  order by id """
        qt_test_26 """ select id,double_col from ${table_name} where double_col =  -123456.789 order by id """
        qt_test_27 """ select id,double_col from ${table_name} where -123456.789  = double_col order by id """
        
        qt_test_28 """ select id,decimal_col1 from ${table_name}   order by id """
        qt_test_29 """ select id,decimal_col1 from ${table_name} where decimal_col1 is null order by id """
        qt_test_30 """ select id,decimal_col1 from ${table_name} where decimal_col1 is not null order by id """

        qt_test_31 """ select id,decimal_col2 from ${table_name}   order by id """
        qt_test_32 """ select id,decimal_col2 from ${table_name} where decimal_col2  = -1234.5678 order by id """
        qt_test_33 """ select id,decimal_col2 from ${table_name} where decimal_col2 != -1234.5678 order by id"""

        qt_test_34 """ select id,decimal_col3 from ${table_name}   order by id """
        qt_test_35 """ select id,decimal_col3 from ${table_name} where decimal_col3 = -123456.789012 order by id """
        qt_test_36 """ select id,decimal_col3 from ${table_name} where decimal_col3 != -123456.789012 order by id """

        qt_test_37 """ select id,decimal_col4 from ${table_name}   order by id """
        qt_test_38 """ select id,decimal_col4 from ${table_name} where decimal_col4 = -123456789.0123457 order by id """
        qt_test_39 """ select id,decimal_col4 from ${table_name} where decimal_col4 != -123456789.0123457  order by id """
        qt_test_40 """ select id,decimal_col4 from ${table_name} where decimal_col4 >= -123456789.0123457  order by id """
        qt_test_41 """ select id,decimal_col4 from ${table_name} where decimal_col4 > -123456789.0123457  order by id """


        qt_test_42 """ select id,string_col from ${table_name}   order by id """
        qt_test_43 """ select id,string_col from ${table_name} where string_col in ("str")  order by id """
        qt_test_44 """ select id,string_col from ${table_name} where string_col = "str"  order by id """
        qt_test_45 """ select id,string_col from ${table_name} where string_col != "str"  order by id """
        qt_test_46 """ select id,string_col from ${table_name} where string_col in ("str","string_value")  order by id """
        qt_test_47 """ select id,string_col from ${table_name} where string_col not in ("str","string_value")  order by id """
        
        qt_test_48 """ select id,date_col from ${table_name}   order by id """
        qt_test_49 """ select id,date_col from ${table_name} where  date_col  = "2024-03-25" order by id """
        qt_test_50 """ select id,date_col from ${table_name} where  date_col  >= "2024-03-25" order by id """
        qt_test_51 """ select id,date_col from ${table_name} where  date_col  != "2024-03-20" order by id """



        qt_test_52 """ select id,datetime_col from ${table_name}   order by id """
        qt_test_53 """ select id,datetime_col from ${table_name} where  datetime_col  != "2024-03-25 12:00:00" order by id """
        qt_test_54 """ select id,datetime_col from ${table_name} where  datetime_col  = "2024-03-25 12:00:00" order by id """
        qt_test_55 """ select id,datetime_col from ${table_name} where  datetime_col  > "2024-03-25 12:00:00" order by id """
        qt_test_56 """ select id,datetime_col from ${table_name} where  datetime_col  < "2024-03-25 12:00:00" order by id """
        qt_test_57 """ select id,datetime_col from ${table_name} where  datetime_col  <= "2024-03-25 12:00:00" order by id """



        qt_test_58 """ select id,timestamp_ntz_col2 from ${table_name}   order by id """
        qt_test_59 """ select id,timestamp_ntz_col2 from ${table_name} where  timestamp_ntz_col2  != "2024-03-25 12:00:00.123456" order by id """
        qt_test_60 """ select id,timestamp_ntz_col2 from ${table_name} where  timestamp_ntz_col2  = "2024-03-25 12:00:00.123456" order by id """
        qt_test_61 """ select id,timestamp_ntz_col2 from ${table_name} where  timestamp_ntz_col2  > "2024-03-25 12:00:00.123456" order by id """
        qt_test_62 """ select id,timestamp_ntz_col2 from ${table_name} where  timestamp_ntz_col2  < "2024-03-25 12:00:00.123456" order by id """
        qt_test_63 """ select id,timestamp_ntz_col2 from ${table_name} where  timestamp_ntz_col2  <= "2024-03-25 12:00:00.123456" order by id """

        qt_test_64 """ select id,timestamp_ntz_col3 from ${table_name}   order by id """
        qt_test_65 """ select id,timestamp_ntz_col3 from ${table_name} where  timestamp_ntz_col3  != "2024-03-25 12:00:00.123456" order by id """
        qt_test_66 """ select id,timestamp_ntz_col3 from ${table_name} where  timestamp_ntz_col3  < "2024-03-25 12:00:00.123456" order by id """
        qt_test_67 """ select id,timestamp_ntz_col3 from ${table_name} where  timestamp_ntz_col3  = "2024-03-25 12:00:00.123456" order by id """
        qt_test_68 """ select id,timestamp_ntz_col3 from ${table_name} where  timestamp_ntz_col3  > "2024-03-25 12:00:00.123456" order by id """
        qt_test_69 """ select id,timestamp_ntz_col3 from ${table_name} where  timestamp_ntz_col3  <= "2024-03-25 12:00:00.123456" order by id """

        qt_test_70 """ select id,timestamp_ntz_col3 from ${table_name} where  timestamp_ntz_col3  != "2024-03-25 12:00:00.124" order by id """
        qt_test_71 """ select id,timestamp_ntz_col3 from ${table_name} where  timestamp_ntz_col3  < "2024-03-25 12:00:00.124" order by id """
        qt_test_72 """ select id,timestamp_ntz_col3 from ${table_name} where  timestamp_ntz_col3  = "2024-03-25 12:00:00.124" order by id """
        qt_test_73 """ select id,timestamp_ntz_col3 from ${table_name} where  timestamp_ntz_col3  > "2024-03-25 12:00:00.124" order by id """
        qt_test_74 """ select id,timestamp_ntz_col3 from ${table_name} where  timestamp_ntz_col3  <= "2024-03-25 12:00:00.124" order by id """


  


        qt_test_75 """ select id,char_col1 from ${table_name} order by id """
        qt_test_76 """ select id,char_col1 from ${table_name} where  char_col1  = "char_value11111" order by id """
        qt_test_77 """ select id,char_col1 from ${table_name} where  char_col1  != "char_value11111" order by id """
        qt_test_78 """ select id,char_col1 from ${table_name} where  char_col1  != "char_value1" order by id """
        qt_test_79 """ select id,char_col1 from ${table_name} where  char_col1  in ("char_value1") order by id """
        qt_test_80 """ select id,char_col1 from ${table_name} where  char_col1  not in ("char_value1") order by id """
        qt_test_81 """ select id,char_col1 from ${table_name} where  char_col1  in ("char_value1","char_value11111") order by id """
        qt_test_82 """ select id,char_col1 from ${table_name} where  char_col1  not in ("char_value1","char_value11111") order by id """


        qt_test_83 """ select id,char_col2 from ${table_name} order by id """
        qt_test_84 """ select id,char_col2 from ${table_name} where  char_col2  = "char_value22222" order by id """
        qt_test_85 """ select id,char_col2 from ${table_name} where  char_col2  != "char_value22222" order by id """
        qt_test_86 """ select id,char_col2 from ${table_name} where  char_col2  != "char_value2" order by id """
        qt_test_87 """ select id,char_col2 from ${table_name} where  char_col2  in ("char_value2") order by id """
        qt_test_88 """ select id,char_col2 from ${table_name} where  char_col2  not in ("char_value2") order by id """
        qt_test_89 """ select id,char_col2 from ${table_name} where  char_col2  in ("char_value2","char_value22222") order by id """
        qt_test_90 """ select id,char_col2 from ${table_name} where  char_col2  not in ("char_value2","char_value22222") order by id """

        qt_test_91 """ select id,char_col3 from ${table_name} order by id """
        qt_test_92 """ select id,char_col3 from ${table_name} where  char_col3  =  "char_value33333" order by id """
        qt_test_93 """ select id,char_col3 from ${table_name} where  char_col3  != "char_value33333" order by id """
        qt_test_94 """ select id,char_col3 from ${table_name} where  char_col3  != "char_value3" order by id """
        qt_test_95 """ select id,char_col3 from ${table_name} where  char_col3  in ("char_value3") order by id """
        qt_test_96 """ select id,char_col3 from ${table_name} where  char_col3  not in ("char_value3") order by id """
        qt_test_97 """ select id,char_col3 from ${table_name} where  char_col3  in ("char_value3","char_value33333") order by id """
        qt_test_98 """ select id,char_col3 from ${table_name} where  char_col3  not in ("char_value3","char_value33333") order by id """


        qt_test_99 """ select id,varchar_col1 from ${table_name} order by id """
        qt_test_100 """ select id,varchar_col1 from ${table_name} where  varchar_col1  = "varchar_value11111" order by id """
        qt_test_101 """ select id,varchar_col1 from ${table_name} where  varchar_col1  != "varchar_value11111" order by id """
        qt_test_102 """ select id,varchar_col1 from ${table_name} where  varchar_col1  != "varchar_value1" order by id """
        qt_test_103 """ select id,varchar_col1 from ${table_name} where  varchar_col1  in ("varchar_value1") order by id """
        qt_test_104 """ select id,varchar_col1 from ${table_name} where  varchar_col1  not in ("varchar_value1") order by id """
        qt_test_105 """ select id,varchar_col1 from ${table_name} where  varchar_col1  in ("varchar_value1","varchar_value11111") order by id """
        qt_test_106 """ select id,varchar_col1 from ${table_name} where  varchar_col1  not in ("varchar_value1","varchar_value11111") order by id """


        qt_test_107 """ select id,varchar_col2 from ${table_name} order by id """
        qt_test_108 """ select id,varchar_col2 from ${table_name} where  varchar_col2  = "varchar_value22222" order by id """
        qt_test_109 """ select id,varchar_col2 from ${table_name} where  varchar_col2  != "varchar_value22222" order by id """
        qt_test_110 """ select id,varchar_col2 from ${table_name} where  varchar_col2  != "varchar_value2" order by id """
        qt_test_111 """ select id,varchar_col2 from ${table_name} where  varchar_col2  in ("varchar_value2") order by id """
        qt_test_112 """ select id,varchar_col2 from ${table_name} where  varchar_col2  not in ("varchar_value2") order by id """
        qt_test_113 """ select id,varchar_col2 from ${table_name} where  varchar_col2  in ("varchar_value2","varchar_value22222") order by id """
        qt_test_114 """ select id,varchar_col2 from ${table_name} where  varchar_col2  not in ("varchar_value2","varchar_value22222") order by id """

        qt_test_115 """ select id,varchar_col3 from ${table_name} order by id """
        qt_test_116 """ select id,varchar_col3 from ${table_name} where  varchar_col3  =  "varchar_value33333" order by id """
        qt_test_117 """ select id,varchar_col3 from ${table_name} where  varchar_col3  != "varchar_value33333" order by id """
        qt_test_118 """ select id,varchar_col3 from ${table_name} where  varchar_col3  != "varchar_value3" order by id """
        qt_test_119 """ select id,varchar_col3 from ${table_name} where  varchar_col3  in ("varchar_value3") order by id """
        qt_test_120 """ select id,varchar_col3 from ${table_name} where  varchar_col3  not in ("varchar_value3") order by id """
        qt_test_121 """ select id,varchar_col3 from ${table_name} where  varchar_col3  in ("varchar_value3","varchar_value33333") order by id """
        qt_test_122 """ select id,varchar_col3 from ${table_name} where  varchar_col3  not in ("varchar_value3","varchar_value33333") order by id """




        qt_test_123 """ select id,t_map_string from ${table_name} order by id """
        qt_test_124 """ select id,t_map_varchar from ${table_name} order by id """
        qt_test_125 """ select id,t_map_char from ${table_name} order by id """
        qt_test_126 """ select id,t_map_int from ${table_name} order by id """
        qt_test_127 """ select id,t_map_bigint from ${table_name} order by id """
        qt_test_128 """ select id,t_map_float from ${table_name} order by id """
        qt_test_129 """ select id,t_map_double from ${table_name} order by id """
        qt_test_130 """ select id,t_map_boolean from ${table_name} order by id """
        qt_test_131 """ select id,t_array_string from ${table_name} order by id """
        qt_test_132 """ select id,t_array_int from ${table_name} order by id """
        qt_test_133 """ select id,t_array_bigint from ${table_name} order by id """
        qt_test_134 """ select id,t_array_float from ${table_name} order by id """
        qt_test_135 """ select id,t_array_double from ${table_name} order by id """
        qt_test_136 """ select id,t_array_boolean from ${table_name} order by id """
        qt_test_137 """ select id,t_array_varchar from ${table_name} order by id """
        qt_test_138 """ select id,t_array_char from ${table_name} order by id """
        qt_test_139 """ select id,t_array_decimal_precision_2 from ${table_name} order by id """
        qt_test_140 """ select id,t_array_decimal_precision_4 from ${table_name} order by id """
        qt_test_141 """ select id,t_array_decimal_precision_8 from ${table_name} order by id """
        qt_test_142 """ select id,t_array_decimal_precision_17 from ${table_name} order by id """
        qt_test_143 """ select id,t_array_decimal_precision_18 from ${table_name} order by id """
        qt_test_144 """ select id,t_array_decimal_precision_4 from ${table_name} order by id """
        qt_test_145 """ select id,t_array_decimal_precision_38 from ${table_name} order by id """
        qt_test_146 """ select id,t_struct_bigint from ${table_name} order by id """
        qt_test_147 """ select id,t_complex from ${table_name} order by id """
        qt_test_148 """ select id,t_struct_nested from ${table_name} order by id """
        qt_test_149 """ select id,t_struct_non_nulls_after_nulls from ${table_name} order by id """
        qt_test_150 """ select id,t_nested_struct_non_nulls_after_nulls from ${table_name} order by id """
        qt_test_151 """ select id,t_array_string_starting_with_nulls from ${table_name} order by id """
        qt_test_152 """ select id,t_array_string_with_nulls_in_between from ${table_name} order by id """
        qt_test_153 """ select id,t_array_string_ending_with_nulls from ${table_name} order by id """
        qt_test_154 """ select id,t_map_char from ${table_name} order by id """




        sql """drop catalog ${mc_catalog_name};"""
    }
}

