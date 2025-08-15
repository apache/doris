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

suite("test_glue_rest_s3tables", "p2,external,iceberg,external_remote,external_remote_iceberg,new_catalog_property") {
    def format_compressions = ["parquet_zstd"]

    def q01 = { String format_compression, String catalog_name ->
        def parts = format_compression.split("_")
        def format = parts[0]
        def compression = parts[1]
        def all_types_table = "iceberg_glue_rest_${format_compression}_master"
        sql """ DROP TABLE IF EXISTS `${all_types_table}`; """
        sql """
        CREATE TABLE `${all_types_table}`(
          `boolean_col` boolean,
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
          `timestamp_col1` datetime,
          `timestamp_col2` datetime,
          `timestamp_col3` datetime,
          `t_map_string` map<string,string>,
          `t_map_int` map<int,int>,
          `t_map_bigint` map<bigint,bigint>,
          `t_map_float` map<float,float>,
          `t_map_double` map<double,double>,
          `t_map_boolean` map<boolean,boolean>,
          `t_map_decimal_precision_2` map<decimal(2,1),decimal(2,1)>,
          `t_map_decimal_precision_4` map<decimal(4,2),decimal(4,2)>,
          `t_map_decimal_precision_8` map<decimal(8,4),decimal(8,4)>,
          `t_map_decimal_precision_17` map<decimal(17,8),decimal(17,8)>,
          `t_map_decimal_precision_18` map<decimal(18,8),decimal(18,8)>,
          `t_map_decimal_precision_38` map<decimal(38,16),decimal(38,16)>,
          `t_array_string` array<string>,
          `t_array_int` array<int>,
          `t_array_bigint` array<bigint>,
          `t_array_float` array<float>,
          `t_array_double` array<double>,
          `t_array_boolean` array<boolean>,
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
          `t_nested_struct_non_nulls_after_nulls` struct<struct_field1:int,struct_field2:string,strict_field3:struct<nested_struct_field1:int,nested_struct_field2:string>>,
          `t_map_null_value` map<string,string>,
          `t_array_string_starting_with_nulls` array<string>,
          `t_array_string_with_nulls_in_between` array<string>,
          `t_array_string_ending_with_nulls` array<string>,
          `t_array_string_all_nulls` array<string>,
          `dt` int) ENGINE=iceberg
          properties (
            "compression-codec" = ${compression},
            "write-format"=${format}
          )
        """

        sql """
        INSERT INTO ${all_types_table}
        VALUES (
          1, -- boolean_col
          2147483647, -- int_col
          9223372036854775807, -- bigint_col
          123.45, -- float_col
          123456.789, -- double_col
          CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          '2024-03-20', -- date_col
          '2024-03-20 12:00:00', -- timestamp_col1
          '2024-03-20 12:00:00.123456789', -- timestamp_col2
          '2024-03-20 12:00:00.123456789', -- timestamp_col3
          MAP('key1', 'value1'), -- t_map_string
          MAP(1, 10), -- t_map_int
          MAP(1, 100000000000), -- t_map_bigint
          MAP(CAST(1.1 AS FLOAT), CAST(10.1 AS FLOAT)), -- t_map_float
          MAP(CAST(1.1 AS DOUBLE), CAST(10.1 AS DOUBLE)), -- t_map_double
          MAP(TRUE, FALSE), -- t_map_boolean
          MAP(CAST(1.1 AS DECIMAL(2,1)), CAST(1.1 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(1.23 AS DECIMAL(4,2)), CAST(1.23 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(1.2345 AS DECIMAL(8,4)), CAST(1.2345 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(1.23456789 AS DECIMAL(17,8)), CAST(1.23456789 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(1.23456789 AS DECIMAL(18,8)), CAST(1.23456789 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(1.234567890123456789 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY('string1', 'string2'), -- t_ARRAY_string
          ARRAY(1, 2, 3), -- t_ARRAY_int
          ARRAY(100000000000, 200000000000), -- t_ARRAY_bigint
          ARRAY(CAST(1.1 AS FLOAT), CAST(2.2 AS FLOAT)), -- t_ARRAY_float
          ARRAY(CAST(1.123456789 AS DOUBLE), CAST(2.123456789 AS DOUBLE)), -- t_ARRAY_double
          ARRAY(TRUE, FALSE), -- t_ARRAY_boolean
          ARRAY(CAST(1.1 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_ARRAY_decimal_precision_2
          ARRAY(CAST(1.23 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_ARRAY_decimal_precision_4
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_ARRAY_decimal_precision_8
          ARRAY(CAST(1.23456789 AS DECIMAL(17,8)), CAST(2.34567891 AS DECIMAL(17,8))), -- t_ARRAY_decimal_precision_17
          ARRAY(1.23456789, 2.34567891), -- t_ARRAY_decimal_precision_18
          ARRAY(1.234567890123456789, 2.345678901234567890), -- t_ARRAY_decimal_precision_38
          NAMED_STRUCT('s_bigint', 1234567890), -- t_struct_bigint
          MAP('key', ARRAY(NAMED_STRUCT('s_int', 123))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', 123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', 123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', 123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_ARRAY_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_ARRAY_string_with_nulls_in_between
          ARRAY('value1', 'value2', null), -- t_ARRAY_string_ending_with_nulls
          ARRAY(null, null, null), -- t_ARRAY_string_all_nulls
          20240320 -- dt
        );
        """
        order_qt_q01 """ select * from ${all_types_table};
        """

        sql """
        INSERT INTO ${all_types_table}
        VALUES (
          1, -- boolean_col
          2147483647, -- int_col
          9223372036854775807, -- bigint_col
          CAST(123.45 AS FLOAT), -- float_col
          CAST(123456.789 AS DOUBLE), -- double_col
          CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          '2024-03-20', -- date_col
          '2024-03-20 12:00:00', -- timestamp_col1
          '2024-03-20 12:00:00.123456789', -- timestamp_col2
          '2024-03-20 12:00:00.123456789', -- timestamp_col3
          MAP('key1', 'value1'), -- t_map_string
          MAP(1, 10), -- t_map_int
          MAP(1, 100000000000), -- t_map_bigint
          MAP(CAST(1.1 AS FLOAT), CAST(10.1 AS FLOAT)), -- t_map_float
          MAP(CAST(1.1 AS DOUBLE), CAST(10.1 AS DOUBLE)), -- t_map_double
          MAP(true, false), -- t_map_boolean
          MAP(CAST(1.1 AS DECIMAL(2,1)), CAST(1.1 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(1.23 AS DECIMAL(4,2)), CAST(1.23 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(1.2345 AS DECIMAL(8,4)), CAST(1.2345 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(1.23456789 AS DECIMAL(17,8)), CAST(1.23456789 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(1.23456789 AS DECIMAL(18,8)), CAST(1.23456789 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(1.234567890123456789 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY('string1', 'string2'), -- t_ARRAY_string
          ARRAY(1, 2, 3), -- t_ARRAY_int
          ARRAY(100000000000, 200000000000), -- t_ARRAY_bigint
          ARRAY(CAST(1.1 AS FLOAT), CAST(2.2 AS FLOAT)), -- t_ARRAY_float
          ARRAY(CAST(1.123456789 AS DOUBLE), CAST(2.123456789 AS DOUBLE)), -- t_ARRAY_double
          ARRAY(true, false), -- t_ARRAY_boolean
          ARRAY(CAST(1.1 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_ARRAY_decimal_precision_2
          ARRAY(CAST(1.23 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_ARRAY_decimal_precision_4
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_ARRAY_decimal_precision_8
          ARRAY(CAST(1.23456789 AS DECIMAL(17,8)), CAST(2.34567891 AS DECIMAL(17,8))), -- t_ARRAY_decimal_precision_17
          ARRAY(CAST(1.23456789 AS DECIMAL(18,8)), CAST(2.34567891 AS DECIMAL(18,8))), -- t_ARRAY_decimal_precision_18
          ARRAY(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_ARRAY_decimal_precision_38
          NAMED_STRUCT('s_bigint', 1234567890), -- t_struct_bigint
          MAP('key', ARRAY(NAMED_STRUCT('s_int', 123))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', 123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', 123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', 123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_ARRAY_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_ARRAY_string_with_nulls_in_between
          ARRAY('value1', 'value2', null), -- t_ARRAY_string_ending_with_nulls
          ARRAY(null, null, null), -- t_ARRAY_string_all_nulls
          20240320 -- dt
       ),
       (
          0, -- boolean_col
          -2147483648, -- int_col
          -9223372036854775808, -- bigint_col
          CAST(-123.45 AS FLOAT), -- float_col
          CAST(-123456.789 AS DOUBLE), -- double_col
          CAST(-123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          '2024-03-21', -- date_col
          '2024-03-21 12:00:00', -- timestamp_col1
          '2024-03-21 12:00:00.123456789', -- timestamp_col2
          '2024-03-21 12:00:00.123456789', -- timestamp_col3
          MAP('key1', 'value1'), -- t_map_string
          MAP(2, 20), -- t_map_int
          MAP(2, 200000000000), -- t_map_bigint
          MAP(CAST(2.2 AS FLOAT), CAST(20.2 AS FLOAT)), -- t_map_float
          MAP(CAST(2.2 AS DOUBLE), CAST(20.2 AS DOUBLE)), -- t_map_double
          MAP(false, true), -- t_map_boolean
          MAP(CAST(2.2 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(2.34 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(2.3456 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(2.34567890 AS DECIMAL(17,8)), CAST(2.34567890 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(2.34567890 AS DECIMAL(18,8)), CAST(2.34567890 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(2.345678901234567890 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY('string1', 'string2'), -- t_ARRAY_string
          ARRAY(4, 5, 6), -- t_ARRAY_int
          ARRAY(300000000000, 400000000000), -- t_ARRAY_bigint
          ARRAY(CAST(3.3 AS FLOAT), CAST(4.4 AS FLOAT)), -- t_ARRAY_float
          ARRAY(CAST(3.123456789 AS DOUBLE), CAST(4.123456789 AS DOUBLE)), -- t_ARRAY_double
          ARRAY(false, true), -- t_ARRAY_boolean
          ARRAY(CAST(3.3 AS DECIMAL(2,1)), CAST(4.4 AS DECIMAL(2,1))), -- t_ARRAY_decimal_precision_2
          ARRAY(CAST(3.45 AS DECIMAL(4,2)), CAST(4.56 AS DECIMAL(4,2))), -- t_ARRAY_decimal_precision_4
          ARRAY(CAST(3.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_ARRAY_decimal_precision_8
          ARRAY(CAST(3.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_ARRAY_decimal_precision_17
          ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_ARRAY_decimal_precision_18
          ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_ARRAY_decimal_precision_38
          NAMED_STRUCT('s_bigint', -1234567890), -- t_struct_bigint
          MAP('key', ARRAY(NAMED_STRUCT('s_int', -123))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', -123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', -123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', -123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_ARRAY_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_ARRAY_string_with_nulls_in_between
          ARRAY('value1', 'value2', null), -- t_ARRAY_string_ending_with_nulls
          ARRAY(null, null, null), -- t_ARRAY_string_all_nulls
          20240321 -- dt
        ),
        (
          0, -- boolean_col
          -2147483648, -- int_col
          -9223372036854775808, -- bigint_col
          CAST(-123.45 AS FLOAT), -- float_col
          CAST(-123456.789 AS DOUBLE), -- double_col
          CAST(-123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          '2024-03-22', -- date_col
          '2024-03-22 12:00:00', -- timestamp_col1
          '2024-03-22 12:00:00.123456789', -- timestamp_col2
          '2024-03-22 12:00:00.123456789', -- timestamp_col3
          MAP('key1', 'value1'), -- t_map_string
          MAP(3, 20), -- t_map_int
          MAP(3, 200000000000), -- t_map_bigint
          MAP(CAST(3.2 AS FLOAT), CAST(20.2 AS FLOAT)), -- t_map_float
          MAP(CAST(3.2 AS DOUBLE), CAST(20.2 AS DOUBLE)), -- t_map_double
          MAP(false, true), -- t_map_boolean
          MAP(CAST(3.2 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(3.34 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(2.3456 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(2.34567890 AS DECIMAL(17,8)), CAST(2.34567890 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(2.34567890 AS DECIMAL(18,8)), CAST(2.34567890 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(3.345678901234567890 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY('string1', 'string2'), -- t_ARRAY_string
          ARRAY(4, 5, 6), -- t_ARRAY_int
          ARRAY(300000000000, 400000000000), -- t_ARRAY_bigint
          ARRAY(CAST(3.3 AS FLOAT), CAST(4.4 AS FLOAT)), -- t_ARRAY_float
          ARRAY(CAST(3.123456789 AS DOUBLE), CAST(4.123456789 AS DOUBLE)), -- t_ARRAY_double
          ARRAY(false, true), -- t_ARRAY_boolean
          ARRAY(CAST(3.3 AS DECIMAL(2,1)), CAST(4.4 AS DECIMAL(2,1))), -- t_ARRAY_decimal_precision_2
          ARRAY(CAST(3.45 AS DECIMAL(4,2)), CAST(4.56 AS DECIMAL(4,2))), -- t_ARRAY_decimal_precision_4
          ARRAY(CAST(8.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_ARRAY_decimal_precision_8
          ARRAY(CAST(3.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_ARRAY_decimal_precision_17
          ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_ARRAY_decimal_precision_18
          ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_ARRAY_decimal_precision_38
          NAMED_STRUCT('s_bigint', -1234567890), -- t_struct_bigint
          MAP('key', ARRAY(NAMED_STRUCT('s_int', -123))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', -123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', -123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', -123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_ARRAY_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_ARRAY_string_with_nulls_in_between
          ARRAY('value11', 'value2', null), -- t_ARRAY_string_ending_with_nulls
          ARRAY(null, null, null), -- t_ARRAY_string_all_nulls
          20240322 -- dt
        );
        """
        order_qt_q02 """ select * from ${all_types_table};
        """

        sql """
        INSERT INTO ${all_types_table}(float_col, t_map_int, t_ARRAY_decimal_precision_8, t_ARRAY_string_starting_with_nulls)
        VALUES (
          CAST(123.45 AS FLOAT), -- float_col
          MAP(1, 10), -- t_map_int
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_ARRAY_decimal_precision_8
          ARRAY(null, 'value1', 'value2') -- t_ARRAY_string_starting_with_nulls
        );
        """
        order_qt_q03 """ select * from ${all_types_table};
        """

        sql """ DROP TABLE ${all_types_table}; """
    }

    def q03 = { String format_compression, String catalog_name ->
        def parts = format_compression.split("_")
        def format = parts[0]
        def compression = parts[1]
        def all_types_partition_table = "iceberg_all_types_par_glue_rest_${format_compression}_master"
        sql """ DROP TABLE IF EXISTS `${all_types_partition_table}`; """
        sql """
        CREATE TABLE `${all_types_partition_table}`(
          `boolean_col` boolean,
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
          `timestamp_col1` datetime,
          `timestamp_col2` datetime,
          `timestamp_col3` datetime,
          `t_map_string` map<string,string>,
          `t_map_int` map<int,int>,
          `t_map_bigint` map<bigint,bigint>,
          `t_map_float` map<float,float>,
          `t_map_double` map<double,double>,
          `t_map_boolean` map<boolean,boolean>,
          `t_map_decimal_precision_2` map<decimal(2,1),decimal(2,1)>,
          `t_map_decimal_precision_4` map<decimal(4,2),decimal(4,2)>,
          `t_map_decimal_precision_8` map<decimal(8,4),decimal(8,4)>,
          `t_map_decimal_precision_17` map<decimal(17,8),decimal(17,8)>,
          `t_map_decimal_precision_18` map<decimal(18,8),decimal(18,8)>,
          `t_map_decimal_precision_38` map<decimal(38,16),decimal(38,16)>,
          `t_array_string` array<string>,
          `t_array_int` array<int>,
          `t_array_bigint` array<bigint>,
          `t_array_float` array<float>,
          `t_array_double` array<double>,
          `t_array_boolean` array<boolean>,
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
          `t_nested_struct_non_nulls_after_nulls` struct<struct_field1:int,struct_field2:string,strict_field3:struct<nested_struct_field1:int,nested_struct_field2:string>>,
          `t_map_null_value` map<string,string>,
          `t_array_string_starting_with_nulls` array<string>,
          `t_array_string_with_nulls_in_between` array<string>,
          `t_array_string_ending_with_nulls` array<string>,
          `t_array_string_all_nulls` array<string>,
          `dt` int) ENGINE=iceberg
          PARTITION BY LIST (dt) ()
          properties (
            "compression-codec" = ${compression},
            "write-format"=${format}
          );
        """

        sql """
        INSERT INTO ${all_types_partition_table}
        VALUES (
          1, -- boolean_col
          2147483647, -- int_col
          9223372036854775807, -- bigint_col
          123.45, -- float_col
          123456.789, -- double_col
          123456789, -- decimal_col1
          1234.5678, -- decimal_col2
          CAST(123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          '2024-03-20', -- date_col
          '2024-03-20 12:00:00', -- timestamp_col1
          '2024-03-20 12:00:00.123456789', -- timestamp_col2
          '2024-03-20 12:00:00.123456789', -- timestamp_col3
          MAP('key1', 'value1'), -- t_map_string
          MAP(1, 10), -- t_map_int
          MAP(1, 100000000000), -- t_map_bigint
          MAP(CAST(1.1 AS FLOAT), CAST(10.1 AS FLOAT)), -- t_map_float
          MAP(CAST(1.1 AS DOUBLE), CAST(10.1 AS DOUBLE)), -- t_map_double
          MAP(true, false), -- t_map_boolean
          MAP(CAST(1.1 AS DECIMAL(2,1)), CAST(1.1 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(1.23 AS DECIMAL(4,2)), CAST(1.23 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(1.2345 AS DECIMAL(8,4)), CAST(1.2345 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(1.23456789 AS DECIMAL(17,8)), CAST(1.23456789 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(1.23456789 AS DECIMAL(18,8)), CAST(1.23456789 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(1.234567890123456789 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY('string1', 'string2'), -- t_ARRAY_string
          ARRAY(1, 2, 3), -- t_ARRAY_int
          ARRAY(100000000000, 200000000000), -- t_ARRAY_bigint
          ARRAY(CAST(1.1 AS FLOAT), CAST(2.2 AS FLOAT)), -- t_ARRAY_float
          ARRAY(CAST(1.123456789 AS DOUBLE), CAST(2.123456789 AS DOUBLE)), -- t_ARRAY_double
          ARRAY(true, false), -- t_ARRAY_boolean
          ARRAY(CAST(1.1 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_ARRAY_decimal_precision_2
          ARRAY(CAST(1.23 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_ARRAY_decimal_precision_4
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_ARRAY_decimal_precision_8
          ARRAY(CAST(1.23456789 AS DECIMAL(17,8)), CAST(2.34567891 AS DECIMAL(17,8))), -- t_ARRAY_decimal_precision_17
          ARRAY(CAST(1.23456789 AS DECIMAL(18,8)), CAST(2.34567891 AS DECIMAL(18,8))), -- t_ARRAY_decimal_precision_18
          ARRAY(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_ARRAY_decimal_precision_38
          NAMED_STRUCT('s_bigint', 1234567890), -- t_struct_bigint
          MAP('key', ARRAY(NAMED_STRUCT('s_int', 123))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', 123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', 123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', 123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_ARRAY_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_ARRAY_string_with_nulls_in_between
          ARRAY('value1', 'value2', null), -- t_ARRAY_string_ending_with_nulls
          ARRAY(null, null, null), -- t_ARRAY_string_all_nulls
          20240320 -- dt
        );
        """
        order_qt_q01 """ select * from ${all_types_partition_table};
        """

        sql """
        INSERT INTO ${all_types_partition_table}
        VALUES  (
          1, -- boolean_col
          2147483647, -- int_col
          9223372036854775807, -- bigint_col
          CAST(123.45 AS FLOAT), -- float_col
          CAST(123456.789 AS DOUBLE), -- double_col
          CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          '2024-03-20', -- date_col
          '2024-03-20 12:00:00', -- timestamp_col1
          '2024-03-20 12:00:00.123456789', -- timestamp_col2
          '2024-03-20 12:00:00.123456789', -- timestamp_col3
          MAP('key1', 'value1'), -- t_map_string
          MAP(1, 10), -- t_map_int
          MAP(1, 100000000000), -- t_map_bigint
          MAP(CAST(1.1 AS FLOAT), CAST(10.1 AS FLOAT)), -- t_map_float
          MAP(CAST(1.1 AS DOUBLE), CAST(10.1 AS DOUBLE)), -- t_map_double
          MAP(true, false), -- t_map_boolean
          MAP(CAST(1.1 AS DECIMAL(2,1)), CAST(1.1 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(1.23 AS DECIMAL(4,2)), CAST(1.23 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(1.2345 AS DECIMAL(8,4)), CAST(1.2345 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(1.23456789 AS DECIMAL(17,8)), CAST(1.23456789 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(1.23456789 AS DECIMAL(18,8)), CAST(1.23456789 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(1.234567890123456789 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY('string1', 'string2'), -- t_ARRAY_string
          ARRAY(1, 2, 3), -- t_ARRAY_int
          ARRAY(100000000000, 200000000000), -- t_ARRAY_bigint
          ARRAY(CAST(1.1 AS FLOAT), CAST(2.2 AS FLOAT)), -- t_ARRAY_float
          ARRAY(CAST(1.123456789 AS DOUBLE), CAST(2.123456789 AS DOUBLE)), -- t_ARRAY_double
          ARRAY(true, false), -- t_ARRAY_boolean
          ARRAY(CAST(1.1 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_ARRAY_decimal_precision_2
          ARRAY(CAST(1.23 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_ARRAY_decimal_precision_4
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_ARRAY_decimal_precision_8
          ARRAY(CAST(1.23456789 AS DECIMAL(17,8)), CAST(2.34567891 AS DECIMAL(17,8))), -- t_ARRAY_decimal_precision_17
          ARRAY(CAST(1.23456789 AS DECIMAL(18,8)), CAST(2.34567891 AS DECIMAL(18,8))), -- t_ARRAY_decimal_precision_18
          ARRAY(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_ARRAY_decimal_precision_38
          NAMED_STRUCT('s_bigint', 1234567890), -- t_struct_bigint
          MAP('key', ARRAY(NAMED_STRUCT('s_int', 123))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', 123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', 123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', 123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_ARRAY_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_ARRAY_string_with_nulls_in_between
          ARRAY('value1', 'value2', null), -- t_ARRAY_string_ending_with_nulls
          ARRAY(null, null, null), -- t_ARRAY_string_all_nulls
          20240320 -- dt
       ),
       (
          0, -- boolean_col
          -2147483648, -- int_col
          -9223372036854775808, -- bigint_col
          CAST(-123.45 AS FLOAT), -- float_col
          CAST(-123456.789 AS DOUBLE), -- double_col
          CAST(-123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          '2024-03-21', -- date_col
          '2024-03-21 12:00:00', -- timestamp_col1
          '2024-03-21 12:00:00.123456789', -- timestamp_col2
          '2024-03-21 12:00:00.123456789', -- timestamp_col3
          MAP('key1', 'value1'), -- t_map_string
          MAP(2, 20), -- t_map_int
          MAP(2, 200000000000), -- t_map_bigint
          MAP(CAST(2.2 AS FLOAT), CAST(20.2 AS FLOAT)), -- t_map_float
          MAP(CAST(2.2 AS DOUBLE), CAST(20.2 AS DOUBLE)), -- t_map_double
          MAP(false, true), -- t_map_boolean
          MAP(CAST(2.2 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(2.34 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(2.3456 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(2.34567890 AS DECIMAL(17,8)), CAST(2.34567890 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(2.34567890 AS DECIMAL(18,8)), CAST(2.34567890 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(2.345678901234567890 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY('string1', 'string2'), -- t_ARRAY_string
          ARRAY(4, 5, 6), -- t_ARRAY_int
          ARRAY(300000000000, 400000000000), -- t_ARRAY_bigint
          ARRAY(CAST(3.3 AS FLOAT), CAST(4.4 AS FLOAT)), -- t_ARRAY_float
          ARRAY(CAST(3.123456789 AS DOUBLE), CAST(4.123456789 AS DOUBLE)), -- t_ARRAY_double
          ARRAY(false, true), -- t_ARRAY_boolean
          ARRAY(CAST(3.3 AS DECIMAL(2,1)), CAST(4.4 AS DECIMAL(2,1))), -- t_ARRAY_decimal_precision_2
          ARRAY(CAST(3.45 AS DECIMAL(4,2)), CAST(4.56 AS DECIMAL(4,2))), -- t_ARRAY_decimal_precision_4
          ARRAY(CAST(3.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_ARRAY_decimal_precision_8
          ARRAY(CAST(3.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_ARRAY_decimal_precision_17
          ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_ARRAY_decimal_precision_18
          ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_ARRAY_decimal_precision_38
          NAMED_STRUCT('s_bigint', -1234567890), -- t_struct_bigint
          MAP('key', ARRAY(NAMED_STRUCT('s_int', -123))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', -123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', -123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', -123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_ARRAY_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_ARRAY_string_with_nulls_in_between
          ARRAY('value1', 'value2', null), -- t_ARRAY_string_ending_with_nulls
          ARRAY(null, null, null), -- t_ARRAY_string_all_nulls
          20240321 -- dt
        ),
        (
          0, -- boolean_col
          -2147483648, -- int_col
          -9223372036854775808, -- bigint_col
          -123.45, -- float_col
          -123456.789, -- double_col
          CAST(-123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          '2024-03-22', -- date_col
          '2024-03-22 12:00:00', -- timestamp_col1
          '2024-03-22 12:00:00.123456789', -- timestamp_col2
          '2024-03-22 12:00:00.123456789', -- timestamp_col3
          MAP('key1', 'value1'), -- t_map_string
          MAP(3, 20), -- t_map_int
          MAP(3, 200000000000), -- t_map_bigint
          MAP(CAST(3.2 AS FLOAT), CAST(20.2 AS FLOAT)), -- t_map_float
          MAP(CAST(3.2 AS DOUBLE), CAST(20.2 AS DOUBLE)), -- t_map_double
          MAP(false, true), -- t_map_boolean
          MAP(CAST(3.2 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(3.34 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(2.3456 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(2.34567890 AS DECIMAL(17,8)), CAST(2.34567890 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(2.34567890 AS DECIMAL(18,8)), CAST(2.34567890 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(3.345678901234567890 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY('string1', 'string2'), -- t_ARRAY_string
          ARRAY(4, 5, 6), -- t_ARRAY_int
          ARRAY(300000000000, 400000000000), -- t_ARRAY_bigint
          ARRAY(CAST(3.3 AS FLOAT), CAST(4.4 AS FLOAT)), -- t_ARRAY_float
          ARRAY(CAST(3.123456789 AS DOUBLE), CAST(4.123456789 AS DOUBLE)), -- t_ARRAY_double
          ARRAY(false, true), -- t_ARRAY_boolean
          ARRAY(CAST(3.3 AS DECIMAL(2,1)), CAST(4.4 AS DECIMAL(2,1))), -- t_ARRAY_decimal_precision_2
          ARRAY(CAST(3.45 AS DECIMAL(4,2)), CAST(4.56 AS DECIMAL(4,2))), -- t_ARRAY_decimal_precision_4
          ARRAY(CAST(8.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_ARRAY_decimal_precision_8
          ARRAY(CAST(3.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_ARRAY_decimal_precision_17
          ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_ARRAY_decimal_precision_18
          ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_ARRAY_decimal_precision_38
          NAMED_STRUCT('s_bigint', -1234567890), -- t_struct_bigint
          MAP('key', ARRAY(NAMED_STRUCT('s_int', -123))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', -123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', -123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', -123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_ARRAY_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_ARRAY_string_with_nulls_in_between
          ARRAY('value11', 'value2', null), -- t_ARRAY_string_ending_with_nulls
          ARRAY(null, null, null), -- t_ARRAY_string_all_nulls
          20240322 -- dt
        );
        """
        order_qt_q02 """ select * from ${all_types_partition_table};
        """

        sql """
        INSERT INTO ${all_types_partition_table}(float_col, t_map_int, t_ARRAY_decimal_precision_8, t_ARRAY_string_starting_with_nulls, dt)
        VALUES (
          123.45, -- float_col
          MAP(1, 10), -- t_map_int
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_ARRAY_decimal_precision_8
          ARRAY(null, 'value1', 'value2'), -- t_ARRAY_string_starting_with_nulls
          20240321 -- dt
        );
        """
        order_qt_q03 """ select * from ${all_types_partition_table};
        """

        // just test
        sql """
            SELECT
              CASE
                WHEN file_size_in_bytes BETWEEN 0 AND 8 * 1024 * 1024 THEN '0-8M'
                WHEN file_size_in_bytes BETWEEN 8 * 1024 * 1024 + 1 AND 32 * 1024 * 1024 THEN '8-32M'
                WHEN file_size_in_bytes BETWEEN 2 * 1024 * 1024 + 1 AND 128 * 1024 * 1024 THEN '32-128M'
                WHEN file_size_in_bytes BETWEEN 128 * 1024 * 1024 + 1 AND 512 * 1024 * 1024 THEN '128-512M'
                WHEN file_size_in_bytes > 512 * 1024 * 1024 THEN '> 512M'
                ELSE 'Unknown'
              END AS SizeRange,
              COUNT(*) AS FileNum
            FROM ${all_types_partition_table}\$data_files
            GROUP BY
              SizeRange;
        """

        sql """ DROP TABLE ${all_types_partition_table}; """
    }

    String enabled = context.config.otherConfigs.get("enableExternalIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test")
        return
    }

    String catalog_name = "test_s3tables_glue_rest"
    String props = context.config.otherConfigs.get("icebergS3TablesCatalogGlueRest")
    sql """drop catalog if exists ${catalog_name};"""
    sql """
        create catalog ${catalog_name} properties (
            ${props}
        );
    """

    sql """ switch ${catalog_name};"""
    sql """ drop database if exists iceberg_s3tables_glue_rest_master force"""
    sql """ create database iceberg_s3tables_glue_rest_master"""
    sql """ use iceberg_s3tables_glue_rest_master;""" 
    sql """ set enable_fallback_to_original_planner=false """

    try {
        for (String format_compression in format_compressions) {
            logger.info("Process format_compression " + format_compression)
            q01(format_compression, catalog_name)
            q03(format_compression, catalog_name)
        }
    } finally {
    }
}
