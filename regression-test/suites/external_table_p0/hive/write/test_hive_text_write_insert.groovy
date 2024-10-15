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

suite("test_hive_text_write_insert", "p0,external,hive,external_docker,external_docker_hive") {
    def q01 = { String format_compression, String catalog_name ->
        logger.info("hive sql: " + """ truncate table all_types_text; """)
        hive_docker """ truncate table all_types_text; """
        sql """refresh catalog ${catalog_name};"""

        sql """
        INSERT INTO all_types_text
        VALUES (
          1, -- boolean_col
          127, -- tinyint_col
          32767, -- smallint_col
          2147483647, -- int_col
          9223372036854775807, -- bigint_col
          123.45, -- float_col
          123456.789, -- double_col
          CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          'binary_value', -- binary_col
          '2024-03-20', -- date_col
          '2024-03-20 12:00:00', -- timestamp_col1
          '2024-03-20 12:00:00.123456789', -- timestamp_col2
          '2024-03-20 12:00:00.123456789', -- timestamp_col3
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
          MAP(CAST(1.1 AS DECIMAL(2,1)), CAST(1.1 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(1.23 AS DECIMAL(4,2)), CAST(1.23 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(1.2345 AS DECIMAL(8,4)), CAST(1.2345 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(1.23456789 AS DECIMAL(17,8)), CAST(1.23456789 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(1.23456789 AS DECIMAL(18,8)), CAST(1.23456789 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(1.234567890123456789 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
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
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', 123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', 123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', 123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_array_string_with_nulls_in_between
          ARRAY('value1', 'value2', null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240320 -- dt
        );
        """
        order_qt_q01 """ select * from all_types_text;
        """

        sql """
        INSERT INTO all_types_text
        VALUES (
          1, -- boolean_col
          127, -- tinyint_col
          32767, -- smallint_col
          2147483647, -- int_col
          9223372036854775807, -- bigint_col
          CAST(123.45 AS FLOAT), -- float_col
          CAST(123456.789 AS DOUBLE), -- double_col
          CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          'binary_value', -- binary_col
          '2024-03-20', -- date_col
          '2024-03-20 12:00:00', -- timestamp_col1
          '2024-03-20 12:00:00.123456789', -- timestamp_col2
          '2024-03-20 12:00:00.123456789', -- timestamp_col3
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
          MAP(true, false), -- t_map_boolean
          MAP(CAST(1.1 AS DECIMAL(2,1)), CAST(1.1 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(1.23 AS DECIMAL(4,2)), CAST(1.23 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(1.2345 AS DECIMAL(8,4)), CAST(1.2345 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(1.23456789 AS DECIMAL(17,8)), CAST(1.23456789 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(1.23456789 AS DECIMAL(18,8)), CAST(1.23456789 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(1.234567890123456789 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY('string1', 'string2'), -- t_array_string
          ARRAY(1, 2, 3), -- t_array_int
          ARRAY(100000000000, 200000000000), -- t_array_bigint
          ARRAY(CAST(1.1 AS FLOAT), CAST(2.2 AS FLOAT)), -- t_array_float
          ARRAY(CAST(1.123456789 AS DOUBLE), CAST(2.123456789 AS DOUBLE)), -- t_array_double
          ARRAY(true, false), -- t_array_boolean
          ARRAY('varchar1', 'varchar2'), -- t_array_varchar
          ARRAY('char1', 'char2'), -- t_array_char
          ARRAY(CAST(1.1 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
          ARRAY(CAST(1.23 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(1.23456789 AS DECIMAL(17,8)), CAST(2.34567891 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(1.23456789 AS DECIMAL(18,8)), CAST(2.34567891 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', 1234567890), -- t_struct_bigint
          MAP('key', ARRAY(NAMED_STRUCT('s_int', 123))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', 123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', 123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', 123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_array_string_with_nulls_in_between
          ARRAY('value1', 'value2', null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240320 -- dt
       ),
       (
          0, -- boolean_col
          -128, -- tinyint_col
          -32768, -- smallint_col
          -2147483648, -- int_col
          -9223372036854775808, -- bigint_col
          CAST(-123.45 AS FLOAT), -- float_col
          CAST(-123456.789 AS DOUBLE), -- double_col
          CAST(-123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          'binary_value', -- binary_col
          '2024-03-21', -- date_col
          '2024-03-21 12:00:00', -- timestamp_col1
          '2024-03-21 12:00:00.123456789', -- timestamp_col2
          '2024-03-21 12:00:00.123456789', -- timestamp_col3
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
          MAP(CAST(2.2 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(2.34 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(2.3456 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(2.34567890 AS DECIMAL(17,8)), CAST(2.34567890 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(2.34567890 AS DECIMAL(18,8)), CAST(2.34567890 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(2.345678901234567890 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
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
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', -123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', -123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', -123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_array_string_with_nulls_in_between
          ARRAY('value1', 'value2', null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240321 -- dt
        ),
        (
          0, -- boolean_col
          -128, -- tinyint_col
          -32768, -- smallint_col
          -2147483648, -- int_col
          -9223372036854775808, -- bigint_col
          CAST(-123.45 AS FLOAT), -- float_col
          CAST(-123456.789 AS DOUBLE), -- double_col
          CAST(-123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          'binary_value', -- binary_col
          '2024-03-22', -- date_col
          '2024-03-22 12:00:00', -- timestamp_col1
          '2024-03-22 12:00:00.123456789', -- timestamp_col2
          '2024-03-22 12:00:00.123456789', -- timestamp_col3
          'char_value1', -- char_col1
          'char_value2', -- char_col2
          'char_value3', -- char_col3
          'varchar_value1', -- varchar_col1
          'varchar_value2', -- varchar_col2
          'varchar_value3', -- varchar_col3
          MAP('key1', 'value1'), -- t_map_string
          MAP('key1', 'value1'), -- t_map_varchar
          MAP('x', 'y'), -- t_map_char
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
          ARRAY(CAST(8.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(3.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', -1234567890), -- t_struct_bigint
          MAP('key', ARRAY(NAMED_STRUCT('s_int', -123))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', -123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', -123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', -123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_array_string_with_nulls_in_between
          ARRAY('value11', 'value2', null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240322 -- dt
        );
        """
        order_qt_q02 """ select * from all_types_text;
        """

        sql """
        INSERT INTO all_types_text(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls)
        VALUES (
          CAST(123.45 AS FLOAT), -- float_col
          MAP(1, 10), -- t_map_int
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(null, 'value1', 'value2') -- t_array_string_starting_with_nulls
        );
        """
        order_qt_q03 """ select * from all_types_text;
        """

        sql """
        insert overwrite table all_types_text
        VALUES (
          0, -- boolean_col
          -7, -- tinyint_col
          -15, -- smallint_col
          16, -- int_col
          -9223372036854775808, -- bigint_col
          CAST(-123.45 AS FLOAT), -- float_col
          CAST(-123456.789 AS DOUBLE), -- double_col
          CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'str', -- string_col
          'binary_value', -- binary_col
          '2024-03-25', -- date_col
          '2024-03-25 12:00:00', -- timestamp_col1
          '2024-03-25 12:00:00.123456789', -- timestamp_col2
          '2024-03-25 12:00:00.123456789', -- timestamp_col3
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
          MAP(CAST(3.2 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(3.34 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(5.3456 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(5.34567890 AS DECIMAL(17,8)), CAST(2.34567890 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(2.34567890 AS DECIMAL(18,8)), CAST(2.34567890 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(7.345678901234567890 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
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
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', -123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', -123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', -123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_array_string_with_nulls_in_between
          ARRAY('value11', 'value2', null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240325 -- dt
        );
        """
        order_qt_q04 """ select * from all_types_text;
        """

        sql """
        INSERT overwrite table all_types_text(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls, dt)
        VALUES (
          123.45, -- float_col
          MAP(1, 10), -- t_map_int
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
          20240321 -- dt
        );
        """
        order_qt_q05 """ select * from all_types_text;
        """
    }

    def q02 = { String format_compression, String catalog_name ->
        logger.info("hive sql: " + """ truncate table all_types_text; """)
        hive_docker """ truncate table all_types_text; """
        sql """refresh catalog ${catalog_name};"""

        sql """
        INSERT INTO all_types_text
        SELECT * FROM all_types_parquet_snappy_src;
        """
        order_qt_q01 """ select * from all_types_text;
        """

        sql """
        INSERT INTO all_types_text
        SELECT boolean_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, decimal_col1, decimal_col2,
         decimal_col3, decimal_col4, string_col, binary_col, date_col, timestamp_col1, timestamp_col2, timestamp_col3, char_col1,
          char_col2, char_col3, varchar_col1, varchar_col2, varchar_col3, t_map_string, t_map_varchar, t_map_char, t_map_int,
           t_map_bigint, t_map_float, t_map_double, t_map_boolean, t_map_decimal_precision_2, t_map_decimal_precision_4,
            t_map_decimal_precision_8, t_map_decimal_precision_17, t_map_decimal_precision_18, t_map_decimal_precision_38,
             t_array_string, t_array_int, t_array_bigint, t_array_float, t_array_double, t_array_boolean, t_array_varchar,
              t_array_char, t_array_decimal_precision_2, t_array_decimal_precision_4, t_array_decimal_precision_8,
               t_array_decimal_precision_17, t_array_decimal_precision_18, t_array_decimal_precision_38, t_struct_bigint, t_complex,
                t_struct_nested, t_struct_null, t_struct_non_nulls_after_nulls, t_nested_struct_non_nulls_after_nulls,
                 t_map_null_value, t_array_string_starting_with_nulls, t_array_string_with_nulls_in_between,
                  t_array_string_ending_with_nulls, t_array_string_all_nulls, dt FROM all_types_parquet_snappy_src;
        """
        order_qt_q02 """ select * from all_types_text;
        """

        sql """
        INSERT INTO all_types_text(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls)
        SELECT float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls FROM all_types_parquet_snappy_src;
        """
        order_qt_q03 """
        select * from all_types_text;
        """

        sql """
        INSERT OVERWRITE TABLE all_types_text(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls)
        SELECT float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls FROM all_types_parquet_snappy_src;
        """
        order_qt_q04 """
        select * from all_types_text;
        """
    }

    def q03 = { String format_compression, String catalog_name ->
        logger.info("hive sql: " + """ truncate table all_types_text; """)
        hive_docker """ truncate table all_types_text; """
        sql """refresh catalog ${catalog_name};"""

        sql """
        INSERT INTO all_types_text
        SELECT * FROM all_types_parquet_snappy_src;
        """
        sql """
        INSERT INTO all_types_text
        SELECT * FROM all_types_text;
        """
        sql """
        INSERT INTO all_types_text
        SELECT * FROM all_types_text;
        """
        sql """
        INSERT INTO all_types_text
        SELECT * FROM all_types_text;
        """
        sql """
        INSERT INTO all_types_text
        SELECT * FROM all_types_text;
        """
        sql """
        INSERT INTO all_types_text
        SELECT * FROM all_types_text;
        """
        sql """
        INSERT INTO all_types_text
        SELECT * FROM all_types_text;
        """
        sql """
        INSERT INTO all_types_text
        SELECT * FROM all_types_text;
        """
        sql """
        INSERT INTO all_types_text
        SELECT * FROM all_types_text;
        """
        sql """
        INSERT INTO all_types_text
        SELECT * FROM all_types_text;
        """
        sql """
        INSERT INTO all_types_text
        SELECT * FROM all_types_text;
        """
        sql """
        INSERT INTO all_types_text
        SELECT * FROM all_types_text;
        """
        sql """
        INSERT INTO all_types_text
        SELECT * FROM all_types_text;
        """

        order_qt_q01 """ select count(1) from all_types_text;
        """
    }

    def q04 = { String format_compression, String catalog_name ->
        logger.info("hive sql: " + """ drop table if exists all_types_par_text_q3; """)
        hive_docker """ drop table if exists all_types_par_text_q3; """
        logger.info("hive sql: " + """ create table all_types_par_text_q3 like all_types_par_text; """)
        hive_docker """ create table all_types_par_text_q3 like all_types_par_text; """
        sql """refresh catalog ${catalog_name};"""

        sql """
        INSERT INTO all_types_par_text_q3
        VALUES (
          1, -- boolean_col
          127, -- tinyint_col
          32767, -- smallint_col
          2147483647, -- int_col
          9223372036854775807, -- bigint_col
          123.45, -- float_col
          123456.789, -- double_col
          123456789, -- decimal_col1
          1234.5678, -- decimal_col2
          CAST(123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          'binary_value', -- binary_col
          '2024-03-20', -- date_col
          '2024-03-20 12:00:00', -- timestamp_col1
          '2024-03-20 12:00:00.123456789', -- timestamp_col2
          '2024-03-20 12:00:00.123456789', -- timestamp_col3
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
          MAP(true, false), -- t_map_boolean
          MAP(CAST(1.1 AS DECIMAL(2,1)), CAST(1.1 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(1.23 AS DECIMAL(4,2)), CAST(1.23 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(1.2345 AS DECIMAL(8,4)), CAST(1.2345 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(1.23456789 AS DECIMAL(17,8)), CAST(1.23456789 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(1.23456789 AS DECIMAL(18,8)), CAST(1.23456789 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(1.234567890123456789 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY('string1', 'string2'), -- t_array_string
          ARRAY(1, 2, 3), -- t_array_int
          ARRAY(100000000000, 200000000000), -- t_array_bigint
          ARRAY(CAST(1.1 AS FLOAT), CAST(2.2 AS FLOAT)), -- t_array_float
          ARRAY(CAST(1.123456789 AS DOUBLE), CAST(2.123456789 AS DOUBLE)), -- t_array_double
          ARRAY(true, false), -- t_array_boolean
          ARRAY('varchar1', 'varchar2'), -- t_array_varchar
          ARRAY('char1', 'char2'), -- t_array_char
          ARRAY(CAST(1.1 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
          ARRAY(CAST(1.23 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(1.23456789 AS DECIMAL(17,8)), CAST(2.34567891 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(1.23456789 AS DECIMAL(18,8)), CAST(2.34567891 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', 1234567890), -- t_struct_bigint
          MAP('key', ARRAY(NAMED_STRUCT('s_int', 123))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', 123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', 123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', 123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_array_string_with_nulls_in_between
          ARRAY('value1', 'value2', null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240320 -- dt
        );
        """
        order_qt_q01 """ select * from all_types_par_text_q3;
        """

        sql """
        INSERT INTO all_types_par_text_q3
        VALUES  (
          1, -- boolean_col
          127, -- tinyint_col
          32767, -- smallint_col
          2147483647, -- int_col
          9223372036854775807, -- bigint_col
          CAST(123.45 AS FLOAT), -- float_col
          CAST(123456.789 AS DOUBLE), -- double_col
          CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          'binary_value', -- binary_col
          '2024-03-20', -- date_col
          '2024-03-20 12:00:00', -- timestamp_col1
          '2024-03-20 12:00:00.123456789', -- timestamp_col2
          '2024-03-20 12:00:00.123456789', -- timestamp_col3
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
          MAP(true, false), -- t_map_boolean
          MAP(CAST(1.1 AS DECIMAL(2,1)), CAST(1.1 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(1.23 AS DECIMAL(4,2)), CAST(1.23 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(1.2345 AS DECIMAL(8,4)), CAST(1.2345 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(1.23456789 AS DECIMAL(17,8)), CAST(1.23456789 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(1.23456789 AS DECIMAL(18,8)), CAST(1.23456789 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(1.234567890123456789 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY('string1', 'string2'), -- t_array_string
          ARRAY(1, 2, 3), -- t_array_int
          ARRAY(100000000000, 200000000000), -- t_array_bigint
          ARRAY(CAST(1.1 AS FLOAT), CAST(2.2 AS FLOAT)), -- t_array_float
          ARRAY(CAST(1.123456789 AS DOUBLE), CAST(2.123456789 AS DOUBLE)), -- t_array_double
          ARRAY(true, false), -- t_array_boolean
          ARRAY('varchar1', 'varchar2'), -- t_array_varchar
          ARRAY('char1', 'char2'), -- t_array_char
          ARRAY(CAST(1.1 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
          ARRAY(CAST(1.23 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(1.23456789 AS DECIMAL(17,8)), CAST(2.34567891 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(1.23456789 AS DECIMAL(18,8)), CAST(2.34567891 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', 1234567890), -- t_struct_bigint
          MAP('key', ARRAY(NAMED_STRUCT('s_int', 123))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', 123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', 123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', 123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_array_string_with_nulls_in_between
          ARRAY('value1', 'value2', null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240320 -- dt
       ),
       (
          0, -- boolean_col
          -128, -- tinyint_col
          -32768, -- smallint_col
          -2147483648, -- int_col
          -9223372036854775808, -- bigint_col
          CAST(-123.45 AS FLOAT), -- float_col
          CAST(-123456.789 AS DOUBLE), -- double_col
          CAST(-123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          'binary_value', -- binary_col
          '2024-03-21', -- date_col
          '2024-03-21 12:00:00', -- timestamp_col1
          '2024-03-21 12:00:00.123456789', -- timestamp_col2
          '2024-03-21 12:00:00.123456789', -- timestamp_col3
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
          MAP(CAST(2.2 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(2.34 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(2.3456 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(2.34567890 AS DECIMAL(17,8)), CAST(2.34567890 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(2.34567890 AS DECIMAL(18,8)), CAST(2.34567890 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(2.345678901234567890 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
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
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', -123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', -123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', -123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_array_string_with_nulls_in_between
          ARRAY('value1', 'value2', null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240321 -- dt
        ),
        (
          0, -- boolean_col
          -128, -- tinyint_col
          -32768, -- smallint_col
          -2147483648, -- int_col
          -9223372036854775808, -- bigint_col
          -123.45, -- float_col
          -123456.789, -- double_col
          CAST(-123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'string_value', -- string_col
          'binary_value', -- binary_col
          '2024-03-22', -- date_col
          '2024-03-22 12:00:00', -- timestamp_col1
          '2024-03-22 12:00:00.123456789', -- timestamp_col2
          '2024-03-22 12:00:00.123456789', -- timestamp_col3
          'char_value1', -- char_col1
          'char_value2', -- char_col2
          'char_value3', -- char_col3
          'varchar_value1', -- varchar_col1
          'varchar_value2', -- varchar_col2
          'varchar_value3', -- varchar_col3
          MAP('key1', 'value1'), -- t_map_string
          MAP('key1', 'value1'), -- t_map_varchar
          MAP('x', 'y'), -- t_map_char
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
          ARRAY(CAST(8.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(3.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', -1234567890), -- t_struct_bigint
          MAP('key', ARRAY(NAMED_STRUCT('s_int', -123))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY('value1', 'value2')), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', -123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', -123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', -123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_array_string_with_nulls_in_between
          ARRAY('value11', 'value2', null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240322 -- dt
        );
        """
        order_qt_q02 """ select * from all_types_par_text_q3;
        """

        sql """
        INSERT INTO all_types_par_text_q3(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls, dt)
        VALUES (
          123.45, -- float_col
          MAP(1, 10), -- t_map_int
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
          20240321 -- dt
        );
        """
        order_qt_q03 """ select * from all_types_par_text_q3;
        """

        sql """
        insert overwrite table all_types_par_text_q3
        VALUES (
          0, -- boolean_col
          -7, -- tinyint_col
          -15, -- smallint_col
          16, -- int_col
          -9223372036854775808, -- bigint_col
          CAST(-123.45 AS FLOAT), -- float_col
          CAST(-123456.789 AS DOUBLE), -- double_col
          CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          'str', -- string_col
          'binary_value', -- binary_col
          '2024-03-25', -- date_col
          '2024-03-25 12:00:00', -- timestamp_col1
          '2024-03-25 12:00:00.123456789', -- timestamp_col2
          '2024-03-25 12:00:00.123456789', -- timestamp_col3
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
          MAP(CAST(3.2 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(3.34 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(5.3456 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(5.34567890 AS DECIMAL(17,8)), CAST(2.34567890 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(2.34567890 AS DECIMAL(18,8)), CAST(2.34567890 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(7.345678901234567890 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
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
          NAMED_STRUCT('struct_field_null', null, 'struct_field_null2', null), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', -123, 'struct_non_nulls_after_nulls2', 'value'), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', -123, 'struct_field2', 'value', 'strict_field3', NAMED_STRUCT('nested_struct_field1', -123, 'nested_struct_field2', 'nested_value')), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
          ARRAY('value1', null, 'value2'), -- t_array_string_with_nulls_in_between
          ARRAY('value11', 'value2', null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240321 -- dt
        );
        """
        order_qt_q04 """ select * from all_types_par_text_q3;
        """

        sql """
        INSERT overwrite table all_types_par_text_q3(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls, dt)
        VALUES (
          CAST(123.45 AS FLOAT), -- float_col
          MAP(1, 10), -- t_map_int
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(null, 'value1', 'value2'), -- t_array_string_starting_with_nulls
          20240321 -- dt
        );
        """
        order_qt_q05 """ select * from all_types_par_text_q3;
        """
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String catalog_name = "test_${hivePrefix}_text_write_insert"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
            );"""
            sql """use `${catalog_name}`.`write_test`"""
            logger.info("hive sql: " + """ use `write_test` """)
            hive_docker """use `write_test`"""

            sql """set enable_fallback_to_original_planner=false;"""

            def format_compressions = ["gzip", "bzip2", "zstd", "snappy", "lz4"]

            for (String format_compression in format_compressions) {
                logger.info("set hive_text_compression = " + format_compression)
                sql """set hive_text_compression = '${format_compression}';"""
                q01(format_compression, catalog_name)
                q02(format_compression, catalog_name)
                q03(format_compression, catalog_name)
                q04(format_compression, catalog_name)
            }
            sql """set hive_text_compression = 'uncompresssed';"""
            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}

