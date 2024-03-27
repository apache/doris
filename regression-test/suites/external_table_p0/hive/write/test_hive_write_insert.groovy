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

suite("test_hive_write_insert", "p0,external,hive,external_docker,external_docker_hive") {
    def format_compressions = ["parquet_snappy", "orc_zlib"]

    def q01 = { String format_compression, String catalog_name ->
        logger.info("hive sql: " + """ truncate table all_types_${format_compression}; """)
        hive_docker """ truncate table all_types_${format_compression}; """
        sql """refresh catalog ${catalog_name};"""

        sql """
        INSERT INTO all_types_${format_compression}
        VALUES (
          CAST(1 AS BOOLEAN), -- boolean_col
          CAST(127 AS TINYINT), -- tinyint_col
          CAST(32767 AS SMALLINT), -- smallint_col
          CAST(2147483647 AS INT), -- int_col
          CAST(9223372036854775807 AS BIGINT), -- bigint_col
          CAST(123.45 AS FLOAT), -- float_col
          CAST(123456.789 AS DOUBLE), -- double_col
          CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          CAST('string_value' AS STRING), -- string_col
          'binary_value', -- binary_col
          '2024-03-20', -- date_col
          '2024-03-20 12:00:00', -- timestamp_col1
          '2024-03-20 12:00:00.123456789', -- timestamp_col2
          '2024-03-20 12:00:00.123456789', -- timestamp_col3
          CAST('char_value1' AS CHAR(50)), -- char_col1
          CAST('char_value2' AS CHAR(100)), -- char_col2
          CAST('char_value3' AS CHAR(255)), -- char_col3
          CAST('varchar_value1' AS VARCHAR(50)), -- varchar_col1
          CAST('varchar_value2' AS VARCHAR(100)), -- varchar_col2
          CAST('varchar_value3' AS VARCHAR(255)), -- varchar_col3
          MAP(CAST('key1' AS STRING), CAST('value1' AS STRING)), -- t_map_string
          MAP(CAST('key1' AS VARCHAR(65535)), CAST('value1' AS VARCHAR(65535))), -- t_map_varchar
          MAP(CAST('a' AS CHAR(10)), CAST('b' AS CHAR(10))), -- t_map_char
          MAP(CAST(1 AS INT), CAST(10 AS INT)), -- t_map_int
          MAP(CAST(1 AS BIGINT), CAST(100000000000 AS BIGINT)), -- t_map_bigint
          MAP(CAST(1.1 AS FLOAT), CAST(10.1 AS FLOAT)), -- t_map_float
          MAP(CAST(1.1 AS DOUBLE), CAST(10.1 AS DOUBLE)), -- t_map_double
          MAP(CAST(true AS BOOLEAN), CAST(false AS BOOLEAN)), -- t_map_boolean
          MAP(CAST(1.1 AS DECIMAL(2,1)), CAST(1.1 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(1.23 AS DECIMAL(4,2)), CAST(1.23 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(1.2345 AS DECIMAL(8,4)), CAST(1.2345 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(1.23456789 AS DECIMAL(17,8)), CAST(1.23456789 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(1.23456789 AS DECIMAL(18,8)), CAST(1.23456789 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(1.234567890123456789 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY(CAST('string1' AS STRING), CAST('string2' AS STRING)), -- t_array_string
          ARRAY(CAST(1 AS INT), CAST(2 AS INT), CAST(3 AS INT)), -- t_array_int
          ARRAY(CAST(100000000000 AS BIGINT), CAST(200000000000 AS BIGINT)), -- t_array_bigint
          ARRAY(CAST(1.1 AS FLOAT), CAST(2.2 AS FLOAT)), -- t_array_float
          ARRAY(CAST(1.123456789 AS DOUBLE), CAST(2.123456789 AS DOUBLE)), -- t_array_double
          ARRAY(CAST(true AS BOOLEAN), CAST(false AS BOOLEAN)), -- t_array_boolean
          ARRAY(CAST('varchar1' AS VARCHAR(65535)), CAST('varchar2' AS VARCHAR(65535))), -- t_array_varchar
          ARRAY(CAST('char1' AS CHAR(10)), CAST('char2' AS CHAR(10))), -- t_array_char
          ARRAY(CAST(1.1 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
          ARRAY(CAST(1.23 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(1.23456789 AS DECIMAL(17,8)), CAST(2.34567891 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(1.23456789 AS DECIMAL(18,8)), CAST(2.34567891 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', CAST(1234567890 AS BIGINT)), -- t_struct_bigint
          MAP(CAST('key' AS STRING), ARRAY(NAMED_STRUCT('s_int', CAST(123 AS INT)))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING))), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', CAST(null AS STRING), 'struct_field_null2', CAST(null AS STRING)), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', CAST(123 AS INT), 'struct_non_nulls_after_nulls2', CAST('value' AS STRING)), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', CAST(123 AS INT), 'struct_field2', CAST('value' AS STRING), 'strict_field3', NAMED_STRUCT('nested_struct_field1', CAST(123 AS INT), 'nested_struct_field2', CAST('nested_value' AS STRING))), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, CAST('value1' AS STRING), CAST('value2' AS STRING)), -- t_array_string_starting_with_nulls
          ARRAY(CAST('value1' AS STRING), null, CAST('value2' AS STRING)), -- t_array_string_with_nulls_in_between
          ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING), null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240320 -- dt
        );
        """
        order_qt_q01 """ select * from all_types_${format_compression};
        """

        sql """
        INSERT INTO all_types_${format_compression}
        VALUES (
          CAST(1 AS BOOLEAN), -- boolean_col
          CAST(127 AS TINYINT), -- tinyint_col
          CAST(32767 AS SMALLINT), -- smallint_col
          CAST(2147483647 AS INT), -- int_col
          CAST(9223372036854775807 AS BIGINT), -- bigint_col
          CAST(123.45 AS FLOAT), -- float_col
          CAST(123456.789 AS DOUBLE), -- double_col
          CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          CAST('string_value' AS STRING), -- string_col
          'binary_value', -- binary_col
          '2024-03-20', -- date_col
          '2024-03-20 12:00:00', -- timestamp_col1
          '2024-03-20 12:00:00.123456789', -- timestamp_col2
          '2024-03-20 12:00:00.123456789', -- timestamp_col3
          CAST('char_value1' AS CHAR(50)), -- char_col1
          CAST('char_value2' AS CHAR(100)), -- char_col2
          CAST('char_value3' AS CHAR(255)), -- char_col3
          CAST('varchar_value1' AS VARCHAR(50)), -- varchar_col1
          CAST('varchar_value2' AS VARCHAR(100)), -- varchar_col2
          CAST('varchar_value3' AS VARCHAR(255)), -- varchar_col3
          MAP(CAST('key1' AS STRING), CAST('value1' AS STRING)), -- t_map_string
          MAP(CAST('key1' AS VARCHAR(65535)), CAST('value1' AS VARCHAR(65535))), -- t_map_varchar
          MAP(CAST('a' AS CHAR(10)), CAST('b' AS CHAR(10))), -- t_map_char
          MAP(CAST(1 AS INT), CAST(10 AS INT)), -- t_map_int
          MAP(CAST(1 AS BIGINT), CAST(100000000000 AS BIGINT)), -- t_map_bigint
          MAP(CAST(1.1 AS FLOAT), CAST(10.1 AS FLOAT)), -- t_map_float
          MAP(CAST(1.1 AS DOUBLE), CAST(10.1 AS DOUBLE)), -- t_map_double
          MAP(CAST(true AS BOOLEAN), CAST(false AS BOOLEAN)), -- t_map_boolean
          MAP(CAST(1.1 AS DECIMAL(2,1)), CAST(1.1 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(1.23 AS DECIMAL(4,2)), CAST(1.23 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(1.2345 AS DECIMAL(8,4)), CAST(1.2345 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(1.23456789 AS DECIMAL(17,8)), CAST(1.23456789 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(1.23456789 AS DECIMAL(18,8)), CAST(1.23456789 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(1.234567890123456789 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY(CAST('string1' AS STRING), CAST('string2' AS STRING)), -- t_array_string
          ARRAY(CAST(1 AS INT), CAST(2 AS INT), CAST(3 AS INT)), -- t_array_int
          ARRAY(CAST(100000000000 AS BIGINT), CAST(200000000000 AS BIGINT)), -- t_array_bigint
          ARRAY(CAST(1.1 AS FLOAT), CAST(2.2 AS FLOAT)), -- t_array_float
          ARRAY(CAST(1.123456789 AS DOUBLE), CAST(2.123456789 AS DOUBLE)), -- t_array_double
          ARRAY(CAST(true AS BOOLEAN), CAST(false AS BOOLEAN)), -- t_array_boolean
          ARRAY(CAST('varchar1' AS VARCHAR(65535)), CAST('varchar2' AS VARCHAR(65535))), -- t_array_varchar
          ARRAY(CAST('char1' AS CHAR(10)), CAST('char2' AS CHAR(10))), -- t_array_char
          ARRAY(CAST(1.1 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
          ARRAY(CAST(1.23 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(1.23456789 AS DECIMAL(17,8)), CAST(2.34567891 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(1.23456789 AS DECIMAL(18,8)), CAST(2.34567891 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', CAST(1234567890 AS BIGINT)), -- t_struct_bigint
          MAP(CAST('key' AS STRING), ARRAY(NAMED_STRUCT('s_int', CAST(123 AS INT)))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING))), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', CAST(null AS STRING), 'struct_field_null2', CAST(null AS STRING)), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', CAST(123 AS INT), 'struct_non_nulls_after_nulls2', CAST('value' AS STRING)), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', CAST(123 AS INT), 'struct_field2', CAST('value' AS STRING), 'strict_field3', NAMED_STRUCT('nested_struct_field1', CAST(123 AS INT), 'nested_struct_field2', CAST('nested_value' AS STRING))), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, CAST('value1' AS STRING), CAST('value2' AS STRING)), -- t_array_string_starting_with_nulls
          ARRAY(CAST('value1' AS STRING), null, CAST('value2' AS STRING)), -- t_array_string_with_nulls_in_between
          ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING), null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240320 -- dt
       ),
       (
          CAST(0 AS BOOLEAN), -- boolean_col
          CAST(-128 AS TINYINT), -- tinyint_col
          CAST(-32768 AS SMALLINT), -- smallint_col
          CAST(-2147483648 AS INT), -- int_col
          CAST(-9223372036854775808 AS BIGINT), -- bigint_col
          CAST(-123.45 AS FLOAT), -- float_col
          CAST(-123456.789 AS DOUBLE), -- double_col
          CAST(-123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          CAST('string_value' AS STRING), -- string_col
          'binary_value', -- binary_col
          '2024-03-21', -- date_col
          '2024-03-21 12:00:00', -- timestamp_col1
          '2024-03-21 12:00:00.123456789', -- timestamp_col2
          '2024-03-21 12:00:00.123456789', -- timestamp_col3
          CAST('char_value1' AS CHAR(50)), -- char_col1
          CAST('char_value2' AS CHAR(100)), -- char_col2
          CAST('char_value3' AS CHAR(255)), -- char_col3
          CAST('varchar_value1' AS VARCHAR(50)), -- varchar_col1
          CAST('varchar_value2' AS VARCHAR(100)), -- varchar_col2
          CAST('varchar_value3' AS VARCHAR(255)), -- varchar_col3
          MAP(CAST('key1' AS STRING), CAST('value1' AS STRING)), -- t_map_string
          MAP(CAST('key1' AS VARCHAR(65535)), CAST('value1' AS VARCHAR(65535))), -- t_map_varchar
          MAP(CAST('x' AS CHAR(10)), CAST('y' AS CHAR(10))), -- t_map_char
          MAP(CAST(2 AS INT), CAST(20 AS INT)), -- t_map_int
          MAP(CAST(2 AS BIGINT), CAST(200000000000 AS BIGINT)), -- t_map_bigint
          MAP(CAST(2.2 AS FLOAT), CAST(20.2 AS FLOAT)), -- t_map_float
          MAP(CAST(2.2 AS DOUBLE), CAST(20.2 AS DOUBLE)), -- t_map_double
          MAP(CAST(false AS BOOLEAN), CAST(true AS BOOLEAN)), -- t_map_boolean
          MAP(CAST(2.2 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(2.34 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(2.3456 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(2.34567890 AS DECIMAL(17,8)), CAST(2.34567890 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(2.34567890 AS DECIMAL(18,8)), CAST(2.34567890 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(2.345678901234567890 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY(CAST('string1' AS STRING), CAST('string2' AS STRING)), -- t_array_string
          ARRAY(CAST(4 AS INT), CAST(5 AS INT), CAST(6 AS INT)), -- t_array_int
          ARRAY(CAST(300000000000 AS BIGINT), CAST(400000000000 AS BIGINT)), -- t_array_bigint
          ARRAY(CAST(3.3 AS FLOAT), CAST(4.4 AS FLOAT)), -- t_array_float
          ARRAY(CAST(3.123456789 AS DOUBLE), CAST(4.123456789 AS DOUBLE)), -- t_array_double
          ARRAY(CAST(false AS BOOLEAN), CAST(true AS BOOLEAN)), -- t_array_boolean
          ARRAY(CAST('varchar1' AS VARCHAR(65535)), CAST('varchar2' AS VARCHAR(65535))), -- t_array_varchar
          ARRAY(CAST('char1' AS CHAR(10)), CAST('char2' AS CHAR(10))), -- t_array_char
          ARRAY(CAST(3.3 AS DECIMAL(2,1)), CAST(4.4 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
          ARRAY(CAST(3.45 AS DECIMAL(4,2)), CAST(4.56 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
          ARRAY(CAST(3.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(3.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', CAST(-1234567890 AS BIGINT)), -- t_struct_bigint
          MAP(CAST('key' AS STRING), ARRAY(NAMED_STRUCT('s_int', CAST(-123 AS INT)))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING))), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', CAST(null AS STRING), 'struct_field_null2', CAST(null AS STRING)), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', CAST(-123 AS INT), 'struct_non_nulls_after_nulls2', CAST('value' AS STRING)), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', CAST(-123 AS INT), 'struct_field2', CAST('value' AS STRING), 'strict_field3', NAMED_STRUCT('nested_struct_field1', CAST(-123 AS INT), 'nested_struct_field2', CAST('nested_value' AS STRING))), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, CAST('value1' AS STRING), CAST('value2' AS STRING)), -- t_array_string_starting_with_nulls
          ARRAY(CAST('value1' AS STRING), null, CAST('value2' AS STRING)), -- t_array_string_with_nulls_in_between
          ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING), null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240321 -- dt
        ),
        (
          CAST(0 AS BOOLEAN), -- boolean_col
          CAST(-128 AS TINYINT), -- tinyint_col
          CAST(-32768 AS SMALLINT), -- smallint_col
          CAST(-2147483648 AS INT), -- int_col
          CAST(-9223372036854775808 AS BIGINT), -- bigint_col
          CAST(-123.45 AS FLOAT), -- float_col
          CAST(-123456.789 AS DOUBLE), -- double_col
          CAST(-123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          CAST('string_value' AS STRING), -- string_col
          'binary_value', -- binary_col
          '2024-03-22', -- date_col
          '2024-03-22 12:00:00', -- timestamp_col1
          '2024-03-22 12:00:00.123456789', -- timestamp_col2
          '2024-03-22 12:00:00.123456789', -- timestamp_col3
          CAST('char_value1' AS CHAR(50)), -- char_col1
          CAST('char_value2' AS CHAR(100)), -- char_col2
          CAST('char_value3' AS CHAR(255)), -- char_col3
          CAST('varchar_value1' AS VARCHAR(50)), -- varchar_col1
          CAST('varchar_value2' AS VARCHAR(100)), -- varchar_col2
          CAST('varchar_value3' AS VARCHAR(255)), -- varchar_col3
          MAP(CAST('key1' AS STRING), CAST('value1' AS STRING)), -- t_map_string
          MAP(CAST('key1' AS VARCHAR(65535)), CAST('value1' AS VARCHAR(65535))), -- t_map_varchar
          MAP(CAST('x' AS CHAR(10)), CAST('y' AS CHAR(10))), -- t_map_char
          MAP(CAST(3 AS INT), CAST(20 AS INT)), -- t_map_int
          MAP(CAST(3 AS BIGINT), CAST(200000000000 AS BIGINT)), -- t_map_bigint
          MAP(CAST(3.2 AS FLOAT), CAST(20.2 AS FLOAT)), -- t_map_float
          MAP(CAST(3.2 AS DOUBLE), CAST(20.2 AS DOUBLE)), -- t_map_double
          MAP(CAST(false AS BOOLEAN), CAST(true AS BOOLEAN)), -- t_map_boolean
          MAP(CAST(3.2 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(3.34 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(2.3456 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(2.34567890 AS DECIMAL(17,8)), CAST(2.34567890 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(2.34567890 AS DECIMAL(18,8)), CAST(2.34567890 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(3.345678901234567890 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY(CAST('string1' AS STRING), CAST('string2' AS STRING)), -- t_array_string
          ARRAY(CAST(4 AS INT), CAST(5 AS INT), CAST(6 AS INT)), -- t_array_int
          ARRAY(CAST(300000000000 AS BIGINT), CAST(400000000000 AS BIGINT)), -- t_array_bigint
          ARRAY(CAST(3.3 AS FLOAT), CAST(4.4 AS FLOAT)), -- t_array_float
          ARRAY(CAST(3.123456789 AS DOUBLE), CAST(4.123456789 AS DOUBLE)), -- t_array_double
          ARRAY(CAST(false AS BOOLEAN), CAST(true AS BOOLEAN)), -- t_array_boolean
          ARRAY(CAST('varchar1' AS VARCHAR(65535)), CAST('varchar2' AS VARCHAR(65535))), -- t_array_varchar
          ARRAY(CAST('char1' AS CHAR(10)), CAST('char2' AS CHAR(10))), -- t_array_char
          ARRAY(CAST(3.3 AS DECIMAL(2,1)), CAST(4.4 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
          ARRAY(CAST(3.45 AS DECIMAL(4,2)), CAST(4.56 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
          ARRAY(CAST(8.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(3.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', CAST(-1234567890 AS BIGINT)), -- t_struct_bigint
          MAP(CAST('key' AS STRING), ARRAY(NAMED_STRUCT('s_int', CAST(-123 AS INT)))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING))), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', CAST(null AS STRING), 'struct_field_null2', CAST(null AS STRING)), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', CAST(-123 AS INT), 'struct_non_nulls_after_nulls2', CAST('value' AS STRING)), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', CAST(-123 AS INT), 'struct_field2', CAST('value' AS STRING), 'strict_field3', NAMED_STRUCT('nested_struct_field1', CAST(-123 AS INT), 'nested_struct_field2', CAST('nested_value' AS STRING))), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, CAST('value1' AS STRING), CAST('value2' AS STRING)), -- t_array_string_starting_with_nulls
          ARRAY(CAST('value1' AS STRING), null, CAST('value2' AS STRING)), -- t_array_string_with_nulls_in_between
          ARRAY(CAST('value11' AS STRING), CAST('value2' AS STRING), null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240322 -- dt
        );
        """
        order_qt_q02 """ select * from all_types_${format_compression};
        """

        sql """
        INSERT INTO all_types_${format_compression}(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls)
        VALUES (
          CAST(123.45 AS FLOAT), -- float_col
          MAP(CAST(1 AS INT), CAST(10 AS INT)), -- t_map_int
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(null, CAST('value1' AS STRING), CAST('value2' AS STRING)) -- t_array_string_starting_with_nulls
        );
        """
        order_qt_q03 """ select * from all_types_${format_compression};
        """

        sql """
        insert overwrite table all_types_${format_compression}
        VALUES (
          CAST(0 AS BOOLEAN), -- boolean_col
          CAST(-7 AS TINYINT), -- tinyint_col
          CAST(-15 AS SMALLINT), -- smallint_col
          CAST(16 AS INT), -- int_col
          CAST(-9223372036854775808 AS BIGINT), -- bigint_col
          CAST(-123.45 AS FLOAT), -- float_col
          CAST(-123456.789 AS DOUBLE), -- double_col
          CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          CAST('str' AS STRING), -- string_col
          'binary_value', -- binary_col
          '2024-03-25', -- date_col
          '2024-03-25 12:00:00', -- timestamp_col1
          '2024-03-25 12:00:00.123456789', -- timestamp_col2
          '2024-03-25 12:00:00.123456789', -- timestamp_col3
          CAST('char_value11111' AS CHAR(50)), -- char_col1
          CAST('char_value22222' AS CHAR(100)), -- char_col2
          CAST('char_value33333' AS CHAR(255)), -- char_col3
          CAST('varchar_value11111' AS VARCHAR(50)), -- varchar_col1
          CAST('varchar_value22222' AS VARCHAR(100)), -- varchar_col2
          CAST('varchar_value33333' AS VARCHAR(255)), -- varchar_col3
          MAP(CAST('key7' AS STRING), CAST('value1' AS STRING)), -- t_map_string
          MAP(CAST('key7' AS VARCHAR(65535)), CAST('value1' AS VARCHAR(65535))), -- t_map_varchar
          MAP(CAST('x' AS CHAR(10)), CAST('y' AS CHAR(10))), -- t_map_char
          MAP(CAST(3 AS INT), CAST(20 AS INT)), -- t_map_int
          MAP(CAST(3 AS BIGINT), CAST(200000000000 AS BIGINT)), -- t_map_bigint
          MAP(CAST(3.2 AS FLOAT), CAST(20.2 AS FLOAT)), -- t_map_float
          MAP(CAST(3.2 AS DOUBLE), CAST(20.2 AS DOUBLE)), -- t_map_double
          MAP(CAST(false AS BOOLEAN), CAST(true AS BOOLEAN)), -- t_map_boolean
          MAP(CAST(3.2 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(3.34 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(5.3456 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(5.34567890 AS DECIMAL(17,8)), CAST(2.34567890 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(2.34567890 AS DECIMAL(18,8)), CAST(2.34567890 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(7.345678901234567890 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY(CAST('string1' AS STRING), CAST('string2' AS STRING)), -- t_array_string
          ARRAY(CAST(4 AS INT), CAST(5 AS INT), CAST(6 AS INT)), -- t_array_int
          ARRAY(CAST(300000000000 AS BIGINT), CAST(400000000000 AS BIGINT)), -- t_array_bigint
          ARRAY(CAST(3.3 AS FLOAT), CAST(4.4 AS FLOAT)), -- t_array_float
          ARRAY(CAST(3.123456789 AS DOUBLE), CAST(4.123456789 AS DOUBLE)), -- t_array_double
          ARRAY(CAST(false AS BOOLEAN), CAST(true AS BOOLEAN)), -- t_array_boolean
          ARRAY(CAST('varchar1' AS VARCHAR(65535)), CAST('varchar2' AS VARCHAR(65535))), -- t_array_varchar
          ARRAY(CAST('char1' AS CHAR(10)), CAST('char2' AS CHAR(10))), -- t_array_char
          ARRAY(CAST(3.3 AS DECIMAL(2,1)), CAST(4.4 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
          ARRAY(CAST(3.45 AS DECIMAL(4,2)), CAST(4.56 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
          ARRAY(CAST(9.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(6.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', CAST(-1234567890 AS BIGINT)), -- t_struct_bigint
          MAP(CAST('key' AS STRING), ARRAY(NAMED_STRUCT('s_int', CAST(-123 AS INT)))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING))), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', CAST(null AS STRING), 'struct_field_null2', CAST(null AS STRING)), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', CAST(-123 AS INT), 'struct_non_nulls_after_nulls2', CAST('value' AS STRING)), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', CAST(-123 AS INT), 'struct_field2', CAST('value' AS STRING), 'strict_field3', NAMED_STRUCT('nested_struct_field1', CAST(-123 AS INT), 'nested_struct_field2', CAST('nested_value' AS STRING))), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, CAST('value1' AS STRING), CAST('value2' AS STRING)), -- t_array_string_starting_with_nulls
          ARRAY(CAST('value1' AS STRING), null, CAST('value2' AS STRING)), -- t_array_string_with_nulls_in_between
          ARRAY(CAST('value11' AS STRING), CAST('value2' AS STRING), null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240325 -- dt
        );
        """
        order_qt_q04 """ select * from all_types_${format_compression};
        """

        sql """
        INSERT overwrite table all_types_${format_compression}(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls, dt)
        VALUES (
          CAST(123.45 AS FLOAT), -- float_col
          MAP(CAST(1 AS INT), CAST(10 AS INT)), -- t_map_int
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(null, CAST('value1' AS STRING), CAST('value2' AS STRING)), -- t_array_string_starting_with_nulls
          20240321 -- dt
        );
        """
        order_qt_q05 """ select * from all_types_${format_compression};
        """

        logger.info("hive sql: " + """ truncate table all_types_${format_compression}; """)
        hive_docker """ truncate table all_types_${format_compression}; """
        order_qt_q06 """ select * from all_types_${format_compression};
        """
    }

    def q02 = { String format_compression, String catalog_name ->
        logger.info("hive sql: " + """ truncate table all_types_${format_compression}; """)
        hive_docker """ truncate table all_types_${format_compression}; """
        sql """refresh catalog ${catalog_name};"""

        sql """
        INSERT INTO all_types_${format_compression}
        SELECT * FROM all_types_parquet_snappy_src;
        """
        order_qt_q01 """ select * from all_types_${format_compression};
        """

        sql """
        INSERT INTO all_types_${format_compression}
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
        order_qt_q02 """ select * from all_types_${format_compression};
        """

        sql """
        INSERT INTO all_types_${format_compression}(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls)
        SELECT float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls FROM all_types_parquet_snappy_src;
        """
        order_qt_q03 """
        select * from all_types_${format_compression};
        """

        sql """
        INSERT OVERWRITE TABLE all_types_${format_compression}(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls)
        SELECT float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls FROM all_types_parquet_snappy_src;
        """
        order_qt_q04 """
        select * from all_types_${format_compression};
        """

        logger.info("hive sql: " + """ truncate table all_types_${format_compression}; """)
        hive_docker """ truncate table all_types_${format_compression}; """
        order_qt_q05 """
        select * from all_types_${format_compression};
        """
    }
    def q03 = { String format_compression, String catalog_name ->
        logger.info("hive sql: " + """ DROP TABLE IF EXISTS all_types_par_${format_compression}_${catalog_name}_q03; """)
        hive_docker """ DROP TABLE IF EXISTS all_types_par_${format_compression}_${catalog_name}_q03; """
        logger.info("hive sql: " + """CREATE TABLE IF NOT EXISTS all_types_par_${format_compression}_${catalog_name}_q03 like all_types_par_${format_compression};""")
        hive_docker """ CREATE TABLE IF NOT EXISTS all_types_par_${format_compression}_${catalog_name}_q03 like all_types_par_${format_compression}; """
        sql """refresh catalog ${catalog_name};"""

        sql """
        INSERT INTO all_types_par_${format_compression}_${catalog_name}_q03
        VALUES (
          CAST(1 AS BOOLEAN), -- boolean_col
          CAST(127 AS TINYINT), -- tinyint_col
          CAST(32767 AS SMALLINT), -- smallint_col
          CAST(2147483647 AS INT), -- int_col
          CAST(9223372036854775807 AS BIGINT), -- bigint_col
          CAST(123.45 AS FLOAT), -- float_col
          CAST(123456.789 AS DOUBLE), -- double_col
          CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          CAST('string_value' AS STRING), -- string_col
          'binary_value', -- binary_col
          '2024-03-20', -- date_col
          '2024-03-20 12:00:00', -- timestamp_col1
          '2024-03-20 12:00:00.123456789', -- timestamp_col2
          '2024-03-20 12:00:00.123456789', -- timestamp_col3
          CAST('char_value1' AS CHAR(50)), -- char_col1
          CAST('char_value2' AS CHAR(100)), -- char_col2
          CAST('char_value3' AS CHAR(255)), -- char_col3
          CAST('varchar_value1' AS VARCHAR(50)), -- varchar_col1
          CAST('varchar_value2' AS VARCHAR(100)), -- varchar_col2
          CAST('varchar_value3' AS VARCHAR(255)), -- varchar_col3
          MAP(CAST('key1' AS STRING), CAST('value1' AS STRING)), -- t_map_string
          MAP(CAST('key1' AS VARCHAR(65535)), CAST('value1' AS VARCHAR(65535))), -- t_map_varchar
          MAP(CAST('a' AS CHAR(10)), CAST('b' AS CHAR(10))), -- t_map_char
          MAP(CAST(1 AS INT), CAST(10 AS INT)), -- t_map_int
          MAP(CAST(1 AS BIGINT), CAST(100000000000 AS BIGINT)), -- t_map_bigint
          MAP(CAST(1.1 AS FLOAT), CAST(10.1 AS FLOAT)), -- t_map_float
          MAP(CAST(1.1 AS DOUBLE), CAST(10.1 AS DOUBLE)), -- t_map_double
          MAP(CAST(true AS BOOLEAN), CAST(false AS BOOLEAN)), -- t_map_boolean
          MAP(CAST(1.1 AS DECIMAL(2,1)), CAST(1.1 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(1.23 AS DECIMAL(4,2)), CAST(1.23 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(1.2345 AS DECIMAL(8,4)), CAST(1.2345 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(1.23456789 AS DECIMAL(17,8)), CAST(1.23456789 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(1.23456789 AS DECIMAL(18,8)), CAST(1.23456789 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(1.234567890123456789 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY(CAST('string1' AS STRING), CAST('string2' AS STRING)), -- t_array_string
          ARRAY(CAST(1 AS INT), CAST(2 AS INT), CAST(3 AS INT)), -- t_array_int
          ARRAY(CAST(100000000000 AS BIGINT), CAST(200000000000 AS BIGINT)), -- t_array_bigint
          ARRAY(CAST(1.1 AS FLOAT), CAST(2.2 AS FLOAT)), -- t_array_float
          ARRAY(CAST(1.123456789 AS DOUBLE), CAST(2.123456789 AS DOUBLE)), -- t_array_double
          ARRAY(CAST(true AS BOOLEAN), CAST(false AS BOOLEAN)), -- t_array_boolean
          ARRAY(CAST('varchar1' AS VARCHAR(65535)), CAST('varchar2' AS VARCHAR(65535))), -- t_array_varchar
          ARRAY(CAST('char1' AS CHAR(10)), CAST('char2' AS CHAR(10))), -- t_array_char
          ARRAY(CAST(1.1 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
          ARRAY(CAST(1.23 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(1.23456789 AS DECIMAL(17,8)), CAST(2.34567891 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(1.23456789 AS DECIMAL(18,8)), CAST(2.34567891 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', CAST(1234567890 AS BIGINT)), -- t_struct_bigint
          MAP(CAST('key' AS STRING), ARRAY(NAMED_STRUCT('s_int', CAST(123 AS INT)))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING))), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', CAST(null AS STRING), 'struct_field_null2', CAST(null AS STRING)), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', CAST(123 AS INT), 'struct_non_nulls_after_nulls2', CAST('value' AS STRING)), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', CAST(123 AS INT), 'struct_field2', CAST('value' AS STRING), 'strict_field3', NAMED_STRUCT('nested_struct_field1', CAST(123 AS INT), 'nested_struct_field2', CAST('nested_value' AS STRING))), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, CAST('value1' AS STRING), CAST('value2' AS STRING)), -- t_array_string_starting_with_nulls
          ARRAY(CAST('value1' AS STRING), null, CAST('value2' AS STRING)), -- t_array_string_with_nulls_in_between
          ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING), null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240320 -- dt
        );
        """
        order_qt_q01 """ select * from all_types_par_${format_compression}_${catalog_name}_q03;
        """

        sql """
        INSERT INTO all_types_par_${format_compression}_${catalog_name}_q03
        VALUES  (
          CAST(1 AS BOOLEAN), -- boolean_col
          CAST(127 AS TINYINT), -- tinyint_col
          CAST(32767 AS SMALLINT), -- smallint_col
          CAST(2147483647 AS INT), -- int_col
          CAST(9223372036854775807 AS BIGINT), -- bigint_col
          CAST(123.45 AS FLOAT), -- float_col
          CAST(123456.789 AS DOUBLE), -- double_col
          CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          CAST('string_value' AS STRING), -- string_col
          'binary_value', -- binary_col
          '2024-03-20', -- date_col
          '2024-03-20 12:00:00', -- timestamp_col1
          '2024-03-20 12:00:00.123456789', -- timestamp_col2
          '2024-03-20 12:00:00.123456789', -- timestamp_col3
          CAST('char_value1' AS CHAR(50)), -- char_col1
          CAST('char_value2' AS CHAR(100)), -- char_col2
          CAST('char_value3' AS CHAR(255)), -- char_col3
          CAST('varchar_value1' AS VARCHAR(50)), -- varchar_col1
          CAST('varchar_value2' AS VARCHAR(100)), -- varchar_col2
          CAST('varchar_value3' AS VARCHAR(255)), -- varchar_col3
          MAP(CAST('key1' AS STRING), CAST('value1' AS STRING)), -- t_map_string
          MAP(CAST('key1' AS VARCHAR(65535)), CAST('value1' AS VARCHAR(65535))), -- t_map_varchar
          MAP(CAST('a' AS CHAR(10)), CAST('b' AS CHAR(10))), -- t_map_char
          MAP(CAST(1 AS INT), CAST(10 AS INT)), -- t_map_int
          MAP(CAST(1 AS BIGINT), CAST(100000000000 AS BIGINT)), -- t_map_bigint
          MAP(CAST(1.1 AS FLOAT), CAST(10.1 AS FLOAT)), -- t_map_float
          MAP(CAST(1.1 AS DOUBLE), CAST(10.1 AS DOUBLE)), -- t_map_double
          MAP(CAST(true AS BOOLEAN), CAST(false AS BOOLEAN)), -- t_map_boolean
          MAP(CAST(1.1 AS DECIMAL(2,1)), CAST(1.1 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(1.23 AS DECIMAL(4,2)), CAST(1.23 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(1.2345 AS DECIMAL(8,4)), CAST(1.2345 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(1.23456789 AS DECIMAL(17,8)), CAST(1.23456789 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(1.23456789 AS DECIMAL(18,8)), CAST(1.23456789 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(1.234567890123456789 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY(CAST('string1' AS STRING), CAST('string2' AS STRING)), -- t_array_string
          ARRAY(CAST(1 AS INT), CAST(2 AS INT), CAST(3 AS INT)), -- t_array_int
          ARRAY(CAST(100000000000 AS BIGINT), CAST(200000000000 AS BIGINT)), -- t_array_bigint
          ARRAY(CAST(1.1 AS FLOAT), CAST(2.2 AS FLOAT)), -- t_array_float
          ARRAY(CAST(1.123456789 AS DOUBLE), CAST(2.123456789 AS DOUBLE)), -- t_array_double
          ARRAY(CAST(true AS BOOLEAN), CAST(false AS BOOLEAN)), -- t_array_boolean
          ARRAY(CAST('varchar1' AS VARCHAR(65535)), CAST('varchar2' AS VARCHAR(65535))), -- t_array_varchar
          ARRAY(CAST('char1' AS CHAR(10)), CAST('char2' AS CHAR(10))), -- t_array_char
          ARRAY(CAST(1.1 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
          ARRAY(CAST(1.23 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(1.23456789 AS DECIMAL(17,8)), CAST(2.34567891 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(1.23456789 AS DECIMAL(18,8)), CAST(2.34567891 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(1.234567890123456789 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', CAST(1234567890 AS BIGINT)), -- t_struct_bigint
          MAP(CAST('key' AS STRING), ARRAY(NAMED_STRUCT('s_int', CAST(123 AS INT)))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING))), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', CAST(null AS STRING), 'struct_field_null2', CAST(null AS STRING)), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', CAST(123 AS INT), 'struct_non_nulls_after_nulls2', CAST('value' AS STRING)), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', CAST(123 AS INT), 'struct_field2', CAST('value' AS STRING), 'strict_field3', NAMED_STRUCT('nested_struct_field1', CAST(123 AS INT), 'nested_struct_field2', CAST('nested_value' AS STRING))), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, CAST('value1' AS STRING), CAST('value2' AS STRING)), -- t_array_string_starting_with_nulls
          ARRAY(CAST('value1' AS STRING), null, CAST('value2' AS STRING)), -- t_array_string_with_nulls_in_between
          ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING), null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240320 -- dt
       ),
       (
          CAST(0 AS BOOLEAN), -- boolean_col
          CAST(-128 AS TINYINT), -- tinyint_col
          CAST(-32768 AS SMALLINT), -- smallint_col
          CAST(-2147483648 AS INT), -- int_col
          CAST(-9223372036854775808 AS BIGINT), -- bigint_col
          CAST(-123.45 AS FLOAT), -- float_col
          CAST(-123456.789 AS DOUBLE), -- double_col
          CAST(-123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          CAST('string_value' AS STRING), -- string_col
          'binary_value', -- binary_col
          '2024-03-21', -- date_col
          '2024-03-21 12:00:00', -- timestamp_col1
          '2024-03-21 12:00:00.123456789', -- timestamp_col2
          '2024-03-21 12:00:00.123456789', -- timestamp_col3
          CAST('char_value1' AS CHAR(50)), -- char_col1
          CAST('char_value2' AS CHAR(100)), -- char_col2
          CAST('char_value3' AS CHAR(255)), -- char_col3
          CAST('varchar_value1' AS VARCHAR(50)), -- varchar_col1
          CAST('varchar_value2' AS VARCHAR(100)), -- varchar_col2
          CAST('varchar_value3' AS VARCHAR(255)), -- varchar_col3
          MAP(CAST('key1' AS STRING), CAST('value1' AS STRING)), -- t_map_string
          MAP(CAST('key1' AS VARCHAR(65535)), CAST('value1' AS VARCHAR(65535))), -- t_map_varchar
          MAP(CAST('x' AS CHAR(10)), CAST('y' AS CHAR(10))), -- t_map_char
          MAP(CAST(2 AS INT), CAST(20 AS INT)), -- t_map_int
          MAP(CAST(2 AS BIGINT), CAST(200000000000 AS BIGINT)), -- t_map_bigint
          MAP(CAST(2.2 AS FLOAT), CAST(20.2 AS FLOAT)), -- t_map_float
          MAP(CAST(2.2 AS DOUBLE), CAST(20.2 AS DOUBLE)), -- t_map_double
          MAP(CAST(false AS BOOLEAN), CAST(true AS BOOLEAN)), -- t_map_boolean
          MAP(CAST(2.2 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(2.34 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(2.3456 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(2.34567890 AS DECIMAL(17,8)), CAST(2.34567890 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(2.34567890 AS DECIMAL(18,8)), CAST(2.34567890 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(2.345678901234567890 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY(CAST('string1' AS STRING), CAST('string2' AS STRING)), -- t_array_string
          ARRAY(CAST(4 AS INT), CAST(5 AS INT), CAST(6 AS INT)), -- t_array_int
          ARRAY(CAST(300000000000 AS BIGINT), CAST(400000000000 AS BIGINT)), -- t_array_bigint
          ARRAY(CAST(3.3 AS FLOAT), CAST(4.4 AS FLOAT)), -- t_array_float
          ARRAY(CAST(3.123456789 AS DOUBLE), CAST(4.123456789 AS DOUBLE)), -- t_array_double
          ARRAY(CAST(false AS BOOLEAN), CAST(true AS BOOLEAN)), -- t_array_boolean
          ARRAY(CAST('varchar1' AS VARCHAR(65535)), CAST('varchar2' AS VARCHAR(65535))), -- t_array_varchar
          ARRAY(CAST('char1' AS CHAR(10)), CAST('char2' AS CHAR(10))), -- t_array_char
          ARRAY(CAST(3.3 AS DECIMAL(2,1)), CAST(4.4 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
          ARRAY(CAST(3.45 AS DECIMAL(4,2)), CAST(4.56 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
          ARRAY(CAST(3.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(3.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', CAST(-1234567890 AS BIGINT)), -- t_struct_bigint
          MAP(CAST('key' AS STRING), ARRAY(NAMED_STRUCT('s_int', CAST(-123 AS INT)))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING))), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', CAST(null AS STRING), 'struct_field_null2', CAST(null AS STRING)), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', CAST(-123 AS INT), 'struct_non_nulls_after_nulls2', CAST('value' AS STRING)), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', CAST(-123 AS INT), 'struct_field2', CAST('value' AS STRING), 'strict_field3', NAMED_STRUCT('nested_struct_field1', CAST(-123 AS INT), 'nested_struct_field2', CAST('nested_value' AS STRING))), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, CAST('value1' AS STRING), CAST('value2' AS STRING)), -- t_array_string_starting_with_nulls
          ARRAY(CAST('value1' AS STRING), null, CAST('value2' AS STRING)), -- t_array_string_with_nulls_in_between
          ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING), null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240321 -- dt
        ),
        (
          CAST(0 AS BOOLEAN), -- boolean_col
          CAST(-128 AS TINYINT), -- tinyint_col
          CAST(-32768 AS SMALLINT), -- smallint_col
          CAST(-2147483648 AS INT), -- int_col
          CAST(-9223372036854775808 AS BIGINT), -- bigint_col
          CAST(-123.45 AS FLOAT), -- float_col
          CAST(-123456.789 AS DOUBLE), -- double_col
          CAST(-123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          CAST('string_value' AS STRING), -- string_col
          'binary_value', -- binary_col
          '2024-03-22', -- date_col
          '2024-03-22 12:00:00', -- timestamp_col1
          '2024-03-22 12:00:00.123456789', -- timestamp_col2
          '2024-03-22 12:00:00.123456789', -- timestamp_col3
          CAST('char_value1' AS CHAR(50)), -- char_col1
          CAST('char_value2' AS CHAR(100)), -- char_col2
          CAST('char_value3' AS CHAR(255)), -- char_col3
          CAST('varchar_value1' AS VARCHAR(50)), -- varchar_col1
          CAST('varchar_value2' AS VARCHAR(100)), -- varchar_col2
          CAST('varchar_value3' AS VARCHAR(255)), -- varchar_col3
          MAP(CAST('key1' AS STRING), CAST('value1' AS STRING)), -- t_map_string
          MAP(CAST('key1' AS VARCHAR(65535)), CAST('value1' AS VARCHAR(65535))), -- t_map_varchar
          MAP(CAST('x' AS CHAR(10)), CAST('y' AS CHAR(10))), -- t_map_char
          MAP(CAST(3 AS INT), CAST(20 AS INT)), -- t_map_int
          MAP(CAST(3 AS BIGINT), CAST(200000000000 AS BIGINT)), -- t_map_bigint
          MAP(CAST(3.2 AS FLOAT), CAST(20.2 AS FLOAT)), -- t_map_float
          MAP(CAST(3.2 AS DOUBLE), CAST(20.2 AS DOUBLE)), -- t_map_double
          MAP(CAST(false AS BOOLEAN), CAST(true AS BOOLEAN)), -- t_map_boolean
          MAP(CAST(3.2 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(3.34 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(2.3456 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(2.34567890 AS DECIMAL(17,8)), CAST(2.34567890 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(2.34567890 AS DECIMAL(18,8)), CAST(2.34567890 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(3.345678901234567890 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY(CAST('string1' AS STRING), CAST('string2' AS STRING)), -- t_array_string
          ARRAY(CAST(4 AS INT), CAST(5 AS INT), CAST(6 AS INT)), -- t_array_int
          ARRAY(CAST(300000000000 AS BIGINT), CAST(400000000000 AS BIGINT)), -- t_array_bigint
          ARRAY(CAST(3.3 AS FLOAT), CAST(4.4 AS FLOAT)), -- t_array_float
          ARRAY(CAST(3.123456789 AS DOUBLE), CAST(4.123456789 AS DOUBLE)), -- t_array_double
          ARRAY(CAST(false AS BOOLEAN), CAST(true AS BOOLEAN)), -- t_array_boolean
          ARRAY(CAST('varchar1' AS VARCHAR(65535)), CAST('varchar2' AS VARCHAR(65535))), -- t_array_varchar
          ARRAY(CAST('char1' AS CHAR(10)), CAST('char2' AS CHAR(10))), -- t_array_char
          ARRAY(CAST(3.3 AS DECIMAL(2,1)), CAST(4.4 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
          ARRAY(CAST(3.45 AS DECIMAL(4,2)), CAST(4.56 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
          ARRAY(CAST(8.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(3.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', CAST(-1234567890 AS BIGINT)), -- t_struct_bigint
          MAP(CAST('key' AS STRING), ARRAY(NAMED_STRUCT('s_int', CAST(-123 AS INT)))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING))), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', CAST(null AS STRING), 'struct_field_null2', CAST(null AS STRING)), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', CAST(-123 AS INT), 'struct_non_nulls_after_nulls2', CAST('value' AS STRING)), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', CAST(-123 AS INT), 'struct_field2', CAST('value' AS STRING), 'strict_field3', NAMED_STRUCT('nested_struct_field1', CAST(-123 AS INT), 'nested_struct_field2', CAST('nested_value' AS STRING))), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, CAST('value1' AS STRING), CAST('value2' AS STRING)), -- t_array_string_starting_with_nulls
          ARRAY(CAST('value1' AS STRING), null, CAST('value2' AS STRING)), -- t_array_string_with_nulls_in_between
          ARRAY(CAST('value11' AS STRING), CAST('value2' AS STRING), null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240322 -- dt
        );
        """
        order_qt_q02 """ select * from all_types_par_${format_compression}_${catalog_name}_q03;
        """

        sql """
        INSERT INTO all_types_par_${format_compression}_${catalog_name}_q03(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls, dt)
        VALUES (
          CAST(123.45 AS FLOAT), -- float_col
          MAP(CAST(1 AS INT), CAST(10 AS INT)), -- t_map_int
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(null, CAST('value1' AS STRING), CAST('value2' AS STRING)), -- t_array_string_starting_with_nulls
          20240321 -- dt
        );
        """
        order_qt_q03 """ select * from all_types_par_${format_compression}_${catalog_name}_q03;
        """

        sql """
        insert overwrite table all_types_par_${format_compression}_${catalog_name}_q03
        VALUES (
          CAST(0 AS BOOLEAN), -- boolean_col
          CAST(-7 AS TINYINT), -- tinyint_col
          CAST(-15 AS SMALLINT), -- smallint_col
          CAST(16 AS INT), -- int_col
          CAST(-9223372036854775808 AS BIGINT), -- bigint_col
          CAST(-123.45 AS FLOAT), -- float_col
          CAST(-123456.789 AS DOUBLE), -- double_col
          CAST(123456789 AS DECIMAL(9,0)), -- decimal_col1
          CAST(-1234.5678 AS DECIMAL(8,4)), -- decimal_col2
          CAST(-123456.789012 AS DECIMAL(18,6)), -- decimal_col3
          CAST(-123456789.012345678901 AS DECIMAL(38,12)), -- decimal_col4
          CAST('str' AS STRING), -- string_col
          'binary_value', -- binary_col
          '2024-03-25', -- date_col
          '2024-03-25 12:00:00', -- timestamp_col1
          '2024-03-25 12:00:00.123456789', -- timestamp_col2
          '2024-03-25 12:00:00.123456789', -- timestamp_col3
          CAST('char_value11111' AS CHAR(50)), -- char_col1
          CAST('char_value22222' AS CHAR(100)), -- char_col2
          CAST('char_value33333' AS CHAR(255)), -- char_col3
          CAST('varchar_value11111' AS VARCHAR(50)), -- varchar_col1
          CAST('varchar_value22222' AS VARCHAR(100)), -- varchar_col2
          CAST('varchar_value33333' AS VARCHAR(255)), -- varchar_col3
          MAP(CAST('key7' AS STRING), CAST('value1' AS STRING)), -- t_map_string
          MAP(CAST('key7' AS VARCHAR(65535)), CAST('value1' AS VARCHAR(65535))), -- t_map_varchar
          MAP(CAST('x' AS CHAR(10)), CAST('y' AS CHAR(10))), -- t_map_char
          MAP(CAST(3 AS INT), CAST(20 AS INT)), -- t_map_int
          MAP(CAST(3 AS BIGINT), CAST(200000000000 AS BIGINT)), -- t_map_bigint
          MAP(CAST(3.2 AS FLOAT), CAST(20.2 AS FLOAT)), -- t_map_float
          MAP(CAST(3.2 AS DOUBLE), CAST(20.2 AS DOUBLE)), -- t_map_double
          MAP(CAST(false AS BOOLEAN), CAST(true AS BOOLEAN)), -- t_map_boolean
          MAP(CAST(3.2 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))), -- t_map_decimal_precision_2
          MAP(CAST(3.34 AS DECIMAL(4,2)), CAST(2.34 AS DECIMAL(4,2))), -- t_map_decimal_precision_4
          MAP(CAST(5.3456 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_map_decimal_precision_8
          MAP(CAST(5.34567890 AS DECIMAL(17,8)), CAST(2.34567890 AS DECIMAL(17,8))), -- t_map_decimal_precision_17
          MAP(CAST(2.34567890 AS DECIMAL(18,8)), CAST(2.34567890 AS DECIMAL(18,8))), -- t_map_decimal_precision_18
          MAP(CAST(7.345678901234567890 AS DECIMAL(38,16)), CAST(2.345678901234567890 AS DECIMAL(38,16))), -- t_map_decimal_precision_38
          ARRAY(CAST('string1' AS STRING), CAST('string2' AS STRING)), -- t_array_string
          ARRAY(CAST(4 AS INT), CAST(5 AS INT), CAST(6 AS INT)), -- t_array_int
          ARRAY(CAST(300000000000 AS BIGINT), CAST(400000000000 AS BIGINT)), -- t_array_bigint
          ARRAY(CAST(3.3 AS FLOAT), CAST(4.4 AS FLOAT)), -- t_array_float
          ARRAY(CAST(3.123456789 AS DOUBLE), CAST(4.123456789 AS DOUBLE)), -- t_array_double
          ARRAY(CAST(false AS BOOLEAN), CAST(true AS BOOLEAN)), -- t_array_boolean
          ARRAY(CAST('varchar1' AS VARCHAR(65535)), CAST('varchar2' AS VARCHAR(65535))), -- t_array_varchar
          ARRAY(CAST('char1' AS CHAR(10)), CAST('char2' AS CHAR(10))), -- t_array_char
          ARRAY(CAST(3.3 AS DECIMAL(2,1)), CAST(4.4 AS DECIMAL(2,1))), -- t_array_decimal_precision_2
          ARRAY(CAST(3.45 AS DECIMAL(4,2)), CAST(4.56 AS DECIMAL(4,2))), -- t_array_decimal_precision_4
          ARRAY(CAST(9.4567 AS DECIMAL(8,4)), CAST(4.5678 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(CAST(6.45678901 AS DECIMAL(17,8)), CAST(4.56789012 AS DECIMAL(17,8))), -- t_array_decimal_precision_17
          ARRAY(CAST(3.45678901 AS DECIMAL(18,8)), CAST(4.56789012 AS DECIMAL(18,8))), -- t_array_decimal_precision_18
          ARRAY(CAST(3.456789012345678901 AS DECIMAL(38,16)), CAST(4.567890123456789012 AS DECIMAL(38,16))), -- t_array_decimal_precision_38
          NAMED_STRUCT('s_bigint', CAST(-1234567890 AS BIGINT)), -- t_struct_bigint
          MAP(CAST('key' AS STRING), ARRAY(NAMED_STRUCT('s_int', CAST(-123 AS INT)))), -- t_complex
          NAMED_STRUCT('struct_field', ARRAY(CAST('value1' AS STRING), CAST('value2' AS STRING))), -- t_struct_nested
          NAMED_STRUCT('struct_field_null', CAST(null AS STRING), 'struct_field_null2', CAST(null AS STRING)), -- t_struct_null
          NAMED_STRUCT('struct_non_nulls_after_nulls1', CAST(-123 AS INT), 'struct_non_nulls_after_nulls2', CAST('value' AS STRING)), -- t_struct_non_nulls_after_nulls
          NAMED_STRUCT('struct_field1', CAST(-123 AS INT), 'struct_field2', CAST('value' AS STRING), 'strict_field3', NAMED_STRUCT('nested_struct_field1', CAST(-123 AS INT), 'nested_struct_field2', CAST('nested_value' AS STRING))), -- t_nested_struct_non_nulls_after_nulls
          MAP('null_key', null), -- t_map_null_value
          ARRAY(null, CAST('value1' AS STRING), CAST('value2' AS STRING)), -- t_array_string_starting_with_nulls
          ARRAY(CAST('value1' AS STRING), null, CAST('value2' AS STRING)), -- t_array_string_with_nulls_in_between
          ARRAY(CAST('value11' AS STRING), CAST('value2' AS STRING), null), -- t_array_string_ending_with_nulls
          ARRAY(null, null, null), -- t_array_string_all_nulls
          20240321 -- dt
        );
        """
        order_qt_q04 """ select * from all_types_par_${format_compression}_${catalog_name}_q03;
        """

        sql """
        INSERT overwrite table all_types_par_${format_compression}_${catalog_name}_q03(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls, dt)
        VALUES (
          CAST(123.45 AS FLOAT), -- float_col
          MAP(CAST(1 AS INT), CAST(10 AS INT)), -- t_map_int
          ARRAY(CAST(1.2345 AS DECIMAL(8,4)), CAST(2.3456 AS DECIMAL(8,4))), -- t_array_decimal_precision_8
          ARRAY(null, CAST('value1' AS STRING), CAST('value2' AS STRING)), -- t_array_string_starting_with_nulls
          20240321 -- dt
        );
        """
        order_qt_q05 """ select * from all_types_par_${format_compression}_${catalog_name}_q03;
        """

        logger.info("hive sql: " + """ DROP TABLE IF EXISTS all_types_par_${format_compression}_${catalog_name}_q03; """)
        hive_docker """ DROP TABLE IF EXISTS all_types_par_${format_compression}_${catalog_name}_q03; """
    }

    def q04 = { String format_compression, String catalog_name ->
        logger.info("hive sql: " + """ DROP TABLE IF EXISTS all_types_par_${format_compression}_${catalog_name}_q04; """)
        hive_docker """ DROP TABLE IF EXISTS all_types_par_${format_compression}_${catalog_name}_q04; """
        logger.info("hive sql: " + """ CREATE TABLE IF NOT EXISTS all_types_par_${format_compression}_${catalog_name}_q04 like all_types_par_${format_compression}; """)
        hive_docker """ CREATE TABLE IF NOT EXISTS all_types_par_${format_compression}_${catalog_name}_q04 like all_types_par_${format_compression}; """
        sql """refresh catalog ${catalog_name};"""

        sql """
        INSERT INTO all_types_par_${format_compression}_${catalog_name}_q04
        SELECT * FROM all_types_par_parquet_snappy_src;
        """
        order_qt_q01 """ select * from all_types_par_${format_compression}_${catalog_name}_q04;
        """

        sql """
        INSERT INTO all_types_par_${format_compression}_${catalog_name}_q04
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
        order_qt_q02 """ select * from all_types_par_${format_compression}_${catalog_name}_q04;
        """

        sql """
        INSERT INTO all_types_par_${format_compression}_${catalog_name}_q04(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls, dt)
        SELECT float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls, dt FROM all_types_parquet_snappy_src;
        """
        order_qt_q03 """ select * from all_types_par_${format_compression}_${catalog_name}_q04;
        """

        sql """
        INSERT OVERWRITE TABLE all_types_par_${format_compression}_${catalog_name}_q04(float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls, dt)
        SELECT float_col, t_map_int, t_array_decimal_precision_8, t_array_string_starting_with_nulls, dt FROM all_types_parquet_snappy_src;
        """
        order_qt_q04 """
        select * from all_types_par_${format_compression}_${catalog_name}_q04;
        """

        logger.info("hive sql: " + """ DROP TABLE IF EXISTS all_types_par_${format_compression}_${catalog_name}_q04; """)
        hive_docker """ DROP TABLE IF EXISTS all_types_par_${format_compression}_${catalog_name}_q04; """
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String hms_port = context.config.otherConfigs.get("hms_port")
            String hdfs_port = context.config.otherConfigs.get("hdfs_port")
            String catalog_name = "test_hive_write_insert"
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

            for (String format_compression in format_compressions) {
                logger.info("Process format_compression" + format_compression)
                q01(format_compression, catalog_name)
                q02(format_compression, catalog_name)
                q03(format_compression, catalog_name)
                q04(format_compression, catalog_name)
            }

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}

