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

suite("test_printf") {
    sql "drop table if exists printf_args;"
    sql """
        create table printf_args (
            k0 int,
            format_str_not_null string not null,
            format_str_null string null,
            format_var_char varchar(10),
            format_char char(10),
            string_arg string,
            var_char_arg varchar(10),
            char_arg char(10),
            byte_arg tinyint,
            short_arg smallint,
            int_arg int,
            long_arg bigint,
            float_arg float,
            double_arg double,
            decimal_arg decimal(10, 2)
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    order_qt_empty_nullable "select printf(format_str_null, string_arg, byte_arg, short_arg, int_arg, long_arg, float_arg, double_arg, decimal_arg) from printf_args"
    order_qt_empty_not_nullable "select printf(format_str_not_null) from printf_args"
    order_qt_empty_partial_nullable "select printf(format_str_not_null, string_arg, byte_arg, short_arg, int_arg, long_arg, float_arg, double_arg, decimal_arg) from printf_args"

    sql """
    insert into printf_args values(
        1,                                                    /* k0 */
        '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf',        /* format_str_not_null */
        null,                                                /* format_str_null */
        '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf',        /* format_var_char */
        '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf',        /* format_char */
        'string',                                            /* string_arg */
        'varchar',                                           /* var_char_arg */
        'char',                                              /* char_arg */
        1,                                                   /* byte_arg */
        2,                                                   /* short_arg */
        3,                                                   /* int_arg */
        4,                                                   /* long_arg */
        5.0,                                                 /* float_arg */
        6.0,                                                 /* double_arg */
        7.0                                                  /* decimal_arg */
    )
    """

    order_qt_all_null "select printf(format_str_null, null, null, null, null, null, null, null, null, null, null) from printf_args"
    order_qt_not_null "select printf(format_str_not_null, string_arg, var_char_arg, char_arg, byte_arg, short_arg, int_arg, long_arg, float_arg, double_arg, decimal_arg) from printf_args"
    order_qt_partial_nullable "select printf(format_str_null, string_arg, var_char_arg, char_arg, byte_arg, short_arg, int_arg, long_arg, float_arg, double_arg, decimal_arg) from printf_args"
    order_qt_nullable_no_null "select printf(format_str_not_null, nullable(string_arg), nullable(var_char_arg), nullable(char_arg), byte_arg, short_arg, int_arg, long_arg, float_arg, double_arg, nullable(decimal_arg)) from printf_args"

    sql "truncate table printf_args"

    sql """
    insert into printf_args values 
        (2, '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', null, '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', 'string', 'varchar', 'char', 1, 2, 3, 4, 5.0, 6.0, 7.0),
        (3, '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', null, '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', 'test', 'test_var', 'test_char', -1, -2, -3, -4, -5.5, -6.6, -7.7),
        (4, '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', null, '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', '特殊字符!@#', '特殊var', '特殊char', 100, 200, 300, 400, 500.5, 600.6, 700.7),
        (5, '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', null, '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', '', '', '', 0, 0, 0, 0, 0.0, 0.0, 0.0),
        (6, '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', null, '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', '%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', 'very_long_string_test', 'long_var', 'long_char', 999999, 999999, 999999, 999999, 999999.9, 999999.9, 999999.9)
    """

    order_qt_all_null "select printf(format_str_null, null, null, null, null, null, null, null, null, null, null) from printf_args"
    order_qt_not_null "select printf(format_str_not_null, string_arg, var_char_arg, char_arg, byte_arg, short_arg, int_arg, long_arg, float_arg, double_arg, decimal_arg) from printf_args"
    order_qt_not_null_var_char "select printf(format_var_char, string_arg, var_char_arg, char_arg, byte_arg, short_arg, int_arg, long_arg, float_arg, double_arg, decimal_arg) from printf_args"
    order_qt_not_null_char "select printf(format_char, string_arg, var_char_arg, char_arg, byte_arg, short_arg, int_arg, long_arg, float_arg, double_arg, decimal_arg) from printf_args"
    order_qt_partial_nullable "select printf(format_str_null, string_arg, var_char_arg, char_arg, byte_arg, short_arg, int_arg, long_arg, float_arg, double_arg, decimal_arg) from printf_args"
    order_qt_nullable_no_null "select printf(format_str_not_null, nullable(string_arg), nullable(var_char_arg), nullable(char_arg), byte_arg, short_arg, int_arg, long_arg, float_arg, double_arg, nullable(decimal_arg)) from printf_args"

    /// consts. most by BE-UT
    order_qt_const_nullable "select printf(format_str_not_null, null, null, null, null, null, null, null, null, null, null) from printf_args"
    order_qt_const_not_nullable "select printf('%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', string_arg, var_char_arg, char_arg, byte_arg, short_arg, int_arg, long_arg, float_arg, double_arg, decimal_arg) from printf_args"
    order_qt_const_nullable_no_null "select printf('%s, %s, %s, %d, %d, %d, %d, %f, %lf, %lf', nullable(string_arg), nullable(var_char_arg), nullable(char_arg), byte_arg, short_arg, int_arg, long_arg, float_arg, double_arg, nullable(decimal_arg)) from printf_args"

    /// cases
    // Basic format specifiers
    order_qt_basic_format_specifiers "select printf('%d', 123)"
    order_qt_basic_format_specifiers "select printf('%5d', 123)"
    order_qt_basic_format_specifiers "select printf('%-5d', 123)"
    order_qt_basic_format_specifiers "select printf('%05d', 123)"
    
    // Integer boundary values
    order_qt_integer_boundary_values "select printf('%d', cast(127 as tinyint))"  // max tinyint
    order_qt_integer_boundary_values "select printf('%d', cast(-128 as tinyint))" // min tinyint
    order_qt_integer_boundary_values "select printf('%d', cast(32767 as smallint))"  // max smallint
    order_qt_integer_boundary_values "select printf('%d', cast(-32768 as smallint))" // min smallint
    order_qt_integer_boundary_values "select printf('%d', 2147483647)"  // max int
    order_qt_integer_boundary_values "select printf('%d', -2147483648)" // min int
    order_qt_integer_boundary_values "select printf('%ld', cast(9223372036854775807 as bigint))"  // max bigint
    order_qt_integer_boundary_values "select printf('%ld', cast(-9223372036854775808 as bigint))" // min bigint
    
    // Float precision and special values
    order_qt_float_precision_and_special_values "select printf('%e', cast(0.0000001 as float))"
    order_qt_float_precision_and_special_values "select printf('%E', 1000000.0)"
    order_qt_float_precision_and_special_values "select printf('%.2f', cast(999999.999 as float))"
    
    // String formatting with various lengths
    order_qt_string_formatting_with_various_lengths "select printf('%s', '')"  // empty string
    order_qt_string_formatting_with_various_lengths "select printf('%20s', 'short')"  // right-aligned padding
    order_qt_string_formatting_with_various_lengths "select printf('%-20s', 'short')" // left-aligned padding
    order_qt_string_formatting_with_various_lengths "select printf('%s', 'very_long_string_that_tests_buffer_handling')"
    order_qt_string_formatting_with_various_lengths '''select printf('%s', '特殊字符!@#$%^&*()')''' // special characters
    
    // Multiple arguments and mixed formats
    order_qt_multiple_arguments_and_mixed_formats "select printf('%d-%s-%.2f', 100, 'test', 3.14)"
    order_qt_multiple_arguments_and_mixed_formats "select printf('%d %d %d %d %d', 1, 2, 3, 4, 5)"
    order_qt_multiple_arguments_and_mixed_formats "select printf('%s=%d, %s=%f', 'int', 42, 'pi', 3.14159)"
    
    // Format string variations
    order_qt_format_string_variations "select printf('no formats')"
    order_qt_format_string_variations "select printf('%%')" // escape %
    order_qt_format_string_variations "select printf('%%%d%%', 100)" // mixed literal % and format
    order_qt_format_string_variations "select printf('%s%s%s', 'a', 'b', 'c')" // consecutive formats
    
    // Decimal number formatting
    order_qt_decimal_number_formatting "select printf('%.2f', cast(123.456 as decimal(10,3)))"
    order_qt_decimal_number_formatting "select printf('%.4f', cast(-987.654321 as decimal(10,6)))"
    order_qt_decimal_number_formatting "select printf('%10.2f', cast(0.01 as decimal(10,2)))"
    
    // Complex combinations
    order_qt_complex_combinations "select printf('Int: %d, Str: %s, Float: %.2f, Hex: %x', 255, 'test', 3.14159, 255)"
    order_qt_complex_combinations "select printf('Padding test: [%10d] [%-10d] [%010d]', 123, 123, 123)"
    order_qt_complex_combinations "select printf('Mixed width: %*d %*s', 5, 10, 10, 'test')"
    
    // Edge cases
    order_qt_edge_cases "select printf('%100s', 'overflow_test')" // large width
    order_qt_edge_cases "select printf('%s %s %s %s %s', 'a', 'b', 'c', 'd', 'e')" // many arguments
    order_qt_edge_cases '''select printf('%1$d %1$d %1$d', 123)''' // same argument multiple times
    order_qt_edge_cases "select printf('%d %o %x %X', 123, 123, 123, 123)" // different integer formats

    // Null handling
    order_qt_null_handling "select printf(null)"
    order_qt_null_handling "select printf('%d %o %x %X', null, null, null, null)"

    /// error cases:
    test {
        sql """ select printf('unsupported format type', cast('2025-03-26' as date)) """
        exception "Function printf does not support printf type: DateV2"
    }
    test {
        sql """ select printf('format failed %d', 3.0) """
        exception "Function printf failed to format string: format failed %d, error: invalid type specifier"
    }

    test {
        sql """ select printf('format complex type %d', map(1, 2)) """
        exception "Function printf does not support printf type: Map"
    }

    test {
        sql """ select printf('format hll type %d', hll_empty()) """
        exception "Function printf does not support printf type: HLL"
    }

    test {
        sql """ select printf('format bitmap type %d', bitmap_empty()) """
        exception "Function printf does not support printf type: BitMap"
    }

    test {
        sql """ select printf('format variant type %d', cast('{"key": "value"}' as variant)) """
        exception "Function printf does not support printf type: Variant"
    }

    test {
        sql """ select printf('format json type %d', cast('{"key": "value"}' as json)) """
        exception "Function printf does not support printf type: JSONB"
    }

    test {
        sql """ select printf('format ipv4 type %d', cast('192.168.1.1' as ipv4)) """
        exception "Function printf does not support printf type: IPv4"
    }

    test {
        sql """ select printf('format ipv6 type %d', cast('2001:db8::1' as ipv6)) """
        exception "Function printf does not support printf type: IPv6"
    }

    test {
        sql """ select printf('format datetimev1 type %d', cast('2025-03-26 10:00:00' as datetime)) """
        exception "Function printf does not support printf type: DateTimeV2"
    }

    test {
        sql """ select printf('format datetimev2 type %d', cast('2025-03-26 10:00:00' as datetimev2)) """
        exception "Function printf does not support printf type: DateTimeV2"
    }

    test {
        sql """ select printf('format datev1 type %d', cast('2025-03-26' as date)) """
        exception "Function printf does not support printf type: DateV2"
    }

    test {
        sql """ select printf('format datev2 type %d', cast('2025-03-26' as datev2)) """
        exception "Function printf does not support printf type: DateV2"
    }

    test {
        sql """ select printf('format array type %d', array(1, 2, 3)) """
        exception "Function printf does not support printf type: Array"
    }

    test {
        sql """ select printf('format struct type %d', struct(1, 'test')) """
        exception "Function printf does not support printf type: Struct"
    }

    test {
        sql """ select printf('format timev1 type %d', cast('10:00:00' as time)) """
        exception "Function printf does not support printf type: timev2"
    }
}
