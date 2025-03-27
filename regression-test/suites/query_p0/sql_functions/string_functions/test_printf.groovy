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

    /// folding
    // Basic format specifiers
    check_fold_consistency "printf('%d', 123)"
    check_fold_consistency "printf('%5d', 123)"
    check_fold_consistency "printf('%-5d', 123)"
    check_fold_consistency "printf('%05d', 123)"
    
    // Integer boundary values
    check_fold_consistency "printf('%d', cast(127 as tinyint))"  // max tinyint
    check_fold_consistency "printf('%d', cast(-128 as tinyint))" // min tinyint
    check_fold_consistency "printf('%d', cast(32767 as smallint))"  // max smallint
    check_fold_consistency "printf('%d', cast(-32768 as smallint))" // min smallint
    check_fold_consistency "printf('%d', 2147483647)"  // max int
    check_fold_consistency "printf('%d', -2147483648)" // min int
    check_fold_consistency "printf('%ld', cast(9223372036854775807 as bigint))"  // max bigint
    check_fold_consistency "printf('%ld', cast(-9223372036854775808 as bigint))" // min bigint
    
    // Float precision and special values
    check_fold_consistency "printf('%e', cast(0.0000001 as float))"
    check_fold_consistency "printf('%E', 1000000.0)"
    check_fold_consistency "printf('%.2f', cast(999999.999 as float))"
    
    // String formatting with various lengths
    check_fold_consistency "printf('%s', '')"  // empty string
    check_fold_consistency "printf('%20s', 'short')"  // right-aligned padding
    check_fold_consistency "printf('%-20s', 'short')" // left-aligned padding
    check_fold_consistency "printf('%s', 'very_long_string_that_tests_buffer_handling')"
    check_fold_consistency '''printf('%s', '特殊字符!@#$%^&*()')''' // special characters
    
    // Multiple arguments and mixed formats
    check_fold_consistency "printf('%d-%s-%.2f', 100, 'test', 3.14)"
    check_fold_consistency "printf('%d %d %d %d %d', 1, 2, 3, 4, 5)"
    check_fold_consistency "printf('%s=%d, %s=%f', 'int', 42, 'pi', 3.14159)"
    
    // Format string variations
    check_fold_consistency "printf('no formats')"
    check_fold_consistency "printf('%%')" // escape %
    check_fold_consistency "printf('%%%d%%', 100)" // mixed literal % and format
    check_fold_consistency "printf('%s%s%s', 'a', 'b', 'c')" // consecutive formats
    
    // Decimal number formatting
    check_fold_consistency "printf('%.2f', cast(123.456 as decimal(10,3)))"
    check_fold_consistency "printf('%.4f', cast(-987.654321 as decimal(10,6)))"
    check_fold_consistency "printf('%10.2f', cast(0.01 as decimal(10,2)))"
    
    // Complex combinations
    check_fold_consistency "printf('Int: %d, Str: %s, Float: %.2f, Hex: %x', 255, 'test', 3.14159, 255)"
    check_fold_consistency "printf('Padding test: [%10d] [%-10d] [%010d]', 123, 123, 123)"
    check_fold_consistency "printf('Mixed width: %*d %*s', 5, 10, 10, 'test')"
    
    // Edge cases
    check_fold_consistency "printf('%100s', 'overflow_test')" // large width
    check_fold_consistency "printf('%s %s %s %s %s', 'a', 'b', 'c', 'd', 'e')" // many arguments
    check_fold_consistency '''printf('%1$d %1$d %1$d', 123)''' // same argument multiple times
    check_fold_consistency "printf('%d %o %x %X', 123, 123, 123, 123)" // different integer formats

    // Null handling
    check_fold_consistency "printf(null)"
    check_fold_consistency "printf('%d %o %x %X', null, null, null, null)"

    /// error cases:
    test {
        sql """ select printf('unsupported format type', cast('2025-03-26' as date)) """
        exception "Function printf does not support printf type: DateV2"
    }
    test {
        sql """ select printf('format failed %d', 3.0) """
        exception "Function printf failed to format string: format failed %d, error: invalid type specifier"
    }
}
