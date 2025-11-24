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


suite("test_cast_to_boolean") {
    // Test casting from integer types to boolean
    sql "set debug_skip_fold_constant=true;"

    sql "set enable_strict_cast=false;"
    qt_cast_int_to_bool """
    SELECT 
        CAST(1 AS BOOLEAN) AS int_1,
        CAST(0 AS BOOLEAN) AS int_0,
        CAST(-1 AS BOOLEAN) AS int_neg_1,
        CAST(9223372036854775807 AS BOOLEAN) AS int_max,
        CAST(-9223372036854775808 AS BOOLEAN) AS int_min;
    """

    // Test casting from string to boolean - standard boolean representations
    qt_cast_str_to_bool_standard """
    SELECT 
        CAST('true' AS BOOLEAN) AS str_true,
        CAST('false' AS BOOLEAN) AS str_false,
        CAST('TRUE' AS BOOLEAN) AS str_TRUE,
        CAST('FALSE' AS BOOLEAN) AS str_FALSE,
        CAST('TrUe' AS BOOLEAN) AS str_TrUe,
        CAST('FaLsE' AS BOOLEAN) AS str_FaLsE;
    """

    // Test casting from string to boolean - numeric representations
    qt_cast_str_to_bool_numeric """
    SELECT 
        CAST('1' AS BOOLEAN) AS str_1,
        CAST('0' AS BOOLEAN) AS str_0,
        CAST(' 1 ' AS BOOLEAN) AS str_space_1_space,
        CAST(' 0 ' AS BOOLEAN) AS str_space_0_space,
        CAST(' 1.1111' AS BOOLEAN) AS str_decimal;
    """

    // Test casting from string to boolean - single character representations
    qt_cast_str_to_bool_single_char """
    SELECT 
        CAST('t' AS BOOLEAN) AS str_t,
        CAST('f' AS BOOLEAN) AS str_f,
        CAST('T' AS BOOLEAN) AS str_T,
        CAST('F' AS BOOLEAN) AS str_F,
        CAST(' t ' AS BOOLEAN) AS str_space_t_space,
        CAST(' f ' AS BOOLEAN) AS str_space_f_space;
    """

    // Test casting from string to boolean - two character representations
    qt_cast_str_to_bool_two_chars """
    SELECT 
        CAST('on' AS BOOLEAN) AS str_on,
        CAST('no' AS BOOLEAN) AS str_no,
        CAST('ON' AS BOOLEAN) AS str_ON,
        CAST('NO' AS BOOLEAN) AS str_NO,
        CAST('On' AS BOOLEAN) AS str_On,
        CAST('nO' AS BOOLEAN) AS str_nO,
        CAST(' on ' AS BOOLEAN) AS str_space_on_space,
        CAST(' no ' AS BOOLEAN) AS str_space_no_space;
    """

    // Test casting from string to boolean - three character representations
    qt_cast_str_to_bool_three_chars """
    SELECT 
        CAST('yes' AS BOOLEAN) AS str_yes,
        CAST('off' AS BOOLEAN) AS str_off,
        CAST('YES' AS BOOLEAN) AS str_YES,
        CAST('OFF' AS BOOLEAN) AS str_OFF,
        CAST('Yes' AS BOOLEAN) AS str_Yes,
        CAST('oFf' AS BOOLEAN) AS str_oFf,
        CAST(' yes ' AS BOOLEAN) AS str_space_yes_space,
        CAST(' off ' AS BOOLEAN) AS str_space_off_space;
    """

    // Test casting from string to boolean - invalid strings
    qt_cast_str_to_bool_invalid """
    SELECT 
        CAST('null' AS BOOLEAN) AS str_null,
        CAST('abc' AS BOOLEAN) AS str_abc,
        CAST('' AS BOOLEAN) AS str_empty,
        CAST(' ' AS BOOLEAN) AS str_space,
        CAST('tr' AS BOOLEAN) AS str_tr,
        CAST('tru' AS BOOLEAN) AS str_tru,
        CAST('fals' AS BOOLEAN) AS str_fals,
        CAST('truth' AS BOOLEAN) AS str_truth;
    """

    // Test casting from floating point to boolean
    qt_cast_float_to_bool """
    SELECT 
        CAST(1.0 AS BOOLEAN) AS float_1_0,
        CAST(0.0 AS BOOLEAN) AS float_0_0, 
        CAST(-1.0 AS BOOLEAN) AS float_neg_1_0,
        CAST(0.5 AS BOOLEAN) AS float_0_5,
        CAST(-0.5 AS BOOLEAN) AS float_neg_0_5;
    """

    // Test casting from boolean to other types
    qt_cast_bool_to_others """
    SELECT 
        CAST(CAST(1 AS BOOLEAN) AS VARCHAR) AS bool_to_str_true,
        CAST(CAST(0 AS BOOLEAN) AS VARCHAR) AS bool_to_str_false,
        CAST(CAST(1 AS BOOLEAN) AS INT) AS bool_to_int_true,
        CAST(CAST(0 AS BOOLEAN) AS INT) AS bool_to_int_false,
        CAST(CAST(1 AS BOOLEAN) AS BIGINT) AS bool_to_bigint_true,
        CAST(CAST(0 AS BOOLEAN) AS BIGINT) AS bool_to_bigint_false;
    """
    
    // Test casting from decimal to boolean
    qt_cast_decimal_to_bool """
    SELECT
        CAST(CAST(1 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_1,
        CAST(CAST(0 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_0,
        CAST(CAST(-1 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_neg_1,
        CAST(CAST(0.5 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_0_5,
        CAST(CAST(-0.5 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_neg_0_5;
    """


    order_qt_cast_str_to_bool_error_strict """
    SELECT 
        CAST('abc' AS BOOLEAN);
    """
    
    order_qt_cast_str_to_bool_error_strict_2 """
    SELECT 
        CAST('null' AS BOOLEAN);
    """
    
    order_qt_cast_str_to_bool_error_strict_3 """
    SELECT 
        CAST('' AS BOOLEAN);
    """
    
    order_qt_cast_str_to_bool_error_strict_4 """
    SELECT 
        CAST('tr' AS BOOLEAN);
    """
    
    order_qt_cast_str_to_bool_error_strict_5 """
    SELECT 
        CAST('truth' AS BOOLEAN);
    """

    // Enable strict mode for the same tests
    sql "set enable_strict_cast=true;"
    
    // Test casting from integer types to boolean (strict mode)
    qt_cast_int_to_bool_strict """
    SELECT 
        CAST(1 AS BOOLEAN) AS int_1,
        CAST(0 AS BOOLEAN) AS int_0,
        CAST(-1 AS BOOLEAN) AS int_neg_1,
        CAST(9223372036854775807 AS BOOLEAN) AS int_max,
        CAST(-9223372036854775808 AS BOOLEAN) AS int_min;
    """

    // Test casting from string to boolean - standard boolean representations (strict mode)
    qt_cast_str_to_bool_standard_strict """
    SELECT 
        CAST('true' AS BOOLEAN) AS str_true,
        CAST('false' AS BOOLEAN) AS str_false,
        CAST('TRUE' AS BOOLEAN) AS str_TRUE,
        CAST('FALSE' AS BOOLEAN) AS str_FALSE,
        CAST('TrUe' AS BOOLEAN) AS str_TrUe,
        CAST('FaLsE' AS BOOLEAN) AS str_FaLsE;
    """

    // Test casting from string to boolean - numeric representations (strict mode)
    qt_cast_str_to_bool_numeric_strict """
    SELECT 
        CAST('1' AS BOOLEAN) AS str_1,
        CAST('0' AS BOOLEAN) AS str_0,
        CAST(' 1 ' AS BOOLEAN) AS str_space_1_space,
        CAST(' 0 ' AS BOOLEAN) AS str_space_0_space;
    """

    // Test casting from string to boolean - various forms (strict mode)
    qt_cast_str_to_bool_special_forms_strict """
    SELECT 
        CAST('t' AS BOOLEAN) AS str_t,
        CAST('f' AS BOOLEAN) AS str_f,
        CAST('on' AS BOOLEAN) AS str_on,
        CAST('no' AS BOOLEAN) AS str_no,
        CAST('yes' AS BOOLEAN) AS str_yes,
        CAST('off' AS BOOLEAN) AS str_off;
    """

    // Test casting from floating point to boolean (strict mode)
    qt_cast_float_to_bool_strict """
    SELECT 
        CAST(1.0 AS BOOLEAN) AS float_1_0,
        CAST(0.0 AS BOOLEAN) AS float_0_0, 
        CAST(-1.0 AS BOOLEAN) AS float_neg_1_0,
        CAST(0.5 AS BOOLEAN) AS float_0_5,
        CAST(-0.5 AS BOOLEAN) AS float_neg_0_5;
    """

    // Test casting from decimal to boolean (strict mode)
    qt_cast_decimal_to_bool_strict """
    SELECT
        CAST(CAST(1 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_1,
        CAST(CAST(0 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_0,
        CAST(CAST(-1 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_neg_1,
        CAST(CAST(0.5 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_0_5,
        CAST(CAST(-0.5 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_neg_0_5;
    """



    test { // wrong grammar. no properties keyword
        sql """
            SELECT CAST('truth' AS BOOLEAN);
        """
        exception ""
    }

// testFoldConst 

    sql "set enable_strict_cast=false;"
    testFoldConst """
    SELECT 
        CAST(1 AS BOOLEAN) AS int_1,
        CAST(0 AS BOOLEAN) AS int_0,
        CAST(-1 AS BOOLEAN) AS int_neg_1,
        CAST(9223372036854775807 AS BOOLEAN) AS int_max,
        CAST(-9223372036854775808 AS BOOLEAN) AS int_min;
    """

    // Test casting from string to boolean - standard boolean representations
    testFoldConst """
    SELECT 
        CAST('true' AS BOOLEAN) AS str_true,
        CAST('false' AS BOOLEAN) AS str_false,
        CAST('TRUE' AS BOOLEAN) AS str_TRUE,
        CAST('FALSE' AS BOOLEAN) AS str_FALSE,
        CAST('TrUe' AS BOOLEAN) AS str_TrUe,
        CAST('FaLsE' AS BOOLEAN) AS str_FaLsE;
    """

    // Test casting from string to boolean - numeric representations
    testFoldConst """
    SELECT 
        CAST('1' AS BOOLEAN) AS str_1,
        CAST('0' AS BOOLEAN) AS str_0,
        CAST(' 1 ' AS BOOLEAN) AS str_space_1_space,
        CAST(' 0 ' AS BOOLEAN) AS str_space_0_space,
        CAST(' 1.1111' AS BOOLEAN) AS str_decimal;
    """

    // Test casting from string to boolean - single character representations
    testFoldConst """
    SELECT 
        CAST('t' AS BOOLEAN) AS str_t,
        CAST('f' AS BOOLEAN) AS str_f,
        CAST('T' AS BOOLEAN) AS str_T,
        CAST('F' AS BOOLEAN) AS str_F,
        CAST(' t ' AS BOOLEAN) AS str_space_t_space,
        CAST(' f ' AS BOOLEAN) AS str_space_f_space;
    """

    // Test casting from string to boolean - two character representations
    testFoldConst """
    SELECT 
        CAST('on' AS BOOLEAN) AS str_on,
        CAST('no' AS BOOLEAN) AS str_no,
        CAST('ON' AS BOOLEAN) AS str_ON,
        CAST('NO' AS BOOLEAN) AS str_NO,
        CAST('On' AS BOOLEAN) AS str_On,
        CAST('nO' AS BOOLEAN) AS str_nO,
        CAST(' on ' AS BOOLEAN) AS str_space_on_space,
        CAST(' no ' AS BOOLEAN) AS str_space_no_space;
    """

    // Test casting from string to boolean - three character representations
    testFoldConst """
    SELECT 
        CAST('yes' AS BOOLEAN) AS str_yes,
        CAST('off' AS BOOLEAN) AS str_off,
        CAST('YES' AS BOOLEAN) AS str_YES,
        CAST('OFF' AS BOOLEAN) AS str_OFF,
        CAST('Yes' AS BOOLEAN) AS str_Yes,
        CAST('oFf' AS BOOLEAN) AS str_oFf,
        CAST(' yes ' AS BOOLEAN) AS str_space_yes_space,
        CAST(' off ' AS BOOLEAN) AS str_space_off_space;
    """

    // Test casting from string to boolean - invalid strings
    testFoldConst """
    SELECT 
        CAST('null' AS BOOLEAN) AS str_null,
        CAST('abc' AS BOOLEAN) AS str_abc,
        CAST('' AS BOOLEAN) AS str_empty,
        CAST(' ' AS BOOLEAN) AS str_space,
        CAST('tr' AS BOOLEAN) AS str_tr,
        CAST('tru' AS BOOLEAN) AS str_tru,
        CAST('fals' AS BOOLEAN) AS str_fals,
        CAST('truth' AS BOOLEAN) AS str_truth;
    """

    // Test casting from floating point to boolean
    testFoldConst """
    SELECT 
        CAST(1.0 AS BOOLEAN) AS float_1_0,
        CAST(0.0 AS BOOLEAN) AS float_0_0, 
        CAST(-1.0 AS BOOLEAN) AS float_neg_1_0,
        CAST(0.5 AS BOOLEAN) AS float_0_5,
        CAST(-0.5 AS BOOLEAN) AS float_neg_0_5;
    """

    // Test casting from boolean to other types
    testFoldConst """
    SELECT 
        CAST(CAST(1 AS BOOLEAN) AS VARCHAR) AS bool_to_str_true,
        CAST(CAST(0 AS BOOLEAN) AS VARCHAR) AS bool_to_str_false,
        CAST(CAST(1 AS BOOLEAN) AS INT) AS bool_to_int_true,
        CAST(CAST(0 AS BOOLEAN) AS INT) AS bool_to_int_false,
        CAST(CAST(1 AS BOOLEAN) AS BIGINT) AS bool_to_bigint_true,
        CAST(CAST(0 AS BOOLEAN) AS BIGINT) AS bool_to_bigint_false;
    """
    
    // Test casting from decimal to boolean
    testFoldConst """
    SELECT
        CAST(CAST(1 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_1,
        CAST(CAST(0 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_0,
        CAST(CAST(-1 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_neg_1,
        CAST(CAST(0.5 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_0_5,
        CAST(CAST(-0.5 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_neg_0_5;
    """


    testFoldConst """
    SELECT 
        CAST('abc' AS BOOLEAN);
    """
    
    testFoldConst """
    SELECT 
        CAST('null' AS BOOLEAN);
    """
    
    testFoldConst """
    SELECT 
        CAST('' AS BOOLEAN);
    """
    
    testFoldConst """
    SELECT 
        CAST('tr' AS BOOLEAN);
    """
    
    testFoldConst """
    SELECT 
        CAST('truth' AS BOOLEAN);
    """

    // Enable strict mode for the same tests
    sql "set enable_strict_cast=true;"
    
    // Test casting from integer types to boolean (strict mode)
    testFoldConst """
    SELECT 
        CAST(1 AS BOOLEAN) AS int_1,
        CAST(0 AS BOOLEAN) AS int_0,
        CAST(-1 AS BOOLEAN) AS int_neg_1,
        CAST(9223372036854775807 AS BOOLEAN) AS int_max,
        CAST(-9223372036854775808 AS BOOLEAN) AS int_min;
    """

    // Test casting from string to boolean - standard boolean representations (strict mode)
    testFoldConst """
    SELECT 
        CAST('true' AS BOOLEAN) AS str_true,
        CAST('false' AS BOOLEAN) AS str_false,
        CAST('TRUE' AS BOOLEAN) AS str_TRUE,
        CAST('FALSE' AS BOOLEAN) AS str_FALSE,
        CAST('TrUe' AS BOOLEAN) AS str_TrUe,
        CAST('FaLsE' AS BOOLEAN) AS str_FaLsE;
    """

    // Test casting from string to boolean - numeric representations (strict mode)
    testFoldConst """
    SELECT 
        CAST('1' AS BOOLEAN) AS str_1,
        CAST('0' AS BOOLEAN) AS str_0,
        CAST(' 1 ' AS BOOLEAN) AS str_space_1_space,
        CAST(' 0 ' AS BOOLEAN) AS str_space_0_space;
    """

    // Test casting from string to boolean - various forms (strict mode)
    testFoldConst """
    SELECT 
        CAST('t' AS BOOLEAN) AS str_t,
        CAST('f' AS BOOLEAN) AS str_f,
        CAST('on' AS BOOLEAN) AS str_on,
        CAST('no' AS BOOLEAN) AS str_no,
        CAST('yes' AS BOOLEAN) AS str_yes,
        CAST('off' AS BOOLEAN) AS str_off;
    """

    // Test casting from floating point to boolean (strict mode)
    testFoldConst """
    SELECT 
        CAST(1.0 AS BOOLEAN) AS float_1_0,
        CAST(0.0 AS BOOLEAN) AS float_0_0, 
        CAST(-1.0 AS BOOLEAN) AS float_neg_1_0,
        CAST(0.5 AS BOOLEAN) AS float_0_5,
        CAST(-0.5 AS BOOLEAN) AS float_neg_0_5;
    """

    // Test casting from decimal to boolean (strict mode)
    testFoldConst """
    SELECT
        CAST(CAST(1 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_1,
        CAST(CAST(0 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_0,
        CAST(CAST(-1 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_neg_1,
        CAST(CAST(0.5 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_0_5,
        CAST(CAST(-0.5 AS DECIMAL(9,2)) AS BOOLEAN) AS dec_neg_0_5;
    """
}