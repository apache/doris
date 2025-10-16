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

suite("test_math_function") {

    testFoldConst""" select acos(cast('nan' as double)), acos(cast('inf' as double)), acos(cast('-inf' as double)), acos(cast('0.0' as double)), acos(cast('1.0' as double)), acos(cast('-1.0' as double)), acos(cast('1.0000001' as double)), acos(cast('-1.0000001' as double)) """
   
    qt_select_acos""" select acos(cast('nan' as double)), acos(cast('inf' as double)), acos(cast('-inf' as double)), acos(cast('0.0' as double)), acos(cast('1.0' as double)), acos(cast('-1.0' as double)), acos(cast('1.0000001' as double)), acos(cast('-1.0000001' as double)) """
   
    testFoldConst""" select acosh(cast('nan' as double)), acosh(cast('inf' as double)), acosh(cast('-inf' as double)), acosh(cast('0.0' as double)), acosh(cast('1.0' as double)), acosh(cast('-1.0' as double)), acosh(cast('0.9999999' as double)), acosh(cast('1.0000001' as double)) """

    qt_select_acosh""" select acosh(cast('nan' as double)), acosh(cast('inf' as double)), acosh(cast('-inf' as double)), acosh(cast('0.0' as double)), acosh(cast('1.0' as double)), acosh(cast('-1.0' as double)), acosh(cast('0.9999999' as double)), acosh(cast('1.0000001' as double)) """

    testFoldConst""" select asin(cast('nan' as double)), asin(cast('inf' as double)), asin(cast('-inf' as double)), asin(cast('0.0' as double)), asin(cast('-0.0' as double)), asin(cast('1.0' as double)), asin(cast('-1.0' as double)), asin(cast('1.0000001' as double)), asin(cast('-1.0000001' as double)) """

    qt_select_asin""" select asin(cast('nan' as double)), asin(cast('inf' as double)), asin(cast('-inf' as double)), asin(cast('0.0' as double)), asin(cast('-0.0' as double)), asin(cast('1.0' as double)), asin(cast('-1.0' as double)), asin(cast('1.0000001' as double)), asin(cast('-1.0000001' as double)) """

    testFoldConst""" select atanh(cast('nan' as double)), atanh(cast('inf' as double)), atanh(cast('-inf' as double)), atanh(cast('0.0' as double)), atanh(cast('-0.0' as double)), atanh(cast('1.0' as double)), atanh(cast('-1.0' as double)), atanh(cast('1.0000001' as double)), atanh(cast('-1.0000001' as double)) """

    qt_select_atanh""" select atanh(cast('nan' as double)), atanh(cast('inf' as double)), atanh(cast('-inf' as double)), atanh(cast('0.0' as double)), atanh(cast('-0.0' as double)), atanh(cast('1.0' as double)), atanh(cast('-1.0' as double)), atanh(cast('1.0000001' as double)), atanh(cast('-1.0000001' as double)) """

    testFoldConst""" select atanh(cast('nan' as double)), atanh(cast('inf' as double)), atanh(cast('-inf' as double)), atanh(cast('0.0' as double)), atanh(cast('-0.0' as double)), atanh(cast('1.0' as double)), atanh(cast('-1.0' as double)), atanh(cast('1.0000001' as double)), atanh(cast('-1.0000001' as double)) """

    qt_select_atanh""" select atanh(cast('nan' as double)), atanh(cast('inf' as double)), atanh(cast('-inf' as double)), atanh(cast('0.0' as double)), atanh(cast('-0.0' as double)), atanh(cast('1.0' as double)), atanh(cast('-1.0' as double)), atanh(cast('1.0000001' as double)), atanh(cast('-1.0000001' as double)) """

    testFoldConst""" select sqrt(cast('nan' as double)), sqrt(cast('inf' as double)), sqrt(cast('-inf' as double)), sqrt(cast('0.0' as double)), sqrt(cast('-0.0' as double)), sqrt(cast('1.0' as double)), sqrt(cast('-1.0' as double)), sqrt(cast('2.0' as double)), sqrt(cast('-2.0' as double)) """

    qt_select_sqrt""" select sqrt(cast('nan' as double)), sqrt(cast('inf' as double)), sqrt(cast('-inf' as double)), sqrt(cast('0.0' as double)), sqrt(cast('-0.0' as double)), sqrt(cast('1.0' as double)), sqrt(cast('-1.0' as double)), sqrt(cast('2.0' as double)), sqrt(cast('-2.0' as double)) """

    testFoldConst""" select asinh(cast('nan' as double)), asinh(cast('inf' as double)), asinh(cast('-inf' as double)), asinh(cast('0.0' as double)), asinh(cast('-0.0' as double)), asinh(cast('1.0' as double)), asinh(cast('-1.0' as double)) """

    qt_select_asinh""" select asinh(cast('nan' as double)), asinh(cast('inf' as double)), asinh(cast('-inf' as double)), asinh(cast('0.0' as double)), asinh(cast('-0.0' as double)), asinh(cast('1.0' as double)), asinh(cast('-1.0' as double))"""

    testFoldConst""" select atan(cast('nan' as double)), atan(cast('inf' as double)), atan(cast('-inf' as double)), atan(cast('0.0' as double)), atan(cast('-0.0' as double)), atan(cast('1.0' as double)), atan(cast('-1.0' as double)), atan(cast('1e308' as double)), atan(cast('-1e308' as double)) """

    qt_select_atan""" select atan(cast('nan' as double)), atan(cast('inf' as double)), atan(cast('-inf' as double)), atan(cast('0.0' as double)), atan(cast('-0.0' as double)), atan(cast('1.0' as double)), atan(cast('-1.0' as double)), atan(cast('1e308' as double)), atan(cast('-1e308' as double)) """

    def mathFuncTestTable = "math_function_test_table";

    sql """ DROP TABLE IF EXISTS ${mathFuncTestTable}; """

    sql """
        CREATE TABLE IF NOT EXISTS ${mathFuncTestTable} (
            id INT,
            single_val DOUBLE,
            y_val DOUBLE,
            x_val DOUBLE
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO ${mathFuncTestTable} VALUES
        (1, 0.0, 0.0, 1.0),
        (2, 1.0, 1.0, 1.0),
        (3, -1.0, -1.0, 1.0),
        (4, 0.5, 1.0, 2.0),
        (5, -0.5, -1.0, 2.0),
        (6, 1.732050808, 3.0, 1.732050808),
        (7, cast('nan' as double), cast('nan' as double), 1.0),
        (8, cast('inf' as double), cast('inf' as double), 1.0),
        (9, cast('-inf' as double), cast('-inf' as double), 1.0),
        (10, 0.0, 0.0, 0.0),
        (11, 1.0, 1.0, 0.0),
        (12, -1.0, -1.0, 0.0),
        (13, 0.0, 0.0, -1.0),
        (14, 1.0, 1.0, -1.0),
        (15, -1.0, -1.0, -1.0),
        (16, cast('inf' as double), cast('inf' as double), cast('inf' as double)),
        (17, cast('-inf' as double), cast('-inf' as double), cast('inf' as double)),
        (18, cast('inf' as double), cast('inf' as double), cast('-inf' as double)),
        (19, cast('-inf' as double), cast('-inf' as double), cast('-inf' as double)),
        (20, 1.0, 1.0, cast('inf' as double)),
        (21, -1.0, -1.0, cast('inf' as double)),
        (22, 1.0, 1.0, cast('-inf' as double)),
        (23, -1.0, -1.0, cast('-inf' as double)),
        (24, -0.0, -0.0, 1.0),
        (25, -0.0, -0.0, -1.0),
        (26, -0.0, -0.0, 0.0),
        (27, -0.0, -0.0, -0.0),
        (28, 1e308, 1e308, 1.0),
        (29, -1e308, -1e308, 1.0);
    """

    testFoldConst""" 
        select
            atan2(cast('nan' as double), cast('1.0' as double)),
            atan2(cast('1.0' as double), cast('nan' as double)),

            atan2(cast('0.0' as double),  cast('1.0' as double)),
            atan2(cast('-0.0' as double), cast('1.0' as double)),
            atan2(cast('0.0' as double),  cast('0.0' as double)),
            atan2(cast('-0.0' as double), cast('0.0' as double)),

            atan2(cast('0.0' as double),  cast('-1.0' as double)),
            atan2(cast('-0.0' as double), cast('-1.0' as double)),
            atan2(cast('0.0' as double),  cast('-0.0' as double)),
            atan2(cast('-0.0' as double), cast('-0.0' as double)),

            atan2(cast('1.0' as double),  cast('0.0' as double)),
            atan2(cast('1.0' as double),  cast('-0.0' as double)),
            atan2(cast('-1.0' as double), cast('0.0' as double)),
            atan2(cast('-1.0' as double), cast('-0.0' as double)),

            atan2(cast('inf' as double),  cast('1.0' as double)),
            atan2(cast('-inf' as double), cast('1.0' as double)),

            atan2(cast('inf' as double),  cast('-inf' as double)),
            atan2(cast('-inf' as double), cast('-inf' as double)),

            atan2(cast('inf' as double),  cast('inf' as double)),
            atan2(cast('-inf' as double), cast('inf' as double)),

            atan2(cast('1.0' as double),  cast('-inf' as double)),
            atan2(cast('-1.0' as double), cast('-inf' as double)),

            atan2(cast('1.0' as double),  cast('inf' as double)),
            atan2(cast('-1.0' as double), cast('inf' as double))
    """

    qt_select_atan2""" 
        select
            atan2(cast('nan' as double), cast('1.0' as double)),
            atan2(cast('1.0' as double), cast('nan' as double)),

            atan2(cast('0.0' as double),  cast('1.0' as double)),
            atan2(cast('-0.0' as double), cast('1.0' as double)),
            atan2(cast('0.0' as double),  cast('0.0' as double)),
            atan2(cast('-0.0' as double), cast('0.0' as double)),

            atan2(cast('0.0' as double),  cast('-1.0' as double)),
            atan2(cast('-0.0' as double), cast('-1.0' as double)),
            atan2(cast('0.0' as double),  cast('-0.0' as double)),
            atan2(cast('-0.0' as double), cast('-0.0' as double)),

            atan2(cast('1.0' as double),  cast('0.0' as double)),
            atan2(cast('1.0' as double),  cast('-0.0' as double)),
            atan2(cast('-1.0' as double), cast('0.0' as double)),
            atan2(cast('-1.0' as double), cast('-0.0' as double)),

            atan2(cast('inf' as double),  cast('1.0' as double)),
            atan2(cast('-inf' as double), cast('1.0' as double)),

            atan2(cast('inf' as double),  cast('-inf' as double)),
            atan2(cast('-inf' as double), cast('-inf' as double)),

            atan2(cast('inf' as double),  cast('inf' as double)),
            atan2(cast('-inf' as double), cast('inf' as double)),

            atan2(cast('1.0' as double),  cast('-inf' as double)),
            atan2(cast('-1.0' as double), cast('-inf' as double)),

            atan2(cast('1.0' as double),  cast('inf' as double)),
            atan2(cast('-1.0' as double), cast('inf' as double))
    """

    qt_select_atan_with_two_args""" 
        select
            atan(cast('nan' as double), cast('1.0' as double)),
            atan(cast('1.0' as double), cast('nan' as double)),

            atan(cast('0.0' as double),  cast('1.0' as double)),
            atan(cast('-0.0' as double), cast('1.0' as double)),
            atan(cast('0.0' as double),  cast('0.0' as double)),
            atan(cast('-0.0' as double), cast('0.0' as double)),

            atan(cast('0.0' as double),  cast('-1.0' as double)),
            atan(cast('-0.0' as double), cast('-1.0' as double)),
            atan(cast('0.0' as double),  cast('-0.0' as double)),
            atan(cast('-0.0' as double), cast('-0.0' as double)),

            atan(cast('1.0' as double),  cast('0.0' as double)),
            atan(cast('1.0' as double),  cast('-0.0' as double)),
            atan(cast('-1.0' as double), cast('0.0' as double)),
            atan(cast('-1.0' as double), cast('-0.0' as double)),

            atan(cast('inf' as double),  cast('1.0' as double)),
            atan(cast('-inf' as double), cast('1.0' as double)),

            atan(cast('inf' as double),  cast('-inf' as double)),
            atan(cast('-inf' as double), cast('-inf' as double)),

            atan(cast('inf' as double),  cast('inf' as double)),
            atan(cast('-inf' as double), cast('inf' as double)),

            atan(cast('1.0' as double),  cast('-inf' as double)),
            atan(cast('-1.0' as double), cast('-inf' as double)),

            atan(cast('1.0' as double),  cast('inf' as double)),
            atan(cast('-1.0' as double), cast('inf' as double))
    """

    qt_select_atan_single_param_from_table """
        SELECT atan(single_val) FROM ${mathFuncTestTable} ORDER BY id;
    """

    qt_select_atan_two_params_from_table """
        SELECT atan(y_val, x_val) FROM ${mathFuncTestTable} ORDER BY id;
    """

    qt_select_atan_with_first_const """
        SELECT atan(cast('1.0' as double), x_val) FROM ${mathFuncTestTable} ORDER BY id;
    """

    qt_select_atan_with_second_const """
        SELECT atan(y_val, cast('1.0' as double)) FROM ${mathFuncTestTable} ORDER BY id;
    """

    testFoldConst""" 
        select
            atan(cast('nan' as double), cast('1.0' as double)),
            atan(cast('1.0' as double), cast('nan' as double)),

            atan(cast('0.0' as double),  cast('1.0' as double)),
            atan(cast('-0.0' as double), cast('1.0' as double)),
            atan(cast('0.0' as double),  cast('0.0' as double)),
            atan(cast('-0.0' as double), cast('0.0' as double)),

            atan(cast('0.0' as double),  cast('-1.0' as double)),
            atan(cast('-0.0' as double), cast('-1.0' as double)),
            atan(cast('0.0' as double),  cast('-0.0' as double)),
            atan(cast('-0.0' as double), cast('-0.0' as double)),

            atan(cast('1.0' as double),  cast('0.0' as double)),
            atan(cast('1.0' as double),  cast('-0.0' as double)),
            atan(cast('-1.0' as double), cast('0.0' as double)),
            atan(cast('-1.0' as double), cast('-0.0' as double)),

            atan(cast('inf' as double),  cast('1.0' as double)),
            atan(cast('-inf' as double), cast('1.0' as double)),

            atan(cast('inf' as double),  cast('-inf' as double)),
            atan(cast('-inf' as double), cast('-inf' as double)),

            atan(cast('inf' as double),  cast('inf' as double)),
            atan(cast('-inf' as double), cast('inf' as double)),

            atan(cast('1.0' as double),  cast('-inf' as double)),
            atan(cast('-1.0' as double), cast('-inf' as double)),

            atan(cast('1.0' as double),  cast('inf' as double)),
            atan(cast('-1.0' as double), cast('inf' as double))
    """

    testFoldConst""" select cbrt(cast('nan' as double)), cbrt(cast('inf' as double)), cbrt(cast('-inf' as double)) """
    qt_select_cbrt""" select cbrt(cast('nan' as double)), cbrt(cast('inf' as double)), cbrt(cast('-inf' as double)) """

    testFoldConst""" select cos(cast('nan' as double)), cos(cast('inf' as double)), cos(cast('-inf' as double)) """
    qt_select_cos""" select cos(cast('nan' as double)), cos(cast('inf' as double)), cos(cast('-inf' as double)) """

    testFoldConst""" select cosh(cast('nan' as double)), cosh(cast('inf' as double)), cosh(cast('-inf' as double)) """
    qt_select_cosh""" select cosh(cast('nan' as double)), cosh(cast('inf' as double)), cosh(cast('-inf' as double)) """

    testFoldConst""" select cot(cast('nan' as double)), cot(cast('inf' as double)), cot(cast('-inf' as double)) """
    qt_select_cot""" select cot(cast('nan' as double)), cot(cast('inf' as double)), cot(cast('-inf' as double)) """

    testFoldConst""" select csc(cast('nan' as double)), csc(cast('inf' as double)), csc(cast('-inf' as double)) """
    qt_select_csc""" select csc(cast('nan' as double)), csc(cast('inf' as double)), csc(cast('-inf' as double)) """

    testFoldConst""" select degrees(cast('nan' as double)), degrees(cast('inf' as double)), degrees(cast('-inf' as double)) """
    qt_select_degrees""" select degrees(cast('nan' as double)), degrees(cast('inf' as double)), degrees(cast('-inf' as double)) """

    testFoldConst""" select sec(cast('nan' as double)), sec(cast('inf' as double)), sec(cast('-inf' as double)) """
    qt_select_sec""" select sec(cast('nan' as double)), sec(cast('inf' as double)), sec(cast('-inf' as double)) """

    testFoldConst""" select sin(cast('nan' as double)), sin(cast('inf' as double)), sin(cast('-inf' as double)) """
    qt_select_sin""" select sin(cast('nan' as double)), sin(cast('inf' as double)), sin(cast('-inf' as double)) """

    testFoldConst""" select sinh(cast('nan' as double)), sinh(cast('inf' as double)), sinh(cast('-inf' as double)) """
    qt_select_sinh""" select sinh(cast('nan' as double)), sinh(cast('inf' as double)), sinh(cast('-inf' as double)) """

    testFoldConst""" select tan(cast('nan' as double)), tan(cast('inf' as double)), tan(cast('-inf' as double)) """
    qt_select_tan""" select tan(cast('nan' as double)), tan(cast('inf' as double)), tan(cast('-inf' as double)) """

    testFoldConst""" select tanh(cast('nan' as double)), tanh(cast('inf' as double)), tanh(cast('-inf' as double)) """
    qt_select_tanh""" select tanh(cast('nan' as double)), tanh(cast('inf' as double)), tanh(cast('-inf' as double)) """

    sql """
        DROP TABLE IF EXISTS ${mathFuncTestTable};
    """
}
