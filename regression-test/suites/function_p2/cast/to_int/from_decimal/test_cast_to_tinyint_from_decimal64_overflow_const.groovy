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


suite("test_cast_to_tinyint_from_decimal64_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_tinyint_from_decimal64_overflow_0_test_data = ["""128""","""-129""","""9999999999""","""-9999999999""","""9999999998""","""-9999999998"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_tinyint_from_decimal64_overflow_0_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(10, 0)) as tinyint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_tinyint_from_decimal64_overflow_0_test_data) {
        qt_sql_test_cast_to_tinyint_from_decimal64_overflow_0 """select cast(cast("${test_str}" as decimalv3(10, 0)) as tinyint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 0)) as tinyint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_tinyint_from_decimal64_overflow_1_test_data = ["""128.00000""","""128.00001""","""128.00009""","""128.99999""","""128.99998""","""128.09999""","""128.90000""","""128.90001""","""-129.00000""","""-129.00001""","""-129.00009""","""-129.99999""","""-129.99998""","""-129.09999""","""-129.90000""","""-129.90001""","""99999.00000""","""99999.00001""","""99999.00009""","""99999.99999""",
        """99999.99998""","""99999.09999""","""99999.90000""","""99999.90001""","""-99999.00000""","""-99999.00001""","""-99999.00009""","""-99999.99999""","""-99999.99998""","""-99999.09999""","""-99999.90000""","""-99999.90001""","""99998.00000""","""99998.00001""","""99998.00009""","""99998.99999""","""99998.99998""","""99998.09999""","""99998.90000""","""99998.90001""",
        """-99998.00000""","""-99998.00001""","""-99998.00009""","""-99998.99999""","""-99998.99998""","""-99998.09999""","""-99998.90000""","""-99998.90001"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_tinyint_from_decimal64_overflow_1_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(10, 5)) as tinyint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_tinyint_from_decimal64_overflow_1_test_data) {
        qt_sql_test_cast_to_tinyint_from_decimal64_overflow_1 """select cast(cast("${test_str}" as decimalv3(10, 5)) as tinyint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 5)) as tinyint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_tinyint_from_decimal64_overflow_4_test_data = ["""128""","""-129""","""999999999999999999""","""-999999999999999999""","""999999999999999998""","""-999999999999999998"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_tinyint_from_decimal64_overflow_4_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(18, 0)) as tinyint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_tinyint_from_decimal64_overflow_4_test_data) {
        qt_sql_test_cast_to_tinyint_from_decimal64_overflow_4 """select cast(cast("${test_str}" as decimalv3(18, 0)) as tinyint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 0)) as tinyint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_tinyint_from_decimal64_overflow_5_test_data = ["""128.0""","""128.1""","""128.9""","""128.9""","""128.8""","""128.0""","""128.9""","""128.8""","""-129.0""","""-129.1""","""-129.9""","""-129.9""","""-129.8""","""-129.0""","""-129.9""","""-129.8""","""99999999999999999.0""","""99999999999999999.1""","""99999999999999999.9""","""99999999999999999.9""",
        """99999999999999999.8""","""99999999999999999.0""","""99999999999999999.9""","""99999999999999999.8""","""-99999999999999999.0""","""-99999999999999999.1""","""-99999999999999999.9""","""-99999999999999999.9""","""-99999999999999999.8""","""-99999999999999999.0""","""-99999999999999999.9""","""-99999999999999999.8""","""99999999999999998.0""","""99999999999999998.1""","""99999999999999998.9""","""99999999999999998.9""","""99999999999999998.8""","""99999999999999998.0""","""99999999999999998.9""","""99999999999999998.8""",
        """-99999999999999998.0""","""-99999999999999998.1""","""-99999999999999998.9""","""-99999999999999998.9""","""-99999999999999998.8""","""-99999999999999998.0""","""-99999999999999998.9""","""-99999999999999998.8"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_tinyint_from_decimal64_overflow_5_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(18, 1)) as tinyint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_tinyint_from_decimal64_overflow_5_test_data) {
        qt_sql_test_cast_to_tinyint_from_decimal64_overflow_5 """select cast(cast("${test_str}" as decimalv3(18, 1)) as tinyint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 1)) as tinyint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_tinyint_from_decimal64_overflow_6_test_data = ["""128.000000000""","""128.000000001""","""128.000000009""","""128.999999999""","""128.999999998""","""128.099999999""","""128.900000000""","""128.900000001""","""-129.000000000""","""-129.000000001""","""-129.000000009""","""-129.999999999""","""-129.999999998""","""-129.099999999""","""-129.900000000""","""-129.900000001""","""999999999.000000000""","""999999999.000000001""","""999999999.000000009""","""999999999.999999999""",
        """999999999.999999998""","""999999999.099999999""","""999999999.900000000""","""999999999.900000001""","""-999999999.000000000""","""-999999999.000000001""","""-999999999.000000009""","""-999999999.999999999""","""-999999999.999999998""","""-999999999.099999999""","""-999999999.900000000""","""-999999999.900000001""","""999999998.000000000""","""999999998.000000001""","""999999998.000000009""","""999999998.999999999""","""999999998.999999998""","""999999998.099999999""","""999999998.900000000""","""999999998.900000001""",
        """-999999998.000000000""","""-999999998.000000001""","""-999999998.000000009""","""-999999998.999999999""","""-999999998.999999998""","""-999999998.099999999""","""-999999998.900000000""","""-999999998.900000001"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_tinyint_from_decimal64_overflow_6_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(18, 9)) as tinyint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_tinyint_from_decimal64_overflow_6_test_data) {
        qt_sql_test_cast_to_tinyint_from_decimal64_overflow_6 """select cast(cast("${test_str}" as decimalv3(18, 9)) as tinyint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 9)) as tinyint);""")
    }
}