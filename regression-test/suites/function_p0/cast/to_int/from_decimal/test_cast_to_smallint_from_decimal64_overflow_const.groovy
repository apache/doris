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


suite("test_cast_to_smallint_from_decimal64_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimal64_overflow_0_test_data = ["""32768""","""-32769""","""9999999999""","""-9999999999""","""9999999998""","""-9999999998"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimal64_overflow_0_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(10, 0)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimal64_overflow_0_test_data) {
        qt_sql_test_cast_to_smallint_from_decimal64_overflow_0 """select cast(cast("${test_str}" as decimalv3(10, 0)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 0)) as smallint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimal64_overflow_1_test_data = ["""32768.00000""","""32768.00001""","""32768.00009""","""32768.99999""","""32768.99998""","""32768.09999""","""32768.90000""","""32768.90001""","""-32769.00000""","""-32769.00001""","""-32769.00009""","""-32769.99999""","""-32769.99998""","""-32769.09999""","""-32769.90000""","""-32769.90001""","""99999.00000""","""99999.00001""","""99999.00009""","""99999.99999""",
        """99999.99998""","""99999.09999""","""99999.90000""","""99999.90001""","""-99999.00000""","""-99999.00001""","""-99999.00009""","""-99999.99999""","""-99999.99998""","""-99999.09999""","""-99999.90000""","""-99999.90001""","""99998.00000""","""99998.00001""","""99998.00009""","""99998.99999""","""99998.99998""","""99998.09999""","""99998.90000""","""99998.90001""",
        """-99998.00000""","""-99998.00001""","""-99998.00009""","""-99998.99999""","""-99998.99998""","""-99998.09999""","""-99998.90000""","""-99998.90001"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimal64_overflow_1_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(10, 5)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimal64_overflow_1_test_data) {
        qt_sql_test_cast_to_smallint_from_decimal64_overflow_1 """select cast(cast("${test_str}" as decimalv3(10, 5)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 5)) as smallint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimal64_overflow_4_test_data = ["""32768""","""-32769""","""999999999999999999""","""-999999999999999999""","""999999999999999998""","""-999999999999999998"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimal64_overflow_4_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(18, 0)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimal64_overflow_4_test_data) {
        qt_sql_test_cast_to_smallint_from_decimal64_overflow_4 """select cast(cast("${test_str}" as decimalv3(18, 0)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 0)) as smallint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimal64_overflow_5_test_data = ["""32768.0""","""32768.1""","""32768.9""","""32768.9""","""32768.8""","""32768.0""","""32768.9""","""32768.8""","""-32769.0""","""-32769.1""","""-32769.9""","""-32769.9""","""-32769.8""","""-32769.0""","""-32769.9""","""-32769.8""","""99999999999999999.0""","""99999999999999999.1""","""99999999999999999.9""","""99999999999999999.9""",
        """99999999999999999.8""","""99999999999999999.0""","""99999999999999999.9""","""99999999999999999.8""","""-99999999999999999.0""","""-99999999999999999.1""","""-99999999999999999.9""","""-99999999999999999.9""","""-99999999999999999.8""","""-99999999999999999.0""","""-99999999999999999.9""","""-99999999999999999.8""","""99999999999999998.0""","""99999999999999998.1""","""99999999999999998.9""","""99999999999999998.9""","""99999999999999998.8""","""99999999999999998.0""","""99999999999999998.9""","""99999999999999998.8""",
        """-99999999999999998.0""","""-99999999999999998.1""","""-99999999999999998.9""","""-99999999999999998.9""","""-99999999999999998.8""","""-99999999999999998.0""","""-99999999999999998.9""","""-99999999999999998.8"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimal64_overflow_5_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(18, 1)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimal64_overflow_5_test_data) {
        qt_sql_test_cast_to_smallint_from_decimal64_overflow_5 """select cast(cast("${test_str}" as decimalv3(18, 1)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 1)) as smallint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimal64_overflow_6_test_data = ["""32768.000000000""","""32768.000000001""","""32768.000000009""","""32768.999999999""","""32768.999999998""","""32768.099999999""","""32768.900000000""","""32768.900000001""","""-32769.000000000""","""-32769.000000001""","""-32769.000000009""","""-32769.999999999""","""-32769.999999998""","""-32769.099999999""","""-32769.900000000""","""-32769.900000001""","""999999999.000000000""","""999999999.000000001""","""999999999.000000009""","""999999999.999999999""",
        """999999999.999999998""","""999999999.099999999""","""999999999.900000000""","""999999999.900000001""","""-999999999.000000000""","""-999999999.000000001""","""-999999999.000000009""","""-999999999.999999999""","""-999999999.999999998""","""-999999999.099999999""","""-999999999.900000000""","""-999999999.900000001""","""999999998.000000000""","""999999998.000000001""","""999999998.000000009""","""999999998.999999999""","""999999998.999999998""","""999999998.099999999""","""999999998.900000000""","""999999998.900000001""",
        """-999999998.000000000""","""-999999998.000000001""","""-999999998.000000009""","""-999999998.999999999""","""-999999998.999999998""","""-999999998.099999999""","""-999999998.900000000""","""-999999998.900000001"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimal64_overflow_6_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(18, 9)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimal64_overflow_6_test_data) {
        qt_sql_test_cast_to_smallint_from_decimal64_overflow_6 """select cast(cast("${test_str}" as decimalv3(18, 9)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 9)) as smallint);""")
    }
}