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


suite("test_cast_to_decimal256_39_from_decimal32_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_decimal256 = true;"
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_39_38_from_decimal_9_0_overflow_14_test_data = ["""10""","""999999998""","""999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_39_38_from_decimal_9_0_overflow_14_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(39, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_39_38_from_decimal_9_0_overflow_14_test_data) {
        qt_sql_test_cast_to_decimal_39_38_from_decimal_9_0_overflow_14 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(39, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(39, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_39_38_from_decimal_9_1_overflow_15_test_data = ["""10.9""","""99999998.9""","""99999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_39_38_from_decimal_9_1_overflow_15_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(39, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_39_38_from_decimal_9_1_overflow_15_test_data) {
        qt_sql_test_cast_to_decimal_39_38_from_decimal_9_1_overflow_15 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(39, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(39, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_39_39_from_decimal_1_0_overflow_18_test_data = ["""1""","""8""","""9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_39_39_from_decimal_1_0_overflow_18_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(1, 0)) as decimalv3(39, 39));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_39_39_from_decimal_1_0_overflow_18_test_data) {
        qt_sql_test_cast_to_decimal_39_39_from_decimal_1_0_overflow_18 """select cast(cast("${test_str}" as decimalv3(1, 0)) as decimalv3(39, 39));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(1, 0)) as decimalv3(39, 39));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_39_39_from_decimal_9_0_overflow_20_test_data = ["""1""","""999999998""","""999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_39_39_from_decimal_9_0_overflow_20_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(39, 39));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_39_39_from_decimal_9_0_overflow_20_test_data) {
        qt_sql_test_cast_to_decimal_39_39_from_decimal_9_0_overflow_20 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(39, 39));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(39, 39));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_39_39_from_decimal_9_1_overflow_21_test_data = ["""1.9""","""99999998.9""","""99999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_39_39_from_decimal_9_1_overflow_21_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(39, 39));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_39_39_from_decimal_9_1_overflow_21_test_data) {
        qt_sql_test_cast_to_decimal_39_39_from_decimal_9_1_overflow_21 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(39, 39));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(39, 39));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_39_39_from_decimal_9_8_overflow_22_test_data = ["""1.99999999""","""8.99999999""","""9.99999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_39_39_from_decimal_9_8_overflow_22_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(39, 39));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_39_39_from_decimal_9_8_overflow_22_test_data) {
        qt_sql_test_cast_to_decimal_39_39_from_decimal_9_8_overflow_22 """select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(39, 39));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(39, 39));""")
    }
}