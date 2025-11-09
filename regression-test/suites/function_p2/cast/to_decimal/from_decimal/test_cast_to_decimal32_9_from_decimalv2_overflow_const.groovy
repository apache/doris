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


suite("test_cast_to_decimal32_9_from_decimalv2_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_9_0_from_decimalv2_27_9_overflow_2_test_data = ["""999999999.999999999""","""999999999.500000000""","""1000000000.999999999""","""1000000000.500000000""","""999999999999999998.999999999""","""999999999999999998.500000000""","""999999999999999999.999999999""","""999999999999999999.500000000"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_9_0_from_decimalv2_27_9_overflow_2_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv2(27, 9)) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_9_0_from_decimalv2_27_9_overflow_2_test_data) {
        qt_sql_test_cast_to_decimal_9_0_from_decimalv2_27_9_overflow_2 """select cast(cast("${test_str}" as decimalv2(27, 9)) as decimalv3(9, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv2(27, 9)) as decimalv3(9, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_9_0_from_decimalv2_20_6_overflow_3_test_data = ["""999999999.999999""","""999999999.500000""","""1000000000.999999""","""1000000000.500000""","""99999999999998.999999""","""99999999999998.500000""","""99999999999999.999999""","""99999999999999.500000"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_9_0_from_decimalv2_20_6_overflow_3_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv2(20, 6)) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_9_0_from_decimalv2_20_6_overflow_3_test_data) {
        qt_sql_test_cast_to_decimal_9_0_from_decimalv2_20_6_overflow_3 """select cast(cast("${test_str}" as decimalv2(20, 6)) as decimalv3(9, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv2(20, 6)) as decimalv3(9, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_9_1_from_decimalv2_27_9_overflow_6_test_data = ["""99999999.999999999""","""99999999.950000000""","""100000000.999999999""","""100000000.950000000""","""999999999999999998.999999999""","""999999999999999998.950000000""","""999999999999999999.999999999""","""999999999999999999.950000000"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_9_1_from_decimalv2_27_9_overflow_6_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv2(27, 9)) as decimalv3(9, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_9_1_from_decimalv2_27_9_overflow_6_test_data) {
        qt_sql_test_cast_to_decimal_9_1_from_decimalv2_27_9_overflow_6 """select cast(cast("${test_str}" as decimalv2(27, 9)) as decimalv3(9, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv2(27, 9)) as decimalv3(9, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_9_1_from_decimalv2_20_6_overflow_7_test_data = ["""99999999.999999""","""99999999.950000""","""100000000.999999""","""100000000.950000""","""99999999999998.999999""","""99999999999998.950000""","""99999999999999.999999""","""99999999999999.950000"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_9_1_from_decimalv2_20_6_overflow_7_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv2(20, 6)) as decimalv3(9, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_9_1_from_decimalv2_20_6_overflow_7_test_data) {
        qt_sql_test_cast_to_decimal_9_1_from_decimalv2_20_6_overflow_7 """select cast(cast("${test_str}" as decimalv2(20, 6)) as decimalv3(9, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv2(20, 6)) as decimalv3(9, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_9_8_from_decimalv2_27_9_overflow_10_test_data = ["""9.999999999""","""9.999999995""","""10.999999999""","""10.999999995""","""999999999999999998.999999999""","""999999999999999998.999999995""","""999999999999999999.999999999""","""999999999999999999.999999995"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_9_8_from_decimalv2_27_9_overflow_10_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv2(27, 9)) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_9_8_from_decimalv2_27_9_overflow_10_test_data) {
        qt_sql_test_cast_to_decimal_9_8_from_decimalv2_27_9_overflow_10 """select cast(cast("${test_str}" as decimalv2(27, 9)) as decimalv3(9, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv2(27, 9)) as decimalv3(9, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_9_8_from_decimalv2_20_6_overflow_11_test_data = ["""10.999999""","""99999999999998.999999""","""99999999999999.999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_9_8_from_decimalv2_20_6_overflow_11_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv2(20, 6)) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_9_8_from_decimalv2_20_6_overflow_11_test_data) {
        qt_sql_test_cast_to_decimal_9_8_from_decimalv2_20_6_overflow_11 """select cast(cast("${test_str}" as decimalv2(20, 6)) as decimalv3(9, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv2(20, 6)) as decimalv3(9, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_9_9_from_decimalv2_1_0_overflow_12_test_data = ["""1""","""8""","""9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_9_9_from_decimalv2_1_0_overflow_12_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv2(1, 0)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_9_9_from_decimalv2_1_0_overflow_12_test_data) {
        qt_sql_test_cast_to_decimal_9_9_from_decimalv2_1_0_overflow_12 """select cast(cast("${test_str}" as decimalv2(1, 0)) as decimalv3(9, 9));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv2(1, 0)) as decimalv3(9, 9));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_9_9_from_decimalv2_27_9_overflow_14_test_data = ["""1.999999999""","""999999999999999998.999999999""","""999999999999999999.999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_9_9_from_decimalv2_27_9_overflow_14_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv2(27, 9)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_9_9_from_decimalv2_27_9_overflow_14_test_data) {
        qt_sql_test_cast_to_decimal_9_9_from_decimalv2_27_9_overflow_14 """select cast(cast("${test_str}" as decimalv2(27, 9)) as decimalv3(9, 9));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv2(27, 9)) as decimalv3(9, 9));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_9_9_from_decimalv2_20_6_overflow_15_test_data = ["""1.999999""","""99999999999998.999999""","""99999999999999.999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_9_9_from_decimalv2_20_6_overflow_15_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv2(20, 6)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_9_9_from_decimalv2_20_6_overflow_15_test_data) {
        qt_sql_test_cast_to_decimal_9_9_from_decimalv2_20_6_overflow_15 """select cast(cast("${test_str}" as decimalv2(20, 6)) as decimalv3(9, 9));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv2(20, 6)) as decimalv3(9, 9));""")
    }
}