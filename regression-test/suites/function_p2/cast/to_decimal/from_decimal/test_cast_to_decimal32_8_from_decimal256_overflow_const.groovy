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


suite("test_cast_to_decimal32_8_from_decimal256_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_decimal256 = true;"
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_0_from_decimal_39_0_overflow_0_test_data = ["""100000000""","""999999999999999999999999999999999999998""","""999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_0_from_decimal_39_0_overflow_0_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_0_from_decimal_39_0_overflow_0_test_data) {
        qt_sql_test_cast_to_decimal_8_0_from_decimal_39_0_overflow_0 """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_0_from_decimal_39_1_overflow_1_test_data = ["""99999999.9""","""99999999.9""","""100000000.9""","""100000000.9""","""99999999999999999999999999999999999998.9""","""99999999999999999999999999999999999998.9""","""99999999999999999999999999999999999999.9""","""99999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_0_from_decimal_39_1_overflow_1_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_0_from_decimal_39_1_overflow_1_test_data) {
        qt_sql_test_cast_to_decimal_8_0_from_decimal_39_1_overflow_1 """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_0_from_decimal_39_19_overflow_2_test_data = ["""99999999.9999999999999999999""","""99999999.9999999999999999999""","""100000000.9999999999999999999""","""100000000.9999999999999999999""","""99999999999999999998.9999999999999999999""","""99999999999999999998.9999999999999999999""","""99999999999999999999.9999999999999999999""","""99999999999999999999.9999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_0_from_decimal_39_19_overflow_2_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_0_from_decimal_39_19_overflow_2_test_data) {
        qt_sql_test_cast_to_decimal_8_0_from_decimal_39_19_overflow_2 """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_0_from_decimal_75_0_overflow_5_test_data = ["""100000000""","""999999999999999999999999999999999999999999999999999999999999999999999999998""","""999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_0_from_decimal_75_0_overflow_5_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_0_from_decimal_75_0_overflow_5_test_data) {
        qt_sql_test_cast_to_decimal_8_0_from_decimal_75_0_overflow_5 """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_0_from_decimal_75_1_overflow_6_test_data = ["""99999999.9""","""99999999.9""","""100000000.9""","""100000000.9""","""99999999999999999999999999999999999999999999999999999999999999999999999998.9""","""99999999999999999999999999999999999999999999999999999999999999999999999998.9""","""99999999999999999999999999999999999999999999999999999999999999999999999999.9""","""99999999999999999999999999999999999999999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_0_from_decimal_75_1_overflow_6_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_0_from_decimal_75_1_overflow_6_test_data) {
        qt_sql_test_cast_to_decimal_8_0_from_decimal_75_1_overflow_6 """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_0_from_decimal_75_37_overflow_7_test_data = ["""99999999.9999999999999999999999999999999999999""","""99999999.9999999999999999999999999999999999999""","""100000000.9999999999999999999999999999999999999""","""100000000.9999999999999999999999999999999999999""","""99999999999999999999999999999999999998.9999999999999999999999999999999999999""","""99999999999999999999999999999999999998.9999999999999999999999999999999999999""","""99999999999999999999999999999999999999.9999999999999999999999999999999999999""","""99999999999999999999999999999999999999.9999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_0_from_decimal_75_37_overflow_7_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_0_from_decimal_75_37_overflow_7_test_data) {
        qt_sql_test_cast_to_decimal_8_0_from_decimal_75_37_overflow_7 """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_0_from_decimal_76_0_overflow_10_test_data = ["""100000000""","""9999999999999999999999999999999999999999999999999999999999999999999999999998""","""9999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_0_from_decimal_76_0_overflow_10_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_0_from_decimal_76_0_overflow_10_test_data) {
        qt_sql_test_cast_to_decimal_8_0_from_decimal_76_0_overflow_10 """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_0_from_decimal_76_1_overflow_11_test_data = ["""99999999.9""","""99999999.9""","""100000000.9""","""100000000.9""","""999999999999999999999999999999999999999999999999999999999999999999999999998.9""","""999999999999999999999999999999999999999999999999999999999999999999999999998.9""","""999999999999999999999999999999999999999999999999999999999999999999999999999.9""","""999999999999999999999999999999999999999999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_0_from_decimal_76_1_overflow_11_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_0_from_decimal_76_1_overflow_11_test_data) {
        qt_sql_test_cast_to_decimal_8_0_from_decimal_76_1_overflow_11 """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_0_from_decimal_76_38_overflow_12_test_data = ["""99999999.99999999999999999999999999999999999999""","""99999999.99999999999999999999999999999999999999""","""100000000.99999999999999999999999999999999999999""","""100000000.99999999999999999999999999999999999999""","""99999999999999999999999999999999999998.99999999999999999999999999999999999999""","""99999999999999999999999999999999999998.99999999999999999999999999999999999999""","""99999999999999999999999999999999999999.99999999999999999999999999999999999999""","""99999999999999999999999999999999999999.99999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_0_from_decimal_76_38_overflow_12_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_0_from_decimal_76_38_overflow_12_test_data) {
        qt_sql_test_cast_to_decimal_8_0_from_decimal_76_38_overflow_12 """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_1_from_decimal_39_0_overflow_15_test_data = ["""10000000""","""999999999999999999999999999999999999998""","""999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_1_from_decimal_39_0_overflow_15_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_1_from_decimal_39_0_overflow_15_test_data) {
        qt_sql_test_cast_to_decimal_8_1_from_decimal_39_0_overflow_15 """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_1_from_decimal_39_1_overflow_16_test_data = ["""10000000.9""","""99999999999999999999999999999999999998.9""","""99999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_1_from_decimal_39_1_overflow_16_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_1_from_decimal_39_1_overflow_16_test_data) {
        qt_sql_test_cast_to_decimal_8_1_from_decimal_39_1_overflow_16 """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_1_from_decimal_39_19_overflow_17_test_data = ["""9999999.9999999999999999999""","""9999999.9999999999999999999""","""10000000.9999999999999999999""","""10000000.9999999999999999999""","""99999999999999999998.9999999999999999999""","""99999999999999999998.9999999999999999999""","""99999999999999999999.9999999999999999999""","""99999999999999999999.9999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_1_from_decimal_39_19_overflow_17_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_1_from_decimal_39_19_overflow_17_test_data) {
        qt_sql_test_cast_to_decimal_8_1_from_decimal_39_19_overflow_17 """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_1_from_decimal_75_0_overflow_20_test_data = ["""10000000""","""999999999999999999999999999999999999999999999999999999999999999999999999998""","""999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_1_from_decimal_75_0_overflow_20_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_1_from_decimal_75_0_overflow_20_test_data) {
        qt_sql_test_cast_to_decimal_8_1_from_decimal_75_0_overflow_20 """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_1_from_decimal_75_1_overflow_21_test_data = ["""10000000.9""","""99999999999999999999999999999999999999999999999999999999999999999999999998.9""","""99999999999999999999999999999999999999999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_1_from_decimal_75_1_overflow_21_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_1_from_decimal_75_1_overflow_21_test_data) {
        qt_sql_test_cast_to_decimal_8_1_from_decimal_75_1_overflow_21 """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_1_from_decimal_75_37_overflow_22_test_data = ["""9999999.9999999999999999999999999999999999999""","""9999999.9999999999999999999999999999999999999""","""10000000.9999999999999999999999999999999999999""","""10000000.9999999999999999999999999999999999999""","""99999999999999999999999999999999999998.9999999999999999999999999999999999999""","""99999999999999999999999999999999999998.9999999999999999999999999999999999999""","""99999999999999999999999999999999999999.9999999999999999999999999999999999999""","""99999999999999999999999999999999999999.9999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_1_from_decimal_75_37_overflow_22_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_1_from_decimal_75_37_overflow_22_test_data) {
        qt_sql_test_cast_to_decimal_8_1_from_decimal_75_37_overflow_22 """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_1_from_decimal_76_0_overflow_25_test_data = ["""10000000""","""9999999999999999999999999999999999999999999999999999999999999999999999999998""","""9999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_1_from_decimal_76_0_overflow_25_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_1_from_decimal_76_0_overflow_25_test_data) {
        qt_sql_test_cast_to_decimal_8_1_from_decimal_76_0_overflow_25 """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_1_from_decimal_76_1_overflow_26_test_data = ["""10000000.9""","""999999999999999999999999999999999999999999999999999999999999999999999999998.9""","""999999999999999999999999999999999999999999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_1_from_decimal_76_1_overflow_26_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_1_from_decimal_76_1_overflow_26_test_data) {
        qt_sql_test_cast_to_decimal_8_1_from_decimal_76_1_overflow_26 """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_1_from_decimal_76_38_overflow_27_test_data = ["""9999999.99999999999999999999999999999999999999""","""9999999.99999999999999999999999999999999999999""","""10000000.99999999999999999999999999999999999999""","""10000000.99999999999999999999999999999999999999""","""99999999999999999999999999999999999998.99999999999999999999999999999999999999""","""99999999999999999999999999999999999998.99999999999999999999999999999999999999""","""99999999999999999999999999999999999999.99999999999999999999999999999999999999""","""99999999999999999999999999999999999999.99999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_1_from_decimal_76_38_overflow_27_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_1_from_decimal_76_38_overflow_27_test_data) {
        qt_sql_test_cast_to_decimal_8_1_from_decimal_76_38_overflow_27 """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_4_from_decimal_39_0_overflow_30_test_data = ["""10000""","""999999999999999999999999999999999999998""","""999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_4_from_decimal_39_0_overflow_30_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_4_from_decimal_39_0_overflow_30_test_data) {
        qt_sql_test_cast_to_decimal_8_4_from_decimal_39_0_overflow_30 """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 4));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 4));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_4_from_decimal_39_1_overflow_31_test_data = ["""10000.9""","""99999999999999999999999999999999999998.9""","""99999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_4_from_decimal_39_1_overflow_31_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_4_from_decimal_39_1_overflow_31_test_data) {
        qt_sql_test_cast_to_decimal_8_4_from_decimal_39_1_overflow_31 """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 4));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 4));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_4_from_decimal_39_19_overflow_32_test_data = ["""9999.9999999999999999999""","""9999.9999999999999999999""","""10000.9999999999999999999""","""10000.9999999999999999999""","""99999999999999999998.9999999999999999999""","""99999999999999999998.9999999999999999999""","""99999999999999999999.9999999999999999999""","""99999999999999999999.9999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_4_from_decimal_39_19_overflow_32_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_4_from_decimal_39_19_overflow_32_test_data) {
        qt_sql_test_cast_to_decimal_8_4_from_decimal_39_19_overflow_32 """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 4));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 4));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_4_from_decimal_75_0_overflow_35_test_data = ["""10000""","""999999999999999999999999999999999999999999999999999999999999999999999999998""","""999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_4_from_decimal_75_0_overflow_35_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_4_from_decimal_75_0_overflow_35_test_data) {
        qt_sql_test_cast_to_decimal_8_4_from_decimal_75_0_overflow_35 """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 4));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 4));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_4_from_decimal_75_1_overflow_36_test_data = ["""10000.9""","""99999999999999999999999999999999999999999999999999999999999999999999999998.9""","""99999999999999999999999999999999999999999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_4_from_decimal_75_1_overflow_36_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_4_from_decimal_75_1_overflow_36_test_data) {
        qt_sql_test_cast_to_decimal_8_4_from_decimal_75_1_overflow_36 """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 4));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 4));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_4_from_decimal_75_37_overflow_37_test_data = ["""9999.9999999999999999999999999999999999999""","""9999.9999999999999999999999999999999999999""","""10000.9999999999999999999999999999999999999""","""10000.9999999999999999999999999999999999999""","""99999999999999999999999999999999999998.9999999999999999999999999999999999999""","""99999999999999999999999999999999999998.9999999999999999999999999999999999999""","""99999999999999999999999999999999999999.9999999999999999999999999999999999999""","""99999999999999999999999999999999999999.9999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_4_from_decimal_75_37_overflow_37_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_4_from_decimal_75_37_overflow_37_test_data) {
        qt_sql_test_cast_to_decimal_8_4_from_decimal_75_37_overflow_37 """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 4));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 4));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_4_from_decimal_76_0_overflow_40_test_data = ["""10000""","""9999999999999999999999999999999999999999999999999999999999999999999999999998""","""9999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_4_from_decimal_76_0_overflow_40_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_4_from_decimal_76_0_overflow_40_test_data) {
        qt_sql_test_cast_to_decimal_8_4_from_decimal_76_0_overflow_40 """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 4));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 4));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_4_from_decimal_76_1_overflow_41_test_data = ["""10000.9""","""999999999999999999999999999999999999999999999999999999999999999999999999998.9""","""999999999999999999999999999999999999999999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_4_from_decimal_76_1_overflow_41_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_4_from_decimal_76_1_overflow_41_test_data) {
        qt_sql_test_cast_to_decimal_8_4_from_decimal_76_1_overflow_41 """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 4));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 4));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_4_from_decimal_76_38_overflow_42_test_data = ["""9999.99999999999999999999999999999999999999""","""9999.99999999999999999999999999999999999999""","""10000.99999999999999999999999999999999999999""","""10000.99999999999999999999999999999999999999""","""99999999999999999999999999999999999998.99999999999999999999999999999999999999""","""99999999999999999999999999999999999998.99999999999999999999999999999999999999""","""99999999999999999999999999999999999999.99999999999999999999999999999999999999""","""99999999999999999999999999999999999999.99999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_4_from_decimal_76_38_overflow_42_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_4_from_decimal_76_38_overflow_42_test_data) {
        qt_sql_test_cast_to_decimal_8_4_from_decimal_76_38_overflow_42 """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 4));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 4));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_7_from_decimal_39_0_overflow_45_test_data = ["""10""","""999999999999999999999999999999999999998""","""999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_7_from_decimal_39_0_overflow_45_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 7));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_7_from_decimal_39_0_overflow_45_test_data) {
        qt_sql_test_cast_to_decimal_8_7_from_decimal_39_0_overflow_45 """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 7));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 7));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_7_from_decimal_39_1_overflow_46_test_data = ["""10.9""","""99999999999999999999999999999999999998.9""","""99999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_7_from_decimal_39_1_overflow_46_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 7));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_7_from_decimal_39_1_overflow_46_test_data) {
        qt_sql_test_cast_to_decimal_8_7_from_decimal_39_1_overflow_46 """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 7));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 7));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_7_from_decimal_39_19_overflow_47_test_data = ["""9.9999999999999999999""","""9.9999999999999999999""","""10.9999999999999999999""","""10.9999999999999999999""","""99999999999999999998.9999999999999999999""","""99999999999999999998.9999999999999999999""","""99999999999999999999.9999999999999999999""","""99999999999999999999.9999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_7_from_decimal_39_19_overflow_47_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 7));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_7_from_decimal_39_19_overflow_47_test_data) {
        qt_sql_test_cast_to_decimal_8_7_from_decimal_39_19_overflow_47 """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 7));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 7));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_7_from_decimal_39_38_overflow_48_test_data = ["""9.99999999999999999999999999999999999999""","""9.99999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_7_from_decimal_39_38_overflow_48_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 38)) as decimalv3(8, 7));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_7_from_decimal_39_38_overflow_48_test_data) {
        qt_sql_test_cast_to_decimal_8_7_from_decimal_39_38_overflow_48 """select cast(cast("${test_str}" as decimalv3(39, 38)) as decimalv3(8, 7));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 38)) as decimalv3(8, 7));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_7_from_decimal_75_0_overflow_50_test_data = ["""10""","""999999999999999999999999999999999999999999999999999999999999999999999999998""","""999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_7_from_decimal_75_0_overflow_50_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 7));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_7_from_decimal_75_0_overflow_50_test_data) {
        qt_sql_test_cast_to_decimal_8_7_from_decimal_75_0_overflow_50 """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 7));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 7));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_7_from_decimal_75_1_overflow_51_test_data = ["""10.9""","""99999999999999999999999999999999999999999999999999999999999999999999999998.9""","""99999999999999999999999999999999999999999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_7_from_decimal_75_1_overflow_51_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 7));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_7_from_decimal_75_1_overflow_51_test_data) {
        qt_sql_test_cast_to_decimal_8_7_from_decimal_75_1_overflow_51 """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 7));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 7));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_7_from_decimal_75_37_overflow_52_test_data = ["""9.9999999999999999999999999999999999999""","""9.9999999999999999999999999999999999999""","""10.9999999999999999999999999999999999999""","""10.9999999999999999999999999999999999999""","""99999999999999999999999999999999999998.9999999999999999999999999999999999999""","""99999999999999999999999999999999999998.9999999999999999999999999999999999999""","""99999999999999999999999999999999999999.9999999999999999999999999999999999999""","""99999999999999999999999999999999999999.9999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_7_from_decimal_75_37_overflow_52_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 7));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_7_from_decimal_75_37_overflow_52_test_data) {
        qt_sql_test_cast_to_decimal_8_7_from_decimal_75_37_overflow_52 """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 7));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 7));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_7_from_decimal_75_74_overflow_53_test_data = ["""9.99999999999999999999999999999999999999999999999999999999999999999999999999""","""9.99999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_7_from_decimal_75_74_overflow_53_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 74)) as decimalv3(8, 7));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_7_from_decimal_75_74_overflow_53_test_data) {
        qt_sql_test_cast_to_decimal_8_7_from_decimal_75_74_overflow_53 """select cast(cast("${test_str}" as decimalv3(75, 74)) as decimalv3(8, 7));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 74)) as decimalv3(8, 7));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_7_from_decimal_76_0_overflow_55_test_data = ["""10""","""9999999999999999999999999999999999999999999999999999999999999999999999999998""","""9999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_7_from_decimal_76_0_overflow_55_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 7));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_7_from_decimal_76_0_overflow_55_test_data) {
        qt_sql_test_cast_to_decimal_8_7_from_decimal_76_0_overflow_55 """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 7));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 7));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_7_from_decimal_76_1_overflow_56_test_data = ["""10.9""","""999999999999999999999999999999999999999999999999999999999999999999999999998.9""","""999999999999999999999999999999999999999999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_7_from_decimal_76_1_overflow_56_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 7));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_7_from_decimal_76_1_overflow_56_test_data) {
        qt_sql_test_cast_to_decimal_8_7_from_decimal_76_1_overflow_56 """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 7));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 7));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_7_from_decimal_76_38_overflow_57_test_data = ["""9.99999999999999999999999999999999999999""","""9.99999999999999999999999999999999999999""","""10.99999999999999999999999999999999999999""","""10.99999999999999999999999999999999999999""","""99999999999999999999999999999999999998.99999999999999999999999999999999999999""","""99999999999999999999999999999999999998.99999999999999999999999999999999999999""","""99999999999999999999999999999999999999.99999999999999999999999999999999999999""","""99999999999999999999999999999999999999.99999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_7_from_decimal_76_38_overflow_57_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 7));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_7_from_decimal_76_38_overflow_57_test_data) {
        qt_sql_test_cast_to_decimal_8_7_from_decimal_76_38_overflow_57 """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 7));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 7));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_7_from_decimal_76_75_overflow_58_test_data = ["""9.999999999999999999999999999999999999999999999999999999999999999999999999999""","""9.999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_7_from_decimal_76_75_overflow_58_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 75)) as decimalv3(8, 7));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_7_from_decimal_76_75_overflow_58_test_data) {
        qt_sql_test_cast_to_decimal_8_7_from_decimal_76_75_overflow_58 """select cast(cast("${test_str}" as decimalv3(76, 75)) as decimalv3(8, 7));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 75)) as decimalv3(8, 7));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_39_0_overflow_60_test_data = ["""1""","""999999999999999999999999999999999999998""","""999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_39_0_overflow_60_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_39_0_overflow_60_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_39_0_overflow_60 """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(8, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_39_1_overflow_61_test_data = ["""1.9""","""99999999999999999999999999999999999998.9""","""99999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_39_1_overflow_61_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_39_1_overflow_61_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_39_1_overflow_61 """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(8, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_39_19_overflow_62_test_data = ["""0.9999999999999999999""","""0.9999999999999999999""","""1.9999999999999999999""","""1.9999999999999999999""","""99999999999999999998.9999999999999999999""","""99999999999999999998.9999999999999999999""","""99999999999999999999.9999999999999999999""","""99999999999999999999.9999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_39_19_overflow_62_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_39_19_overflow_62_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_39_19_overflow_62 """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(8, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_39_38_overflow_63_test_data = ["""0.99999999999999999999999999999999999999""","""0.99999999999999999999999999999999999999""","""1.99999999999999999999999999999999999999""","""1.99999999999999999999999999999999999999""","""8.99999999999999999999999999999999999999""","""8.99999999999999999999999999999999999999""","""9.99999999999999999999999999999999999999""","""9.99999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_39_38_overflow_63_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 38)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_39_38_overflow_63_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_39_38_overflow_63 """select cast(cast("${test_str}" as decimalv3(39, 38)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 38)) as decimalv3(8, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_39_39_overflow_64_test_data = ["""0.999999999999999999999999999999999999999""","""0.999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_39_39_overflow_64_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(39, 39)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_39_39_overflow_64_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_39_39_overflow_64 """select cast(cast("${test_str}" as decimalv3(39, 39)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 39)) as decimalv3(8, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_75_0_overflow_65_test_data = ["""1""","""999999999999999999999999999999999999999999999999999999999999999999999999998""","""999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_75_0_overflow_65_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_75_0_overflow_65_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_75_0_overflow_65 """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(8, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_75_1_overflow_66_test_data = ["""1.9""","""99999999999999999999999999999999999999999999999999999999999999999999999998.9""","""99999999999999999999999999999999999999999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_75_1_overflow_66_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_75_1_overflow_66_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_75_1_overflow_66 """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(8, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_75_37_overflow_67_test_data = ["""0.9999999999999999999999999999999999999""","""0.9999999999999999999999999999999999999""","""1.9999999999999999999999999999999999999""","""1.9999999999999999999999999999999999999""","""99999999999999999999999999999999999998.9999999999999999999999999999999999999""","""99999999999999999999999999999999999998.9999999999999999999999999999999999999""","""99999999999999999999999999999999999999.9999999999999999999999999999999999999""","""99999999999999999999999999999999999999.9999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_75_37_overflow_67_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_75_37_overflow_67_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_75_37_overflow_67 """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(8, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_75_74_overflow_68_test_data = ["""0.99999999999999999999999999999999999999999999999999999999999999999999999999""","""0.99999999999999999999999999999999999999999999999999999999999999999999999999""","""1.99999999999999999999999999999999999999999999999999999999999999999999999999""","""1.99999999999999999999999999999999999999999999999999999999999999999999999999""","""8.99999999999999999999999999999999999999999999999999999999999999999999999999""","""8.99999999999999999999999999999999999999999999999999999999999999999999999999""","""9.99999999999999999999999999999999999999999999999999999999999999999999999999""","""9.99999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_75_74_overflow_68_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 74)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_75_74_overflow_68_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_75_74_overflow_68 """select cast(cast("${test_str}" as decimalv3(75, 74)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 74)) as decimalv3(8, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_75_75_overflow_69_test_data = ["""0.999999999999999999999999999999999999999999999999999999999999999999999999999""","""0.999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_75_75_overflow_69_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(75, 75)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_75_75_overflow_69_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_75_75_overflow_69 """select cast(cast("${test_str}" as decimalv3(75, 75)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 75)) as decimalv3(8, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_76_0_overflow_70_test_data = ["""1""","""9999999999999999999999999999999999999999999999999999999999999999999999999998""","""9999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_76_0_overflow_70_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_76_0_overflow_70_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_76_0_overflow_70 """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(8, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_76_1_overflow_71_test_data = ["""1.9""","""999999999999999999999999999999999999999999999999999999999999999999999999998.9""","""999999999999999999999999999999999999999999999999999999999999999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_76_1_overflow_71_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_76_1_overflow_71_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_76_1_overflow_71 """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(8, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_76_38_overflow_72_test_data = ["""0.99999999999999999999999999999999999999""","""0.99999999999999999999999999999999999999""","""1.99999999999999999999999999999999999999""","""1.99999999999999999999999999999999999999""","""99999999999999999999999999999999999998.99999999999999999999999999999999999999""","""99999999999999999999999999999999999998.99999999999999999999999999999999999999""","""99999999999999999999999999999999999999.99999999999999999999999999999999999999""","""99999999999999999999999999999999999999.99999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_76_38_overflow_72_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_76_38_overflow_72_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_76_38_overflow_72 """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(8, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_76_75_overflow_73_test_data = ["""0.999999999999999999999999999999999999999999999999999999999999999999999999999""","""0.999999999999999999999999999999999999999999999999999999999999999999999999999""","""1.999999999999999999999999999999999999999999999999999999999999999999999999999""","""1.999999999999999999999999999999999999999999999999999999999999999999999999999""","""8.999999999999999999999999999999999999999999999999999999999999999999999999999""","""8.999999999999999999999999999999999999999999999999999999999999999999999999999""","""9.999999999999999999999999999999999999999999999999999999999999999999999999999""","""9.999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_76_75_overflow_73_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 75)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_76_75_overflow_73_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_76_75_overflow_73 """select cast(cast("${test_str}" as decimalv3(76, 75)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 75)) as decimalv3(8, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_8_8_from_decimal_76_76_overflow_74_test_data = ["""0.9999999999999999999999999999999999999999999999999999999999999999999999999999""","""0.9999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_8_8_from_decimal_76_76_overflow_74_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(76, 76)) as decimalv3(8, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_8_8_from_decimal_76_76_overflow_74_test_data) {
        qt_sql_test_cast_to_decimal_8_8_from_decimal_76_76_overflow_74 """select cast(cast("${test_str}" as decimalv3(76, 76)) as decimalv3(8, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 76)) as decimalv3(8, 8));""")
    }
}