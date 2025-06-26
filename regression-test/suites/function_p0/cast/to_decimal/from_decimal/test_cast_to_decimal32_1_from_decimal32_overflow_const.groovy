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


suite("test_cast_to_decimal32_1_from_decimal32_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_0_from_decimal_4_0_overflow_2_test_data = ["""10""","""9998""","""9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_0_from_decimal_4_0_overflow_2_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_0_from_decimal_4_0_overflow_2_test_data) {
        qt_sql_test_cast_to_decimal_1_0_from_decimal_4_0_overflow_2 """select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_0_from_decimal_4_1_overflow_3_test_data = ["""9.9""","""9.9""","""10.9""","""10.9""","""998.9""","""998.9""","""999.9""","""999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_0_from_decimal_4_1_overflow_3_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_0_from_decimal_4_1_overflow_3_test_data) {
        qt_sql_test_cast_to_decimal_1_0_from_decimal_4_1_overflow_3 """select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_0_from_decimal_4_2_overflow_4_test_data = ["""9.99""","""9.99""","""10.99""","""10.99""","""98.99""","""98.99""","""99.99""","""99.99"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_0_from_decimal_4_2_overflow_4_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_0_from_decimal_4_2_overflow_4_test_data) {
        qt_sql_test_cast_to_decimal_1_0_from_decimal_4_2_overflow_4 """select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_0_from_decimal_4_3_overflow_5_test_data = ["""9.999""","""9.999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_0_from_decimal_4_3_overflow_5_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_0_from_decimal_4_3_overflow_5_test_data) {
        qt_sql_test_cast_to_decimal_1_0_from_decimal_4_3_overflow_5 """select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_0_from_decimal_8_0_overflow_7_test_data = ["""10""","""99999998""","""99999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_0_from_decimal_8_0_overflow_7_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_0_from_decimal_8_0_overflow_7_test_data) {
        qt_sql_test_cast_to_decimal_1_0_from_decimal_8_0_overflow_7 """select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_0_from_decimal_8_1_overflow_8_test_data = ["""9.9""","""9.9""","""10.9""","""10.9""","""9999998.9""","""9999998.9""","""9999999.9""","""9999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_0_from_decimal_8_1_overflow_8_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_0_from_decimal_8_1_overflow_8_test_data) {
        qt_sql_test_cast_to_decimal_1_0_from_decimal_8_1_overflow_8 """select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_0_from_decimal_8_4_overflow_9_test_data = ["""9.9999""","""9.9999""","""10.9999""","""10.9999""","""9998.9999""","""9998.9999""","""9999.9999""","""9999.9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_0_from_decimal_8_4_overflow_9_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_0_from_decimal_8_4_overflow_9_test_data) {
        qt_sql_test_cast_to_decimal_1_0_from_decimal_8_4_overflow_9 """select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_0_from_decimal_8_7_overflow_10_test_data = ["""9.9999999""","""9.9999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_0_from_decimal_8_7_overflow_10_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_0_from_decimal_8_7_overflow_10_test_data) {
        qt_sql_test_cast_to_decimal_1_0_from_decimal_8_7_overflow_10 """select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_0_from_decimal_9_0_overflow_12_test_data = ["""10""","""999999998""","""999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_0_from_decimal_9_0_overflow_12_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_0_from_decimal_9_0_overflow_12_test_data) {
        qt_sql_test_cast_to_decimal_1_0_from_decimal_9_0_overflow_12 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_0_from_decimal_9_1_overflow_13_test_data = ["""9.9""","""9.9""","""10.9""","""10.9""","""99999998.9""","""99999998.9""","""99999999.9""","""99999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_0_from_decimal_9_1_overflow_13_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_0_from_decimal_9_1_overflow_13_test_data) {
        qt_sql_test_cast_to_decimal_1_0_from_decimal_9_1_overflow_13 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_0_from_decimal_9_4_overflow_14_test_data = ["""9.9999""","""9.9999""","""10.9999""","""10.9999""","""99998.9999""","""99998.9999""","""99999.9999""","""99999.9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_0_from_decimal_9_4_overflow_14_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_0_from_decimal_9_4_overflow_14_test_data) {
        qt_sql_test_cast_to_decimal_1_0_from_decimal_9_4_overflow_14 """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_0_from_decimal_9_8_overflow_15_test_data = ["""9.99999999""","""9.99999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_0_from_decimal_9_8_overflow_15_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_0_from_decimal_9_8_overflow_15_test_data) {
        qt_sql_test_cast_to_decimal_1_0_from_decimal_9_8_overflow_15 """select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_1_0_overflow_17_test_data = ["""1""","""8""","""9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_1_0_overflow_17_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(1, 0)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_1_0_overflow_17_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_1_0_overflow_17 """select cast(cast("${test_str}" as decimalv3(1, 0)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(1, 0)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_4_0_overflow_19_test_data = ["""1""","""9998""","""9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_4_0_overflow_19_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_4_0_overflow_19_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_4_0_overflow_19 """select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_4_1_overflow_20_test_data = ["""1.9""","""998.9""","""999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_4_1_overflow_20_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_4_1_overflow_20_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_4_1_overflow_20 """select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_4_2_overflow_21_test_data = ["""0.99""","""0.99""","""1.99""","""1.99""","""98.99""","""98.99""","""99.99""","""99.99"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_4_2_overflow_21_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_4_2_overflow_21_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_4_2_overflow_21 """select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_4_3_overflow_22_test_data = ["""0.999""","""0.999""","""1.999""","""1.999""","""8.999""","""8.999""","""9.999""","""9.999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_4_3_overflow_22_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_4_3_overflow_22_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_4_3_overflow_22 """select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_4_4_overflow_23_test_data = ["""0.9999""","""0.9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_4_4_overflow_23_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 4)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_4_4_overflow_23_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_4_4_overflow_23 """select cast(cast("${test_str}" as decimalv3(4, 4)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 4)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_8_0_overflow_24_test_data = ["""1""","""99999998""","""99999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_8_0_overflow_24_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_8_0_overflow_24_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_8_0_overflow_24 """select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_8_1_overflow_25_test_data = ["""1.9""","""9999998.9""","""9999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_8_1_overflow_25_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_8_1_overflow_25_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_8_1_overflow_25 """select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_8_4_overflow_26_test_data = ["""0.9999""","""0.9999""","""1.9999""","""1.9999""","""9998.9999""","""9998.9999""","""9999.9999""","""9999.9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_8_4_overflow_26_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_8_4_overflow_26_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_8_4_overflow_26 """select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_8_7_overflow_27_test_data = ["""0.9999999""","""0.9999999""","""1.9999999""","""1.9999999""","""8.9999999""","""8.9999999""","""9.9999999""","""9.9999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_8_7_overflow_27_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_8_7_overflow_27_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_8_7_overflow_27 """select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_8_8_overflow_28_test_data = ["""0.99999999""","""0.99999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_8_8_overflow_28_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 8)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_8_8_overflow_28_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_8_8_overflow_28 """select cast(cast("${test_str}" as decimalv3(8, 8)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 8)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_9_0_overflow_29_test_data = ["""1""","""999999998""","""999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_9_0_overflow_29_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_9_0_overflow_29_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_9_0_overflow_29 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_9_1_overflow_30_test_data = ["""1.9""","""99999998.9""","""99999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_9_1_overflow_30_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_9_1_overflow_30_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_9_1_overflow_30 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_9_4_overflow_31_test_data = ["""0.9999""","""0.9999""","""1.9999""","""1.9999""","""99998.9999""","""99998.9999""","""99999.9999""","""99999.9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_9_4_overflow_31_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_9_4_overflow_31_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_9_4_overflow_31 """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_9_8_overflow_32_test_data = ["""0.99999999""","""0.99999999""","""1.99999999""","""1.99999999""","""8.99999999""","""8.99999999""","""9.99999999""","""9.99999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_9_8_overflow_32_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_9_8_overflow_32_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_9_8_overflow_32 """select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(1, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_1_1_from_decimal_9_9_overflow_33_test_data = ["""0.999999999""","""0.999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_1_1_from_decimal_9_9_overflow_33_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 9)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_1_1_from_decimal_9_9_overflow_33_test_data) {
        qt_sql_test_cast_to_decimal_1_1_from_decimal_9_9_overflow_33 """select cast(cast("${test_str}" as decimalv3(9, 9)) as decimalv3(1, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 9)) as decimalv3(1, 1));""")
    }
}