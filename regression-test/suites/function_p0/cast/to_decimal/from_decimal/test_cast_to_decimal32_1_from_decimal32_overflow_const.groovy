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
    def test_cast_to_decimal32_1_0_from_decimal32_4_0_vals_2 = [("10"),("9998"),("9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal32_4_0_vals_2) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 0)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_4_0_vals_2) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal32_4_0 """select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_4_0_vals_2) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal32_4_1_vals_3 = [("9.9"),("9.9"),("10.9"),("10.9"),("998.9"),
        ("998.9"),("999.9"),("999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal32_4_1_vals_3) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 1)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_4_1_vals_3) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal32_4_1 """select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_4_1_vals_3) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal32_4_2_vals_4 = [("9.99"),("9.99"),("10.99"),("10.99"),("98.99"),
        ("98.99"),("99.99"),("99.99")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal32_4_2_vals_4) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 2)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_4_2_vals_4) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal32_4_2 """select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_4_2_vals_4) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal32_4_3_vals_5 = [("9.999"),("9.999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal32_4_3_vals_5) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 3)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_4_3_vals_5) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal32_4_3 """select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_4_3_vals_5) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal32_8_0_vals_7 = [("10"),("99999998"),("99999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal32_8_0_vals_7) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 0)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_8_0_vals_7) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal32_8_0 """select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_8_0_vals_7) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal32_8_1_vals_8 = [("9.9"),("9.9"),("10.9"),("10.9"),("9999998.9"),
        ("9999998.9"),("9999999.9"),("9999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal32_8_1_vals_8) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 1)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_8_1_vals_8) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal32_8_1 """select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_8_1_vals_8) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal32_8_4_vals_9 = [("9.9999"),("9.9999"),("10.9999"),("10.9999"),("9998.9999"),
        ("9998.9999"),("9999.9999"),("9999.9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal32_8_4_vals_9) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 4)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_8_4_vals_9) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal32_8_4 """select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_8_4_vals_9) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal32_8_7_vals_10 = [("9.9999999"),("9.9999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal32_8_7_vals_10) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 7)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_8_7_vals_10) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal32_8_7 """select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_8_7_vals_10) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal32_9_0_vals_12 = [("10"),("999999998"),("999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal32_9_0_vals_12) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 0)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_9_0_vals_12) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal32_9_0 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_9_0_vals_12) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal32_9_1_vals_13 = [("9.9"),("9.9"),("10.9"),("10.9"),("99999998.9"),
        ("99999998.9"),("99999999.9"),("99999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal32_9_1_vals_13) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 1)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_9_1_vals_13) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal32_9_1 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_9_1_vals_13) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal32_9_4_vals_14 = [("9.9999"),("9.9999"),("10.9999"),("10.9999"),("99998.9999"),
        ("99998.9999"),("99999.9999"),("99999.9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal32_9_4_vals_14) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 4)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_9_4_vals_14) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal32_9_4 """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_9_4_vals_14) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal32_9_8_vals_15 = [("9.99999999"),("9.99999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal32_9_8_vals_15) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 8)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_9_8_vals_15) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal32_9_8 """select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal32_9_8_vals_15) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_1_0_vals_17 = [("1"),("8"),("9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_1_0_vals_17) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(1, 0)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_1_0_vals_17) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_1_0 """select cast(cast("${test_str}" as decimalv3(1, 0)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_1_0_vals_17) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(1, 0)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_4_0_vals_19 = [("1"),("9998"),("9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_0_vals_19) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 0)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_0_vals_19) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_4_0 """select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_0_vals_19) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_4_1_vals_20 = [("1.9"),("998.9"),("999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_1_vals_20) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 1)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_1_vals_20) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_4_1 """select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_1_vals_20) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_4_2_vals_21 = [("0.99"),("0.99"),("1.99"),("1.99"),("98.99"),
        ("98.99"),("99.99"),("99.99")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_2_vals_21) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 2)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_2_vals_21) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_4_2 """select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_2_vals_21) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_4_3_vals_22 = [("0.999"),("0.999"),("1.999"),("1.999"),("8.999"),
        ("8.999"),("9.999"),("9.999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_3_vals_22) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 3)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_3_vals_22) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_4_3 """select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_3_vals_22) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_4_4_vals_23 = [("0.9999"),("0.9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_4_vals_23) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 4)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_4_vals_23) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_4_4 """select cast(cast("${test_str}" as decimalv3(4, 4)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_4_4_vals_23) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 4)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_8_0_vals_24 = [("1"),("99999998"),("99999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_0_vals_24) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 0)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_0_vals_24) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_8_0 """select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_0_vals_24) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_8_1_vals_25 = [("1.9"),("9999998.9"),("9999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_1_vals_25) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 1)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_1_vals_25) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_8_1 """select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_1_vals_25) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_8_4_vals_26 = [("0.9999"),("0.9999"),("1.9999"),("1.9999"),("9998.9999"),
        ("9998.9999"),("9999.9999"),("9999.9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_4_vals_26) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 4)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_4_vals_26) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_8_4 """select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_4_vals_26) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_8_7_vals_27 = [("0.9999999"),("0.9999999"),("1.9999999"),("1.9999999"),("8.9999999"),
        ("8.9999999"),("9.9999999"),("9.9999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_7_vals_27) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 7)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_7_vals_27) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_8_7 """select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_7_vals_27) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_8_8_vals_28 = [("0.99999999"),("0.99999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_8_vals_28) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 8)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_8_vals_28) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_8_8 """select cast(cast("${test_str}" as decimalv3(8, 8)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_8_8_vals_28) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 8)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_9_0_vals_29 = [("1"),("999999998"),("999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_0_vals_29) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 0)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_0_vals_29) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_9_0 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_0_vals_29) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_9_1_vals_30 = [("1.9"),("99999998.9"),("99999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_1_vals_30) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 1)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_1_vals_30) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_9_1 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_1_vals_30) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_9_4_vals_31 = [("0.9999"),("0.9999"),("1.9999"),("1.9999"),("99998.9999"),
        ("99998.9999"),("99999.9999"),("99999.9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_4_vals_31) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 4)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_4_vals_31) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_9_4 """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_4_vals_31) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_9_8_vals_32 = [("0.99999999"),("0.99999999"),("1.99999999"),("1.99999999"),("8.99999999"),
        ("8.99999999"),("9.99999999"),("9.99999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_8_vals_32) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 8)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_8_vals_32) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_9_8 """select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_8_vals_32) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal32_9_9_vals_33 = [("0.999999999"),("0.999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_9_vals_33) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 9)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_9_vals_33) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal32_9_9 """select cast(cast("${test_str}" as decimalv3(9, 9)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal32_9_9_vals_33) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 9)) as decimalv3(1, 1));""")
    }
}