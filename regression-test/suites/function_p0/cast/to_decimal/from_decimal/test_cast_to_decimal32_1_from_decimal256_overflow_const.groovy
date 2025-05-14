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


suite("test_cast_to_decimal32_1_from_decimal256_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_decimal256 = true;"
    def test_cast_to_decimal32_1_0_from_decimal256_38_0_vals_0 = [("10"),("99999999999999999999999999999999999998"),("99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_38_0_vals_0) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 0)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_38_0_vals_0) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_38_0 """select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_38_0_vals_0) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_38_1_vals_1 = [("9.9"),("9.9"),("10.9"),("10.9"),("9999999999999999999999999999999999998.9"),
        ("9999999999999999999999999999999999998.9"),("9999999999999999999999999999999999999.9"),("9999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_38_1_vals_1) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 1)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_38_1_vals_1) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_38_1 """select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_38_1_vals_1) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_38_19_vals_2 = [("9.9999999999999999999"),("9.9999999999999999999"),("10.9999999999999999999"),("10.9999999999999999999"),("9999999999999999998.9999999999999999999"),
        ("9999999999999999998.9999999999999999999"),("9999999999999999999.9999999999999999999"),("9999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_38_19_vals_2) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 19)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_38_19_vals_2) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_38_19 """select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_38_19_vals_2) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_38_37_vals_3 = [("9.9999999999999999999999999999999999999"),("9.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_38_37_vals_3) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 37)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_38_37_vals_3) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_38_37 """select cast(cast("${test_str}" as decimalv3(38, 37)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_38_37_vals_3) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 37)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_39_0_vals_5 = [("10"),("999999999999999999999999999999999999998"),("999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_39_0_vals_5) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 0)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_39_0_vals_5) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_39_0 """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_39_0_vals_5) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_39_1_vals_6 = [("9.9"),("9.9"),("10.9"),("10.9"),("99999999999999999999999999999999999998.9"),
        ("99999999999999999999999999999999999998.9"),("99999999999999999999999999999999999999.9"),("99999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_39_1_vals_6) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 1)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_39_1_vals_6) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_39_1 """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_39_1_vals_6) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_39_19_vals_7 = [("9.9999999999999999999"),("9.9999999999999999999"),("10.9999999999999999999"),("10.9999999999999999999"),("99999999999999999998.9999999999999999999"),
        ("99999999999999999998.9999999999999999999"),("99999999999999999999.9999999999999999999"),("99999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_39_19_vals_7) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 19)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_39_19_vals_7) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_39_19 """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_39_19_vals_7) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_39_38_vals_8 = [("9.99999999999999999999999999999999999999"),("9.99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_39_38_vals_8) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 38)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_39_38_vals_8) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_39_38 """select cast(cast("${test_str}" as decimalv3(39, 38)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_39_38_vals_8) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 38)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_75_0_vals_10 = [("10"),("999999999999999999999999999999999999999999999999999999999999999999999999998"),("999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_75_0_vals_10) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 0)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_75_0_vals_10) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_75_0 """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_75_0_vals_10) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_75_1_vals_11 = [("9.9"),("9.9"),("10.9"),("10.9"),("99999999999999999999999999999999999999999999999999999999999999999999999998.9"),
        ("99999999999999999999999999999999999999999999999999999999999999999999999998.9"),("99999999999999999999999999999999999999999999999999999999999999999999999999.9"),("99999999999999999999999999999999999999999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_75_1_vals_11) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 1)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_75_1_vals_11) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_75_1 """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_75_1_vals_11) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_75_37_vals_12 = [("9.9999999999999999999999999999999999999"),("9.9999999999999999999999999999999999999"),("10.9999999999999999999999999999999999999"),("10.9999999999999999999999999999999999999"),("99999999999999999999999999999999999998.9999999999999999999999999999999999999"),
        ("99999999999999999999999999999999999998.9999999999999999999999999999999999999"),("99999999999999999999999999999999999999.9999999999999999999999999999999999999"),("99999999999999999999999999999999999999.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_75_37_vals_12) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 37)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_75_37_vals_12) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_75_37 """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_75_37_vals_12) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_75_74_vals_13 = [("9.99999999999999999999999999999999999999999999999999999999999999999999999999"),("9.99999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_75_74_vals_13) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 74)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_75_74_vals_13) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_75_74 """select cast(cast("${test_str}" as decimalv3(75, 74)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_75_74_vals_13) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 74)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_76_0_vals_15 = [("10"),("9999999999999999999999999999999999999999999999999999999999999999999999999998"),("9999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_76_0_vals_15) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 0)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_76_0_vals_15) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_76_0 """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_76_0_vals_15) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_76_1_vals_16 = [("9.9"),("9.9"),("10.9"),("10.9"),("999999999999999999999999999999999999999999999999999999999999999999999999998.9"),
        ("999999999999999999999999999999999999999999999999999999999999999999999999998.9"),("999999999999999999999999999999999999999999999999999999999999999999999999999.9"),("999999999999999999999999999999999999999999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_76_1_vals_16) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 1)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_76_1_vals_16) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_76_1 """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_76_1_vals_16) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_76_38_vals_17 = [("9.99999999999999999999999999999999999999"),("9.99999999999999999999999999999999999999"),("10.99999999999999999999999999999999999999"),("10.99999999999999999999999999999999999999"),("99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
        ("99999999999999999999999999999999999998.99999999999999999999999999999999999999"),("99999999999999999999999999999999999999.99999999999999999999999999999999999999"),("99999999999999999999999999999999999999.99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_76_38_vals_17) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 38)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_76_38_vals_17) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_76_38 """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_76_38_vals_17) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_0_from_decimal256_76_75_vals_18 = [("9.999999999999999999999999999999999999999999999999999999999999999999999999999"),("9.999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_decimal256_76_75_vals_18) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 75)) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_76_75_vals_18) {
        qt_sql_test_cast_to_decimal32_1_0_from_decimal256_76_75 """select cast(cast("${test_str}" as decimalv3(76, 75)) as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_decimal256_76_75_vals_18) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 75)) as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_38_0_vals_20 = [("1"),("99999999999999999999999999999999999998"),("99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_0_vals_20) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 0)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_0_vals_20) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_38_0 """select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_0_vals_20) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_38_1_vals_21 = [("1.9"),("9999999999999999999999999999999999998.9"),("9999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_1_vals_21) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 1)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_1_vals_21) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_38_1 """select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_1_vals_21) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_38_19_vals_22 = [("0.9999999999999999999"),("0.9999999999999999999"),("1.9999999999999999999"),("1.9999999999999999999"),("9999999999999999998.9999999999999999999"),
        ("9999999999999999998.9999999999999999999"),("9999999999999999999.9999999999999999999"),("9999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_19_vals_22) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 19)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_19_vals_22) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_38_19 """select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_19_vals_22) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_38_37_vals_23 = [("0.9999999999999999999999999999999999999"),("0.9999999999999999999999999999999999999"),("1.9999999999999999999999999999999999999"),("1.9999999999999999999999999999999999999"),("8.9999999999999999999999999999999999999"),
        ("8.9999999999999999999999999999999999999"),("9.9999999999999999999999999999999999999"),("9.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_37_vals_23) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 37)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_37_vals_23) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_38_37 """select cast(cast("${test_str}" as decimalv3(38, 37)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_37_vals_23) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 37)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_38_38_vals_24 = [("0.99999999999999999999999999999999999999"),("0.99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_38_vals_24) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 38)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_38_vals_24) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_38_38 """select cast(cast("${test_str}" as decimalv3(38, 38)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_38_38_vals_24) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 38)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_39_0_vals_25 = [("1"),("999999999999999999999999999999999999998"),("999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_0_vals_25) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 0)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_0_vals_25) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_39_0 """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_0_vals_25) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_39_1_vals_26 = [("1.9"),("99999999999999999999999999999999999998.9"),("99999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_1_vals_26) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 1)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_1_vals_26) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_39_1 """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_1_vals_26) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_39_19_vals_27 = [("0.9999999999999999999"),("0.9999999999999999999"),("1.9999999999999999999"),("1.9999999999999999999"),("99999999999999999998.9999999999999999999"),
        ("99999999999999999998.9999999999999999999"),("99999999999999999999.9999999999999999999"),("99999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_19_vals_27) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 19)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_19_vals_27) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_39_19 """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_19_vals_27) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_39_38_vals_28 = [("0.99999999999999999999999999999999999999"),("0.99999999999999999999999999999999999999"),("1.99999999999999999999999999999999999999"),("1.99999999999999999999999999999999999999"),("8.99999999999999999999999999999999999999"),
        ("8.99999999999999999999999999999999999999"),("9.99999999999999999999999999999999999999"),("9.99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_38_vals_28) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 38)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_38_vals_28) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_39_38 """select cast(cast("${test_str}" as decimalv3(39, 38)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_38_vals_28) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 38)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_39_39_vals_29 = [("0.999999999999999999999999999999999999999"),("0.999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_39_vals_29) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 39)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_39_vals_29) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_39_39 """select cast(cast("${test_str}" as decimalv3(39, 39)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_39_39_vals_29) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 39)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_75_0_vals_30 = [("1"),("999999999999999999999999999999999999999999999999999999999999999999999999998"),("999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_0_vals_30) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 0)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_0_vals_30) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_75_0 """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_0_vals_30) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_75_1_vals_31 = [("1.9"),("99999999999999999999999999999999999999999999999999999999999999999999999998.9"),("99999999999999999999999999999999999999999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_1_vals_31) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 1)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_1_vals_31) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_75_1 """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_1_vals_31) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_75_37_vals_32 = [("0.9999999999999999999999999999999999999"),("0.9999999999999999999999999999999999999"),("1.9999999999999999999999999999999999999"),("1.9999999999999999999999999999999999999"),("99999999999999999999999999999999999998.9999999999999999999999999999999999999"),
        ("99999999999999999999999999999999999998.9999999999999999999999999999999999999"),("99999999999999999999999999999999999999.9999999999999999999999999999999999999"),("99999999999999999999999999999999999999.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_37_vals_32) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 37)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_37_vals_32) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_75_37 """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_37_vals_32) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_75_74_vals_33 = [("0.99999999999999999999999999999999999999999999999999999999999999999999999999"),("0.99999999999999999999999999999999999999999999999999999999999999999999999999"),("1.99999999999999999999999999999999999999999999999999999999999999999999999999"),("1.99999999999999999999999999999999999999999999999999999999999999999999999999"),("8.99999999999999999999999999999999999999999999999999999999999999999999999999"),
        ("8.99999999999999999999999999999999999999999999999999999999999999999999999999"),("9.99999999999999999999999999999999999999999999999999999999999999999999999999"),("9.99999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_74_vals_33) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 74)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_74_vals_33) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_75_74 """select cast(cast("${test_str}" as decimalv3(75, 74)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_74_vals_33) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 74)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_75_75_vals_34 = [("0.999999999999999999999999999999999999999999999999999999999999999999999999999"),("0.999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_75_vals_34) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 75)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_75_vals_34) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_75_75 """select cast(cast("${test_str}" as decimalv3(75, 75)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_75_75_vals_34) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 75)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_76_0_vals_35 = [("1"),("9999999999999999999999999999999999999999999999999999999999999999999999999998"),("9999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_0_vals_35) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 0)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_0_vals_35) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_76_0 """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_0_vals_35) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_76_1_vals_36 = [("1.9"),("999999999999999999999999999999999999999999999999999999999999999999999999998.9"),("999999999999999999999999999999999999999999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_1_vals_36) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 1)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_1_vals_36) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_76_1 """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_1_vals_36) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_76_38_vals_37 = [("0.99999999999999999999999999999999999999"),("0.99999999999999999999999999999999999999"),("1.99999999999999999999999999999999999999"),("1.99999999999999999999999999999999999999"),("99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
        ("99999999999999999999999999999999999998.99999999999999999999999999999999999999"),("99999999999999999999999999999999999999.99999999999999999999999999999999999999"),("99999999999999999999999999999999999999.99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_38_vals_37) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 38)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_38_vals_37) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_76_38 """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_38_vals_37) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_76_75_vals_38 = [("0.999999999999999999999999999999999999999999999999999999999999999999999999999"),("0.999999999999999999999999999999999999999999999999999999999999999999999999999"),("1.999999999999999999999999999999999999999999999999999999999999999999999999999"),("1.999999999999999999999999999999999999999999999999999999999999999999999999999"),("8.999999999999999999999999999999999999999999999999999999999999999999999999999"),
        ("8.999999999999999999999999999999999999999999999999999999999999999999999999999"),("9.999999999999999999999999999999999999999999999999999999999999999999999999999"),("9.999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_75_vals_38) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 75)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_75_vals_38) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_76_75 """select cast(cast("${test_str}" as decimalv3(76, 75)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_75_vals_38) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 75)) as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_1_1_from_decimal256_76_76_vals_39 = [("0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),("0.9999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_76_vals_39) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 76)) as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_76_vals_39) {
        qt_sql_test_cast_to_decimal32_1_1_from_decimal256_76_76 """select cast(cast("${test_str}" as decimalv3(76, 76)) as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_1_1_from_decimal256_76_76_vals_39) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 76)) as decimalv3(1, 1));""")
    }
}