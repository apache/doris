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


suite("test_cast_to_decimal32_9_from_decimal128i_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_9_0_from_decimal128i_19_0_vals_0 = [("1000000000"),("9999999999999999998"),("9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_19_0_vals_0) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 0)) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_19_0_vals_0) {
        qt_sql_test_cast_to_decimal32_9_0_from_decimal128i_19_0 """select cast(cast("${test_str}" as decimalv3(19, 0)) as decimalv3(9, 0));"""
    }

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_19_0_vals_0) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 0)) as decimalv3(9, 0));""")
    }
    def test_cast_to_decimal32_9_0_from_decimal128i_19_1_vals_1 = [("999999999.9"),("999999999.9"),("1000000000.9"),("1000000000.9"),("999999999999999998.9"),
        ("999999999999999998.9"),("999999999999999999.9"),("999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_19_1_vals_1) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 1)) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_19_1_vals_1) {
        qt_sql_test_cast_to_decimal32_9_0_from_decimal128i_19_1 """select cast(cast("${test_str}" as decimalv3(19, 1)) as decimalv3(9, 0));"""
    }

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_19_1_vals_1) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 1)) as decimalv3(9, 0));""")
    }
    def test_cast_to_decimal32_9_0_from_decimal128i_19_9_vals_2 = [("999999999.999999999"),("999999999.999999999"),("1000000000.999999999"),("1000000000.999999999"),("9999999998.999999999"),
        ("9999999998.999999999"),("9999999999.999999999"),("9999999999.999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_19_9_vals_2) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 9)) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_19_9_vals_2) {
        qt_sql_test_cast_to_decimal32_9_0_from_decimal128i_19_9 """select cast(cast("${test_str}" as decimalv3(19, 9)) as decimalv3(9, 0));"""
    }

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_19_9_vals_2) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 9)) as decimalv3(9, 0));""")
    }
    def test_cast_to_decimal32_9_0_from_decimal128i_37_0_vals_5 = [("1000000000"),("9999999999999999999999999999999999998"),("9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_37_0_vals_5) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 0)) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_37_0_vals_5) {
        qt_sql_test_cast_to_decimal32_9_0_from_decimal128i_37_0 """select cast(cast("${test_str}" as decimalv3(37, 0)) as decimalv3(9, 0));"""
    }

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_37_0_vals_5) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 0)) as decimalv3(9, 0));""")
    }
    def test_cast_to_decimal32_9_0_from_decimal128i_37_1_vals_6 = [("999999999.9"),("999999999.9"),("1000000000.9"),("1000000000.9"),("999999999999999999999999999999999998.9"),
        ("999999999999999999999999999999999998.9"),("999999999999999999999999999999999999.9"),("999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_37_1_vals_6) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 1)) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_37_1_vals_6) {
        qt_sql_test_cast_to_decimal32_9_0_from_decimal128i_37_1 """select cast(cast("${test_str}" as decimalv3(37, 1)) as decimalv3(9, 0));"""
    }

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_37_1_vals_6) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 1)) as decimalv3(9, 0));""")
    }
    def test_cast_to_decimal32_9_0_from_decimal128i_37_18_vals_7 = [("999999999.999999999999999999"),("999999999.999999999999999999"),("1000000000.999999999999999999"),("1000000000.999999999999999999"),("9999999999999999998.999999999999999999"),
        ("9999999999999999998.999999999999999999"),("9999999999999999999.999999999999999999"),("9999999999999999999.999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_37_18_vals_7) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 18)) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_37_18_vals_7) {
        qt_sql_test_cast_to_decimal32_9_0_from_decimal128i_37_18 """select cast(cast("${test_str}" as decimalv3(37, 18)) as decimalv3(9, 0));"""
    }

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_37_18_vals_7) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 18)) as decimalv3(9, 0));""")
    }
    def test_cast_to_decimal32_9_0_from_decimal128i_38_0_vals_10 = [("1000000000"),("99999999999999999999999999999999999998"),("99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_38_0_vals_10) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 0)) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_38_0_vals_10) {
        qt_sql_test_cast_to_decimal32_9_0_from_decimal128i_38_0 """select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(9, 0));"""
    }

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_38_0_vals_10) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(9, 0));""")
    }
    def test_cast_to_decimal32_9_0_from_decimal128i_38_1_vals_11 = [("999999999.9"),("999999999.9"),("1000000000.9"),("1000000000.9"),("9999999999999999999999999999999999998.9"),
        ("9999999999999999999999999999999999998.9"),("9999999999999999999999999999999999999.9"),("9999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_38_1_vals_11) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 1)) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_38_1_vals_11) {
        qt_sql_test_cast_to_decimal32_9_0_from_decimal128i_38_1 """select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(9, 0));"""
    }

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_38_1_vals_11) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(9, 0));""")
    }
    def test_cast_to_decimal32_9_0_from_decimal128i_38_19_vals_12 = [("999999999.9999999999999999999"),("999999999.9999999999999999999"),("1000000000.9999999999999999999"),("1000000000.9999999999999999999"),("9999999999999999998.9999999999999999999"),
        ("9999999999999999998.9999999999999999999"),("9999999999999999999.9999999999999999999"),("9999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_38_19_vals_12) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 19)) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_38_19_vals_12) {
        qt_sql_test_cast_to_decimal32_9_0_from_decimal128i_38_19 """select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(9, 0));"""
    }

    for (test_str in test_cast_to_decimal32_9_0_from_decimal128i_38_19_vals_12) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(9, 0));""")
    }
    def test_cast_to_decimal32_9_1_from_decimal128i_19_0_vals_15 = [("100000000"),("9999999999999999998"),("9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_19_0_vals_15) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 0)) as decimalv3(9, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_19_0_vals_15) {
        qt_sql_test_cast_to_decimal32_9_1_from_decimal128i_19_0 """select cast(cast("${test_str}" as decimalv3(19, 0)) as decimalv3(9, 1));"""
    }

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_19_0_vals_15) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 0)) as decimalv3(9, 1));""")
    }
    def test_cast_to_decimal32_9_1_from_decimal128i_19_1_vals_16 = [("100000000.9"),("999999999999999998.9"),("999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_19_1_vals_16) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 1)) as decimalv3(9, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_19_1_vals_16) {
        qt_sql_test_cast_to_decimal32_9_1_from_decimal128i_19_1 """select cast(cast("${test_str}" as decimalv3(19, 1)) as decimalv3(9, 1));"""
    }

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_19_1_vals_16) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 1)) as decimalv3(9, 1));""")
    }
    def test_cast_to_decimal32_9_1_from_decimal128i_19_9_vals_17 = [("99999999.999999999"),("99999999.999999999"),("100000000.999999999"),("100000000.999999999"),("9999999998.999999999"),
        ("9999999998.999999999"),("9999999999.999999999"),("9999999999.999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_19_9_vals_17) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 9)) as decimalv3(9, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_19_9_vals_17) {
        qt_sql_test_cast_to_decimal32_9_1_from_decimal128i_19_9 """select cast(cast("${test_str}" as decimalv3(19, 9)) as decimalv3(9, 1));"""
    }

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_19_9_vals_17) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 9)) as decimalv3(9, 1));""")
    }
    def test_cast_to_decimal32_9_1_from_decimal128i_37_0_vals_20 = [("100000000"),("9999999999999999999999999999999999998"),("9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_37_0_vals_20) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 0)) as decimalv3(9, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_37_0_vals_20) {
        qt_sql_test_cast_to_decimal32_9_1_from_decimal128i_37_0 """select cast(cast("${test_str}" as decimalv3(37, 0)) as decimalv3(9, 1));"""
    }

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_37_0_vals_20) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 0)) as decimalv3(9, 1));""")
    }
    def test_cast_to_decimal32_9_1_from_decimal128i_37_1_vals_21 = [("100000000.9"),("999999999999999999999999999999999998.9"),("999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_37_1_vals_21) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 1)) as decimalv3(9, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_37_1_vals_21) {
        qt_sql_test_cast_to_decimal32_9_1_from_decimal128i_37_1 """select cast(cast("${test_str}" as decimalv3(37, 1)) as decimalv3(9, 1));"""
    }

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_37_1_vals_21) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 1)) as decimalv3(9, 1));""")
    }
    def test_cast_to_decimal32_9_1_from_decimal128i_37_18_vals_22 = [("99999999.999999999999999999"),("99999999.999999999999999999"),("100000000.999999999999999999"),("100000000.999999999999999999"),("9999999999999999998.999999999999999999"),
        ("9999999999999999998.999999999999999999"),("9999999999999999999.999999999999999999"),("9999999999999999999.999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_37_18_vals_22) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 18)) as decimalv3(9, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_37_18_vals_22) {
        qt_sql_test_cast_to_decimal32_9_1_from_decimal128i_37_18 """select cast(cast("${test_str}" as decimalv3(37, 18)) as decimalv3(9, 1));"""
    }

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_37_18_vals_22) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 18)) as decimalv3(9, 1));""")
    }
    def test_cast_to_decimal32_9_1_from_decimal128i_38_0_vals_25 = [("100000000"),("99999999999999999999999999999999999998"),("99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_38_0_vals_25) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 0)) as decimalv3(9, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_38_0_vals_25) {
        qt_sql_test_cast_to_decimal32_9_1_from_decimal128i_38_0 """select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(9, 1));"""
    }

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_38_0_vals_25) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(9, 1));""")
    }
    def test_cast_to_decimal32_9_1_from_decimal128i_38_1_vals_26 = [("100000000.9"),("9999999999999999999999999999999999998.9"),("9999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_38_1_vals_26) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 1)) as decimalv3(9, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_38_1_vals_26) {
        qt_sql_test_cast_to_decimal32_9_1_from_decimal128i_38_1 """select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(9, 1));"""
    }

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_38_1_vals_26) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(9, 1));""")
    }
    def test_cast_to_decimal32_9_1_from_decimal128i_38_19_vals_27 = [("99999999.9999999999999999999"),("99999999.9999999999999999999"),("100000000.9999999999999999999"),("100000000.9999999999999999999"),("9999999999999999998.9999999999999999999"),
        ("9999999999999999998.9999999999999999999"),("9999999999999999999.9999999999999999999"),("9999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_38_19_vals_27) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 19)) as decimalv3(9, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_38_19_vals_27) {
        qt_sql_test_cast_to_decimal32_9_1_from_decimal128i_38_19 """select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(9, 1));"""
    }

    for (test_str in test_cast_to_decimal32_9_1_from_decimal128i_38_19_vals_27) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(9, 1));""")
    }
    def test_cast_to_decimal32_9_4_from_decimal128i_19_0_vals_30 = [("100000"),("9999999999999999998"),("9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_19_0_vals_30) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 0)) as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_19_0_vals_30) {
        qt_sql_test_cast_to_decimal32_9_4_from_decimal128i_19_0 """select cast(cast("${test_str}" as decimalv3(19, 0)) as decimalv3(9, 4));"""
    }

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_19_0_vals_30) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 0)) as decimalv3(9, 4));""")
    }
    def test_cast_to_decimal32_9_4_from_decimal128i_19_1_vals_31 = [("100000.9"),("999999999999999998.9"),("999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_19_1_vals_31) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 1)) as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_19_1_vals_31) {
        qt_sql_test_cast_to_decimal32_9_4_from_decimal128i_19_1 """select cast(cast("${test_str}" as decimalv3(19, 1)) as decimalv3(9, 4));"""
    }

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_19_1_vals_31) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 1)) as decimalv3(9, 4));""")
    }
    def test_cast_to_decimal32_9_4_from_decimal128i_19_9_vals_32 = [("99999.999999999"),("99999.999999999"),("100000.999999999"),("100000.999999999"),("9999999998.999999999"),
        ("9999999998.999999999"),("9999999999.999999999"),("9999999999.999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_19_9_vals_32) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 9)) as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_19_9_vals_32) {
        qt_sql_test_cast_to_decimal32_9_4_from_decimal128i_19_9 """select cast(cast("${test_str}" as decimalv3(19, 9)) as decimalv3(9, 4));"""
    }

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_19_9_vals_32) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 9)) as decimalv3(9, 4));""")
    }
    def test_cast_to_decimal32_9_4_from_decimal128i_37_0_vals_35 = [("100000"),("9999999999999999999999999999999999998"),("9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_37_0_vals_35) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 0)) as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_37_0_vals_35) {
        qt_sql_test_cast_to_decimal32_9_4_from_decimal128i_37_0 """select cast(cast("${test_str}" as decimalv3(37, 0)) as decimalv3(9, 4));"""
    }

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_37_0_vals_35) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 0)) as decimalv3(9, 4));""")
    }
    def test_cast_to_decimal32_9_4_from_decimal128i_37_1_vals_36 = [("100000.9"),("999999999999999999999999999999999998.9"),("999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_37_1_vals_36) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 1)) as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_37_1_vals_36) {
        qt_sql_test_cast_to_decimal32_9_4_from_decimal128i_37_1 """select cast(cast("${test_str}" as decimalv3(37, 1)) as decimalv3(9, 4));"""
    }

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_37_1_vals_36) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 1)) as decimalv3(9, 4));""")
    }
    def test_cast_to_decimal32_9_4_from_decimal128i_37_18_vals_37 = [("99999.999999999999999999"),("99999.999999999999999999"),("100000.999999999999999999"),("100000.999999999999999999"),("9999999999999999998.999999999999999999"),
        ("9999999999999999998.999999999999999999"),("9999999999999999999.999999999999999999"),("9999999999999999999.999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_37_18_vals_37) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 18)) as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_37_18_vals_37) {
        qt_sql_test_cast_to_decimal32_9_4_from_decimal128i_37_18 """select cast(cast("${test_str}" as decimalv3(37, 18)) as decimalv3(9, 4));"""
    }

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_37_18_vals_37) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 18)) as decimalv3(9, 4));""")
    }
    def test_cast_to_decimal32_9_4_from_decimal128i_38_0_vals_40 = [("100000"),("99999999999999999999999999999999999998"),("99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_38_0_vals_40) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 0)) as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_38_0_vals_40) {
        qt_sql_test_cast_to_decimal32_9_4_from_decimal128i_38_0 """select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(9, 4));"""
    }

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_38_0_vals_40) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(9, 4));""")
    }
    def test_cast_to_decimal32_9_4_from_decimal128i_38_1_vals_41 = [("100000.9"),("9999999999999999999999999999999999998.9"),("9999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_38_1_vals_41) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 1)) as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_38_1_vals_41) {
        qt_sql_test_cast_to_decimal32_9_4_from_decimal128i_38_1 """select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(9, 4));"""
    }

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_38_1_vals_41) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(9, 4));""")
    }
    def test_cast_to_decimal32_9_4_from_decimal128i_38_19_vals_42 = [("99999.9999999999999999999"),("99999.9999999999999999999"),("100000.9999999999999999999"),("100000.9999999999999999999"),("9999999999999999998.9999999999999999999"),
        ("9999999999999999998.9999999999999999999"),("9999999999999999999.9999999999999999999"),("9999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_38_19_vals_42) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 19)) as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_38_19_vals_42) {
        qt_sql_test_cast_to_decimal32_9_4_from_decimal128i_38_19 """select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(9, 4));"""
    }

    for (test_str in test_cast_to_decimal32_9_4_from_decimal128i_38_19_vals_42) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(9, 4));""")
    }
    def test_cast_to_decimal32_9_8_from_decimal128i_19_0_vals_45 = [("10"),("9999999999999999998"),("9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_19_0_vals_45) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 0)) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_19_0_vals_45) {
        qt_sql_test_cast_to_decimal32_9_8_from_decimal128i_19_0 """select cast(cast("${test_str}" as decimalv3(19, 0)) as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_19_0_vals_45) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 0)) as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_9_8_from_decimal128i_19_1_vals_46 = [("10.9"),("999999999999999998.9"),("999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_19_1_vals_46) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 1)) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_19_1_vals_46) {
        qt_sql_test_cast_to_decimal32_9_8_from_decimal128i_19_1 """select cast(cast("${test_str}" as decimalv3(19, 1)) as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_19_1_vals_46) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 1)) as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_9_8_from_decimal128i_19_9_vals_47 = [("9.999999999"),("9.999999999"),("10.999999999"),("10.999999999"),("9999999998.999999999"),
        ("9999999998.999999999"),("9999999999.999999999"),("9999999999.999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_19_9_vals_47) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 9)) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_19_9_vals_47) {
        qt_sql_test_cast_to_decimal32_9_8_from_decimal128i_19_9 """select cast(cast("${test_str}" as decimalv3(19, 9)) as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_19_9_vals_47) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 9)) as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_9_8_from_decimal128i_19_18_vals_48 = [("9.999999999999999999"),("9.999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_19_18_vals_48) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 18)) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_19_18_vals_48) {
        qt_sql_test_cast_to_decimal32_9_8_from_decimal128i_19_18 """select cast(cast("${test_str}" as decimalv3(19, 18)) as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_19_18_vals_48) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 18)) as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_9_8_from_decimal128i_37_0_vals_50 = [("10"),("9999999999999999999999999999999999998"),("9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_37_0_vals_50) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 0)) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_37_0_vals_50) {
        qt_sql_test_cast_to_decimal32_9_8_from_decimal128i_37_0 """select cast(cast("${test_str}" as decimalv3(37, 0)) as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_37_0_vals_50) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 0)) as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_9_8_from_decimal128i_37_1_vals_51 = [("10.9"),("999999999999999999999999999999999998.9"),("999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_37_1_vals_51) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 1)) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_37_1_vals_51) {
        qt_sql_test_cast_to_decimal32_9_8_from_decimal128i_37_1 """select cast(cast("${test_str}" as decimalv3(37, 1)) as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_37_1_vals_51) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 1)) as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_9_8_from_decimal128i_37_18_vals_52 = [("9.999999999999999999"),("9.999999999999999999"),("10.999999999999999999"),("10.999999999999999999"),("9999999999999999998.999999999999999999"),
        ("9999999999999999998.999999999999999999"),("9999999999999999999.999999999999999999"),("9999999999999999999.999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_37_18_vals_52) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 18)) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_37_18_vals_52) {
        qt_sql_test_cast_to_decimal32_9_8_from_decimal128i_37_18 """select cast(cast("${test_str}" as decimalv3(37, 18)) as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_37_18_vals_52) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 18)) as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_9_8_from_decimal128i_37_36_vals_53 = [("9.999999999999999999999999999999999999"),("9.999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_37_36_vals_53) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 36)) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_37_36_vals_53) {
        qt_sql_test_cast_to_decimal32_9_8_from_decimal128i_37_36 """select cast(cast("${test_str}" as decimalv3(37, 36)) as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_37_36_vals_53) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 36)) as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_9_8_from_decimal128i_38_0_vals_55 = [("10"),("99999999999999999999999999999999999998"),("99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_38_0_vals_55) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 0)) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_38_0_vals_55) {
        qt_sql_test_cast_to_decimal32_9_8_from_decimal128i_38_0 """select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_38_0_vals_55) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_9_8_from_decimal128i_38_1_vals_56 = [("10.9"),("9999999999999999999999999999999999998.9"),("9999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_38_1_vals_56) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 1)) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_38_1_vals_56) {
        qt_sql_test_cast_to_decimal32_9_8_from_decimal128i_38_1 """select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_38_1_vals_56) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_9_8_from_decimal128i_38_19_vals_57 = [("9.9999999999999999999"),("9.9999999999999999999"),("10.9999999999999999999"),("10.9999999999999999999"),("9999999999999999998.9999999999999999999"),
        ("9999999999999999998.9999999999999999999"),("9999999999999999999.9999999999999999999"),("9999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_38_19_vals_57) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 19)) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_38_19_vals_57) {
        qt_sql_test_cast_to_decimal32_9_8_from_decimal128i_38_19 """select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_38_19_vals_57) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_9_8_from_decimal128i_38_37_vals_58 = [("9.9999999999999999999999999999999999999"),("9.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_38_37_vals_58) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 37)) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_38_37_vals_58) {
        qt_sql_test_cast_to_decimal32_9_8_from_decimal128i_38_37 """select cast(cast("${test_str}" as decimalv3(38, 37)) as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_decimal128i_38_37_vals_58) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 37)) as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_19_0_vals_60 = [("1"),("9999999999999999998"),("9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_0_vals_60) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 0)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_0_vals_60) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_19_0 """select cast(cast("${test_str}" as decimalv3(19, 0)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_0_vals_60) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 0)) as decimalv3(9, 9));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_19_1_vals_61 = [("1.9"),("999999999999999998.9"),("999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_1_vals_61) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 1)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_1_vals_61) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_19_1 """select cast(cast("${test_str}" as decimalv3(19, 1)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_1_vals_61) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 1)) as decimalv3(9, 9));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_19_9_vals_62 = [("1.999999999"),("9999999998.999999999"),("9999999999.999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_9_vals_62) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 9)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_9_vals_62) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_19_9 """select cast(cast("${test_str}" as decimalv3(19, 9)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_9_vals_62) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 9)) as decimalv3(9, 9));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_19_18_vals_63 = [("0.999999999999999999"),("0.999999999999999999"),("1.999999999999999999"),("1.999999999999999999"),("8.999999999999999999"),
        ("8.999999999999999999"),("9.999999999999999999"),("9.999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_18_vals_63) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 18)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_18_vals_63) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_19_18 """select cast(cast("${test_str}" as decimalv3(19, 18)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_18_vals_63) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 18)) as decimalv3(9, 9));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_19_19_vals_64 = [("0.9999999999999999999"),("0.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_19_vals_64) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 19)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_19_vals_64) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_19_19 """select cast(cast("${test_str}" as decimalv3(19, 19)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_19_19_vals_64) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 19)) as decimalv3(9, 9));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_37_0_vals_65 = [("1"),("9999999999999999999999999999999999998"),("9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_0_vals_65) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 0)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_0_vals_65) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_37_0 """select cast(cast("${test_str}" as decimalv3(37, 0)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_0_vals_65) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 0)) as decimalv3(9, 9));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_37_1_vals_66 = [("1.9"),("999999999999999999999999999999999998.9"),("999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_1_vals_66) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 1)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_1_vals_66) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_37_1 """select cast(cast("${test_str}" as decimalv3(37, 1)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_1_vals_66) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 1)) as decimalv3(9, 9));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_37_18_vals_67 = [("0.999999999999999999"),("0.999999999999999999"),("1.999999999999999999"),("1.999999999999999999"),("9999999999999999998.999999999999999999"),
        ("9999999999999999998.999999999999999999"),("9999999999999999999.999999999999999999"),("9999999999999999999.999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_18_vals_67) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 18)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_18_vals_67) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_37_18 """select cast(cast("${test_str}" as decimalv3(37, 18)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_18_vals_67) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 18)) as decimalv3(9, 9));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_37_36_vals_68 = [("0.999999999999999999999999999999999999"),("0.999999999999999999999999999999999999"),("1.999999999999999999999999999999999999"),("1.999999999999999999999999999999999999"),("8.999999999999999999999999999999999999"),
        ("8.999999999999999999999999999999999999"),("9.999999999999999999999999999999999999"),("9.999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_36_vals_68) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 36)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_36_vals_68) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_37_36 """select cast(cast("${test_str}" as decimalv3(37, 36)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_36_vals_68) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 36)) as decimalv3(9, 9));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_37_37_vals_69 = [("0.9999999999999999999999999999999999999"),("0.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_37_vals_69) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 37)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_37_vals_69) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_37_37 """select cast(cast("${test_str}" as decimalv3(37, 37)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_37_37_vals_69) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 37)) as decimalv3(9, 9));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_38_0_vals_70 = [("1"),("99999999999999999999999999999999999998"),("99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_0_vals_70) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 0)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_0_vals_70) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_38_0 """select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_0_vals_70) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(9, 9));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_38_1_vals_71 = [("1.9"),("9999999999999999999999999999999999998.9"),("9999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_1_vals_71) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 1)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_1_vals_71) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_38_1 """select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_1_vals_71) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(9, 9));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_38_19_vals_72 = [("0.9999999999999999999"),("0.9999999999999999999"),("1.9999999999999999999"),("1.9999999999999999999"),("9999999999999999998.9999999999999999999"),
        ("9999999999999999998.9999999999999999999"),("9999999999999999999.9999999999999999999"),("9999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_19_vals_72) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 19)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_19_vals_72) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_38_19 """select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_19_vals_72) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(9, 9));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_38_37_vals_73 = [("0.9999999999999999999999999999999999999"),("0.9999999999999999999999999999999999999"),("1.9999999999999999999999999999999999999"),("1.9999999999999999999999999999999999999"),("8.9999999999999999999999999999999999999"),
        ("8.9999999999999999999999999999999999999"),("9.9999999999999999999999999999999999999"),("9.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_37_vals_73) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 37)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_37_vals_73) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_38_37 """select cast(cast("${test_str}" as decimalv3(38, 37)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_37_vals_73) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 37)) as decimalv3(9, 9));""")
    }
    def test_cast_to_decimal32_9_9_from_decimal128i_38_38_vals_74 = [("0.99999999999999999999999999999999999999"),("0.99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_38_vals_74) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 38)) as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_38_vals_74) {
        qt_sql_test_cast_to_decimal32_9_9_from_decimal128i_38_38 """select cast(cast("${test_str}" as decimalv3(38, 38)) as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_9_9_from_decimal128i_38_38_vals_74) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 38)) as decimalv3(9, 9));""")
    }
}