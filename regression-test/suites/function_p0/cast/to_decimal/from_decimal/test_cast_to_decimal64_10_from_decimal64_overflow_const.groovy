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


suite("test_cast_to_decimal64_10_from_decimal64_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal64_10_0_from_decimal64_17_0_vals_10 = [("10000000000"),("99999999999999998"),("99999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_0_from_decimal64_17_0_vals_10) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 0)) as decimalv3(10, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_0_from_decimal64_17_0_vals_10) {
        qt_sql_test_cast_to_decimal64_10_0_from_decimal64_17_0 """select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(10, 0));"""
    }

    for (test_str in test_cast_to_decimal64_10_0_from_decimal64_17_0_vals_10) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(10, 0));""")
    }
    def test_cast_to_decimal64_10_0_from_decimal64_17_1_vals_11 = [("9999999999.9"),("9999999999.9"),("10000000000.9"),("10000000000.9"),("9999999999999998.9"),
        ("9999999999999998.9"),("9999999999999999.9"),("9999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_0_from_decimal64_17_1_vals_11) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 1)) as decimalv3(10, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_0_from_decimal64_17_1_vals_11) {
        qt_sql_test_cast_to_decimal64_10_0_from_decimal64_17_1 """select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(10, 0));"""
    }

    for (test_str in test_cast_to_decimal64_10_0_from_decimal64_17_1_vals_11) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(10, 0));""")
    }
    def test_cast_to_decimal64_10_0_from_decimal64_18_0_vals_15 = [("10000000000"),("999999999999999998"),("999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_0_from_decimal64_18_0_vals_15) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 0)) as decimalv3(10, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_0_from_decimal64_18_0_vals_15) {
        qt_sql_test_cast_to_decimal64_10_0_from_decimal64_18_0 """select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(10, 0));"""
    }

    for (test_str in test_cast_to_decimal64_10_0_from_decimal64_18_0_vals_15) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(10, 0));""")
    }
    def test_cast_to_decimal64_10_0_from_decimal64_18_1_vals_16 = [("9999999999.9"),("9999999999.9"),("10000000000.9"),("10000000000.9"),("99999999999999998.9"),
        ("99999999999999998.9"),("99999999999999999.9"),("99999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_0_from_decimal64_18_1_vals_16) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 1)) as decimalv3(10, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_0_from_decimal64_18_1_vals_16) {
        qt_sql_test_cast_to_decimal64_10_0_from_decimal64_18_1 """select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(10, 0));"""
    }

    for (test_str in test_cast_to_decimal64_10_0_from_decimal64_18_1_vals_16) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(10, 0));""")
    }
    def test_cast_to_decimal64_10_1_from_decimal64_10_0_vals_25 = [("1000000000"),("9999999998"),("9999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_1_from_decimal64_10_0_vals_25) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(10, 0)) as decimalv3(10, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_1_from_decimal64_10_0_vals_25) {
        qt_sql_test_cast_to_decimal64_10_1_from_decimal64_10_0 """select cast(cast("${test_str}" as decimalv3(10, 0)) as decimalv3(10, 1));"""
    }

    for (test_str in test_cast_to_decimal64_10_1_from_decimal64_10_0_vals_25) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 0)) as decimalv3(10, 1));""")
    }
    def test_cast_to_decimal64_10_1_from_decimal64_17_0_vals_30 = [("1000000000"),("99999999999999998"),("99999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_1_from_decimal64_17_0_vals_30) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 0)) as decimalv3(10, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_1_from_decimal64_17_0_vals_30) {
        qt_sql_test_cast_to_decimal64_10_1_from_decimal64_17_0 """select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(10, 1));"""
    }

    for (test_str in test_cast_to_decimal64_10_1_from_decimal64_17_0_vals_30) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(10, 1));""")
    }
    def test_cast_to_decimal64_10_1_from_decimal64_17_1_vals_31 = [("1000000000.9"),("9999999999999998.9"),("9999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_1_from_decimal64_17_1_vals_31) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 1)) as decimalv3(10, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_1_from_decimal64_17_1_vals_31) {
        qt_sql_test_cast_to_decimal64_10_1_from_decimal64_17_1 """select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(10, 1));"""
    }

    for (test_str in test_cast_to_decimal64_10_1_from_decimal64_17_1_vals_31) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(10, 1));""")
    }
    def test_cast_to_decimal64_10_1_from_decimal64_17_8_vals_32 = [("999999999.99999999"),("999999999.99999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_1_from_decimal64_17_8_vals_32) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 8)) as decimalv3(10, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_1_from_decimal64_17_8_vals_32) {
        qt_sql_test_cast_to_decimal64_10_1_from_decimal64_17_8 """select cast(cast("${test_str}" as decimalv3(17, 8)) as decimalv3(10, 1));"""
    }

    for (test_str in test_cast_to_decimal64_10_1_from_decimal64_17_8_vals_32) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 8)) as decimalv3(10, 1));""")
    }
    def test_cast_to_decimal64_10_1_from_decimal64_18_0_vals_35 = [("1000000000"),("999999999999999998"),("999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_1_from_decimal64_18_0_vals_35) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 0)) as decimalv3(10, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_1_from_decimal64_18_0_vals_35) {
        qt_sql_test_cast_to_decimal64_10_1_from_decimal64_18_0 """select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(10, 1));"""
    }

    for (test_str in test_cast_to_decimal64_10_1_from_decimal64_18_0_vals_35) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(10, 1));""")
    }
    def test_cast_to_decimal64_10_1_from_decimal64_18_1_vals_36 = [("1000000000.9"),("99999999999999998.9"),("99999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_1_from_decimal64_18_1_vals_36) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 1)) as decimalv3(10, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_1_from_decimal64_18_1_vals_36) {
        qt_sql_test_cast_to_decimal64_10_1_from_decimal64_18_1 """select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(10, 1));"""
    }

    for (test_str in test_cast_to_decimal64_10_1_from_decimal64_18_1_vals_36) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(10, 1));""")
    }
    def test_cast_to_decimal64_10_1_from_decimal64_18_9_vals_37 = [("999999999.999999999"),("999999999.999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_1_from_decimal64_18_9_vals_37) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 9)) as decimalv3(10, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_1_from_decimal64_18_9_vals_37) {
        qt_sql_test_cast_to_decimal64_10_1_from_decimal64_18_9 """select cast(cast("${test_str}" as decimalv3(18, 9)) as decimalv3(10, 1));"""
    }

    for (test_str in test_cast_to_decimal64_10_1_from_decimal64_18_9_vals_37) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 9)) as decimalv3(10, 1));""")
    }
    def test_cast_to_decimal64_10_5_from_decimal64_9_0_vals_40 = [("100000"),("999999998"),("999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_5_from_decimal64_9_0_vals_40) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 0)) as decimalv3(10, 5));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_9_0_vals_40) {
        qt_sql_test_cast_to_decimal64_10_5_from_decimal64_9_0 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(10, 5));"""
    }

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_9_0_vals_40) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(10, 5));""")
    }
    def test_cast_to_decimal64_10_5_from_decimal64_9_1_vals_41 = [("100000.9"),("99999998.9"),("99999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_5_from_decimal64_9_1_vals_41) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 1)) as decimalv3(10, 5));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_9_1_vals_41) {
        qt_sql_test_cast_to_decimal64_10_5_from_decimal64_9_1 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(10, 5));"""
    }

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_9_1_vals_41) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(10, 5));""")
    }
    def test_cast_to_decimal64_10_5_from_decimal64_10_0_vals_45 = [("100000"),("9999999998"),("9999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_5_from_decimal64_10_0_vals_45) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(10, 0)) as decimalv3(10, 5));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_10_0_vals_45) {
        qt_sql_test_cast_to_decimal64_10_5_from_decimal64_10_0 """select cast(cast("${test_str}" as decimalv3(10, 0)) as decimalv3(10, 5));"""
    }

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_10_0_vals_45) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 0)) as decimalv3(10, 5));""")
    }
    def test_cast_to_decimal64_10_5_from_decimal64_10_1_vals_46 = [("100000.9"),("999999998.9"),("999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_5_from_decimal64_10_1_vals_46) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(10, 1)) as decimalv3(10, 5));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_10_1_vals_46) {
        qt_sql_test_cast_to_decimal64_10_5_from_decimal64_10_1 """select cast(cast("${test_str}" as decimalv3(10, 1)) as decimalv3(10, 5));"""
    }

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_10_1_vals_46) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 1)) as decimalv3(10, 5));""")
    }
    def test_cast_to_decimal64_10_5_from_decimal64_17_0_vals_50 = [("100000"),("99999999999999998"),("99999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_5_from_decimal64_17_0_vals_50) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 0)) as decimalv3(10, 5));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_17_0_vals_50) {
        qt_sql_test_cast_to_decimal64_10_5_from_decimal64_17_0 """select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(10, 5));"""
    }

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_17_0_vals_50) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(10, 5));""")
    }
    def test_cast_to_decimal64_10_5_from_decimal64_17_1_vals_51 = [("100000.9"),("9999999999999998.9"),("9999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_5_from_decimal64_17_1_vals_51) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 1)) as decimalv3(10, 5));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_17_1_vals_51) {
        qt_sql_test_cast_to_decimal64_10_5_from_decimal64_17_1 """select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(10, 5));"""
    }

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_17_1_vals_51) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(10, 5));""")
    }
    def test_cast_to_decimal64_10_5_from_decimal64_17_8_vals_52 = [("99999.99999999"),("99999.99999999"),("100000.99999999"),("100000.99999999"),("999999998.99999999"),
        ("999999998.99999999"),("999999999.99999999"),("999999999.99999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_5_from_decimal64_17_8_vals_52) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 8)) as decimalv3(10, 5));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_17_8_vals_52) {
        qt_sql_test_cast_to_decimal64_10_5_from_decimal64_17_8 """select cast(cast("${test_str}" as decimalv3(17, 8)) as decimalv3(10, 5));"""
    }

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_17_8_vals_52) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 8)) as decimalv3(10, 5));""")
    }
    def test_cast_to_decimal64_10_5_from_decimal64_18_0_vals_55 = [("100000"),("999999999999999998"),("999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_5_from_decimal64_18_0_vals_55) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 0)) as decimalv3(10, 5));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_18_0_vals_55) {
        qt_sql_test_cast_to_decimal64_10_5_from_decimal64_18_0 """select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(10, 5));"""
    }

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_18_0_vals_55) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(10, 5));""")
    }
    def test_cast_to_decimal64_10_5_from_decimal64_18_1_vals_56 = [("100000.9"),("99999999999999998.9"),("99999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_5_from_decimal64_18_1_vals_56) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 1)) as decimalv3(10, 5));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_18_1_vals_56) {
        qt_sql_test_cast_to_decimal64_10_5_from_decimal64_18_1 """select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(10, 5));"""
    }

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_18_1_vals_56) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(10, 5));""")
    }
    def test_cast_to_decimal64_10_5_from_decimal64_18_9_vals_57 = [("99999.999999999"),("99999.999999999"),("100000.999999999"),("100000.999999999"),("999999998.999999999"),
        ("999999998.999999999"),("999999999.999999999"),("999999999.999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_5_from_decimal64_18_9_vals_57) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 9)) as decimalv3(10, 5));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_18_9_vals_57) {
        qt_sql_test_cast_to_decimal64_10_5_from_decimal64_18_9 """select cast(cast("${test_str}" as decimalv3(18, 9)) as decimalv3(10, 5));"""
    }

    for (test_str in test_cast_to_decimal64_10_5_from_decimal64_18_9_vals_57) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 9)) as decimalv3(10, 5));""")
    }
    def test_cast_to_decimal64_10_9_from_decimal64_9_0_vals_60 = [("10"),("999999998"),("999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_9_from_decimal64_9_0_vals_60) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 0)) as decimalv3(10, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_9_0_vals_60) {
        qt_sql_test_cast_to_decimal64_10_9_from_decimal64_9_0 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(10, 9));"""
    }

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_9_0_vals_60) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(10, 9));""")
    }
    def test_cast_to_decimal64_10_9_from_decimal64_9_1_vals_61 = [("10.9"),("99999998.9"),("99999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_9_from_decimal64_9_1_vals_61) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 1)) as decimalv3(10, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_9_1_vals_61) {
        qt_sql_test_cast_to_decimal64_10_9_from_decimal64_9_1 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(10, 9));"""
    }

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_9_1_vals_61) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(10, 9));""")
    }
    def test_cast_to_decimal64_10_9_from_decimal64_9_4_vals_62 = [("10.9999"),("99998.9999"),("99999.9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_9_from_decimal64_9_4_vals_62) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 4)) as decimalv3(10, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_9_4_vals_62) {
        qt_sql_test_cast_to_decimal64_10_9_from_decimal64_9_4 """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(10, 9));"""
    }

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_9_4_vals_62) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(10, 9));""")
    }
    def test_cast_to_decimal64_10_9_from_decimal64_10_0_vals_65 = [("10"),("9999999998"),("9999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_9_from_decimal64_10_0_vals_65) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(10, 0)) as decimalv3(10, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_10_0_vals_65) {
        qt_sql_test_cast_to_decimal64_10_9_from_decimal64_10_0 """select cast(cast("${test_str}" as decimalv3(10, 0)) as decimalv3(10, 9));"""
    }

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_10_0_vals_65) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 0)) as decimalv3(10, 9));""")
    }
    def test_cast_to_decimal64_10_9_from_decimal64_10_1_vals_66 = [("10.9"),("999999998.9"),("999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_9_from_decimal64_10_1_vals_66) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(10, 1)) as decimalv3(10, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_10_1_vals_66) {
        qt_sql_test_cast_to_decimal64_10_9_from_decimal64_10_1 """select cast(cast("${test_str}" as decimalv3(10, 1)) as decimalv3(10, 9));"""
    }

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_10_1_vals_66) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 1)) as decimalv3(10, 9));""")
    }
    def test_cast_to_decimal64_10_9_from_decimal64_10_5_vals_67 = [("10.99999"),("99998.99999"),("99999.99999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_9_from_decimal64_10_5_vals_67) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(10, 5)) as decimalv3(10, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_10_5_vals_67) {
        qt_sql_test_cast_to_decimal64_10_9_from_decimal64_10_5 """select cast(cast("${test_str}" as decimalv3(10, 5)) as decimalv3(10, 9));"""
    }

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_10_5_vals_67) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 5)) as decimalv3(10, 9));""")
    }
    def test_cast_to_decimal64_10_9_from_decimal64_17_0_vals_70 = [("10"),("99999999999999998"),("99999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_9_from_decimal64_17_0_vals_70) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 0)) as decimalv3(10, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_17_0_vals_70) {
        qt_sql_test_cast_to_decimal64_10_9_from_decimal64_17_0 """select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(10, 9));"""
    }

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_17_0_vals_70) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(10, 9));""")
    }
    def test_cast_to_decimal64_10_9_from_decimal64_17_1_vals_71 = [("10.9"),("9999999999999998.9"),("9999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_9_from_decimal64_17_1_vals_71) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 1)) as decimalv3(10, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_17_1_vals_71) {
        qt_sql_test_cast_to_decimal64_10_9_from_decimal64_17_1 """select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(10, 9));"""
    }

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_17_1_vals_71) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(10, 9));""")
    }
    def test_cast_to_decimal64_10_9_from_decimal64_17_8_vals_72 = [("10.99999999"),("999999998.99999999"),("999999999.99999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_9_from_decimal64_17_8_vals_72) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 8)) as decimalv3(10, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_17_8_vals_72) {
        qt_sql_test_cast_to_decimal64_10_9_from_decimal64_17_8 """select cast(cast("${test_str}" as decimalv3(17, 8)) as decimalv3(10, 9));"""
    }

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_17_8_vals_72) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 8)) as decimalv3(10, 9));""")
    }
    def test_cast_to_decimal64_10_9_from_decimal64_17_16_vals_73 = [("9.9999999999999999"),("9.9999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_9_from_decimal64_17_16_vals_73) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 16)) as decimalv3(10, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_17_16_vals_73) {
        qt_sql_test_cast_to_decimal64_10_9_from_decimal64_17_16 """select cast(cast("${test_str}" as decimalv3(17, 16)) as decimalv3(10, 9));"""
    }

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_17_16_vals_73) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 16)) as decimalv3(10, 9));""")
    }
    def test_cast_to_decimal64_10_9_from_decimal64_18_0_vals_75 = [("10"),("999999999999999998"),("999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_9_from_decimal64_18_0_vals_75) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 0)) as decimalv3(10, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_18_0_vals_75) {
        qt_sql_test_cast_to_decimal64_10_9_from_decimal64_18_0 """select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(10, 9));"""
    }

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_18_0_vals_75) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(10, 9));""")
    }
    def test_cast_to_decimal64_10_9_from_decimal64_18_1_vals_76 = [("10.9"),("99999999999999998.9"),("99999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_9_from_decimal64_18_1_vals_76) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 1)) as decimalv3(10, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_18_1_vals_76) {
        qt_sql_test_cast_to_decimal64_10_9_from_decimal64_18_1 """select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(10, 9));"""
    }

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_18_1_vals_76) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(10, 9));""")
    }
    def test_cast_to_decimal64_10_9_from_decimal64_18_9_vals_77 = [("10.999999999"),("999999998.999999999"),("999999999.999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_9_from_decimal64_18_9_vals_77) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 9)) as decimalv3(10, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_18_9_vals_77) {
        qt_sql_test_cast_to_decimal64_10_9_from_decimal64_18_9 """select cast(cast("${test_str}" as decimalv3(18, 9)) as decimalv3(10, 9));"""
    }

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_18_9_vals_77) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 9)) as decimalv3(10, 9));""")
    }
    def test_cast_to_decimal64_10_9_from_decimal64_18_17_vals_78 = [("9.99999999999999999"),("9.99999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_9_from_decimal64_18_17_vals_78) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 17)) as decimalv3(10, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_18_17_vals_78) {
        qt_sql_test_cast_to_decimal64_10_9_from_decimal64_18_17 """select cast(cast("${test_str}" as decimalv3(18, 17)) as decimalv3(10, 9));"""
    }

    for (test_str in test_cast_to_decimal64_10_9_from_decimal64_18_17_vals_78) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 17)) as decimalv3(10, 9));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_9_0_vals_80 = [("1"),("999999998"),("999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_9_0_vals_80) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 0)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_9_0_vals_80) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_9_0 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_9_0_vals_80) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_9_1_vals_81 = [("1.9"),("99999998.9"),("99999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_9_1_vals_81) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 1)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_9_1_vals_81) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_9_1 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_9_1_vals_81) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_9_4_vals_82 = [("1.9999"),("99998.9999"),("99999.9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_9_4_vals_82) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 4)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_9_4_vals_82) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_9_4 """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_9_4_vals_82) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_9_8_vals_83 = [("1.99999999"),("8.99999999"),("9.99999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_9_8_vals_83) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 8)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_9_8_vals_83) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_9_8 """select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_9_8_vals_83) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_10_0_vals_85 = [("1"),("9999999998"),("9999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_10_0_vals_85) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(10, 0)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_10_0_vals_85) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_10_0 """select cast(cast("${test_str}" as decimalv3(10, 0)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_10_0_vals_85) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 0)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_10_1_vals_86 = [("1.9"),("999999998.9"),("999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_10_1_vals_86) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(10, 1)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_10_1_vals_86) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_10_1 """select cast(cast("${test_str}" as decimalv3(10, 1)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_10_1_vals_86) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 1)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_10_5_vals_87 = [("1.99999"),("99998.99999"),("99999.99999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_10_5_vals_87) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(10, 5)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_10_5_vals_87) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_10_5 """select cast(cast("${test_str}" as decimalv3(10, 5)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_10_5_vals_87) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 5)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_10_9_vals_88 = [("1.999999999"),("8.999999999"),("9.999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_10_9_vals_88) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(10, 9)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_10_9_vals_88) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_10_9 """select cast(cast("${test_str}" as decimalv3(10, 9)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_10_9_vals_88) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 9)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_17_0_vals_90 = [("1"),("99999999999999998"),("99999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_0_vals_90) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 0)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_0_vals_90) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_17_0 """select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_0_vals_90) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_17_1_vals_91 = [("1.9"),("9999999999999998.9"),("9999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_1_vals_91) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 1)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_1_vals_91) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_17_1 """select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_1_vals_91) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_17_8_vals_92 = [("1.99999999"),("999999998.99999999"),("999999999.99999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_8_vals_92) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 8)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_8_vals_92) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_17_8 """select cast(cast("${test_str}" as decimalv3(17, 8)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_8_vals_92) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 8)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_17_16_vals_93 = [("0.9999999999999999"),("0.9999999999999999"),("1.9999999999999999"),("1.9999999999999999"),("8.9999999999999999"),
        ("8.9999999999999999"),("9.9999999999999999"),("9.9999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_16_vals_93) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 16)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_16_vals_93) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_17_16 """select cast(cast("${test_str}" as decimalv3(17, 16)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_16_vals_93) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 16)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_17_17_vals_94 = [("0.99999999999999999"),("0.99999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_17_vals_94) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(17, 17)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_17_vals_94) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_17_17 """select cast(cast("${test_str}" as decimalv3(17, 17)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_17_17_vals_94) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 17)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_18_0_vals_95 = [("1"),("999999999999999998"),("999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_0_vals_95) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 0)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_0_vals_95) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_18_0 """select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_0_vals_95) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_18_1_vals_96 = [("1.9"),("99999999999999998.9"),("99999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_1_vals_96) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 1)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_1_vals_96) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_18_1 """select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_1_vals_96) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_18_9_vals_97 = [("1.999999999"),("999999998.999999999"),("999999999.999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_9_vals_97) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 9)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_9_vals_97) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_18_9 """select cast(cast("${test_str}" as decimalv3(18, 9)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_9_vals_97) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 9)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_18_17_vals_98 = [("0.99999999999999999"),("0.99999999999999999"),("1.99999999999999999"),("1.99999999999999999"),("8.99999999999999999"),
        ("8.99999999999999999"),("9.99999999999999999"),("9.99999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_17_vals_98) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 17)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_17_vals_98) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_18_17 """select cast(cast("${test_str}" as decimalv3(18, 17)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_17_vals_98) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 17)) as decimalv3(10, 10));""")
    }
    def test_cast_to_decimal64_10_10_from_decimal64_18_18_vals_99 = [("0.999999999999999999"),("0.999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_18_vals_99) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(18, 18)) as decimalv3(10, 10));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_18_vals_99) {
        qt_sql_test_cast_to_decimal64_10_10_from_decimal64_18_18 """select cast(cast("${test_str}" as decimalv3(18, 18)) as decimalv3(10, 10));"""
    }

    for (test_str in test_cast_to_decimal64_10_10_from_decimal64_18_18_vals_99) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 18)) as decimalv3(10, 10));""")
    }
}