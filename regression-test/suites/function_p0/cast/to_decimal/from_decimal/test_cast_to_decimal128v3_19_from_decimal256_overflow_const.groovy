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


suite("test_cast_to_decimal128v3_19_from_decimal256_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_decimal256 = true;"
    def test_cast_to_decimal128v3_19_0_from_decimal256_38_0_vals_0 = [("10000000000000000000"),("99999999999999999999999999999999999998"),("99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_38_0_vals_0) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 0)) as decimalv3(19, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_38_0_vals_0) {
        qt_sql_test_cast_to_decimal128v3_19_0_from_decimal256_38_0 """select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(19, 0));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_38_0_vals_0) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(19, 0));""")
    }
    def test_cast_to_decimal128v3_19_0_from_decimal256_38_1_vals_1 = [("9999999999999999999.9"),("9999999999999999999.9"),("10000000000000000000.9"),("10000000000000000000.9"),("9999999999999999999999999999999999998.9"),
        ("9999999999999999999999999999999999998.9"),("9999999999999999999999999999999999999.9"),("9999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_38_1_vals_1) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 1)) as decimalv3(19, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_38_1_vals_1) {
        qt_sql_test_cast_to_decimal128v3_19_0_from_decimal256_38_1 """select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(19, 0));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_38_1_vals_1) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(19, 0));""")
    }
    def test_cast_to_decimal128v3_19_0_from_decimal256_38_19_vals_2 = [("9999999999999999999.9999999999999999999"),("9999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_38_19_vals_2) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 19)) as decimalv3(19, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_38_19_vals_2) {
        qt_sql_test_cast_to_decimal128v3_19_0_from_decimal256_38_19 """select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(19, 0));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_38_19_vals_2) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(19, 0));""")
    }
    def test_cast_to_decimal128v3_19_0_from_decimal256_39_0_vals_5 = [("10000000000000000000"),("999999999999999999999999999999999999998"),("999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_39_0_vals_5) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 0)) as decimalv3(19, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_39_0_vals_5) {
        qt_sql_test_cast_to_decimal128v3_19_0_from_decimal256_39_0 """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(19, 0));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_39_0_vals_5) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(19, 0));""")
    }
    def test_cast_to_decimal128v3_19_0_from_decimal256_39_1_vals_6 = [("9999999999999999999.9"),("9999999999999999999.9"),("10000000000000000000.9"),("10000000000000000000.9"),("99999999999999999999999999999999999998.9"),
        ("99999999999999999999999999999999999998.9"),("99999999999999999999999999999999999999.9"),("99999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_39_1_vals_6) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 1)) as decimalv3(19, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_39_1_vals_6) {
        qt_sql_test_cast_to_decimal128v3_19_0_from_decimal256_39_1 """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(19, 0));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_39_1_vals_6) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(19, 0));""")
    }
    def test_cast_to_decimal128v3_19_0_from_decimal256_39_19_vals_7 = [("9999999999999999999.9999999999999999999"),("9999999999999999999.9999999999999999999"),("10000000000000000000.9999999999999999999"),("10000000000000000000.9999999999999999999"),("99999999999999999998.9999999999999999999"),
        ("99999999999999999998.9999999999999999999"),("99999999999999999999.9999999999999999999"),("99999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_39_19_vals_7) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 19)) as decimalv3(19, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_39_19_vals_7) {
        qt_sql_test_cast_to_decimal128v3_19_0_from_decimal256_39_19 """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(19, 0));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_39_19_vals_7) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(19, 0));""")
    }
    def test_cast_to_decimal128v3_19_0_from_decimal256_75_0_vals_10 = [("10000000000000000000"),("999999999999999999999999999999999999999999999999999999999999999999999999998"),("999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_75_0_vals_10) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 0)) as decimalv3(19, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_75_0_vals_10) {
        qt_sql_test_cast_to_decimal128v3_19_0_from_decimal256_75_0 """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(19, 0));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_75_0_vals_10) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(19, 0));""")
    }
    def test_cast_to_decimal128v3_19_0_from_decimal256_75_1_vals_11 = [("9999999999999999999.9"),("9999999999999999999.9"),("10000000000000000000.9"),("10000000000000000000.9"),("99999999999999999999999999999999999999999999999999999999999999999999999998.9"),
        ("99999999999999999999999999999999999999999999999999999999999999999999999998.9"),("99999999999999999999999999999999999999999999999999999999999999999999999999.9"),("99999999999999999999999999999999999999999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_75_1_vals_11) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 1)) as decimalv3(19, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_75_1_vals_11) {
        qt_sql_test_cast_to_decimal128v3_19_0_from_decimal256_75_1 """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(19, 0));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_75_1_vals_11) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(19, 0));""")
    }
    def test_cast_to_decimal128v3_19_0_from_decimal256_75_37_vals_12 = [("9999999999999999999.9999999999999999999999999999999999999"),("9999999999999999999.9999999999999999999999999999999999999"),("10000000000000000000.9999999999999999999999999999999999999"),("10000000000000000000.9999999999999999999999999999999999999"),("99999999999999999999999999999999999998.9999999999999999999999999999999999999"),
        ("99999999999999999999999999999999999998.9999999999999999999999999999999999999"),("99999999999999999999999999999999999999.9999999999999999999999999999999999999"),("99999999999999999999999999999999999999.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_75_37_vals_12) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 37)) as decimalv3(19, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_75_37_vals_12) {
        qt_sql_test_cast_to_decimal128v3_19_0_from_decimal256_75_37 """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(19, 0));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_75_37_vals_12) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(19, 0));""")
    }
    def test_cast_to_decimal128v3_19_0_from_decimal256_76_0_vals_15 = [("10000000000000000000"),("9999999999999999999999999999999999999999999999999999999999999999999999999998"),("9999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_76_0_vals_15) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 0)) as decimalv3(19, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_76_0_vals_15) {
        qt_sql_test_cast_to_decimal128v3_19_0_from_decimal256_76_0 """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(19, 0));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_76_0_vals_15) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(19, 0));""")
    }
    def test_cast_to_decimal128v3_19_0_from_decimal256_76_1_vals_16 = [("9999999999999999999.9"),("9999999999999999999.9"),("10000000000000000000.9"),("10000000000000000000.9"),("999999999999999999999999999999999999999999999999999999999999999999999999998.9"),
        ("999999999999999999999999999999999999999999999999999999999999999999999999998.9"),("999999999999999999999999999999999999999999999999999999999999999999999999999.9"),("999999999999999999999999999999999999999999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_76_1_vals_16) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 1)) as decimalv3(19, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_76_1_vals_16) {
        qt_sql_test_cast_to_decimal128v3_19_0_from_decimal256_76_1 """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(19, 0));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_76_1_vals_16) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(19, 0));""")
    }
    def test_cast_to_decimal128v3_19_0_from_decimal256_76_38_vals_17 = [("9999999999999999999.99999999999999999999999999999999999999"),("9999999999999999999.99999999999999999999999999999999999999"),("10000000000000000000.99999999999999999999999999999999999999"),("10000000000000000000.99999999999999999999999999999999999999"),("99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
        ("99999999999999999999999999999999999998.99999999999999999999999999999999999999"),("99999999999999999999999999999999999999.99999999999999999999999999999999999999"),("99999999999999999999999999999999999999.99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_76_38_vals_17) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 38)) as decimalv3(19, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_76_38_vals_17) {
        qt_sql_test_cast_to_decimal128v3_19_0_from_decimal256_76_38 """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(19, 0));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_0_from_decimal256_76_38_vals_17) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(19, 0));""")
    }
    def test_cast_to_decimal128v3_19_1_from_decimal256_38_0_vals_20 = [("1000000000000000000"),("99999999999999999999999999999999999998"),("99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_38_0_vals_20) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 0)) as decimalv3(19, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_38_0_vals_20) {
        qt_sql_test_cast_to_decimal128v3_19_1_from_decimal256_38_0 """select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(19, 1));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_38_0_vals_20) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(19, 1));""")
    }
    def test_cast_to_decimal128v3_19_1_from_decimal256_38_1_vals_21 = [("1000000000000000000.9"),("9999999999999999999999999999999999998.9"),("9999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_38_1_vals_21) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 1)) as decimalv3(19, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_38_1_vals_21) {
        qt_sql_test_cast_to_decimal128v3_19_1_from_decimal256_38_1 """select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(19, 1));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_38_1_vals_21) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(19, 1));""")
    }
    def test_cast_to_decimal128v3_19_1_from_decimal256_38_19_vals_22 = [("999999999999999999.9999999999999999999"),("999999999999999999.9999999999999999999"),("1000000000000000000.9999999999999999999"),("1000000000000000000.9999999999999999999"),("9999999999999999998.9999999999999999999"),
        ("9999999999999999998.9999999999999999999"),("9999999999999999999.9999999999999999999"),("9999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_38_19_vals_22) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 19)) as decimalv3(19, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_38_19_vals_22) {
        qt_sql_test_cast_to_decimal128v3_19_1_from_decimal256_38_19 """select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(19, 1));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_38_19_vals_22) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(19, 1));""")
    }
    def test_cast_to_decimal128v3_19_1_from_decimal256_39_0_vals_25 = [("1000000000000000000"),("999999999999999999999999999999999999998"),("999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_39_0_vals_25) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 0)) as decimalv3(19, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_39_0_vals_25) {
        qt_sql_test_cast_to_decimal128v3_19_1_from_decimal256_39_0 """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(19, 1));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_39_0_vals_25) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(19, 1));""")
    }
    def test_cast_to_decimal128v3_19_1_from_decimal256_39_1_vals_26 = [("1000000000000000000.9"),("99999999999999999999999999999999999998.9"),("99999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_39_1_vals_26) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 1)) as decimalv3(19, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_39_1_vals_26) {
        qt_sql_test_cast_to_decimal128v3_19_1_from_decimal256_39_1 """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(19, 1));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_39_1_vals_26) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(19, 1));""")
    }
    def test_cast_to_decimal128v3_19_1_from_decimal256_39_19_vals_27 = [("999999999999999999.9999999999999999999"),("999999999999999999.9999999999999999999"),("1000000000000000000.9999999999999999999"),("1000000000000000000.9999999999999999999"),("99999999999999999998.9999999999999999999"),
        ("99999999999999999998.9999999999999999999"),("99999999999999999999.9999999999999999999"),("99999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_39_19_vals_27) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 19)) as decimalv3(19, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_39_19_vals_27) {
        qt_sql_test_cast_to_decimal128v3_19_1_from_decimal256_39_19 """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(19, 1));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_39_19_vals_27) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(19, 1));""")
    }
    def test_cast_to_decimal128v3_19_1_from_decimal256_75_0_vals_30 = [("1000000000000000000"),("999999999999999999999999999999999999999999999999999999999999999999999999998"),("999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_75_0_vals_30) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 0)) as decimalv3(19, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_75_0_vals_30) {
        qt_sql_test_cast_to_decimal128v3_19_1_from_decimal256_75_0 """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(19, 1));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_75_0_vals_30) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(19, 1));""")
    }
    def test_cast_to_decimal128v3_19_1_from_decimal256_75_1_vals_31 = [("1000000000000000000.9"),("99999999999999999999999999999999999999999999999999999999999999999999999998.9"),("99999999999999999999999999999999999999999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_75_1_vals_31) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 1)) as decimalv3(19, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_75_1_vals_31) {
        qt_sql_test_cast_to_decimal128v3_19_1_from_decimal256_75_1 """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(19, 1));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_75_1_vals_31) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(19, 1));""")
    }
    def test_cast_to_decimal128v3_19_1_from_decimal256_75_37_vals_32 = [("999999999999999999.9999999999999999999999999999999999999"),("999999999999999999.9999999999999999999999999999999999999"),("1000000000000000000.9999999999999999999999999999999999999"),("1000000000000000000.9999999999999999999999999999999999999"),("99999999999999999999999999999999999998.9999999999999999999999999999999999999"),
        ("99999999999999999999999999999999999998.9999999999999999999999999999999999999"),("99999999999999999999999999999999999999.9999999999999999999999999999999999999"),("99999999999999999999999999999999999999.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_75_37_vals_32) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 37)) as decimalv3(19, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_75_37_vals_32) {
        qt_sql_test_cast_to_decimal128v3_19_1_from_decimal256_75_37 """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(19, 1));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_75_37_vals_32) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(19, 1));""")
    }
    def test_cast_to_decimal128v3_19_1_from_decimal256_76_0_vals_35 = [("1000000000000000000"),("9999999999999999999999999999999999999999999999999999999999999999999999999998"),("9999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_76_0_vals_35) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 0)) as decimalv3(19, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_76_0_vals_35) {
        qt_sql_test_cast_to_decimal128v3_19_1_from_decimal256_76_0 """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(19, 1));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_76_0_vals_35) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(19, 1));""")
    }
    def test_cast_to_decimal128v3_19_1_from_decimal256_76_1_vals_36 = [("1000000000000000000.9"),("999999999999999999999999999999999999999999999999999999999999999999999999998.9"),("999999999999999999999999999999999999999999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_76_1_vals_36) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 1)) as decimalv3(19, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_76_1_vals_36) {
        qt_sql_test_cast_to_decimal128v3_19_1_from_decimal256_76_1 """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(19, 1));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_76_1_vals_36) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(19, 1));""")
    }
    def test_cast_to_decimal128v3_19_1_from_decimal256_76_38_vals_37 = [("999999999999999999.99999999999999999999999999999999999999"),("999999999999999999.99999999999999999999999999999999999999"),("1000000000000000000.99999999999999999999999999999999999999"),("1000000000000000000.99999999999999999999999999999999999999"),("99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
        ("99999999999999999999999999999999999998.99999999999999999999999999999999999999"),("99999999999999999999999999999999999999.99999999999999999999999999999999999999"),("99999999999999999999999999999999999999.99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_76_38_vals_37) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 38)) as decimalv3(19, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_76_38_vals_37) {
        qt_sql_test_cast_to_decimal128v3_19_1_from_decimal256_76_38 """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(19, 1));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_1_from_decimal256_76_38_vals_37) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(19, 1));""")
    }
    def test_cast_to_decimal128v3_19_9_from_decimal256_38_0_vals_40 = [("10000000000"),("99999999999999999999999999999999999998"),("99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_38_0_vals_40) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 0)) as decimalv3(19, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_38_0_vals_40) {
        qt_sql_test_cast_to_decimal128v3_19_9_from_decimal256_38_0 """select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(19, 9));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_38_0_vals_40) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(19, 9));""")
    }
    def test_cast_to_decimal128v3_19_9_from_decimal256_38_1_vals_41 = [("10000000000.9"),("9999999999999999999999999999999999998.9"),("9999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_38_1_vals_41) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 1)) as decimalv3(19, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_38_1_vals_41) {
        qt_sql_test_cast_to_decimal128v3_19_9_from_decimal256_38_1 """select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(19, 9));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_38_1_vals_41) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(19, 9));""")
    }
    def test_cast_to_decimal128v3_19_9_from_decimal256_38_19_vals_42 = [("9999999999.9999999999999999999"),("9999999999.9999999999999999999"),("10000000000.9999999999999999999"),("10000000000.9999999999999999999"),("9999999999999999998.9999999999999999999"),
        ("9999999999999999998.9999999999999999999"),("9999999999999999999.9999999999999999999"),("9999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_38_19_vals_42) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 19)) as decimalv3(19, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_38_19_vals_42) {
        qt_sql_test_cast_to_decimal128v3_19_9_from_decimal256_38_19 """select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(19, 9));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_38_19_vals_42) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(19, 9));""")
    }
    def test_cast_to_decimal128v3_19_9_from_decimal256_39_0_vals_45 = [("10000000000"),("999999999999999999999999999999999999998"),("999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_39_0_vals_45) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 0)) as decimalv3(19, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_39_0_vals_45) {
        qt_sql_test_cast_to_decimal128v3_19_9_from_decimal256_39_0 """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(19, 9));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_39_0_vals_45) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(19, 9));""")
    }
    def test_cast_to_decimal128v3_19_9_from_decimal256_39_1_vals_46 = [("10000000000.9"),("99999999999999999999999999999999999998.9"),("99999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_39_1_vals_46) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 1)) as decimalv3(19, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_39_1_vals_46) {
        qt_sql_test_cast_to_decimal128v3_19_9_from_decimal256_39_1 """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(19, 9));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_39_1_vals_46) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(19, 9));""")
    }
    def test_cast_to_decimal128v3_19_9_from_decimal256_39_19_vals_47 = [("9999999999.9999999999999999999"),("9999999999.9999999999999999999"),("10000000000.9999999999999999999"),("10000000000.9999999999999999999"),("99999999999999999998.9999999999999999999"),
        ("99999999999999999998.9999999999999999999"),("99999999999999999999.9999999999999999999"),("99999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_39_19_vals_47) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 19)) as decimalv3(19, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_39_19_vals_47) {
        qt_sql_test_cast_to_decimal128v3_19_9_from_decimal256_39_19 """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(19, 9));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_39_19_vals_47) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(19, 9));""")
    }
    def test_cast_to_decimal128v3_19_9_from_decimal256_75_0_vals_50 = [("10000000000"),("999999999999999999999999999999999999999999999999999999999999999999999999998"),("999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_75_0_vals_50) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 0)) as decimalv3(19, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_75_0_vals_50) {
        qt_sql_test_cast_to_decimal128v3_19_9_from_decimal256_75_0 """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(19, 9));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_75_0_vals_50) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(19, 9));""")
    }
    def test_cast_to_decimal128v3_19_9_from_decimal256_75_1_vals_51 = [("10000000000.9"),("99999999999999999999999999999999999999999999999999999999999999999999999998.9"),("99999999999999999999999999999999999999999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_75_1_vals_51) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 1)) as decimalv3(19, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_75_1_vals_51) {
        qt_sql_test_cast_to_decimal128v3_19_9_from_decimal256_75_1 """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(19, 9));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_75_1_vals_51) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(19, 9));""")
    }
    def test_cast_to_decimal128v3_19_9_from_decimal256_75_37_vals_52 = [("9999999999.9999999999999999999999999999999999999"),("9999999999.9999999999999999999999999999999999999"),("10000000000.9999999999999999999999999999999999999"),("10000000000.9999999999999999999999999999999999999"),("99999999999999999999999999999999999998.9999999999999999999999999999999999999"),
        ("99999999999999999999999999999999999998.9999999999999999999999999999999999999"),("99999999999999999999999999999999999999.9999999999999999999999999999999999999"),("99999999999999999999999999999999999999.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_75_37_vals_52) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 37)) as decimalv3(19, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_75_37_vals_52) {
        qt_sql_test_cast_to_decimal128v3_19_9_from_decimal256_75_37 """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(19, 9));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_75_37_vals_52) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(19, 9));""")
    }
    def test_cast_to_decimal128v3_19_9_from_decimal256_76_0_vals_55 = [("10000000000"),("9999999999999999999999999999999999999999999999999999999999999999999999999998"),("9999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_76_0_vals_55) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 0)) as decimalv3(19, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_76_0_vals_55) {
        qt_sql_test_cast_to_decimal128v3_19_9_from_decimal256_76_0 """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(19, 9));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_76_0_vals_55) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(19, 9));""")
    }
    def test_cast_to_decimal128v3_19_9_from_decimal256_76_1_vals_56 = [("10000000000.9"),("999999999999999999999999999999999999999999999999999999999999999999999999998.9"),("999999999999999999999999999999999999999999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_76_1_vals_56) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 1)) as decimalv3(19, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_76_1_vals_56) {
        qt_sql_test_cast_to_decimal128v3_19_9_from_decimal256_76_1 """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(19, 9));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_76_1_vals_56) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(19, 9));""")
    }
    def test_cast_to_decimal128v3_19_9_from_decimal256_76_38_vals_57 = [("9999999999.99999999999999999999999999999999999999"),("9999999999.99999999999999999999999999999999999999"),("10000000000.99999999999999999999999999999999999999"),("10000000000.99999999999999999999999999999999999999"),("99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
        ("99999999999999999999999999999999999998.99999999999999999999999999999999999999"),("99999999999999999999999999999999999999.99999999999999999999999999999999999999"),("99999999999999999999999999999999999999.99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_76_38_vals_57) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 38)) as decimalv3(19, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_76_38_vals_57) {
        qt_sql_test_cast_to_decimal128v3_19_9_from_decimal256_76_38 """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(19, 9));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_9_from_decimal256_76_38_vals_57) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(19, 9));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_38_0_vals_60 = [("10"),("99999999999999999999999999999999999998"),("99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_38_0_vals_60) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 0)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_38_0_vals_60) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_38_0 """select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_38_0_vals_60) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_38_1_vals_61 = [("10.9"),("9999999999999999999999999999999999998.9"),("9999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_38_1_vals_61) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 1)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_38_1_vals_61) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_38_1 """select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_38_1_vals_61) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_38_19_vals_62 = [("9.9999999999999999999"),("9.9999999999999999999"),("10.9999999999999999999"),("10.9999999999999999999"),("9999999999999999998.9999999999999999999"),
        ("9999999999999999998.9999999999999999999"),("9999999999999999999.9999999999999999999"),("9999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_38_19_vals_62) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 19)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_38_19_vals_62) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_38_19 """select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_38_19_vals_62) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_38_37_vals_63 = [("9.9999999999999999999999999999999999999"),("9.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_38_37_vals_63) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 37)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_38_37_vals_63) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_38_37 """select cast(cast("${test_str}" as decimalv3(38, 37)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_38_37_vals_63) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 37)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_39_0_vals_65 = [("10"),("999999999999999999999999999999999999998"),("999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_39_0_vals_65) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 0)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_39_0_vals_65) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_39_0 """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_39_0_vals_65) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_39_1_vals_66 = [("10.9"),("99999999999999999999999999999999999998.9"),("99999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_39_1_vals_66) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 1)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_39_1_vals_66) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_39_1 """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_39_1_vals_66) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_39_19_vals_67 = [("9.9999999999999999999"),("9.9999999999999999999"),("10.9999999999999999999"),("10.9999999999999999999"),("99999999999999999998.9999999999999999999"),
        ("99999999999999999998.9999999999999999999"),("99999999999999999999.9999999999999999999"),("99999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_39_19_vals_67) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 19)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_39_19_vals_67) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_39_19 """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_39_19_vals_67) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_39_38_vals_68 = [("9.99999999999999999999999999999999999999"),("9.99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_39_38_vals_68) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 38)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_39_38_vals_68) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_39_38 """select cast(cast("${test_str}" as decimalv3(39, 38)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_39_38_vals_68) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 38)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_75_0_vals_70 = [("10"),("999999999999999999999999999999999999999999999999999999999999999999999999998"),("999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_75_0_vals_70) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 0)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_75_0_vals_70) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_75_0 """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_75_0_vals_70) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_75_1_vals_71 = [("10.9"),("99999999999999999999999999999999999999999999999999999999999999999999999998.9"),("99999999999999999999999999999999999999999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_75_1_vals_71) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 1)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_75_1_vals_71) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_75_1 """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_75_1_vals_71) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_75_37_vals_72 = [("9.9999999999999999999999999999999999999"),("9.9999999999999999999999999999999999999"),("10.9999999999999999999999999999999999999"),("10.9999999999999999999999999999999999999"),("99999999999999999999999999999999999998.9999999999999999999999999999999999999"),
        ("99999999999999999999999999999999999998.9999999999999999999999999999999999999"),("99999999999999999999999999999999999999.9999999999999999999999999999999999999"),("99999999999999999999999999999999999999.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_75_37_vals_72) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 37)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_75_37_vals_72) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_75_37 """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_75_37_vals_72) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_75_74_vals_73 = [("9.99999999999999999999999999999999999999999999999999999999999999999999999999"),("9.99999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_75_74_vals_73) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 74)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_75_74_vals_73) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_75_74 """select cast(cast("${test_str}" as decimalv3(75, 74)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_75_74_vals_73) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 74)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_76_0_vals_75 = [("10"),("9999999999999999999999999999999999999999999999999999999999999999999999999998"),("9999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_76_0_vals_75) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 0)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_76_0_vals_75) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_76_0 """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_76_0_vals_75) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_76_1_vals_76 = [("10.9"),("999999999999999999999999999999999999999999999999999999999999999999999999998.9"),("999999999999999999999999999999999999999999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_76_1_vals_76) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 1)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_76_1_vals_76) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_76_1 """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_76_1_vals_76) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_76_38_vals_77 = [("9.99999999999999999999999999999999999999"),("9.99999999999999999999999999999999999999"),("10.99999999999999999999999999999999999999"),("10.99999999999999999999999999999999999999"),("99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
        ("99999999999999999999999999999999999998.99999999999999999999999999999999999999"),("99999999999999999999999999999999999999.99999999999999999999999999999999999999"),("99999999999999999999999999999999999999.99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_76_38_vals_77) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 38)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_76_38_vals_77) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_76_38 """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_76_38_vals_77) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_18_from_decimal256_76_75_vals_78 = [("9.999999999999999999999999999999999999999999999999999999999999999999999999999"),("9.999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_76_75_vals_78) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 75)) as decimalv3(19, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_76_75_vals_78) {
        qt_sql_test_cast_to_decimal128v3_19_18_from_decimal256_76_75 """select cast(cast("${test_str}" as decimalv3(76, 75)) as decimalv3(19, 18));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_18_from_decimal256_76_75_vals_78) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 75)) as decimalv3(19, 18));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_38_0_vals_80 = [("1"),("99999999999999999999999999999999999998"),("99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_0_vals_80) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 0)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_0_vals_80) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_38_0 """select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_0_vals_80) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_38_1_vals_81 = [("1.9"),("9999999999999999999999999999999999998.9"),("9999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_1_vals_81) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 1)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_1_vals_81) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_38_1 """select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_1_vals_81) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_38_19_vals_82 = [("1.9999999999999999999"),("9999999999999999998.9999999999999999999"),("9999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_19_vals_82) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 19)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_19_vals_82) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_38_19 """select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_19_vals_82) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_38_37_vals_83 = [("0.9999999999999999999999999999999999999"),("0.9999999999999999999999999999999999999"),("1.9999999999999999999999999999999999999"),("1.9999999999999999999999999999999999999"),("8.9999999999999999999999999999999999999"),
        ("8.9999999999999999999999999999999999999"),("9.9999999999999999999999999999999999999"),("9.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_37_vals_83) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 37)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_37_vals_83) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_38_37 """select cast(cast("${test_str}" as decimalv3(38, 37)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_37_vals_83) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 37)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_38_38_vals_84 = [("0.99999999999999999999999999999999999999"),("0.99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_38_vals_84) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 38)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_38_vals_84) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_38_38 """select cast(cast("${test_str}" as decimalv3(38, 38)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_38_38_vals_84) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 38)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_39_0_vals_85 = [("1"),("999999999999999999999999999999999999998"),("999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_0_vals_85) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 0)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_0_vals_85) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_39_0 """select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_0_vals_85) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 0)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_39_1_vals_86 = [("1.9"),("99999999999999999999999999999999999998.9"),("99999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_1_vals_86) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 1)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_1_vals_86) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_39_1 """select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_1_vals_86) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 1)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_39_19_vals_87 = [("1.9999999999999999999"),("99999999999999999998.9999999999999999999"),("99999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_19_vals_87) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 19)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_19_vals_87) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_39_19 """select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_19_vals_87) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 19)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_39_38_vals_88 = [("0.99999999999999999999999999999999999999"),("0.99999999999999999999999999999999999999"),("1.99999999999999999999999999999999999999"),("1.99999999999999999999999999999999999999"),("8.99999999999999999999999999999999999999"),
        ("8.99999999999999999999999999999999999999"),("9.99999999999999999999999999999999999999"),("9.99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_38_vals_88) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 38)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_38_vals_88) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_39_38 """select cast(cast("${test_str}" as decimalv3(39, 38)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_38_vals_88) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 38)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_39_39_vals_89 = [("0.999999999999999999999999999999999999999"),("0.999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_39_vals_89) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(39, 39)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_39_vals_89) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_39_39 """select cast(cast("${test_str}" as decimalv3(39, 39)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_39_39_vals_89) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(39, 39)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_75_0_vals_90 = [("1"),("999999999999999999999999999999999999999999999999999999999999999999999999998"),("999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_0_vals_90) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 0)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_0_vals_90) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_75_0 """select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_0_vals_90) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 0)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_75_1_vals_91 = [("1.9"),("99999999999999999999999999999999999999999999999999999999999999999999999998.9"),("99999999999999999999999999999999999999999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_1_vals_91) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 1)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_1_vals_91) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_75_1 """select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_1_vals_91) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 1)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_75_37_vals_92 = [("0.9999999999999999999999999999999999999"),("0.9999999999999999999999999999999999999"),("1.9999999999999999999999999999999999999"),("1.9999999999999999999999999999999999999"),("99999999999999999999999999999999999998.9999999999999999999999999999999999999"),
        ("99999999999999999999999999999999999998.9999999999999999999999999999999999999"),("99999999999999999999999999999999999999.9999999999999999999999999999999999999"),("99999999999999999999999999999999999999.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_37_vals_92) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 37)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_37_vals_92) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_75_37 """select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_37_vals_92) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 37)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_75_74_vals_93 = [("0.99999999999999999999999999999999999999999999999999999999999999999999999999"),("0.99999999999999999999999999999999999999999999999999999999999999999999999999"),("1.99999999999999999999999999999999999999999999999999999999999999999999999999"),("1.99999999999999999999999999999999999999999999999999999999999999999999999999"),("8.99999999999999999999999999999999999999999999999999999999999999999999999999"),
        ("8.99999999999999999999999999999999999999999999999999999999999999999999999999"),("9.99999999999999999999999999999999999999999999999999999999999999999999999999"),("9.99999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_74_vals_93) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 74)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_74_vals_93) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_75_74 """select cast(cast("${test_str}" as decimalv3(75, 74)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_74_vals_93) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 74)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_75_75_vals_94 = [("0.999999999999999999999999999999999999999999999999999999999999999999999999999"),("0.999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_75_vals_94) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(75, 75)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_75_vals_94) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_75_75 """select cast(cast("${test_str}" as decimalv3(75, 75)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_75_75_vals_94) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(75, 75)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_76_0_vals_95 = [("1"),("9999999999999999999999999999999999999999999999999999999999999999999999999998"),("9999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_0_vals_95) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 0)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_0_vals_95) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_76_0 """select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_0_vals_95) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 0)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_76_1_vals_96 = [("1.9"),("999999999999999999999999999999999999999999999999999999999999999999999999998.9"),("999999999999999999999999999999999999999999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_1_vals_96) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 1)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_1_vals_96) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_76_1 """select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_1_vals_96) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 1)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_76_38_vals_97 = [("0.99999999999999999999999999999999999999"),("0.99999999999999999999999999999999999999"),("1.99999999999999999999999999999999999999"),("1.99999999999999999999999999999999999999"),("99999999999999999999999999999999999998.99999999999999999999999999999999999999"),
        ("99999999999999999999999999999999999998.99999999999999999999999999999999999999"),("99999999999999999999999999999999999999.99999999999999999999999999999999999999"),("99999999999999999999999999999999999999.99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_38_vals_97) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 38)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_38_vals_97) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_76_38 """select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_38_vals_97) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 38)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_76_75_vals_98 = [("0.999999999999999999999999999999999999999999999999999999999999999999999999999"),("0.999999999999999999999999999999999999999999999999999999999999999999999999999"),("1.999999999999999999999999999999999999999999999999999999999999999999999999999"),("1.999999999999999999999999999999999999999999999999999999999999999999999999999"),("8.999999999999999999999999999999999999999999999999999999999999999999999999999"),
        ("8.999999999999999999999999999999999999999999999999999999999999999999999999999"),("9.999999999999999999999999999999999999999999999999999999999999999999999999999"),("9.999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_75_vals_98) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 75)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_75_vals_98) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_76_75 """select cast(cast("${test_str}" as decimalv3(76, 75)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_75_vals_98) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 75)) as decimalv3(19, 19));""")
    }
    def test_cast_to_decimal128v3_19_19_from_decimal256_76_76_vals_99 = [("0.9999999999999999999999999999999999999999999999999999999999999999999999999999"),("0.9999999999999999999999999999999999999999999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_76_vals_99) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(76, 76)) as decimalv3(19, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_76_vals_99) {
        qt_sql_test_cast_to_decimal128v3_19_19_from_decimal256_76_76 """select cast(cast("${test_str}" as decimalv3(76, 76)) as decimalv3(19, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_19_19_from_decimal256_76_76_vals_99) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(76, 76)) as decimalv3(19, 19));""")
    }
}