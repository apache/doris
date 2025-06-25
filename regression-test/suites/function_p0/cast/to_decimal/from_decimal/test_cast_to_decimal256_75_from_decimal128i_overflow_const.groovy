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


suite("test_cast_to_decimal256_75_from_decimal128i_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_decimal256 = true;"
    def test_cast_to_decimal256_75_74_from_decimal128i_19_0_vals_45 = [("10"),("9999999999999999998"),("9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_19_0_vals_45) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 0)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_19_0_vals_45) {
        qt_sql_test_cast_to_decimal256_75_74_from_decimal128i_19_0 """select cast(cast("${test_str}" as decimalv3(19, 0)) as decimalv3(75, 74));"""
    }

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_19_0_vals_45) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 0)) as decimalv3(75, 74));""")
    }
    def test_cast_to_decimal256_75_74_from_decimal128i_19_1_vals_46 = [("10.9"),("999999999999999998.9"),("999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_19_1_vals_46) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 1)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_19_1_vals_46) {
        qt_sql_test_cast_to_decimal256_75_74_from_decimal128i_19_1 """select cast(cast("${test_str}" as decimalv3(19, 1)) as decimalv3(75, 74));"""
    }

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_19_1_vals_46) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 1)) as decimalv3(75, 74));""")
    }
    def test_cast_to_decimal256_75_74_from_decimal128i_19_9_vals_47 = [("10.999999999"),("9999999998.999999999"),("9999999999.999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_19_9_vals_47) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 9)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_19_9_vals_47) {
        qt_sql_test_cast_to_decimal256_75_74_from_decimal128i_19_9 """select cast(cast("${test_str}" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    }

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_19_9_vals_47) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 9)) as decimalv3(75, 74));""")
    }
    def test_cast_to_decimal256_75_74_from_decimal128i_37_0_vals_50 = [("10"),("9999999999999999999999999999999999998"),("9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_37_0_vals_50) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 0)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_37_0_vals_50) {
        qt_sql_test_cast_to_decimal256_75_74_from_decimal128i_37_0 """select cast(cast("${test_str}" as decimalv3(37, 0)) as decimalv3(75, 74));"""
    }

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_37_0_vals_50) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 0)) as decimalv3(75, 74));""")
    }
    def test_cast_to_decimal256_75_74_from_decimal128i_37_1_vals_51 = [("10.9"),("999999999999999999999999999999999998.9"),("999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_37_1_vals_51) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 1)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_37_1_vals_51) {
        qt_sql_test_cast_to_decimal256_75_74_from_decimal128i_37_1 """select cast(cast("${test_str}" as decimalv3(37, 1)) as decimalv3(75, 74));"""
    }

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_37_1_vals_51) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 1)) as decimalv3(75, 74));""")
    }
    def test_cast_to_decimal256_75_74_from_decimal128i_37_18_vals_52 = [("10.999999999999999999"),("9999999999999999998.999999999999999999"),("9999999999999999999.999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_37_18_vals_52) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 18)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_37_18_vals_52) {
        qt_sql_test_cast_to_decimal256_75_74_from_decimal128i_37_18 """select cast(cast("${test_str}" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    }

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_37_18_vals_52) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 18)) as decimalv3(75, 74));""")
    }
    def test_cast_to_decimal256_75_74_from_decimal128i_38_0_vals_55 = [("10"),("99999999999999999999999999999999999998"),("99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_38_0_vals_55) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 0)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_38_0_vals_55) {
        qt_sql_test_cast_to_decimal256_75_74_from_decimal128i_38_0 """select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(75, 74));"""
    }

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_38_0_vals_55) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(75, 74));""")
    }
    def test_cast_to_decimal256_75_74_from_decimal128i_38_1_vals_56 = [("10.9"),("9999999999999999999999999999999999998.9"),("9999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_38_1_vals_56) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 1)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_38_1_vals_56) {
        qt_sql_test_cast_to_decimal256_75_74_from_decimal128i_38_1 """select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(75, 74));"""
    }

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_38_1_vals_56) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(75, 74));""")
    }
    def test_cast_to_decimal256_75_74_from_decimal128i_38_19_vals_57 = [("10.9999999999999999999"),("9999999999999999998.9999999999999999999"),("9999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_38_19_vals_57) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 19)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_38_19_vals_57) {
        qt_sql_test_cast_to_decimal256_75_74_from_decimal128i_38_19 """select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    }

    for (test_str in test_cast_to_decimal256_75_74_from_decimal128i_38_19_vals_57) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(75, 74));""")
    }
    def test_cast_to_decimal256_75_75_from_decimal128i_19_0_vals_60 = [("1"),("9999999999999999998"),("9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_19_0_vals_60) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 0)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_19_0_vals_60) {
        qt_sql_test_cast_to_decimal256_75_75_from_decimal128i_19_0 """select cast(cast("${test_str}" as decimalv3(19, 0)) as decimalv3(75, 75));"""
    }

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_19_0_vals_60) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 0)) as decimalv3(75, 75));""")
    }
    def test_cast_to_decimal256_75_75_from_decimal128i_19_1_vals_61 = [("1.9"),("999999999999999998.9"),("999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_19_1_vals_61) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 1)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_19_1_vals_61) {
        qt_sql_test_cast_to_decimal256_75_75_from_decimal128i_19_1 """select cast(cast("${test_str}" as decimalv3(19, 1)) as decimalv3(75, 75));"""
    }

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_19_1_vals_61) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 1)) as decimalv3(75, 75));""")
    }
    def test_cast_to_decimal256_75_75_from_decimal128i_19_9_vals_62 = [("1.999999999"),("9999999998.999999999"),("9999999999.999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_19_9_vals_62) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 9)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_19_9_vals_62) {
        qt_sql_test_cast_to_decimal256_75_75_from_decimal128i_19_9 """select cast(cast("${test_str}" as decimalv3(19, 9)) as decimalv3(75, 75));"""
    }

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_19_9_vals_62) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 9)) as decimalv3(75, 75));""")
    }
    def test_cast_to_decimal256_75_75_from_decimal128i_19_18_vals_63 = [("1.999999999999999999"),("8.999999999999999999"),("9.999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_19_18_vals_63) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(19, 18)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_19_18_vals_63) {
        qt_sql_test_cast_to_decimal256_75_75_from_decimal128i_19_18 """select cast(cast("${test_str}" as decimalv3(19, 18)) as decimalv3(75, 75));"""
    }

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_19_18_vals_63) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 18)) as decimalv3(75, 75));""")
    }
    def test_cast_to_decimal256_75_75_from_decimal128i_37_0_vals_65 = [("1"),("9999999999999999999999999999999999998"),("9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_37_0_vals_65) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 0)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_37_0_vals_65) {
        qt_sql_test_cast_to_decimal256_75_75_from_decimal128i_37_0 """select cast(cast("${test_str}" as decimalv3(37, 0)) as decimalv3(75, 75));"""
    }

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_37_0_vals_65) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 0)) as decimalv3(75, 75));""")
    }
    def test_cast_to_decimal256_75_75_from_decimal128i_37_1_vals_66 = [("1.9"),("999999999999999999999999999999999998.9"),("999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_37_1_vals_66) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 1)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_37_1_vals_66) {
        qt_sql_test_cast_to_decimal256_75_75_from_decimal128i_37_1 """select cast(cast("${test_str}" as decimalv3(37, 1)) as decimalv3(75, 75));"""
    }

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_37_1_vals_66) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 1)) as decimalv3(75, 75));""")
    }
    def test_cast_to_decimal256_75_75_from_decimal128i_37_18_vals_67 = [("1.999999999999999999"),("9999999999999999998.999999999999999999"),("9999999999999999999.999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_37_18_vals_67) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 18)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_37_18_vals_67) {
        qt_sql_test_cast_to_decimal256_75_75_from_decimal128i_37_18 """select cast(cast("${test_str}" as decimalv3(37, 18)) as decimalv3(75, 75));"""
    }

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_37_18_vals_67) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 18)) as decimalv3(75, 75));""")
    }
    def test_cast_to_decimal256_75_75_from_decimal128i_37_36_vals_68 = [("1.999999999999999999999999999999999999"),("8.999999999999999999999999999999999999"),("9.999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_37_36_vals_68) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(37, 36)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_37_36_vals_68) {
        qt_sql_test_cast_to_decimal256_75_75_from_decimal128i_37_36 """select cast(cast("${test_str}" as decimalv3(37, 36)) as decimalv3(75, 75));"""
    }

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_37_36_vals_68) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(37, 36)) as decimalv3(75, 75));""")
    }
    def test_cast_to_decimal256_75_75_from_decimal128i_38_0_vals_70 = [("1"),("99999999999999999999999999999999999998"),("99999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_38_0_vals_70) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 0)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_38_0_vals_70) {
        qt_sql_test_cast_to_decimal256_75_75_from_decimal128i_38_0 """select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(75, 75));"""
    }

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_38_0_vals_70) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as decimalv3(75, 75));""")
    }
    def test_cast_to_decimal256_75_75_from_decimal128i_38_1_vals_71 = [("1.9"),("9999999999999999999999999999999999998.9"),("9999999999999999999999999999999999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_38_1_vals_71) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 1)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_38_1_vals_71) {
        qt_sql_test_cast_to_decimal256_75_75_from_decimal128i_38_1 """select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(75, 75));"""
    }

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_38_1_vals_71) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as decimalv3(75, 75));""")
    }
    def test_cast_to_decimal256_75_75_from_decimal128i_38_19_vals_72 = [("1.9999999999999999999"),("9999999999999999998.9999999999999999999"),("9999999999999999999.9999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_38_19_vals_72) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 19)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_38_19_vals_72) {
        qt_sql_test_cast_to_decimal256_75_75_from_decimal128i_38_19 """select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(75, 75));"""
    }

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_38_19_vals_72) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as decimalv3(75, 75));""")
    }
    def test_cast_to_decimal256_75_75_from_decimal128i_38_37_vals_73 = [("1.9999999999999999999999999999999999999"),("8.9999999999999999999999999999999999999"),("9.9999999999999999999999999999999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_38_37_vals_73) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(38, 37)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_38_37_vals_73) {
        qt_sql_test_cast_to_decimal256_75_75_from_decimal128i_38_37 """select cast(cast("${test_str}" as decimalv3(38, 37)) as decimalv3(75, 75));"""
    }

    for (test_str in test_cast_to_decimal256_75_75_from_decimal128i_38_37_vals_73) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 37)) as decimalv3(75, 75));""")
    }
}