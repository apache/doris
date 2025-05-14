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


suite("test_cast_to_decimal128v3_37_from_decimal32_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal128v3_37_36_from_decimal32_4_0_vals_53 = [("10"),("9998"),("9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_4_0_vals_53) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 0)) as decimalv3(37, 36));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_4_0_vals_53) {
        qt_sql_test_cast_to_decimal128v3_37_36_from_decimal32_4_0 """select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(37, 36));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_4_0_vals_53) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(37, 36));""")
    }
    def test_cast_to_decimal128v3_37_36_from_decimal32_4_1_vals_54 = [("10.9"),("998.9"),("999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_4_1_vals_54) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 1)) as decimalv3(37, 36));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_4_1_vals_54) {
        qt_sql_test_cast_to_decimal128v3_37_36_from_decimal32_4_1 """select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(37, 36));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_4_1_vals_54) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(37, 36));""")
    }
    def test_cast_to_decimal128v3_37_36_from_decimal32_4_2_vals_55 = [("10.99"),("98.99"),("99.99")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_4_2_vals_55) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 2)) as decimalv3(37, 36));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_4_2_vals_55) {
        qt_sql_test_cast_to_decimal128v3_37_36_from_decimal32_4_2 """select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(37, 36));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_4_2_vals_55) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(37, 36));""")
    }
    def test_cast_to_decimal128v3_37_36_from_decimal32_8_0_vals_58 = [("10"),("99999998"),("99999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_8_0_vals_58) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 0)) as decimalv3(37, 36));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_8_0_vals_58) {
        qt_sql_test_cast_to_decimal128v3_37_36_from_decimal32_8_0 """select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(37, 36));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_8_0_vals_58) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(37, 36));""")
    }
    def test_cast_to_decimal128v3_37_36_from_decimal32_8_1_vals_59 = [("10.9"),("9999998.9"),("9999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_8_1_vals_59) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 1)) as decimalv3(37, 36));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_8_1_vals_59) {
        qt_sql_test_cast_to_decimal128v3_37_36_from_decimal32_8_1 """select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(37, 36));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_8_1_vals_59) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(37, 36));""")
    }
    def test_cast_to_decimal128v3_37_36_from_decimal32_8_4_vals_60 = [("10.9999"),("9998.9999"),("9999.9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_8_4_vals_60) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 4)) as decimalv3(37, 36));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_8_4_vals_60) {
        qt_sql_test_cast_to_decimal128v3_37_36_from_decimal32_8_4 """select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(37, 36));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_8_4_vals_60) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(37, 36));""")
    }
    def test_cast_to_decimal128v3_37_36_from_decimal32_9_0_vals_63 = [("10"),("999999998"),("999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_9_0_vals_63) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 0)) as decimalv3(37, 36));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_9_0_vals_63) {
        qt_sql_test_cast_to_decimal128v3_37_36_from_decimal32_9_0 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(37, 36));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_9_0_vals_63) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(37, 36));""")
    }
    def test_cast_to_decimal128v3_37_36_from_decimal32_9_1_vals_64 = [("10.9"),("99999998.9"),("99999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_9_1_vals_64) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 1)) as decimalv3(37, 36));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_9_1_vals_64) {
        qt_sql_test_cast_to_decimal128v3_37_36_from_decimal32_9_1 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(37, 36));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_9_1_vals_64) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(37, 36));""")
    }
    def test_cast_to_decimal128v3_37_36_from_decimal32_9_4_vals_65 = [("10.9999"),("99998.9999"),("99999.9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_9_4_vals_65) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 4)) as decimalv3(37, 36));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_9_4_vals_65) {
        qt_sql_test_cast_to_decimal128v3_37_36_from_decimal32_9_4 """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(37, 36));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_36_from_decimal32_9_4_vals_65) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(37, 36));""")
    }
    def test_cast_to_decimal128v3_37_37_from_decimal32_1_0_vals_68 = [("1"),("8"),("9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_1_0_vals_68) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(1, 0)) as decimalv3(37, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_1_0_vals_68) {
        qt_sql_test_cast_to_decimal128v3_37_37_from_decimal32_1_0 """select cast(cast("${test_str}" as decimalv3(1, 0)) as decimalv3(37, 37));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_1_0_vals_68) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(1, 0)) as decimalv3(37, 37));""")
    }
    def test_cast_to_decimal128v3_37_37_from_decimal32_4_0_vals_70 = [("1"),("9998"),("9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_4_0_vals_70) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 0)) as decimalv3(37, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_4_0_vals_70) {
        qt_sql_test_cast_to_decimal128v3_37_37_from_decimal32_4_0 """select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(37, 37));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_4_0_vals_70) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(37, 37));""")
    }
    def test_cast_to_decimal128v3_37_37_from_decimal32_4_1_vals_71 = [("1.9"),("998.9"),("999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_4_1_vals_71) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 1)) as decimalv3(37, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_4_1_vals_71) {
        qt_sql_test_cast_to_decimal128v3_37_37_from_decimal32_4_1 """select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(37, 37));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_4_1_vals_71) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(37, 37));""")
    }
    def test_cast_to_decimal128v3_37_37_from_decimal32_4_2_vals_72 = [("1.99"),("98.99"),("99.99")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_4_2_vals_72) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 2)) as decimalv3(37, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_4_2_vals_72) {
        qt_sql_test_cast_to_decimal128v3_37_37_from_decimal32_4_2 """select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(37, 37));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_4_2_vals_72) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(37, 37));""")
    }
    def test_cast_to_decimal128v3_37_37_from_decimal32_4_3_vals_73 = [("1.999"),("8.999"),("9.999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_4_3_vals_73) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(4, 3)) as decimalv3(37, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_4_3_vals_73) {
        qt_sql_test_cast_to_decimal128v3_37_37_from_decimal32_4_3 """select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(37, 37));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_4_3_vals_73) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(37, 37));""")
    }
    def test_cast_to_decimal128v3_37_37_from_decimal32_8_0_vals_75 = [("1"),("99999998"),("99999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_8_0_vals_75) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 0)) as decimalv3(37, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_8_0_vals_75) {
        qt_sql_test_cast_to_decimal128v3_37_37_from_decimal32_8_0 """select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(37, 37));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_8_0_vals_75) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(37, 37));""")
    }
    def test_cast_to_decimal128v3_37_37_from_decimal32_8_1_vals_76 = [("1.9"),("9999998.9"),("9999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_8_1_vals_76) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 1)) as decimalv3(37, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_8_1_vals_76) {
        qt_sql_test_cast_to_decimal128v3_37_37_from_decimal32_8_1 """select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(37, 37));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_8_1_vals_76) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(37, 37));""")
    }
    def test_cast_to_decimal128v3_37_37_from_decimal32_8_4_vals_77 = [("1.9999"),("9998.9999"),("9999.9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_8_4_vals_77) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 4)) as decimalv3(37, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_8_4_vals_77) {
        qt_sql_test_cast_to_decimal128v3_37_37_from_decimal32_8_4 """select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(37, 37));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_8_4_vals_77) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(37, 37));""")
    }
    def test_cast_to_decimal128v3_37_37_from_decimal32_8_7_vals_78 = [("1.9999999"),("8.9999999"),("9.9999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_8_7_vals_78) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(8, 7)) as decimalv3(37, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_8_7_vals_78) {
        qt_sql_test_cast_to_decimal128v3_37_37_from_decimal32_8_7 """select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(37, 37));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_8_7_vals_78) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(37, 37));""")
    }
    def test_cast_to_decimal128v3_37_37_from_decimal32_9_0_vals_80 = [("1"),("999999998"),("999999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_9_0_vals_80) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 0)) as decimalv3(37, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_9_0_vals_80) {
        qt_sql_test_cast_to_decimal128v3_37_37_from_decimal32_9_0 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(37, 37));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_9_0_vals_80) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(37, 37));""")
    }
    def test_cast_to_decimal128v3_37_37_from_decimal32_9_1_vals_81 = [("1.9"),("99999998.9"),("99999999.9")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_9_1_vals_81) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 1)) as decimalv3(37, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_9_1_vals_81) {
        qt_sql_test_cast_to_decimal128v3_37_37_from_decimal32_9_1 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(37, 37));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_9_1_vals_81) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(37, 37));""")
    }
    def test_cast_to_decimal128v3_37_37_from_decimal32_9_4_vals_82 = [("1.9999"),("99998.9999"),("99999.9999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_9_4_vals_82) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 4)) as decimalv3(37, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_9_4_vals_82) {
        qt_sql_test_cast_to_decimal128v3_37_37_from_decimal32_9_4 """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(37, 37));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_9_4_vals_82) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(37, 37));""")
    }
    def test_cast_to_decimal128v3_37_37_from_decimal32_9_8_vals_83 = [("1.99999999"),("8.99999999"),("9.99999999")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_9_8_vals_83) {
            test {
                sql """select cast(cast(\"${test_str}\" as decimalv3(9, 8)) as decimalv3(37, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_9_8_vals_83) {
        qt_sql_test_cast_to_decimal128v3_37_37_from_decimal32_9_8 """select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(37, 37));"""
    }

    for (test_str in test_cast_to_decimal128v3_37_37_from_decimal32_9_8_vals_83) {
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(37, 37));""")
    }
}