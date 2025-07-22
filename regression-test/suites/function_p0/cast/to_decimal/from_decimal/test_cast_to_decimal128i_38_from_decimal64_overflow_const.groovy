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


suite("test_cast_to_decimal128i_38_from_decimal64_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_37_from_decimal_9_0_overflow_60_test_data = ["""10""","""999999998""","""999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_37_from_decimal_9_0_overflow_60_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(38, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_37_from_decimal_9_0_overflow_60_test_data) {
        qt_sql_test_cast_to_decimal_38_37_from_decimal_9_0_overflow_60 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(38, 37));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(38, 37));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_37_from_decimal_9_1_overflow_61_test_data = ["""10.9""","""99999998.9""","""99999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_37_from_decimal_9_1_overflow_61_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(38, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_37_from_decimal_9_1_overflow_61_test_data) {
        qt_sql_test_cast_to_decimal_38_37_from_decimal_9_1_overflow_61 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(38, 37));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(38, 37));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_37_from_decimal_9_4_overflow_62_test_data = ["""10.9999""","""99998.9999""","""99999.9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_37_from_decimal_9_4_overflow_62_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(38, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_37_from_decimal_9_4_overflow_62_test_data) {
        qt_sql_test_cast_to_decimal_38_37_from_decimal_9_4_overflow_62 """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(38, 37));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(38, 37));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_37_from_decimal_10_0_overflow_65_test_data = ["""10""","""9999999998""","""9999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_37_from_decimal_10_0_overflow_65_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(10, 0)) as decimalv3(38, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_37_from_decimal_10_0_overflow_65_test_data) {
        qt_sql_test_cast_to_decimal_38_37_from_decimal_10_0_overflow_65 """select cast(cast("${test_str}" as decimalv3(10, 0)) as decimalv3(38, 37));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 0)) as decimalv3(38, 37));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_37_from_decimal_10_1_overflow_66_test_data = ["""10.9""","""999999998.9""","""999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_37_from_decimal_10_1_overflow_66_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(10, 1)) as decimalv3(38, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_37_from_decimal_10_1_overflow_66_test_data) {
        qt_sql_test_cast_to_decimal_38_37_from_decimal_10_1_overflow_66 """select cast(cast("${test_str}" as decimalv3(10, 1)) as decimalv3(38, 37));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 1)) as decimalv3(38, 37));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_37_from_decimal_10_5_overflow_67_test_data = ["""10.99999""","""99998.99999""","""99999.99999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_37_from_decimal_10_5_overflow_67_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(10, 5)) as decimalv3(38, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_37_from_decimal_10_5_overflow_67_test_data) {
        qt_sql_test_cast_to_decimal_38_37_from_decimal_10_5_overflow_67 """select cast(cast("${test_str}" as decimalv3(10, 5)) as decimalv3(38, 37));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 5)) as decimalv3(38, 37));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_37_from_decimal_17_0_overflow_70_test_data = ["""10""","""99999999999999998""","""99999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_37_from_decimal_17_0_overflow_70_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(38, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_37_from_decimal_17_0_overflow_70_test_data) {
        qt_sql_test_cast_to_decimal_38_37_from_decimal_17_0_overflow_70 """select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(38, 37));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(38, 37));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_37_from_decimal_17_1_overflow_71_test_data = ["""10.9""","""9999999999999998.9""","""9999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_37_from_decimal_17_1_overflow_71_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(38, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_37_from_decimal_17_1_overflow_71_test_data) {
        qt_sql_test_cast_to_decimal_38_37_from_decimal_17_1_overflow_71 """select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(38, 37));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(38, 37));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_37_from_decimal_17_8_overflow_72_test_data = ["""10.99999999""","""999999998.99999999""","""999999999.99999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_37_from_decimal_17_8_overflow_72_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(17, 8)) as decimalv3(38, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_37_from_decimal_17_8_overflow_72_test_data) {
        qt_sql_test_cast_to_decimal_38_37_from_decimal_17_8_overflow_72 """select cast(cast("${test_str}" as decimalv3(17, 8)) as decimalv3(38, 37));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 8)) as decimalv3(38, 37));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_37_from_decimal_18_0_overflow_75_test_data = ["""10""","""999999999999999998""","""999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_37_from_decimal_18_0_overflow_75_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(38, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_37_from_decimal_18_0_overflow_75_test_data) {
        qt_sql_test_cast_to_decimal_38_37_from_decimal_18_0_overflow_75 """select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(38, 37));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(38, 37));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_37_from_decimal_18_1_overflow_76_test_data = ["""10.9""","""99999999999999998.9""","""99999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_37_from_decimal_18_1_overflow_76_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(38, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_37_from_decimal_18_1_overflow_76_test_data) {
        qt_sql_test_cast_to_decimal_38_37_from_decimal_18_1_overflow_76 """select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(38, 37));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(38, 37));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_37_from_decimal_18_9_overflow_77_test_data = ["""10.999999999""","""999999998.999999999""","""999999999.999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_37_from_decimal_18_9_overflow_77_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(18, 9)) as decimalv3(38, 37));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_37_from_decimal_18_9_overflow_77_test_data) {
        qt_sql_test_cast_to_decimal_38_37_from_decimal_18_9_overflow_77 """select cast(cast("${test_str}" as decimalv3(18, 9)) as decimalv3(38, 37));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 9)) as decimalv3(38, 37));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_9_0_overflow_80_test_data = ["""1""","""999999998""","""999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_9_0_overflow_80_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_9_0_overflow_80_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_9_0_overflow_80 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_9_1_overflow_81_test_data = ["""1.9""","""99999998.9""","""99999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_9_1_overflow_81_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_9_1_overflow_81_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_9_1_overflow_81 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_9_4_overflow_82_test_data = ["""1.9999""","""99998.9999""","""99999.9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_9_4_overflow_82_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_9_4_overflow_82_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_9_4_overflow_82 """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_9_8_overflow_83_test_data = ["""1.99999999""","""8.99999999""","""9.99999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_9_8_overflow_83_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_9_8_overflow_83_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_9_8_overflow_83 """select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_10_0_overflow_85_test_data = ["""1""","""9999999998""","""9999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_10_0_overflow_85_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(10, 0)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_10_0_overflow_85_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_10_0_overflow_85 """select cast(cast("${test_str}" as decimalv3(10, 0)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 0)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_10_1_overflow_86_test_data = ["""1.9""","""999999998.9""","""999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_10_1_overflow_86_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(10, 1)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_10_1_overflow_86_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_10_1_overflow_86 """select cast(cast("${test_str}" as decimalv3(10, 1)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 1)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_10_5_overflow_87_test_data = ["""1.99999""","""99998.99999""","""99999.99999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_10_5_overflow_87_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(10, 5)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_10_5_overflow_87_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_10_5_overflow_87 """select cast(cast("${test_str}" as decimalv3(10, 5)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 5)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_10_9_overflow_88_test_data = ["""1.999999999""","""8.999999999""","""9.999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_10_9_overflow_88_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(10, 9)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_10_9_overflow_88_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_10_9_overflow_88 """select cast(cast("${test_str}" as decimalv3(10, 9)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(10, 9)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_17_0_overflow_90_test_data = ["""1""","""99999999999999998""","""99999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_17_0_overflow_90_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_17_0_overflow_90_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_17_0_overflow_90 """select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 0)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_17_1_overflow_91_test_data = ["""1.9""","""9999999999999998.9""","""9999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_17_1_overflow_91_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_17_1_overflow_91_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_17_1_overflow_91 """select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 1)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_17_8_overflow_92_test_data = ["""1.99999999""","""999999998.99999999""","""999999999.99999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_17_8_overflow_92_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(17, 8)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_17_8_overflow_92_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_17_8_overflow_92 """select cast(cast("${test_str}" as decimalv3(17, 8)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 8)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_17_16_overflow_93_test_data = ["""1.9999999999999999""","""8.9999999999999999""","""9.9999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_17_16_overflow_93_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(17, 16)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_17_16_overflow_93_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_17_16_overflow_93 """select cast(cast("${test_str}" as decimalv3(17, 16)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(17, 16)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_18_0_overflow_95_test_data = ["""1""","""999999999999999998""","""999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_18_0_overflow_95_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_18_0_overflow_95_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_18_0_overflow_95 """select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 0)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_18_1_overflow_96_test_data = ["""1.9""","""99999999999999998.9""","""99999999999999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_18_1_overflow_96_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_18_1_overflow_96_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_18_1_overflow_96 """select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 1)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_18_9_overflow_97_test_data = ["""1.999999999""","""999999998.999999999""","""999999999.999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_18_9_overflow_97_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(18, 9)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_18_9_overflow_97_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_18_9_overflow_97 """select cast(cast("${test_str}" as decimalv3(18, 9)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 9)) as decimalv3(38, 38));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_38_38_from_decimal_18_17_overflow_98_test_data = ["""1.99999999999999999""","""8.99999999999999999""","""9.99999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_38_38_from_decimal_18_17_overflow_98_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(18, 17)) as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_38_38_from_decimal_18_17_overflow_98_test_data) {
        qt_sql_test_cast_to_decimal_38_38_from_decimal_18_17_overflow_98 """select cast(cast("${test_str}" as decimalv3(18, 17)) as decimalv3(38, 38));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(18, 17)) as decimalv3(38, 38));""")
    }
}