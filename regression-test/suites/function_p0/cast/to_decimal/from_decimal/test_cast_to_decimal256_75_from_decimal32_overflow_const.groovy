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


suite("test_cast_to_decimal256_75_from_decimal32_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_decimal256 = true;"
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_74_from_decimal_4_0_overflow_53_test_data = ["""10""","""9998""","""9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_74_from_decimal_4_0_overflow_53_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_74_from_decimal_4_0_overflow_53_test_data) {
        qt_sql_test_cast_to_decimal_75_74_from_decimal_4_0_overflow_53 """select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(75, 74));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(75, 74));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_74_from_decimal_4_1_overflow_54_test_data = ["""10.9""","""998.9""","""999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_74_from_decimal_4_1_overflow_54_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_74_from_decimal_4_1_overflow_54_test_data) {
        qt_sql_test_cast_to_decimal_75_74_from_decimal_4_1_overflow_54 """select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(75, 74));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(75, 74));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_74_from_decimal_4_2_overflow_55_test_data = ["""10.99""","""98.99""","""99.99"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_74_from_decimal_4_2_overflow_55_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_74_from_decimal_4_2_overflow_55_test_data) {
        qt_sql_test_cast_to_decimal_75_74_from_decimal_4_2_overflow_55 """select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(75, 74));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(75, 74));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_74_from_decimal_8_0_overflow_58_test_data = ["""10""","""99999998""","""99999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_74_from_decimal_8_0_overflow_58_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_74_from_decimal_8_0_overflow_58_test_data) {
        qt_sql_test_cast_to_decimal_75_74_from_decimal_8_0_overflow_58 """select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(75, 74));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(75, 74));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_74_from_decimal_8_1_overflow_59_test_data = ["""10.9""","""9999998.9""","""9999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_74_from_decimal_8_1_overflow_59_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_74_from_decimal_8_1_overflow_59_test_data) {
        qt_sql_test_cast_to_decimal_75_74_from_decimal_8_1_overflow_59 """select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(75, 74));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(75, 74));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_74_from_decimal_8_4_overflow_60_test_data = ["""10.9999""","""9998.9999""","""9999.9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_74_from_decimal_8_4_overflow_60_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_74_from_decimal_8_4_overflow_60_test_data) {
        qt_sql_test_cast_to_decimal_75_74_from_decimal_8_4_overflow_60 """select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(75, 74));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(75, 74));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_74_from_decimal_9_0_overflow_63_test_data = ["""10""","""999999998""","""999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_74_from_decimal_9_0_overflow_63_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_74_from_decimal_9_0_overflow_63_test_data) {
        qt_sql_test_cast_to_decimal_75_74_from_decimal_9_0_overflow_63 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(75, 74));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(75, 74));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_74_from_decimal_9_1_overflow_64_test_data = ["""10.9""","""99999998.9""","""99999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_74_from_decimal_9_1_overflow_64_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_74_from_decimal_9_1_overflow_64_test_data) {
        qt_sql_test_cast_to_decimal_75_74_from_decimal_9_1_overflow_64 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(75, 74));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(75, 74));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_74_from_decimal_9_4_overflow_65_test_data = ["""10.9999""","""99998.9999""","""99999.9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_74_from_decimal_9_4_overflow_65_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(75, 74));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_74_from_decimal_9_4_overflow_65_test_data) {
        qt_sql_test_cast_to_decimal_75_74_from_decimal_9_4_overflow_65 """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(75, 74));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(75, 74));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_75_from_decimal_1_0_overflow_68_test_data = ["""1""","""8""","""9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_75_from_decimal_1_0_overflow_68_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(1, 0)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_75_from_decimal_1_0_overflow_68_test_data) {
        qt_sql_test_cast_to_decimal_75_75_from_decimal_1_0_overflow_68 """select cast(cast("${test_str}" as decimalv3(1, 0)) as decimalv3(75, 75));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(1, 0)) as decimalv3(75, 75));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_75_from_decimal_4_0_overflow_70_test_data = ["""1""","""9998""","""9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_75_from_decimal_4_0_overflow_70_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_75_from_decimal_4_0_overflow_70_test_data) {
        qt_sql_test_cast_to_decimal_75_75_from_decimal_4_0_overflow_70 """select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(75, 75));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 0)) as decimalv3(75, 75));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_75_from_decimal_4_1_overflow_71_test_data = ["""1.9""","""998.9""","""999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_75_from_decimal_4_1_overflow_71_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_75_from_decimal_4_1_overflow_71_test_data) {
        qt_sql_test_cast_to_decimal_75_75_from_decimal_4_1_overflow_71 """select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(75, 75));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 1)) as decimalv3(75, 75));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_75_from_decimal_4_2_overflow_72_test_data = ["""1.99""","""98.99""","""99.99"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_75_from_decimal_4_2_overflow_72_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_75_from_decimal_4_2_overflow_72_test_data) {
        qt_sql_test_cast_to_decimal_75_75_from_decimal_4_2_overflow_72 """select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(75, 75));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 2)) as decimalv3(75, 75));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_75_from_decimal_4_3_overflow_73_test_data = ["""1.999""","""8.999""","""9.999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_75_from_decimal_4_3_overflow_73_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_75_from_decimal_4_3_overflow_73_test_data) {
        qt_sql_test_cast_to_decimal_75_75_from_decimal_4_3_overflow_73 """select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(75, 75));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(4, 3)) as decimalv3(75, 75));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_75_from_decimal_8_0_overflow_75_test_data = ["""1""","""99999998""","""99999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_75_from_decimal_8_0_overflow_75_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_75_from_decimal_8_0_overflow_75_test_data) {
        qt_sql_test_cast_to_decimal_75_75_from_decimal_8_0_overflow_75 """select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(75, 75));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 0)) as decimalv3(75, 75));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_75_from_decimal_8_1_overflow_76_test_data = ["""1.9""","""9999998.9""","""9999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_75_from_decimal_8_1_overflow_76_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_75_from_decimal_8_1_overflow_76_test_data) {
        qt_sql_test_cast_to_decimal_75_75_from_decimal_8_1_overflow_76 """select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(75, 75));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 1)) as decimalv3(75, 75));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_75_from_decimal_8_4_overflow_77_test_data = ["""1.9999""","""9998.9999""","""9999.9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_75_from_decimal_8_4_overflow_77_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_75_from_decimal_8_4_overflow_77_test_data) {
        qt_sql_test_cast_to_decimal_75_75_from_decimal_8_4_overflow_77 """select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(75, 75));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 4)) as decimalv3(75, 75));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_75_from_decimal_8_7_overflow_78_test_data = ["""1.9999999""","""8.9999999""","""9.9999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_75_from_decimal_8_7_overflow_78_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_75_from_decimal_8_7_overflow_78_test_data) {
        qt_sql_test_cast_to_decimal_75_75_from_decimal_8_7_overflow_78 """select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(75, 75));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(8, 7)) as decimalv3(75, 75));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_75_from_decimal_9_0_overflow_80_test_data = ["""1""","""999999998""","""999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_75_from_decimal_9_0_overflow_80_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_75_from_decimal_9_0_overflow_80_test_data) {
        qt_sql_test_cast_to_decimal_75_75_from_decimal_9_0_overflow_80 """select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(75, 75));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as decimalv3(75, 75));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_75_from_decimal_9_1_overflow_81_test_data = ["""1.9""","""99999998.9""","""99999999.9"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_75_from_decimal_9_1_overflow_81_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_75_from_decimal_9_1_overflow_81_test_data) {
        qt_sql_test_cast_to_decimal_75_75_from_decimal_9_1_overflow_81 """select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(75, 75));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as decimalv3(75, 75));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_75_from_decimal_9_4_overflow_82_test_data = ["""1.9999""","""99998.9999""","""99999.9999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_75_from_decimal_9_4_overflow_82_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_75_from_decimal_9_4_overflow_82_test_data) {
        qt_sql_test_cast_to_decimal_75_75_from_decimal_9_4_overflow_82 """select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(75, 75));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 4)) as decimalv3(75, 75));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal_75_75_from_decimal_9_8_overflow_83_test_data = ["""1.99999999""","""8.99999999""","""9.99999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal_75_75_from_decimal_9_8_overflow_83_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(75, 75));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal_75_75_from_decimal_9_8_overflow_83_test_data) {
        qt_sql_test_cast_to_decimal_75_75_from_decimal_9_8_overflow_83 """select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(75, 75));"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 8)) as decimalv3(75, 75));""")
    }
}