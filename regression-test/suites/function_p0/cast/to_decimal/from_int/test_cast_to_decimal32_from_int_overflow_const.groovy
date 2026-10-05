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


suite("test_cast_to_decimal32_from_int_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_0_test_data = ["""-128""","""-10""","""10""","""127"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_0_test_data) {
            test {
                sql """select cast(cast("${test_str}" as tinyint) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_0_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_0 """select cast(cast("${test_str}" as tinyint) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as tinyint) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_2_test_data = ["""-128""","""-100""","""100""","""127"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_2_test_data) {
            test {
                sql """select cast(cast("${test_str}" as tinyint) as decimalv3(4, 2));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_2_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_2 """select cast(cast("${test_str}" as tinyint) as decimalv3(4, 2));"""
        testFoldConst("""select cast(cast("${test_str}" as tinyint) as decimalv3(4, 2));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_3_test_data = ["""-128""","""-10""","""10""","""127"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_3_test_data) {
            test {
                sql """select cast(cast("${test_str}" as tinyint) as decimalv3(4, 3));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_3_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_3 """select cast(cast("${test_str}" as tinyint) as decimalv3(4, 3));"""
        testFoldConst("""select cast(cast("${test_str}" as tinyint) as decimalv3(4, 3));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_6_test_data = ["""-128""","""-10""","""10""","""127"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_6_test_data) {
            test {
                sql """select cast(cast("${test_str}" as tinyint) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_6_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_6 """select cast(cast("${test_str}" as tinyint) as decimalv3(9, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as tinyint) as decimalv3(9, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_7_test_data = ["""-32768""","""-10""","""10""","""32767"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_7_test_data) {
            test {
                sql """select cast(cast("${test_str}" as smallint) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_7_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_7 """select cast(cast("${test_str}" as smallint) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as smallint) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_8_test_data = ["""-32768""","""-10000""","""10000""","""32767"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_8_test_data) {
            test {
                sql """select cast(cast("${test_str}" as smallint) as decimalv3(4, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_8_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_8 """select cast(cast("${test_str}" as smallint) as decimalv3(4, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as smallint) as decimalv3(4, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_9_test_data = ["""-32768""","""-100""","""100""","""32767"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_9_test_data) {
            test {
                sql """select cast(cast("${test_str}" as smallint) as decimalv3(4, 2));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_9_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_9 """select cast(cast("${test_str}" as smallint) as decimalv3(4, 2));"""
        testFoldConst("""select cast(cast("${test_str}" as smallint) as decimalv3(4, 2));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_10_test_data = ["""-32768""","""-10""","""10""","""32767"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_10_test_data) {
            test {
                sql """select cast(cast("${test_str}" as smallint) as decimalv3(4, 3));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_10_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_10 """select cast(cast("${test_str}" as smallint) as decimalv3(4, 3));"""
        testFoldConst("""select cast(cast("${test_str}" as smallint) as decimalv3(4, 3));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_13_test_data = ["""-32768""","""-10""","""10""","""32767"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_13_test_data) {
            test {
                sql """select cast(cast("${test_str}" as smallint) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_13_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_13 """select cast(cast("${test_str}" as smallint) as decimalv3(9, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as smallint) as decimalv3(9, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_14_test_data = ["""-2147483648""","""-10""","""10""","""2147483647"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_14_test_data) {
            test {
                sql """select cast(cast("${test_str}" as int) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_14_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_14 """select cast(cast("${test_str}" as int) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as int) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_15_test_data = ["""-2147483648""","""-10000""","""10000""","""2147483647"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_15_test_data) {
            test {
                sql """select cast(cast("${test_str}" as int) as decimalv3(4, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_15_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_15 """select cast(cast("${test_str}" as int) as decimalv3(4, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as int) as decimalv3(4, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_16_test_data = ["""-2147483648""","""-100""","""100""","""2147483647"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_16_test_data) {
            test {
                sql """select cast(cast("${test_str}" as int) as decimalv3(4, 2));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_16_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_16 """select cast(cast("${test_str}" as int) as decimalv3(4, 2));"""
        testFoldConst("""select cast(cast("${test_str}" as int) as decimalv3(4, 2));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_17_test_data = ["""-2147483648""","""-10""","""10""","""2147483647"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_17_test_data) {
            test {
                sql """select cast(cast("${test_str}" as int) as decimalv3(4, 3));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_17_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_17 """select cast(cast("${test_str}" as int) as decimalv3(4, 3));"""
        testFoldConst("""select cast(cast("${test_str}" as int) as decimalv3(4, 3));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_18_test_data = ["""-2147483648""","""-1000000000""","""1000000000""","""2147483647"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_18_test_data) {
            test {
                sql """select cast(cast("${test_str}" as int) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_18_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_18 """select cast(cast("${test_str}" as int) as decimalv3(9, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as int) as decimalv3(9, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_19_test_data = ["""-2147483648""","""-100000""","""100000""","""2147483647"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_19_test_data) {
            test {
                sql """select cast(cast("${test_str}" as int) as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_19_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_19 """select cast(cast("${test_str}" as int) as decimalv3(9, 4));"""
        testFoldConst("""select cast(cast("${test_str}" as int) as decimalv3(9, 4));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_20_test_data = ["""-2147483648""","""-10""","""10""","""2147483647"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_20_test_data) {
            test {
                sql """select cast(cast("${test_str}" as int) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_20_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_20 """select cast(cast("${test_str}" as int) as decimalv3(9, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as int) as decimalv3(9, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_21_test_data = ["""-9223372036854775808""","""-10""","""10""","""9223372036854775807"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_21_test_data) {
            test {
                sql """select cast(cast("${test_str}" as bigint) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_21_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_21 """select cast(cast("${test_str}" as bigint) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as bigint) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_22_test_data = ["""-9223372036854775808""","""-10000""","""10000""","""9223372036854775807"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_22_test_data) {
            test {
                sql """select cast(cast("${test_str}" as bigint) as decimalv3(4, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_22_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_22 """select cast(cast("${test_str}" as bigint) as decimalv3(4, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as bigint) as decimalv3(4, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_23_test_data = ["""-9223372036854775808""","""-100""","""100""","""9223372036854775807"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_23_test_data) {
            test {
                sql """select cast(cast("${test_str}" as bigint) as decimalv3(4, 2));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_23_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_23 """select cast(cast("${test_str}" as bigint) as decimalv3(4, 2));"""
        testFoldConst("""select cast(cast("${test_str}" as bigint) as decimalv3(4, 2));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_24_test_data = ["""-9223372036854775808""","""-10""","""10""","""9223372036854775807"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_24_test_data) {
            test {
                sql """select cast(cast("${test_str}" as bigint) as decimalv3(4, 3));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_24_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_24 """select cast(cast("${test_str}" as bigint) as decimalv3(4, 3));"""
        testFoldConst("""select cast(cast("${test_str}" as bigint) as decimalv3(4, 3));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_25_test_data = ["""-9223372036854775808""","""-1000000000""","""1000000000""","""9223372036854775807"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_25_test_data) {
            test {
                sql """select cast(cast("${test_str}" as bigint) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_25_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_25 """select cast(cast("${test_str}" as bigint) as decimalv3(9, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as bigint) as decimalv3(9, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_26_test_data = ["""-9223372036854775808""","""-100000""","""100000""","""9223372036854775807"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_26_test_data) {
            test {
                sql """select cast(cast("${test_str}" as bigint) as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_26_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_26 """select cast(cast("${test_str}" as bigint) as decimalv3(9, 4));"""
        testFoldConst("""select cast(cast("${test_str}" as bigint) as decimalv3(9, 4));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_27_test_data = ["""-9223372036854775808""","""-10""","""10""","""9223372036854775807"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_27_test_data) {
            test {
                sql """select cast(cast("${test_str}" as bigint) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_27_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_27 """select cast(cast("${test_str}" as bigint) as decimalv3(9, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as bigint) as decimalv3(9, 8));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_28_test_data = ["""-170141183460469231731687303715884105728""","""-10""","""10""","""170141183460469231731687303715884105727"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_28_test_data) {
            test {
                sql """select cast(cast("${test_str}" as largeint) as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_28_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_28 """select cast(cast("${test_str}" as largeint) as decimalv3(1, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as largeint) as decimalv3(1, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_29_test_data = ["""-170141183460469231731687303715884105728""","""-10000""","""10000""","""170141183460469231731687303715884105727"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_29_test_data) {
            test {
                sql """select cast(cast("${test_str}" as largeint) as decimalv3(4, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_29_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_29 """select cast(cast("${test_str}" as largeint) as decimalv3(4, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as largeint) as decimalv3(4, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_30_test_data = ["""-170141183460469231731687303715884105728""","""-100""","""100""","""170141183460469231731687303715884105727"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_30_test_data) {
            test {
                sql """select cast(cast("${test_str}" as largeint) as decimalv3(4, 2));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_30_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_30 """select cast(cast("${test_str}" as largeint) as decimalv3(4, 2));"""
        testFoldConst("""select cast(cast("${test_str}" as largeint) as decimalv3(4, 2));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_31_test_data = ["""-170141183460469231731687303715884105728""","""-10""","""10""","""170141183460469231731687303715884105727"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_31_test_data) {
            test {
                sql """select cast(cast("${test_str}" as largeint) as decimalv3(4, 3));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_31_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_31 """select cast(cast("${test_str}" as largeint) as decimalv3(4, 3));"""
        testFoldConst("""select cast(cast("${test_str}" as largeint) as decimalv3(4, 3));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_32_test_data = ["""-170141183460469231731687303715884105728""","""-1000000000""","""1000000000""","""170141183460469231731687303715884105727"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_32_test_data) {
            test {
                sql """select cast(cast("${test_str}" as largeint) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_32_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_32 """select cast(cast("${test_str}" as largeint) as decimalv3(9, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as largeint) as decimalv3(9, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_33_test_data = ["""-170141183460469231731687303715884105728""","""-100000""","""100000""","""170141183460469231731687303715884105727"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_33_test_data) {
            test {
                sql """select cast(cast("${test_str}" as largeint) as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_33_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_33 """select cast(cast("${test_str}" as largeint) as decimalv3(9, 4));"""
        testFoldConst("""select cast(cast("${test_str}" as largeint) as decimalv3(9, 4));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_int_overflow_34_test_data = ["""-170141183460469231731687303715884105728""","""-10""","""10""","""170141183460469231731687303715884105727"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_int_overflow_34_test_data) {
            test {
                sql """select cast(cast("${test_str}" as largeint) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_int_overflow_34_test_data) {
        qt_sql_test_cast_to_decimal32_from_int_overflow_34 """select cast(cast("${test_str}" as largeint) as decimalv3(9, 8));"""
        testFoldConst("""select cast(cast("${test_str}" as largeint) as decimalv3(9, 8));""")
    }
}