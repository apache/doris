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
    def test_cast_to_decimal32_1_0_from_tinyint_overflow_vals = [(-128),(-10),(10),(127)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_tinyint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_1_0_from_tinyint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_1_0_from_tinyint_overflow """select cast(${test_str} as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_tinyint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_4_2_from_tinyint_overflow_vals = [(-128),(-100),(100),(127)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_4_2_from_tinyint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(4, 2));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_4_2_from_tinyint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_4_2_from_tinyint_overflow """select cast(${test_str} as decimalv3(4, 2));"""
    }

    for (test_str in test_cast_to_decimal32_4_2_from_tinyint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(4, 2));""")
    }
    def test_cast_to_decimal32_4_3_from_tinyint_overflow_vals = [(-128),(-10),(10),(127)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_4_3_from_tinyint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(4, 3));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_4_3_from_tinyint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_4_3_from_tinyint_overflow """select cast(${test_str} as decimalv3(4, 3));"""
    }

    for (test_str in test_cast_to_decimal32_4_3_from_tinyint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(4, 3));""")
    }
    def test_cast_to_decimal32_9_8_from_tinyint_overflow_vals = [(-128),(-10),(10),(127)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_tinyint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_9_8_from_tinyint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_9_8_from_tinyint_overflow """select cast(${test_str} as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_tinyint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_1_0_from_smallint_overflow_vals = [(-32768),(-10),(10),(32767)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_smallint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_1_0_from_smallint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_1_0_from_smallint_overflow """select cast(${test_str} as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_smallint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_4_0_from_smallint_overflow_vals = [(-32768),(-10000),(10000),(32767)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_4_0_from_smallint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(4, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_4_0_from_smallint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_4_0_from_smallint_overflow """select cast(${test_str} as decimalv3(4, 0));"""
    }

    for (test_str in test_cast_to_decimal32_4_0_from_smallint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(4, 0));""")
    }
    def test_cast_to_decimal32_4_2_from_smallint_overflow_vals = [(-32768),(-100),(100),(32767)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_4_2_from_smallint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(4, 2));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_4_2_from_smallint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_4_2_from_smallint_overflow """select cast(${test_str} as decimalv3(4, 2));"""
    }

    for (test_str in test_cast_to_decimal32_4_2_from_smallint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(4, 2));""")
    }
    def test_cast_to_decimal32_4_3_from_smallint_overflow_vals = [(-32768),(-10),(10),(32767)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_4_3_from_smallint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(4, 3));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_4_3_from_smallint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_4_3_from_smallint_overflow """select cast(${test_str} as decimalv3(4, 3));"""
    }

    for (test_str in test_cast_to_decimal32_4_3_from_smallint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(4, 3));""")
    }
    def test_cast_to_decimal32_9_8_from_smallint_overflow_vals = [(-32768),(-10),(10),(32767)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_smallint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_9_8_from_smallint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_9_8_from_smallint_overflow """select cast(${test_str} as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_smallint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_1_0_from_int_overflow_vals = [(-2147483648),(-10),(10),(2147483647)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_int_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_1_0_from_int_overflow_vals) {
        qt_sql_test_cast_to_decimal32_1_0_from_int_overflow """select cast(${test_str} as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_int_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_4_0_from_int_overflow_vals = [(-2147483648),(-10000),(10000),(2147483647)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_4_0_from_int_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(4, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_4_0_from_int_overflow_vals) {
        qt_sql_test_cast_to_decimal32_4_0_from_int_overflow """select cast(${test_str} as decimalv3(4, 0));"""
    }

    for (test_str in test_cast_to_decimal32_4_0_from_int_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(4, 0));""")
    }
    def test_cast_to_decimal32_4_2_from_int_overflow_vals = [(-2147483648),(-100),(100),(2147483647)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_4_2_from_int_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(4, 2));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_4_2_from_int_overflow_vals) {
        qt_sql_test_cast_to_decimal32_4_2_from_int_overflow """select cast(${test_str} as decimalv3(4, 2));"""
    }

    for (test_str in test_cast_to_decimal32_4_2_from_int_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(4, 2));""")
    }
    def test_cast_to_decimal32_4_3_from_int_overflow_vals = [(-2147483648),(-10),(10),(2147483647)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_4_3_from_int_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(4, 3));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_4_3_from_int_overflow_vals) {
        qt_sql_test_cast_to_decimal32_4_3_from_int_overflow """select cast(${test_str} as decimalv3(4, 3));"""
    }

    for (test_str in test_cast_to_decimal32_4_3_from_int_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(4, 3));""")
    }
    def test_cast_to_decimal32_9_0_from_int_overflow_vals = [(-2147483648),(-1000000000),(1000000000),(2147483647)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_0_from_int_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_9_0_from_int_overflow_vals) {
        qt_sql_test_cast_to_decimal32_9_0_from_int_overflow """select cast(${test_str} as decimalv3(9, 0));"""
    }

    for (test_str in test_cast_to_decimal32_9_0_from_int_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(9, 0));""")
    }
    def test_cast_to_decimal32_9_4_from_int_overflow_vals = [(-2147483648),(-100000),(100000),(2147483647)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_4_from_int_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_9_4_from_int_overflow_vals) {
        qt_sql_test_cast_to_decimal32_9_4_from_int_overflow """select cast(${test_str} as decimalv3(9, 4));"""
    }

    for (test_str in test_cast_to_decimal32_9_4_from_int_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(9, 4));""")
    }
    def test_cast_to_decimal32_9_8_from_int_overflow_vals = [(-2147483648),(-10),(10),(2147483647)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_int_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_9_8_from_int_overflow_vals) {
        qt_sql_test_cast_to_decimal32_9_8_from_int_overflow """select cast(${test_str} as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_int_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_1_0_from_bigint_overflow_vals = [(-9223372036854775808),(-10),(10),(9223372036854775807)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_bigint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_1_0_from_bigint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_1_0_from_bigint_overflow """select cast(${test_str} as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_bigint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_4_0_from_bigint_overflow_vals = [(-9223372036854775808),(-10000),(10000),(9223372036854775807)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_4_0_from_bigint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(4, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_4_0_from_bigint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_4_0_from_bigint_overflow """select cast(${test_str} as decimalv3(4, 0));"""
    }

    for (test_str in test_cast_to_decimal32_4_0_from_bigint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(4, 0));""")
    }
    def test_cast_to_decimal32_4_2_from_bigint_overflow_vals = [(-9223372036854775808),(-100),(100),(9223372036854775807)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_4_2_from_bigint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(4, 2));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_4_2_from_bigint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_4_2_from_bigint_overflow """select cast(${test_str} as decimalv3(4, 2));"""
    }

    for (test_str in test_cast_to_decimal32_4_2_from_bigint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(4, 2));""")
    }
    def test_cast_to_decimal32_4_3_from_bigint_overflow_vals = [(-9223372036854775808),(-10),(10),(9223372036854775807)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_4_3_from_bigint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(4, 3));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_4_3_from_bigint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_4_3_from_bigint_overflow """select cast(${test_str} as decimalv3(4, 3));"""
    }

    for (test_str in test_cast_to_decimal32_4_3_from_bigint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(4, 3));""")
    }
    def test_cast_to_decimal32_9_0_from_bigint_overflow_vals = [(-9223372036854775808),(-1000000000),(1000000000),(9223372036854775807)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_0_from_bigint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_9_0_from_bigint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_9_0_from_bigint_overflow """select cast(${test_str} as decimalv3(9, 0));"""
    }

    for (test_str in test_cast_to_decimal32_9_0_from_bigint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(9, 0));""")
    }
    def test_cast_to_decimal32_9_4_from_bigint_overflow_vals = [(-9223372036854775808),(-100000),(100000),(9223372036854775807)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_4_from_bigint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_9_4_from_bigint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_9_4_from_bigint_overflow """select cast(${test_str} as decimalv3(9, 4));"""
    }

    for (test_str in test_cast_to_decimal32_9_4_from_bigint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(9, 4));""")
    }
    def test_cast_to_decimal32_9_8_from_bigint_overflow_vals = [(-9223372036854775808),(-10),(10),(9223372036854775807)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_bigint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_9_8_from_bigint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_9_8_from_bigint_overflow """select cast(${test_str} as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_bigint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(9, 8));""")
    }
    def test_cast_to_decimal32_1_0_from_largeint_overflow_vals = [(-170141183460469231731687303715884105728),(-10),(10),(170141183460469231731687303715884105727)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_1_0_from_largeint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_1_0_from_largeint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_1_0_from_largeint_overflow """select cast(${test_str} as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_1_0_from_largeint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_4_0_from_largeint_overflow_vals = [(-170141183460469231731687303715884105728),(-10000),(10000),(170141183460469231731687303715884105727)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_4_0_from_largeint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(4, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_4_0_from_largeint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_4_0_from_largeint_overflow """select cast(${test_str} as decimalv3(4, 0));"""
    }

    for (test_str in test_cast_to_decimal32_4_0_from_largeint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(4, 0));""")
    }
    def test_cast_to_decimal32_4_2_from_largeint_overflow_vals = [(-170141183460469231731687303715884105728),(-100),(100),(170141183460469231731687303715884105727)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_4_2_from_largeint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(4, 2));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_4_2_from_largeint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_4_2_from_largeint_overflow """select cast(${test_str} as decimalv3(4, 2));"""
    }

    for (test_str in test_cast_to_decimal32_4_2_from_largeint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(4, 2));""")
    }
    def test_cast_to_decimal32_4_3_from_largeint_overflow_vals = [(-170141183460469231731687303715884105728),(-10),(10),(170141183460469231731687303715884105727)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_4_3_from_largeint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(4, 3));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_4_3_from_largeint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_4_3_from_largeint_overflow """select cast(${test_str} as decimalv3(4, 3));"""
    }

    for (test_str in test_cast_to_decimal32_4_3_from_largeint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(4, 3));""")
    }
    def test_cast_to_decimal32_9_0_from_largeint_overflow_vals = [(-170141183460469231731687303715884105728),(-1000000000),(1000000000),(170141183460469231731687303715884105727)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_0_from_largeint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_9_0_from_largeint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_9_0_from_largeint_overflow """select cast(${test_str} as decimalv3(9, 0));"""
    }

    for (test_str in test_cast_to_decimal32_9_0_from_largeint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(9, 0));""")
    }
    def test_cast_to_decimal32_9_4_from_largeint_overflow_vals = [(-170141183460469231731687303715884105728),(-100000),(100000),(170141183460469231731687303715884105727)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_4_from_largeint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_9_4_from_largeint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_9_4_from_largeint_overflow """select cast(${test_str} as decimalv3(9, 4));"""
    }

    for (test_str in test_cast_to_decimal32_9_4_from_largeint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(9, 4));""")
    }
    def test_cast_to_decimal32_9_8_from_largeint_overflow_vals = [(-170141183460469231731687303715884105728),(-10),(10),(170141183460469231731687303715884105727)]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_largeint_overflow_vals) {
            test {
                sql """select cast(${test_str} as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

     for (test_str in test_cast_to_decimal32_9_8_from_largeint_overflow_vals) {
        qt_sql_test_cast_to_decimal32_9_8_from_largeint_overflow """select cast(${test_str} as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_largeint_overflow_vals) {
        testFoldConst("""select cast(${test_str} as decimalv3(9, 8));""")
    }
}