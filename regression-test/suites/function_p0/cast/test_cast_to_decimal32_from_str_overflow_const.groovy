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


suite("test_cast_to_decimal32_from_str_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_from_str_overflow_0_1_0_strs = ["-10","-10000000000000000000000000000000000000000000000000000000000000000000000000000","-19","-2147483648","-57896044618658097711785492504343953926634992332820282019728792003956564819968",
        "-9.5","-90","-91","-99","-9999999999999999999999999999999999999999999999999999999999999999999999999999",
        "10","10000000000000000000000000000000000000000000000000000000000000000000000000000","19","2147483647","57896044618658097711785492504343953926634992332820282019728792003956564819967",
        "9.5","90","91","99","9999999999999999999999999999999999999999999999999999999999999999999999999999"]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_str_overflow_0_1_0_strs) {
            test {
                sql """select cast("${test_str}" as decimalv3(1, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_str_overflow_0_1_0_strs) {
        qt_sql_test_cast_to_decimal32_from_str_overflow_0_1_0 """select cast("${test_str}" as decimalv3(1, 0));"""
    }

    for (test_str in test_cast_to_decimal32_from_str_overflow_0_1_0_strs) {
        testFoldConst("""select cast("${test_str}" as decimalv3(1, 0));""")
    }
    def test_cast_to_decimal32_from_str_overflow_1_1_1_strs = ["-.95","-01","-09","-1","-10",
        "-10000000000000000000000000000000000000000000000000000000000000000000000000000","-2147483648","-57896044618658097711785492504343953926634992332820282019728792003956564819968","-9999999999999999999999999999999999999999999999999999999999999999999999999999",".95",
        "01","09","1","10","10000000000000000000000000000000000000000000000000000000000000000000000000000",
        "2147483647","57896044618658097711785492504343953926634992332820282019728792003956564819967","9999999999999999999999999999999999999999999999999999999999999999999999999999"]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_str_overflow_1_1_1_strs) {
            test {
                sql """select cast("${test_str}" as decimalv3(1, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_str_overflow_1_1_1_strs) {
        qt_sql_test_cast_to_decimal32_from_str_overflow_1_1_1 """select cast("${test_str}" as decimalv3(1, 1));"""
    }

    for (test_str in test_cast_to_decimal32_from_str_overflow_1_1_1_strs) {
        testFoldConst("""select cast("${test_str}" as decimalv3(1, 1));""")
    }
    def test_cast_to_decimal32_from_str_overflow_2_9_0_strs = ["-1000000000","-10000000000000000000000000000000000000000000000000000000000000000000000000000","-1999999999","-2147483648","-57896044618658097711785492504343953926634992332820282019728792003956564819968",
        "-999999999.5","-9999999990","-9999999991","-9999999999","-9999999999999999999999999999999999999999999999999999999999999999999999999999",
        "1000000000","10000000000000000000000000000000000000000000000000000000000000000000000000000","1999999999","2147483647","57896044618658097711785492504343953926634992332820282019728792003956564819967",
        "999999999.5","9999999990","9999999991","9999999999","9999999999999999999999999999999999999999999999999999999999999999999999999999"]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_str_overflow_2_9_0_strs) {
            test {
                sql """select cast("${test_str}" as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_str_overflow_2_9_0_strs) {
        qt_sql_test_cast_to_decimal32_from_str_overflow_2_9_0 """select cast("${test_str}" as decimalv3(9, 0));"""
    }

    for (test_str in test_cast_to_decimal32_from_str_overflow_2_9_0_strs) {
        testFoldConst("""select cast("${test_str}" as decimalv3(9, 0));""")
    }
    def test_cast_to_decimal32_from_str_overflow_3_9_1_strs = ["-100000000","-10000000000000000000000000000000000000000000000000000000000000000000000000000","-199999999","-2147483648","-57896044618658097711785492504343953926634992332820282019728792003956564819968",
        "-99999999.95","-999999990","-999999991","-999999999","-9999999999999999999999999999999999999999999999999999999999999999999999999999",
        "100000000","10000000000000000000000000000000000000000000000000000000000000000000000000000","199999999","2147483647","57896044618658097711785492504343953926634992332820282019728792003956564819967",
        "99999999.95","999999990","999999991","999999999","9999999999999999999999999999999999999999999999999999999999999999999999999999"]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_str_overflow_3_9_1_strs) {
            test {
                sql """select cast("${test_str}" as decimalv3(9, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_str_overflow_3_9_1_strs) {
        qt_sql_test_cast_to_decimal32_from_str_overflow_3_9_1 """select cast("${test_str}" as decimalv3(9, 1));"""
    }

    for (test_str in test_cast_to_decimal32_from_str_overflow_3_9_1_strs) {
        testFoldConst("""select cast("${test_str}" as decimalv3(9, 1));""")
    }
    def test_cast_to_decimal32_from_str_overflow_4_9_4_strs = ["-100000","-10000000000000000000000000000000000000000000000000000000000000000000000000000","-199999","-2147483648","-57896044618658097711785492504343953926634992332820282019728792003956564819968",
        "-99999.99995","-999990","-999991","-999999","-9999999999999999999999999999999999999999999999999999999999999999999999999999",
        "100000","10000000000000000000000000000000000000000000000000000000000000000000000000000","199999","2147483647","57896044618658097711785492504343953926634992332820282019728792003956564819967",
        "99999.99995","999990","999991","999999","9999999999999999999999999999999999999999999999999999999999999999999999999999"]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_str_overflow_4_9_4_strs) {
            test {
                sql """select cast("${test_str}" as decimalv3(9, 4));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_str_overflow_4_9_4_strs) {
        qt_sql_test_cast_to_decimal32_from_str_overflow_4_9_4 """select cast("${test_str}" as decimalv3(9, 4));"""
    }

    for (test_str in test_cast_to_decimal32_from_str_overflow_4_9_4_strs) {
        testFoldConst("""select cast("${test_str}" as decimalv3(9, 4));""")
    }
    def test_cast_to_decimal32_from_str_overflow_5_9_9_strs = ["-.9999999995","-01","-09","-1","-10",
        "-10000000000000000000000000000000000000000000000000000000000000000000000000000","-2147483648","-57896044618658097711785492504343953926634992332820282019728792003956564819968","-9999999999999999999999999999999999999999999999999999999999999999999999999999",".9999999995",
        "01","09","1","10","10000000000000000000000000000000000000000000000000000000000000000000000000000",
        "2147483647","57896044618658097711785492504343953926634992332820282019728792003956564819967","9999999999999999999999999999999999999999999999999999999999999999999999999999"]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_from_str_overflow_5_9_9_strs) {
            test {
                sql """select cast("${test_str}" as decimalv3(9, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_from_str_overflow_5_9_9_strs) {
        qt_sql_test_cast_to_decimal32_from_str_overflow_5_9_9 """select cast("${test_str}" as decimalv3(9, 9));"""
    }

    for (test_str in test_cast_to_decimal32_from_str_overflow_5_9_9_strs) {
        testFoldConst("""select cast("${test_str}" as decimalv3(9, 9));""")
    }
}