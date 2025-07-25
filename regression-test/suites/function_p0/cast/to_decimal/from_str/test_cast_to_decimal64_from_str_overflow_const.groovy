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


suite("test_cast_to_decimal64_from_str_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal64_from_str_overflow_0_test_data = ["""-1000000000000000000""","""-10000000000000000000000000000000000000000000000000000000000000000000000000000""","""-1999999999999999999""","""-57896044618658097711785492504343953926634992332820282019728792003956564819968""","""-9223372036854775808""","""-999999999999999999.5""","""-9999999999999999990""","""-9999999999999999991""","""-9999999999999999999""","""-9999999999999999999999999999999999999999999999999999999999999999999999999999""","""1000000000000000000""","""10000000000000000000000000000000000000000000000000000000000000000000000000000""","""1999999999999999999""","""57896044618658097711785492504343953926634992332820282019728792003956564819967""","""9223372036854775807""","""999999999999999999.5""","""9999999999999999990""","""9999999999999999991""","""9999999999999999999""","""9999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_from_str_overflow_0_test_data) {
            test {
                sql """select cast(cast("${test_str}" as string) as decimalv3(18, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_from_str_overflow_0_test_data) {
        qt_sql_test_cast_to_decimal64_from_str_overflow_0 """select cast(cast("${test_str}" as string) as decimalv3(18, 0));"""
        testFoldConst("""select cast(cast("${test_str}" as string) as decimalv3(18, 0));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal64_from_str_overflow_1_test_data = ["""-100000000000000000""","""-10000000000000000000000000000000000000000000000000000000000000000000000000000""","""-199999999999999999""","""-57896044618658097711785492504343953926634992332820282019728792003956564819968""","""-9223372036854775808""","""-99999999999999999.95""","""-999999999999999990""","""-999999999999999991""","""-999999999999999999""","""-9999999999999999999999999999999999999999999999999999999999999999999999999999""","""100000000000000000""","""10000000000000000000000000000000000000000000000000000000000000000000000000000""","""199999999999999999""","""57896044618658097711785492504343953926634992332820282019728792003956564819967""","""9223372036854775807""","""99999999999999999.95""","""999999999999999990""","""999999999999999991""","""999999999999999999""","""9999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_from_str_overflow_1_test_data) {
            test {
                sql """select cast(cast("${test_str}" as string) as decimalv3(18, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_from_str_overflow_1_test_data) {
        qt_sql_test_cast_to_decimal64_from_str_overflow_1 """select cast(cast("${test_str}" as string) as decimalv3(18, 1));"""
        testFoldConst("""select cast(cast("${test_str}" as string) as decimalv3(18, 1));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal64_from_str_overflow_2_test_data = ["""-1000000000""","""-10000000000000000000000000000000000000000000000000000000000000000000000000000""","""-1999999999""","""-57896044618658097711785492504343953926634992332820282019728792003956564819968""","""-9223372036854775808""","""-999999999.9999999995""","""-9999999990""","""-9999999991""","""-9999999999""","""-9999999999999999999999999999999999999999999999999999999999999999999999999999""","""1000000000""","""10000000000000000000000000000000000000000000000000000000000000000000000000000""","""1999999999""","""57896044618658097711785492504343953926634992332820282019728792003956564819967""","""9223372036854775807""","""999999999.9999999995""","""9999999990""","""9999999991""","""9999999999""","""9999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_from_str_overflow_2_test_data) {
            test {
                sql """select cast(cast("${test_str}" as string) as decimalv3(18, 9));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_from_str_overflow_2_test_data) {
        qt_sql_test_cast_to_decimal64_from_str_overflow_2 """select cast(cast("${test_str}" as string) as decimalv3(18, 9));"""
        testFoldConst("""select cast(cast("${test_str}" as string) as decimalv3(18, 9));""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal64_from_str_overflow_3_test_data = ["""-.9999999999999999995""","""-01""","""-09""","""-1""","""-10""","""-10000000000000000000000000000000000000000000000000000000000000000000000000000""","""-57896044618658097711785492504343953926634992332820282019728792003956564819968""","""-9223372036854775808""","""-9999999999999999999999999999999999999999999999999999999999999999999999999999""",""".9999999999999999995""","""01""","""09""","""1""","""10""","""10000000000000000000000000000000000000000000000000000000000000000000000000000""","""57896044618658097711785492504343953926634992332820282019728792003956564819967""","""9223372036854775807""","""9999999999999999999999999999999999999999999999999999999999999999999999999999"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal64_from_str_overflow_3_test_data) {
            test {
                sql """select cast(cast("${test_str}" as string) as decimalv3(18, 18));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal64_from_str_overflow_3_test_data) {
        qt_sql_test_cast_to_decimal64_from_str_overflow_3 """select cast(cast("${test_str}" as string) as decimalv3(18, 18));"""
        testFoldConst("""select cast(cast("${test_str}" as string) as decimalv3(18, 18));""")
    }
}