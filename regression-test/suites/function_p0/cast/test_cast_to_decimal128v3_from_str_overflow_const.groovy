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


suite("test_cast_to_decimal128v3_from_str_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    def test_cast_to_decimal128v3_from_str_overflow_0_38_0_strs = ["-100000000000000000000000000000000000000","-10000000000000000000000000000000000000000000000000000000000000000000000000000","-170141183460469231731687303715884105728","-199999999999999999999999999999999999999","-57896044618658097711785492504343953926634992332820282019728792003956564819968",
        "-99999999999999999999999999999999999999.5","-999999999999999999999999999999999999990","-999999999999999999999999999999999999991","-999999999999999999999999999999999999999","-9999999999999999999999999999999999999999999999999999999999999999999999999999",
        "100000000000000000000000000000000000000","10000000000000000000000000000000000000000000000000000000000000000000000000000","170141183460469231731687303715884105727","199999999999999999999999999999999999999","57896044618658097711785492504343953926634992332820282019728792003956564819967",
        "99999999999999999999999999999999999999.5","999999999999999999999999999999999999990","999999999999999999999999999999999999991","999999999999999999999999999999999999999","9999999999999999999999999999999999999999999999999999999999999999999999999999"]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_from_str_overflow_0_38_0_strs) {
            test {
                sql """select cast("${test_str}" as decimalv3(38, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_from_str_overflow_0_38_0_strs) {
        qt_sql_test_cast_to_decimal128v3_from_str_overflow_0_38_0 """select cast("${test_str}" as decimalv3(38, 0));"""
    }

    for (test_str in test_cast_to_decimal128v3_from_str_overflow_0_38_0_strs) {
        testFoldConst("""select cast("${test_str}" as decimalv3(38, 0));""")
    }
    def test_cast_to_decimal128v3_from_str_overflow_1_38_1_strs = ["-10000000000000000000000000000000000000","-10000000000000000000000000000000000000000000000000000000000000000000000000000","-170141183460469231731687303715884105728","-19999999999999999999999999999999999999","-57896044618658097711785492504343953926634992332820282019728792003956564819968",
        "-9999999999999999999999999999999999999.95","-99999999999999999999999999999999999990","-99999999999999999999999999999999999991","-99999999999999999999999999999999999999","-9999999999999999999999999999999999999999999999999999999999999999999999999999",
        "10000000000000000000000000000000000000","10000000000000000000000000000000000000000000000000000000000000000000000000000","170141183460469231731687303715884105727","19999999999999999999999999999999999999","57896044618658097711785492504343953926634992332820282019728792003956564819967",
        "9999999999999999999999999999999999999.95","99999999999999999999999999999999999990","99999999999999999999999999999999999991","99999999999999999999999999999999999999","9999999999999999999999999999999999999999999999999999999999999999999999999999"]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_from_str_overflow_1_38_1_strs) {
            test {
                sql """select cast("${test_str}" as decimalv3(38, 1));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_from_str_overflow_1_38_1_strs) {
        qt_sql_test_cast_to_decimal128v3_from_str_overflow_1_38_1 """select cast("${test_str}" as decimalv3(38, 1));"""
    }

    for (test_str in test_cast_to_decimal128v3_from_str_overflow_1_38_1_strs) {
        testFoldConst("""select cast("${test_str}" as decimalv3(38, 1));""")
    }
    def test_cast_to_decimal128v3_from_str_overflow_2_38_19_strs = ["-10000000000000000000","-10000000000000000000000000000000000000000000000000000000000000000000000000000","-170141183460469231731687303715884105728","-19999999999999999999","-57896044618658097711785492504343953926634992332820282019728792003956564819968",
        "-9999999999999999999.99999999999999999995","-99999999999999999990","-99999999999999999991","-99999999999999999999","-9999999999999999999999999999999999999999999999999999999999999999999999999999",
        "10000000000000000000","10000000000000000000000000000000000000000000000000000000000000000000000000000","170141183460469231731687303715884105727","19999999999999999999","57896044618658097711785492504343953926634992332820282019728792003956564819967",
        "9999999999999999999.99999999999999999995","99999999999999999990","99999999999999999991","99999999999999999999","9999999999999999999999999999999999999999999999999999999999999999999999999999"]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_from_str_overflow_2_38_19_strs) {
            test {
                sql """select cast("${test_str}" as decimalv3(38, 19));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_from_str_overflow_2_38_19_strs) {
        qt_sql_test_cast_to_decimal128v3_from_str_overflow_2_38_19 """select cast("${test_str}" as decimalv3(38, 19));"""
    }

    for (test_str in test_cast_to_decimal128v3_from_str_overflow_2_38_19_strs) {
        testFoldConst("""select cast("${test_str}" as decimalv3(38, 19));""")
    }
    def test_cast_to_decimal128v3_from_str_overflow_3_38_38_strs = ["-.999999999999999999999999999999999999995","-01","-09","-1","-10",
        "-10000000000000000000000000000000000000000000000000000000000000000000000000000","-170141183460469231731687303715884105728","-57896044618658097711785492504343953926634992332820282019728792003956564819968","-9999999999999999999999999999999999999999999999999999999999999999999999999999",".999999999999999999999999999999999999995",
        "01","09","1","10","10000000000000000000000000000000000000000000000000000000000000000000000000000",
        "170141183460469231731687303715884105727","57896044618658097711785492504343953926634992332820282019728792003956564819967","9999999999999999999999999999999999999999999999999999999999999999999999999999"]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal128v3_from_str_overflow_3_38_38_strs) {
            test {
                sql """select cast("${test_str}" as decimalv3(38, 38));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal128v3_from_str_overflow_3_38_38_strs) {
        qt_sql_test_cast_to_decimal128v3_from_str_overflow_3_38_38 """select cast("${test_str}" as decimalv3(38, 38));"""
    }

    for (test_str in test_cast_to_decimal128v3_from_str_overflow_3_38_38_strs) {
        testFoldConst("""select cast("${test_str}" as decimalv3(38, 38));""")
    }
}