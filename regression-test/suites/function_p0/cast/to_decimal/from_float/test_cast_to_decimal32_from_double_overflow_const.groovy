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


suite("test_cast_to_decimal32_from_double_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_decimal32_9_0_from_float64_overflow_vals = [("999999999.95"),("-999999999.95"),("1000000000.95"),("-1000000000.95"),("inf"),
        ("-inf"),("nan"),("-nan")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_0_from_float64_overflow_vals) {
            test {
                sql """select cast(cast("${test_str}" as double) as decimalv3(9, 0));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_0_from_float64_overflow_vals) {
        qt_sql_test_cast_to_decimal32_9_0_from_float64_overflow """select cast(cast("${test_str}" as double) as decimalv3(9, 0));"""
    }

    for (test_str in test_cast_to_decimal32_9_0_from_float64_overflow_vals) {
        testFoldConst("""select cast(cast("${test_str}" as double) as decimalv3(9, 0));""")
    }
    def test_cast_to_decimal32_9_3_from_float64_overflow_vals = [("999999.9995"),("-999999.9995"),("1000000.9995"),("-1000000.9995"),("inf"),
        ("-inf"),("nan"),("-nan")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_3_from_float64_overflow_vals) {
            test {
                sql """select cast(cast("${test_str}" as double) as decimalv3(9, 3));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_3_from_float64_overflow_vals) {
        qt_sql_test_cast_to_decimal32_9_3_from_float64_overflow """select cast(cast("${test_str}" as double) as decimalv3(9, 3));"""
    }

    for (test_str in test_cast_to_decimal32_9_3_from_float64_overflow_vals) {
        testFoldConst("""select cast(cast("${test_str}" as double) as decimalv3(9, 3));""")
    }
    def test_cast_to_decimal32_9_8_from_float64_overflow_vals = [("9.999999995"),("-9.999999995"),("10.999999995"),("-10.999999995"),("inf"),
        ("-inf"),("nan"),("-nan")]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_decimal32_9_8_from_float64_overflow_vals) {
            test {
                sql """select cast(cast("${test_str}" as double) as decimalv3(9, 8));"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_decimal32_9_8_from_float64_overflow_vals) {
        qt_sql_test_cast_to_decimal32_9_8_from_float64_overflow """select cast(cast("${test_str}" as double) as decimalv3(9, 8));"""
    }

    for (test_str in test_cast_to_decimal32_9_8_from_float64_overflow_vals) {
        testFoldConst("""select cast(cast("${test_str}" as double) as decimalv3(9, 8));""")
    }
}