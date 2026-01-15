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


suite("test_cast_to_smallint_from_decimal32_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimal32_overflow_2_test_data = ["""32768""","""-32769""","""999999999""","""-999999999""","""999999998""","""-999999998"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimal32_overflow_2_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 0)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimal32_overflow_2_test_data) {
        qt_sql_test_cast_to_smallint_from_decimal32_overflow_2 """select cast(cast("${test_str}" as decimalv3(9, 0)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 0)) as smallint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimal32_overflow_3_test_data = ["""32768.0""","""32768.1""","""32768.9""","""32768.9""","""32768.8""","""32768.0""","""32768.9""","""32768.8""","""-32769.0""","""-32769.1""","""-32769.9""","""-32769.9""","""-32769.8""","""-32769.0""","""-32769.9""","""-32769.8""","""99999999.0""","""99999999.1""","""99999999.9""","""99999999.9""",
        """99999999.8""","""99999999.0""","""99999999.9""","""99999999.8""","""-99999999.0""","""-99999999.1""","""-99999999.9""","""-99999999.9""","""-99999999.8""","""-99999999.0""","""-99999999.9""","""-99999999.8""","""99999998.0""","""99999998.1""","""99999998.9""","""99999998.9""","""99999998.8""","""99999998.0""","""99999998.9""","""99999998.8""",
        """-99999998.0""","""-99999998.1""","""-99999998.9""","""-99999998.9""","""-99999998.8""","""-99999998.0""","""-99999998.9""","""-99999998.8"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimal32_overflow_3_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 1)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimal32_overflow_3_test_data) {
        qt_sql_test_cast_to_smallint_from_decimal32_overflow_3 """select cast(cast("${test_str}" as decimalv3(9, 1)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 1)) as smallint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimal32_overflow_4_test_data = ["""32768.0000""","""32768.0001""","""32768.0009""","""32768.9999""","""32768.9998""","""32768.0999""","""32768.9000""","""32768.9001""","""-32769.0000""","""-32769.0001""","""-32769.0009""","""-32769.9999""","""-32769.9998""","""-32769.0999""","""-32769.9000""","""-32769.9001""","""99999.0000""","""99999.0001""","""99999.0009""","""99999.9999""",
        """99999.9998""","""99999.0999""","""99999.9000""","""99999.9001""","""-99999.0000""","""-99999.0001""","""-99999.0009""","""-99999.9999""","""-99999.9998""","""-99999.0999""","""-99999.9000""","""-99999.9001""","""99998.0000""","""99998.0001""","""99998.0009""","""99998.9999""","""99998.9998""","""99998.0999""","""99998.9000""","""99998.9001""",
        """-99998.0000""","""-99998.0001""","""-99998.0009""","""-99998.9999""","""-99998.9998""","""-99998.0999""","""-99998.9000""","""-99998.9001"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimal32_overflow_4_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(9, 4)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimal32_overflow_4_test_data) {
        qt_sql_test_cast_to_smallint_from_decimal32_overflow_4 """select cast(cast("${test_str}" as decimalv3(9, 4)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(9, 4)) as smallint);""")
    }
}