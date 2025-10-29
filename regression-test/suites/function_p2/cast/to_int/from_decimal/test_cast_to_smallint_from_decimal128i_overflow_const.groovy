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


suite("test_cast_to_smallint_from_decimal128i_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimal128i_overflow_0_test_data = ["""32768""","""-32769""","""9999999999999999999""","""-9999999999999999999""","""9999999999999999998""","""-9999999999999999998"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimal128i_overflow_0_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(19, 0)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimal128i_overflow_0_test_data) {
        qt_sql_test_cast_to_smallint_from_decimal128i_overflow_0 """select cast(cast("${test_str}" as decimalv3(19, 0)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 0)) as smallint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimal128i_overflow_1_test_data = ["""32768.000000000""","""32768.000000001""","""32768.000000009""","""32768.999999999""","""32768.999999998""","""32768.099999999""","""32768.900000000""","""32768.900000001""","""-32769.000000000""","""-32769.000000001""","""-32769.000000009""","""-32769.999999999""","""-32769.999999998""","""-32769.099999999""","""-32769.900000000""","""-32769.900000001""","""9999999999.000000000""","""9999999999.000000001""","""9999999999.000000009""","""9999999999.999999999""",
        """9999999999.999999998""","""9999999999.099999999""","""9999999999.900000000""","""9999999999.900000001""","""-9999999999.000000000""","""-9999999999.000000001""","""-9999999999.000000009""","""-9999999999.999999999""","""-9999999999.999999998""","""-9999999999.099999999""","""-9999999999.900000000""","""-9999999999.900000001""","""9999999998.000000000""","""9999999998.000000001""","""9999999998.000000009""","""9999999998.999999999""","""9999999998.999999998""","""9999999998.099999999""","""9999999998.900000000""","""9999999998.900000001""",
        """-9999999998.000000000""","""-9999999998.000000001""","""-9999999998.000000009""","""-9999999998.999999999""","""-9999999998.999999998""","""-9999999998.099999999""","""-9999999998.900000000""","""-9999999998.900000001"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimal128i_overflow_1_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(19, 9)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimal128i_overflow_1_test_data) {
        qt_sql_test_cast_to_smallint_from_decimal128i_overflow_1 """select cast(cast("${test_str}" as decimalv3(19, 9)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(19, 9)) as smallint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimal128i_overflow_4_test_data = ["""32768""","""-32769""","""99999999999999999999999999999999999999""","""-99999999999999999999999999999999999999""","""99999999999999999999999999999999999998""","""-99999999999999999999999999999999999998"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimal128i_overflow_4_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(38, 0)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimal128i_overflow_4_test_data) {
        qt_sql_test_cast_to_smallint_from_decimal128i_overflow_4 """select cast(cast("${test_str}" as decimalv3(38, 0)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 0)) as smallint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimal128i_overflow_5_test_data = ["""32768.0""","""32768.1""","""32768.9""","""32768.9""","""32768.8""","""32768.0""","""32768.9""","""32768.8""","""-32769.0""","""-32769.1""","""-32769.9""","""-32769.9""","""-32769.8""","""-32769.0""","""-32769.9""","""-32769.8""","""9999999999999999999999999999999999999.0""","""9999999999999999999999999999999999999.1""","""9999999999999999999999999999999999999.9""","""9999999999999999999999999999999999999.9""",
        """9999999999999999999999999999999999999.8""","""9999999999999999999999999999999999999.0""","""9999999999999999999999999999999999999.9""","""9999999999999999999999999999999999999.8""","""-9999999999999999999999999999999999999.0""","""-9999999999999999999999999999999999999.1""","""-9999999999999999999999999999999999999.9""","""-9999999999999999999999999999999999999.9""","""-9999999999999999999999999999999999999.8""","""-9999999999999999999999999999999999999.0""","""-9999999999999999999999999999999999999.9""","""-9999999999999999999999999999999999999.8""","""9999999999999999999999999999999999998.0""","""9999999999999999999999999999999999998.1""","""9999999999999999999999999999999999998.9""","""9999999999999999999999999999999999998.9""","""9999999999999999999999999999999999998.8""","""9999999999999999999999999999999999998.0""","""9999999999999999999999999999999999998.9""","""9999999999999999999999999999999999998.8""",
        """-9999999999999999999999999999999999998.0""","""-9999999999999999999999999999999999998.1""","""-9999999999999999999999999999999999998.9""","""-9999999999999999999999999999999999998.9""","""-9999999999999999999999999999999999998.8""","""-9999999999999999999999999999999999998.0""","""-9999999999999999999999999999999999998.9""","""-9999999999999999999999999999999999998.8"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimal128i_overflow_5_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(38, 1)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimal128i_overflow_5_test_data) {
        qt_sql_test_cast_to_smallint_from_decimal128i_overflow_5 """select cast(cast("${test_str}" as decimalv3(38, 1)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 1)) as smallint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimal128i_overflow_6_test_data = ["""32768.0000000000000000000""","""32768.0000000000000000001""","""32768.0000000000000000009""","""32768.9999999999999999999""","""32768.9999999999999999998""","""32768.0999999999999999999""","""32768.9000000000000000000""","""32768.9000000000000000001""","""-32769.0000000000000000000""","""-32769.0000000000000000001""","""-32769.0000000000000000009""","""-32769.9999999999999999999""","""-32769.9999999999999999998""","""-32769.0999999999999999999""","""-32769.9000000000000000000""","""-32769.9000000000000000001""","""9999999999999999999.0000000000000000000""","""9999999999999999999.0000000000000000001""","""9999999999999999999.0000000000000000009""","""9999999999999999999.9999999999999999999""",
        """9999999999999999999.9999999999999999998""","""9999999999999999999.0999999999999999999""","""9999999999999999999.9000000000000000000""","""9999999999999999999.9000000000000000001""","""-9999999999999999999.0000000000000000000""","""-9999999999999999999.0000000000000000001""","""-9999999999999999999.0000000000000000009""","""-9999999999999999999.9999999999999999999""","""-9999999999999999999.9999999999999999998""","""-9999999999999999999.0999999999999999999""","""-9999999999999999999.9000000000000000000""","""-9999999999999999999.9000000000000000001""","""9999999999999999998.0000000000000000000""","""9999999999999999998.0000000000000000001""","""9999999999999999998.0000000000000000009""","""9999999999999999998.9999999999999999999""","""9999999999999999998.9999999999999999998""","""9999999999999999998.0999999999999999999""","""9999999999999999998.9000000000000000000""","""9999999999999999998.9000000000000000001""",
        """-9999999999999999998.0000000000000000000""","""-9999999999999999998.0000000000000000001""","""-9999999999999999998.0000000000000000009""","""-9999999999999999998.9999999999999999999""","""-9999999999999999998.9999999999999999998""","""-9999999999999999998.0999999999999999999""","""-9999999999999999998.9000000000000000000""","""-9999999999999999998.9000000000000000001"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimal128i_overflow_6_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv3(38, 19)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimal128i_overflow_6_test_data) {
        qt_sql_test_cast_to_smallint_from_decimal128i_overflow_6 """select cast(cast("${test_str}" as decimalv3(38, 19)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv3(38, 19)) as smallint);""")
    }
}