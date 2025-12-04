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


suite("test_cast_to_smallint_from_decimalv2_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimalv2_overflow_2_test_data = ["""32768.000000000""","""32768.000000001""","""32768.000000009""","""32768.999999999""","""32768.999999998""","""32768.099999999""","""32768.900000000""","""32768.900000001""","""-32769.000000000""","""-32769.000000001""","""-32769.000000009""","""-32769.999999999""","""-32769.999999998""","""-32769.099999999""","""-32769.900000000""","""-32769.900000001""","""999999999999999999.000000000""","""999999999999999999.000000001""","""999999999999999999.000000009""","""999999999999999999.999999999""",
        """999999999999999999.999999998""","""999999999999999999.099999999""","""999999999999999999.900000000""","""999999999999999999.900000001""","""-999999999999999999.000000000""","""-999999999999999999.000000001""","""-999999999999999999.000000009""","""-999999999999999999.999999999""","""-999999999999999999.999999998""","""-999999999999999999.099999999""","""-999999999999999999.900000000""","""-999999999999999999.900000001""","""999999999999999998.000000000""","""999999999999999998.000000001""","""999999999999999998.000000009""","""999999999999999998.999999999""","""999999999999999998.999999998""","""999999999999999998.099999999""","""999999999999999998.900000000""","""999999999999999998.900000001""",
        """-999999999999999998.000000000""","""-999999999999999998.000000001""","""-999999999999999998.000000009""","""-999999999999999998.999999999""","""-999999999999999998.999999998""","""-999999999999999998.099999999""","""-999999999999999998.900000000""","""-999999999999999998.900000001"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimalv2_overflow_2_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv2(27, 9)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimalv2_overflow_2_test_data) {
        qt_sql_test_cast_to_smallint_from_decimalv2_overflow_2 """select cast(cast("${test_str}" as decimalv2(27, 9)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv2(27, 9)) as smallint);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_decimalv2_overflow_3_test_data = ["""32768.000000""","""32768.000000""","""32768.000000""","""32768.001000""","""32768.001000""","""32768.000100""","""32768.000900""","""32768.000900""","""-32769.000000""","""-32769.000000""","""-32769.000000""","""-32769.001000""","""-32769.001000""","""-32769.000100""","""-32769.000900""","""-32769.000900""","""9999999999999.000000""","""9999999999999.000000""","""9999999999999.000000""","""9999999999999.001000""",
        """9999999999999.001000""","""9999999999999.000100""","""9999999999999.000900""","""9999999999999.000900""","""-9999999999999.000000""","""-9999999999999.000000""","""-9999999999999.000000""","""-9999999999999.001000""","""-9999999999999.001000""","""-9999999999999.000100""","""-9999999999999.000900""","""-9999999999999.000900""","""9999999999998.000000""","""9999999999998.000000""","""9999999999998.000000""","""9999999999998.001000""","""9999999999998.001000""","""9999999999998.000100""","""9999999999998.000900""","""9999999999998.000900""",
        """-9999999999998.000000""","""-9999999999998.000000""","""-9999999999998.000000""","""-9999999999998.001000""","""-9999999999998.001000""","""-9999999999998.000100""","""-9999999999998.000900""","""-9999999999998.000900"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_decimalv2_overflow_3_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv2(19, 6)) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_decimalv2_overflow_3_test_data) {
        qt_sql_test_cast_to_smallint_from_decimalv2_overflow_3 """select cast(cast("${test_str}" as decimalv2(19, 6)) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv2(19, 6)) as smallint);""")
    }
}