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


suite("test_cast_to_int_from_decimalv2_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_int_from_decimalv2_overflow_2_test_data = ["""2147483648.000000000""","""2147483648.000000001""","""2147483648.000000009""","""2147483648.999999999""","""2147483648.999999998""","""2147483648.099999999""","""2147483648.900000000""","""2147483648.900000001""","""-2147483649.000000000""","""-2147483649.000000001""","""-2147483649.000000009""","""-2147483649.999999999""","""-2147483649.999999998""","""-2147483649.099999999""","""-2147483649.900000000""","""-2147483649.900000001""","""999999999999999999.000000000""","""999999999999999999.000000001""","""999999999999999999.000000009""","""999999999999999999.999999999""",
        """999999999999999999.999999998""","""999999999999999999.099999999""","""999999999999999999.900000000""","""999999999999999999.900000001""","""-999999999999999999.000000000""","""-999999999999999999.000000001""","""-999999999999999999.000000009""","""-999999999999999999.999999999""","""-999999999999999999.999999998""","""-999999999999999999.099999999""","""-999999999999999999.900000000""","""-999999999999999999.900000001""","""999999999999999998.000000000""","""999999999999999998.000000001""","""999999999999999998.000000009""","""999999999999999998.999999999""","""999999999999999998.999999998""","""999999999999999998.099999999""","""999999999999999998.900000000""","""999999999999999998.900000001""",
        """-999999999999999998.000000000""","""-999999999999999998.000000001""","""-999999999999999998.000000009""","""-999999999999999998.999999999""","""-999999999999999998.999999998""","""-999999999999999998.099999999""","""-999999999999999998.900000000""","""-999999999999999998.900000001"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_int_from_decimalv2_overflow_2_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv2(27, 9)) as int);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_int_from_decimalv2_overflow_2_test_data) {
        qt_sql_test_cast_to_int_from_decimalv2_overflow_2 """select cast(cast("${test_str}" as decimalv2(27, 9)) as int);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv2(27, 9)) as int);""")
    }
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_int_from_decimalv2_overflow_3_test_data = ["""2147483648.000000""","""2147483648.000000""","""2147483648.000000""","""2147483648.001000""","""2147483648.001000""","""2147483648.000100""","""2147483648.000900""","""2147483648.000900""","""-2147483649.000000""","""-2147483649.000000""","""-2147483649.000000""","""-2147483649.001000""","""-2147483649.001000""","""-2147483649.000100""","""-2147483649.000900""","""-2147483649.000900""","""9999999999999.000000""","""9999999999999.000000""","""9999999999999.000000""","""9999999999999.001000""",
        """9999999999999.001000""","""9999999999999.000100""","""9999999999999.000900""","""9999999999999.000900""","""-9999999999999.000000""","""-9999999999999.000000""","""-9999999999999.000000""","""-9999999999999.001000""","""-9999999999999.001000""","""-9999999999999.000100""","""-9999999999999.000900""","""-9999999999999.000900""","""9999999999998.000000""","""9999999999998.000000""","""9999999999998.000000""","""9999999999998.001000""","""9999999999998.001000""","""9999999999998.000100""","""9999999999998.000900""","""9999999999998.000900""",
        """-9999999999998.000000""","""-9999999999998.000000""","""-9999999999998.000000""","""-9999999999998.001000""","""-9999999999998.001000""","""-9999999999998.000100""","""-9999999999998.000900""","""-9999999999998.000900"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_int_from_decimalv2_overflow_3_test_data) {
            test {
                sql """select cast(cast("${test_str}" as decimalv2(19, 6)) as int);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_int_from_decimalv2_overflow_3_test_data) {
        qt_sql_test_cast_to_int_from_decimalv2_overflow_3 """select cast(cast("${test_str}" as decimalv2(19, 6)) as int);"""
        testFoldConst("""select cast(cast("${test_str}" as decimalv2(19, 6)) as int);""")
    }
}