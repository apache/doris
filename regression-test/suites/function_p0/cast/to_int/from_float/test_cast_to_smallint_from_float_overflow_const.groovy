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


suite("test_cast_to_smallint_from_float_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_smallint_from_float_overflow_0_test_data = ["""3.4028235e+38""","""-3.4028235e+38""","""inf""","""nan""","""32768""","""-32769""","""4294967300""","""-4294967300""","""1.8446744e+19""","""-1.8446744e+19""","""inf""","""-inf"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_smallint_from_float_overflow_0_test_data) {
            test {
                sql """select cast(cast("${test_str}" as float) as smallint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_smallint_from_float_overflow_0_test_data) {
        qt_sql_test_cast_to_smallint_from_float_overflow_0 """select cast(cast("${test_str}" as float) as smallint);"""
        testFoldConst("""select cast(cast("${test_str}" as float) as smallint);""")
    }
}