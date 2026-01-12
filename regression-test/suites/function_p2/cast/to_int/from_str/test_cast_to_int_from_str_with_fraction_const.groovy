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


suite("test_cast_to_int_from_str_with_fraction_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_int_from_str_with_fraction_0_strs = ["""+0000.4""","""+0000.5""","""+0001.4""","""+0001.5""","""+0009.4""","""+0009.5""","""+000123.4""","""+000123.5""","""+0002147483647.4""","""+0002147483647.5""","""-0001.4""","""-0001.5""","""-0009.4""","""-0009.5""","""-000123.4""","""-000123.5""","""-0002147483648.4""","""-0002147483648.5""","""+0002147483646.4""","""+0002147483646.5""",
        """-0002147483647.4""","""-0002147483647.5""","""+0.4""","""+0.5""","""+1.4""","""+1.5""","""+9.4""","""+9.5""","""+123.4""","""+123.5""","""+2147483647.4""","""+2147483647.5""","""-1.4""","""-1.5""","""-9.4""","""-9.5""","""-123.4""","""-123.5""","""-2147483648.4""","""-2147483648.5""",
        """+2147483646.4""","""+2147483646.5""","""-2147483647.4""","""-2147483647.5""","""0000.4""","""0000.5""","""0001.4""","""0001.5""","""0009.4""","""0009.5""","""000123.4""","""000123.5""","""0002147483647.4""","""0002147483647.5""","""-0001.4""","""-0001.5""","""-0009.4""","""-0009.5""","""-000123.4""","""-000123.5""",
        """-0002147483648.4""","""-0002147483648.5""","""0002147483646.4""","""0002147483646.5""","""-0002147483647.4""","""-0002147483647.5""","""0.4""","""0.5""","""1.4""","""1.5""","""9.4""","""9.5""","""123.4""","""123.5""","""2147483647.4""","""2147483647.5""","""-1.4""","""-1.5""","""-9.4""","""-9.5""",
        """-123.4""","""-123.5""","""-2147483648.4""","""-2147483648.5""","""2147483646.4""","""2147483646.5""","""-2147483647.4""","""-2147483647.5"""]

    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_int_from_str_with_fraction_0_strs) {
            test {
                sql """select cast("${test_str}" as int);"""
                exception ""
            }
        }
    }

    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_int_from_str_with_fraction_0_strs) {
        qt_sql_test_cast_to_int_from_str_with_fraction_0 """select cast("${test_str}" as int);"""
        testFoldConst("""select cast("${test_str}" as int);""")
    }
}