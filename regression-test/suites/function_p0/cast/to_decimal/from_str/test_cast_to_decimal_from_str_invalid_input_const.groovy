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


suite("test_cast_to_decimal_from_str_invalid_input_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    def test_cast_to_decimal_from_str_invalid_input_strs = ["""""",""".""",""" ""","""	""","""abc""","""1 23""","""1	23""","""1.2.3""","""a123.456""",""" a123.456""","""	a123.456""","""123.456a""","""123.456a	""","""123.456	a""","""123.456
a""","""12a3.456""","""123a.456""","""123.a456""","""123.4a56""","""+-123.456""",
        """+- 123.456""","""-+123.456""","""++123.456""","""--123.456""","""+-.456""","""-+.456""","""++.456""","""--.456""","""0x123""","""0x123.456""","""e""","""-e""","""+e""","""e+""","""e-""","""e1""","""e+1""","""e-1""",""".e""","""+.e""",
        """-.e""",""".e+""",""".e-""",""".e+""","""1e""","""1e+""","""1e-""","""1e1a""","""1ea1""","""1e1.1""","""1e+1.1""","""1e-1.1""","""1e2e3"""]
    sql "set enable_strict_cast=true;"
    for (test_str in test_cast_to_decimal_from_str_invalid_input_strs) {
        test {
            sql """select cast("${test_str}" as decimalv3(9, 3));"""
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    for (test_str in test_cast_to_decimal_from_str_invalid_input_strs) {
        qt_sql_test_cast_to_decimal_from_str_invalid_input """select cast("${test_str}" as decimalv3(9, 3));"""
    }
}