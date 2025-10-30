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


suite("test_cast_to_int_from_string_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_int_from_string_overflow_0_test_data = ["""2147483648""","""-2147483649""","""2147483649""","""-2147483650""","""2147483650""","""-2147483651""","""2147483651""","""-2147483652""","""2147483652""","""-2147483653""","""2147483653""","""-2147483654""","""2147483654""","""-2147483655""","""2147483655""","""-2147483656""","""2147483656""","""-2147483657""","""2147483657""","""-2147483658""",
        """4294967295""","""-4294967295""","""4294967294""","""-4294967294""","""4294967293""","""-4294967293""","""4294967292""","""-4294967292""","""4294967291""","""-4294967291""","""4294967290""","""-4294967290""","""4294967289""","""-4294967289""","""4294967288""","""-4294967288""","""4294967287""","""-4294967287""","""4294967286""","""-4294967286""",
        """9223372036854775807""","""-9223372036854775808""","""170141183460469231731687303715884105727""","""-170141183460469231731687303715884105728""","""57896044618658097711785492504343953926634992332820282019728792003956564819967""","""-57896044618658097711785492504343953926634992332820282019728792003956564819968"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_int_from_string_overflow_0_test_data) {
            test {
                sql """select cast("${{test_str}}" as int);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_int_from_string_overflow_0_test_data) {
        qt_sql_test_cast_to_int_from_string_overflow_0 """select cast("${{test_str}}" as int);"""
        testFoldConst("""select cast("${{test_str}}" as int);""")
    }
}