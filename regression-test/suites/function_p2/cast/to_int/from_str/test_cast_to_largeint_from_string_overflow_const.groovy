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


suite("test_cast_to_largeint_from_string_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    def test_cast_to_largeint_from_string_overflow_0_test_data = ["""170141183460469231731687303715884105728""","""-170141183460469231731687303715884105729""","""170141183460469231731687303715884105729""","""-170141183460469231731687303715884105730""","""170141183460469231731687303715884105730""","""-170141183460469231731687303715884105731""","""170141183460469231731687303715884105731""","""-170141183460469231731687303715884105732""","""170141183460469231731687303715884105732""","""-170141183460469231731687303715884105733""","""170141183460469231731687303715884105733""","""-170141183460469231731687303715884105734""","""170141183460469231731687303715884105734""","""-170141183460469231731687303715884105735""","""170141183460469231731687303715884105735""","""-170141183460469231731687303715884105736""","""170141183460469231731687303715884105736""","""-170141183460469231731687303715884105737""","""170141183460469231731687303715884105737""","""-170141183460469231731687303715884105738""",
        """340282366920938463463374607431768211455""","""-340282366920938463463374607431768211455""","""340282366920938463463374607431768211454""","""-340282366920938463463374607431768211454""","""340282366920938463463374607431768211453""","""-340282366920938463463374607431768211453""","""340282366920938463463374607431768211452""","""-340282366920938463463374607431768211452""","""340282366920938463463374607431768211451""","""-340282366920938463463374607431768211451""","""340282366920938463463374607431768211450""","""-340282366920938463463374607431768211450""","""340282366920938463463374607431768211449""","""-340282366920938463463374607431768211449""","""340282366920938463463374607431768211448""","""-340282366920938463463374607431768211448""","""340282366920938463463374607431768211447""","""-340282366920938463463374607431768211447""","""340282366920938463463374607431768211446""","""-340282366920938463463374607431768211446""",
        """57896044618658097711785492504343953926634992332820282019728792003956564819967""","""-57896044618658097711785492504343953926634992332820282019728792003956564819968"""]
    sql "set enable_strict_cast=true;"

    for (b in ["false", "true"]) {
        sql """set debug_skip_fold_constant = "${b}";"""
        for (test_str in test_cast_to_largeint_from_string_overflow_0_test_data) {
            test {
                sql """select cast("${{test_str}}" as largeint);"""
                exception ""
            }
        }
    }
    sql "set enable_strict_cast=false;"

    for (test_str in test_cast_to_largeint_from_string_overflow_0_test_data) {
        qt_sql_test_cast_to_largeint_from_string_overflow_0 """select cast("${{test_str}}" as largeint);"""
        testFoldConst("""select cast("${{test_str}}" as largeint);""")
    }
}