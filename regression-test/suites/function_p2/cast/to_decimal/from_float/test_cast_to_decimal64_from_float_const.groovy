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


suite("test_cast_to_decimal64_from_float_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_strict_cast=true;"
    def const_sql_0 = """select cast(cast("0.03" as float) as decimalv3(18, 0));"""
    qt_sql_0_strict "${const_sql_0}"
    testFoldConst("${const_sql_0}")
    def const_sql_1 = """select cast(cast("1.03" as float) as decimalv3(18, 0));"""
    qt_sql_1_strict "${const_sql_1}"
    testFoldConst("${const_sql_1}")
    def const_sql_2 = """select cast(cast("9.03" as float) as decimalv3(18, 0));"""
    qt_sql_2_strict "${const_sql_2}"
    testFoldConst("${const_sql_2}")
    def const_sql_3 = """select cast(cast("0.09" as float) as decimalv3(18, 0));"""
    qt_sql_3_strict "${const_sql_3}"
    testFoldConst("${const_sql_3}")
    def const_sql_4 = """select cast(cast("1.09" as float) as decimalv3(18, 0));"""
    qt_sql_4_strict "${const_sql_4}"
    testFoldConst("${const_sql_4}")
    def const_sql_5 = """select cast(cast("9.09" as float) as decimalv3(18, 0));"""
    qt_sql_5_strict "${const_sql_5}"
    testFoldConst("${const_sql_5}")
    def const_sql_6 = """select cast(cast("-0.03" as float) as decimalv3(18, 0));"""
    qt_sql_6_strict "${const_sql_6}"
    testFoldConst("${const_sql_6}")
    def const_sql_7 = """select cast(cast("-1.03" as float) as decimalv3(18, 0));"""
    qt_sql_7_strict "${const_sql_7}"
    testFoldConst("${const_sql_7}")
    def const_sql_8 = """select cast(cast("-9.03" as float) as decimalv3(18, 0));"""
    qt_sql_8_strict "${const_sql_8}"
    testFoldConst("${const_sql_8}")
    def const_sql_9 = """select cast(cast("-0.09" as float) as decimalv3(18, 0));"""
    qt_sql_9_strict "${const_sql_9}"
    testFoldConst("${const_sql_9}")
    def const_sql_10 = """select cast(cast("-1.09" as float) as decimalv3(18, 0));"""
    qt_sql_10_strict "${const_sql_10}"
    testFoldConst("${const_sql_10}")
    def const_sql_11 = """select cast(cast("-9.09" as float) as decimalv3(18, 0));"""
    qt_sql_11_strict "${const_sql_11}"
    testFoldConst("${const_sql_11}")
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict "${const_sql_0}"
    testFoldConst("${const_sql_0}")
    qt_sql_1_non_strict "${const_sql_1}"
    testFoldConst("${const_sql_1}")
    qt_sql_2_non_strict "${const_sql_2}"
    testFoldConst("${const_sql_2}")
    qt_sql_3_non_strict "${const_sql_3}"
    testFoldConst("${const_sql_3}")
    qt_sql_4_non_strict "${const_sql_4}"
    testFoldConst("${const_sql_4}")
    qt_sql_5_non_strict "${const_sql_5}"
    testFoldConst("${const_sql_5}")
    qt_sql_6_non_strict "${const_sql_6}"
    testFoldConst("${const_sql_6}")
    qt_sql_7_non_strict "${const_sql_7}"
    testFoldConst("${const_sql_7}")
    qt_sql_8_non_strict "${const_sql_8}"
    testFoldConst("${const_sql_8}")
    qt_sql_9_non_strict "${const_sql_9}"
    testFoldConst("${const_sql_9}")
    qt_sql_10_non_strict "${const_sql_10}"
    testFoldConst("${const_sql_10}")
    qt_sql_11_non_strict "${const_sql_11}"
    testFoldConst("${const_sql_11}")
    sql "set enable_strict_cast=true;"
    sql "set enable_strict_cast=false;"
    sql "set enable_strict_cast=true;"
    sql "set enable_strict_cast=false;"
}