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


suite("test_cast_to_decimal32_from_float_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_strict_cast=true;"
    def const_sql_0 = """select cast(cast("1.4e-08" as float) as decimalv3(9, 8));"""
    qt_sql_0_strict "${const_sql_0}"
    testFoldConst("${const_sql_0}")
    def const_sql_1 = """select cast(cast("1.5e-08" as float) as decimalv3(9, 8));"""
    qt_sql_1_strict "${const_sql_1}"
    testFoldConst("${const_sql_1}")
    def const_sql_2 = """select cast(cast("-1.4e-08" as float) as decimalv3(9, 8));"""
    qt_sql_2_strict "${const_sql_2}"
    testFoldConst("${const_sql_2}")
    def const_sql_3 = """select cast(cast("-1.5e-08" as float) as decimalv3(9, 8));"""
    qt_sql_3_strict "${const_sql_3}"
    testFoldConst("${const_sql_3}")
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict "${const_sql_0}"
    testFoldConst("${const_sql_0}")
    qt_sql_1_non_strict "${const_sql_1}"
    testFoldConst("${const_sql_1}")
    qt_sql_2_non_strict "${const_sql_2}"
    testFoldConst("${const_sql_2}")
    qt_sql_3_non_strict "${const_sql_3}"
    testFoldConst("${const_sql_3}")
}