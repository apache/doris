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


suite("test_cast_to_decimal32_1_1_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_1_0 = """select "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647", cast(cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as string) as decimalv3(1, 1));"""
    qt_sql_1_0_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    def const_sql_1_1 = """select "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647", cast(cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as string) as decimalv3(1, 1));"""
    qt_sql_1_1_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    def const_sql_1_2 = """select ".04", cast(cast(".04" as string) as decimalv3(1, 1));"""
    qt_sql_1_2_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    def const_sql_1_3 = """select ".14", cast(cast(".14" as string) as decimalv3(1, 1));"""
    qt_sql_1_3_strict "${const_sql_1_3}"
    testFoldConst("${const_sql_1_3}")
    def const_sql_1_4 = """select ".84", cast(cast(".84" as string) as decimalv3(1, 1));"""
    qt_sql_1_4_strict "${const_sql_1_4}"
    testFoldConst("${const_sql_1_4}")
    def const_sql_1_5 = """select ".94", cast(cast(".94" as string) as decimalv3(1, 1));"""
    qt_sql_1_5_strict "${const_sql_1_5}"
    testFoldConst("${const_sql_1_5}")
    def const_sql_1_6 = """select ".05", cast(cast(".05" as string) as decimalv3(1, 1));"""
    qt_sql_1_6_strict "${const_sql_1_6}"
    testFoldConst("${const_sql_1_6}")
    def const_sql_1_7 = """select ".15", cast(cast(".15" as string) as decimalv3(1, 1));"""
    qt_sql_1_7_strict "${const_sql_1_7}"
    testFoldConst("${const_sql_1_7}")
    def const_sql_1_8 = """select ".85", cast(cast(".85" as string) as decimalv3(1, 1));"""
    qt_sql_1_8_strict "${const_sql_1_8}"
    testFoldConst("${const_sql_1_8}")
    def const_sql_1_9 = """select ".94", cast(cast(".94" as string) as decimalv3(1, 1));"""
    qt_sql_1_9_strict "${const_sql_1_9}"
    testFoldConst("${const_sql_1_9}")
    def const_sql_1_10 = """select "-.04", cast(cast("-.04" as string) as decimalv3(1, 1));"""
    qt_sql_1_10_strict "${const_sql_1_10}"
    testFoldConst("${const_sql_1_10}")
    def const_sql_1_11 = """select "-.14", cast(cast("-.14" as string) as decimalv3(1, 1));"""
    qt_sql_1_11_strict "${const_sql_1_11}"
    testFoldConst("${const_sql_1_11}")
    def const_sql_1_12 = """select "-.84", cast(cast("-.84" as string) as decimalv3(1, 1));"""
    qt_sql_1_12_strict "${const_sql_1_12}"
    testFoldConst("${const_sql_1_12}")
    def const_sql_1_13 = """select "-.94", cast(cast("-.94" as string) as decimalv3(1, 1));"""
    qt_sql_1_13_strict "${const_sql_1_13}"
    testFoldConst("${const_sql_1_13}")
    def const_sql_1_14 = """select "-.05", cast(cast("-.05" as string) as decimalv3(1, 1));"""
    qt_sql_1_14_strict "${const_sql_1_14}"
    testFoldConst("${const_sql_1_14}")
    def const_sql_1_15 = """select "-.15", cast(cast("-.15" as string) as decimalv3(1, 1));"""
    qt_sql_1_15_strict "${const_sql_1_15}"
    testFoldConst("${const_sql_1_15}")
    def const_sql_1_16 = """select "-.85", cast(cast("-.85" as string) as decimalv3(1, 1));"""
    qt_sql_1_16_strict "${const_sql_1_16}"
    testFoldConst("${const_sql_1_16}")
    def const_sql_1_17 = """select "-.94", cast(cast("-.94" as string) as decimalv3(1, 1));"""
    qt_sql_1_17_strict "${const_sql_1_17}"
    testFoldConst("${const_sql_1_17}")

    sql "set enable_strict_cast=false;"
    qt_sql_1_0_non_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    qt_sql_1_1_non_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    qt_sql_1_2_non_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    qt_sql_1_3_non_strict "${const_sql_1_3}"
    testFoldConst("${const_sql_1_3}")
    qt_sql_1_4_non_strict "${const_sql_1_4}"
    testFoldConst("${const_sql_1_4}")
    qt_sql_1_5_non_strict "${const_sql_1_5}"
    testFoldConst("${const_sql_1_5}")
    qt_sql_1_6_non_strict "${const_sql_1_6}"
    testFoldConst("${const_sql_1_6}")
    qt_sql_1_7_non_strict "${const_sql_1_7}"
    testFoldConst("${const_sql_1_7}")
    qt_sql_1_8_non_strict "${const_sql_1_8}"
    testFoldConst("${const_sql_1_8}")
    qt_sql_1_9_non_strict "${const_sql_1_9}"
    testFoldConst("${const_sql_1_9}")
    qt_sql_1_10_non_strict "${const_sql_1_10}"
    testFoldConst("${const_sql_1_10}")
    qt_sql_1_11_non_strict "${const_sql_1_11}"
    testFoldConst("${const_sql_1_11}")
    qt_sql_1_12_non_strict "${const_sql_1_12}"
    testFoldConst("${const_sql_1_12}")
    qt_sql_1_13_non_strict "${const_sql_1_13}"
    testFoldConst("${const_sql_1_13}")
    qt_sql_1_14_non_strict "${const_sql_1_14}"
    testFoldConst("${const_sql_1_14}")
    qt_sql_1_15_non_strict "${const_sql_1_15}"
    testFoldConst("${const_sql_1_15}")
    qt_sql_1_16_non_strict "${const_sql_1_16}"
    testFoldConst("${const_sql_1_16}")
    qt_sql_1_17_non_strict "${const_sql_1_17}"
    testFoldConst("${const_sql_1_17}")
}