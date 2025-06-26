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


suite("test_cast_to_decimal32_1_0_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_strict_cast=true;"
    def const_sql_0 = """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(1, 0));"""
    qt_sql_0_strict "${const_sql_0}"
    testFoldConst("${const_sql_0}")
    def const_sql_1 = """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(1, 0));"""
    qt_sql_1_strict "${const_sql_1}"
    testFoldConst("${const_sql_1}")
    def const_sql_2 = """select cast("0" as decimalv3(1, 0));"""
    qt_sql_2_strict "${const_sql_2}"
    testFoldConst("${const_sql_2}")
    def const_sql_3 = """select cast("1" as decimalv3(1, 0));"""
    qt_sql_3_strict "${const_sql_3}"
    testFoldConst("${const_sql_3}")
    def const_sql_4 = """select cast("8" as decimalv3(1, 0));"""
    qt_sql_4_strict "${const_sql_4}"
    testFoldConst("${const_sql_4}")
    def const_sql_5 = """select cast("9" as decimalv3(1, 0));"""
    qt_sql_5_strict "${const_sql_5}"
    testFoldConst("${const_sql_5}")
    def const_sql_6 = """select cast("0." as decimalv3(1, 0));"""
    qt_sql_6_strict "${const_sql_6}"
    testFoldConst("${const_sql_6}")
    def const_sql_7 = """select cast("1." as decimalv3(1, 0));"""
    qt_sql_7_strict "${const_sql_7}"
    testFoldConst("${const_sql_7}")
    def const_sql_8 = """select cast("8." as decimalv3(1, 0));"""
    qt_sql_8_strict "${const_sql_8}"
    testFoldConst("${const_sql_8}")
    def const_sql_9 = """select cast("9." as decimalv3(1, 0));"""
    qt_sql_9_strict "${const_sql_9}"
    testFoldConst("${const_sql_9}")
    def const_sql_10 = """select cast("-0" as decimalv3(1, 0));"""
    qt_sql_10_strict "${const_sql_10}"
    testFoldConst("${const_sql_10}")
    def const_sql_11 = """select cast("-1" as decimalv3(1, 0));"""
    qt_sql_11_strict "${const_sql_11}"
    testFoldConst("${const_sql_11}")
    def const_sql_12 = """select cast("-8" as decimalv3(1, 0));"""
    qt_sql_12_strict "${const_sql_12}"
    testFoldConst("${const_sql_12}")
    def const_sql_13 = """select cast("-9" as decimalv3(1, 0));"""
    qt_sql_13_strict "${const_sql_13}"
    testFoldConst("${const_sql_13}")
    def const_sql_14 = """select cast("-0." as decimalv3(1, 0));"""
    qt_sql_14_strict "${const_sql_14}"
    testFoldConst("${const_sql_14}")
    def const_sql_15 = """select cast("-1." as decimalv3(1, 0));"""
    qt_sql_15_strict "${const_sql_15}"
    testFoldConst("${const_sql_15}")
    def const_sql_16 = """select cast("-8." as decimalv3(1, 0));"""
    qt_sql_16_strict "${const_sql_16}"
    testFoldConst("${const_sql_16}")
    def const_sql_17 = """select cast("-9." as decimalv3(1, 0));"""
    qt_sql_17_strict "${const_sql_17}"
    testFoldConst("${const_sql_17}")
    def const_sql_18 = """select cast("0.49999" as decimalv3(1, 0));"""
    qt_sql_18_strict "${const_sql_18}"
    testFoldConst("${const_sql_18}")
    def const_sql_19 = """select cast("1.49999" as decimalv3(1, 0));"""
    qt_sql_19_strict "${const_sql_19}"
    testFoldConst("${const_sql_19}")
    def const_sql_20 = """select cast("8.49999" as decimalv3(1, 0));"""
    qt_sql_20_strict "${const_sql_20}"
    testFoldConst("${const_sql_20}")
    def const_sql_21 = """select cast("9.49999" as decimalv3(1, 0));"""
    qt_sql_21_strict "${const_sql_21}"
    testFoldConst("${const_sql_21}")
    def const_sql_22 = """select cast("0.5" as decimalv3(1, 0));"""
    qt_sql_22_strict "${const_sql_22}"
    testFoldConst("${const_sql_22}")
    def const_sql_23 = """select cast("1.5" as decimalv3(1, 0));"""
    qt_sql_23_strict "${const_sql_23}"
    testFoldConst("${const_sql_23}")
    def const_sql_24 = """select cast("8.5" as decimalv3(1, 0));"""
    qt_sql_24_strict "${const_sql_24}"
    testFoldConst("${const_sql_24}")
    def const_sql_25 = """select cast("9.49999" as decimalv3(1, 0));"""
    qt_sql_25_strict "${const_sql_25}"
    testFoldConst("${const_sql_25}")
    def const_sql_26 = """select cast("-0.49999" as decimalv3(1, 0));"""
    qt_sql_26_strict "${const_sql_26}"
    testFoldConst("${const_sql_26}")
    def const_sql_27 = """select cast("-1.49999" as decimalv3(1, 0));"""
    qt_sql_27_strict "${const_sql_27}"
    testFoldConst("${const_sql_27}")
    def const_sql_28 = """select cast("-8.49999" as decimalv3(1, 0));"""
    qt_sql_28_strict "${const_sql_28}"
    testFoldConst("${const_sql_28}")
    def const_sql_29 = """select cast("-9.49999" as decimalv3(1, 0));"""
    qt_sql_29_strict "${const_sql_29}"
    testFoldConst("${const_sql_29}")
    def const_sql_30 = """select cast("-0.5" as decimalv3(1, 0));"""
    qt_sql_30_strict "${const_sql_30}"
    testFoldConst("${const_sql_30}")
    def const_sql_31 = """select cast("-1.5" as decimalv3(1, 0));"""
    qt_sql_31_strict "${const_sql_31}"
    testFoldConst("${const_sql_31}")
    def const_sql_32 = """select cast("-8.5" as decimalv3(1, 0));"""
    qt_sql_32_strict "${const_sql_32}"
    testFoldConst("${const_sql_32}")
    def const_sql_33 = """select cast("-9.49999" as decimalv3(1, 0));"""
    qt_sql_33_strict "${const_sql_33}"
    testFoldConst("${const_sql_33}")
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
    qt_sql_12_non_strict "${const_sql_12}"
    testFoldConst("${const_sql_12}")
    qt_sql_13_non_strict "${const_sql_13}"
    testFoldConst("${const_sql_13}")
    qt_sql_14_non_strict "${const_sql_14}"
    testFoldConst("${const_sql_14}")
    qt_sql_15_non_strict "${const_sql_15}"
    testFoldConst("${const_sql_15}")
    qt_sql_16_non_strict "${const_sql_16}"
    testFoldConst("${const_sql_16}")
    qt_sql_17_non_strict "${const_sql_17}"
    testFoldConst("${const_sql_17}")
    qt_sql_18_non_strict "${const_sql_18}"
    testFoldConst("${const_sql_18}")
    qt_sql_19_non_strict "${const_sql_19}"
    testFoldConst("${const_sql_19}")
    qt_sql_20_non_strict "${const_sql_20}"
    testFoldConst("${const_sql_20}")
    qt_sql_21_non_strict "${const_sql_21}"
    testFoldConst("${const_sql_21}")
    qt_sql_22_non_strict "${const_sql_22}"
    testFoldConst("${const_sql_22}")
    qt_sql_23_non_strict "${const_sql_23}"
    testFoldConst("${const_sql_23}")
    qt_sql_24_non_strict "${const_sql_24}"
    testFoldConst("${const_sql_24}")
    qt_sql_25_non_strict "${const_sql_25}"
    testFoldConst("${const_sql_25}")
    qt_sql_26_non_strict "${const_sql_26}"
    testFoldConst("${const_sql_26}")
    qt_sql_27_non_strict "${const_sql_27}"
    testFoldConst("${const_sql_27}")
    qt_sql_28_non_strict "${const_sql_28}"
    testFoldConst("${const_sql_28}")
    qt_sql_29_non_strict "${const_sql_29}"
    testFoldConst("${const_sql_29}")
    qt_sql_30_non_strict "${const_sql_30}"
    testFoldConst("${const_sql_30}")
    qt_sql_31_non_strict "${const_sql_31}"
    testFoldConst("${const_sql_31}")
    qt_sql_32_non_strict "${const_sql_32}"
    testFoldConst("${const_sql_32}")
    qt_sql_33_non_strict "${const_sql_33}"
    testFoldConst("${const_sql_33}")
}