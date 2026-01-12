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


suite("test_cast_to_decimal32_9_9_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_32_0 = """select "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647", cast(cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as string) as decimalv3(9, 9));"""
    qt_sql_32_0_strict "${const_sql_32_0}"
    testFoldConst("${const_sql_32_0}")
    def const_sql_32_1 = """select "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647", cast(cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as string) as decimalv3(9, 9));"""
    qt_sql_32_1_strict "${const_sql_32_1}"
    testFoldConst("${const_sql_32_1}")
    def const_sql_32_2 = """select ".0000000004", cast(cast(".0000000004" as string) as decimalv3(9, 9));"""
    qt_sql_32_2_strict "${const_sql_32_2}"
    testFoldConst("${const_sql_32_2}")
    def const_sql_32_3 = """select ".0000000014", cast(cast(".0000000014" as string) as decimalv3(9, 9));"""
    qt_sql_32_3_strict "${const_sql_32_3}"
    testFoldConst("${const_sql_32_3}")
    def const_sql_32_4 = """select ".0000000094", cast(cast(".0000000094" as string) as decimalv3(9, 9));"""
    qt_sql_32_4_strict "${const_sql_32_4}"
    testFoldConst("${const_sql_32_4}")
    def const_sql_32_5 = """select ".0999999994", cast(cast(".0999999994" as string) as decimalv3(9, 9));"""
    qt_sql_32_5_strict "${const_sql_32_5}"
    testFoldConst("${const_sql_32_5}")
    def const_sql_32_6 = """select ".9000000004", cast(cast(".9000000004" as string) as decimalv3(9, 9));"""
    qt_sql_32_6_strict "${const_sql_32_6}"
    testFoldConst("${const_sql_32_6}")
    def const_sql_32_7 = """select ".9000000014", cast(cast(".9000000014" as string) as decimalv3(9, 9));"""
    qt_sql_32_7_strict "${const_sql_32_7}"
    testFoldConst("${const_sql_32_7}")
    def const_sql_32_8 = """select ".9999999984", cast(cast(".9999999984" as string) as decimalv3(9, 9));"""
    qt_sql_32_8_strict "${const_sql_32_8}"
    testFoldConst("${const_sql_32_8}")
    def const_sql_32_9 = """select ".9999999994", cast(cast(".9999999994" as string) as decimalv3(9, 9));"""
    qt_sql_32_9_strict "${const_sql_32_9}"
    testFoldConst("${const_sql_32_9}")
    def const_sql_32_10 = """select ".0000000005", cast(cast(".0000000005" as string) as decimalv3(9, 9));"""
    qt_sql_32_10_strict "${const_sql_32_10}"
    testFoldConst("${const_sql_32_10}")
    def const_sql_32_11 = """select ".0000000015", cast(cast(".0000000015" as string) as decimalv3(9, 9));"""
    qt_sql_32_11_strict "${const_sql_32_11}"
    testFoldConst("${const_sql_32_11}")
    def const_sql_32_12 = """select ".0000000095", cast(cast(".0000000095" as string) as decimalv3(9, 9));"""
    qt_sql_32_12_strict "${const_sql_32_12}"
    testFoldConst("${const_sql_32_12}")
    def const_sql_32_13 = """select ".0999999995", cast(cast(".0999999995" as string) as decimalv3(9, 9));"""
    qt_sql_32_13_strict "${const_sql_32_13}"
    testFoldConst("${const_sql_32_13}")
    def const_sql_32_14 = """select ".9000000005", cast(cast(".9000000005" as string) as decimalv3(9, 9));"""
    qt_sql_32_14_strict "${const_sql_32_14}"
    testFoldConst("${const_sql_32_14}")
    def const_sql_32_15 = """select ".9000000015", cast(cast(".9000000015" as string) as decimalv3(9, 9));"""
    qt_sql_32_15_strict "${const_sql_32_15}"
    testFoldConst("${const_sql_32_15}")
    def const_sql_32_16 = """select ".9999999985", cast(cast(".9999999985" as string) as decimalv3(9, 9));"""
    qt_sql_32_16_strict "${const_sql_32_16}"
    testFoldConst("${const_sql_32_16}")
    def const_sql_32_17 = """select ".9999999994", cast(cast(".9999999994" as string) as decimalv3(9, 9));"""
    qt_sql_32_17_strict "${const_sql_32_17}"
    testFoldConst("${const_sql_32_17}")
    def const_sql_32_18 = """select "-.0000000004", cast(cast("-.0000000004" as string) as decimalv3(9, 9));"""
    qt_sql_32_18_strict "${const_sql_32_18}"
    testFoldConst("${const_sql_32_18}")
    def const_sql_32_19 = """select "-.0000000014", cast(cast("-.0000000014" as string) as decimalv3(9, 9));"""
    qt_sql_32_19_strict "${const_sql_32_19}"
    testFoldConst("${const_sql_32_19}")
    def const_sql_32_20 = """select "-.0000000094", cast(cast("-.0000000094" as string) as decimalv3(9, 9));"""
    qt_sql_32_20_strict "${const_sql_32_20}"
    testFoldConst("${const_sql_32_20}")
    def const_sql_32_21 = """select "-.0999999994", cast(cast("-.0999999994" as string) as decimalv3(9, 9));"""
    qt_sql_32_21_strict "${const_sql_32_21}"
    testFoldConst("${const_sql_32_21}")
    def const_sql_32_22 = """select "-.9000000004", cast(cast("-.9000000004" as string) as decimalv3(9, 9));"""
    qt_sql_32_22_strict "${const_sql_32_22}"
    testFoldConst("${const_sql_32_22}")
    def const_sql_32_23 = """select "-.9000000014", cast(cast("-.9000000014" as string) as decimalv3(9, 9));"""
    qt_sql_32_23_strict "${const_sql_32_23}"
    testFoldConst("${const_sql_32_23}")
    def const_sql_32_24 = """select "-.9999999984", cast(cast("-.9999999984" as string) as decimalv3(9, 9));"""
    qt_sql_32_24_strict "${const_sql_32_24}"
    testFoldConst("${const_sql_32_24}")
    def const_sql_32_25 = """select "-.9999999994", cast(cast("-.9999999994" as string) as decimalv3(9, 9));"""
    qt_sql_32_25_strict "${const_sql_32_25}"
    testFoldConst("${const_sql_32_25}")
    def const_sql_32_26 = """select "-.0000000005", cast(cast("-.0000000005" as string) as decimalv3(9, 9));"""
    qt_sql_32_26_strict "${const_sql_32_26}"
    testFoldConst("${const_sql_32_26}")
    def const_sql_32_27 = """select "-.0000000015", cast(cast("-.0000000015" as string) as decimalv3(9, 9));"""
    qt_sql_32_27_strict "${const_sql_32_27}"
    testFoldConst("${const_sql_32_27}")
    def const_sql_32_28 = """select "-.0000000095", cast(cast("-.0000000095" as string) as decimalv3(9, 9));"""
    qt_sql_32_28_strict "${const_sql_32_28}"
    testFoldConst("${const_sql_32_28}")
    def const_sql_32_29 = """select "-.0999999995", cast(cast("-.0999999995" as string) as decimalv3(9, 9));"""
    qt_sql_32_29_strict "${const_sql_32_29}"
    testFoldConst("${const_sql_32_29}")
    def const_sql_32_30 = """select "-.9000000005", cast(cast("-.9000000005" as string) as decimalv3(9, 9));"""
    qt_sql_32_30_strict "${const_sql_32_30}"
    testFoldConst("${const_sql_32_30}")
    def const_sql_32_31 = """select "-.9000000015", cast(cast("-.9000000015" as string) as decimalv3(9, 9));"""
    qt_sql_32_31_strict "${const_sql_32_31}"
    testFoldConst("${const_sql_32_31}")
    def const_sql_32_32 = """select "-.9999999985", cast(cast("-.9999999985" as string) as decimalv3(9, 9));"""
    qt_sql_32_32_strict "${const_sql_32_32}"
    testFoldConst("${const_sql_32_32}")
    def const_sql_32_33 = """select "-.9999999994", cast(cast("-.9999999994" as string) as decimalv3(9, 9));"""
    qt_sql_32_33_strict "${const_sql_32_33}"
    testFoldConst("${const_sql_32_33}")

    sql "set enable_strict_cast=false;"
    qt_sql_32_0_non_strict "${const_sql_32_0}"
    testFoldConst("${const_sql_32_0}")
    qt_sql_32_1_non_strict "${const_sql_32_1}"
    testFoldConst("${const_sql_32_1}")
    qt_sql_32_2_non_strict "${const_sql_32_2}"
    testFoldConst("${const_sql_32_2}")
    qt_sql_32_3_non_strict "${const_sql_32_3}"
    testFoldConst("${const_sql_32_3}")
    qt_sql_32_4_non_strict "${const_sql_32_4}"
    testFoldConst("${const_sql_32_4}")
    qt_sql_32_5_non_strict "${const_sql_32_5}"
    testFoldConst("${const_sql_32_5}")
    qt_sql_32_6_non_strict "${const_sql_32_6}"
    testFoldConst("${const_sql_32_6}")
    qt_sql_32_7_non_strict "${const_sql_32_7}"
    testFoldConst("${const_sql_32_7}")
    qt_sql_32_8_non_strict "${const_sql_32_8}"
    testFoldConst("${const_sql_32_8}")
    qt_sql_32_9_non_strict "${const_sql_32_9}"
    testFoldConst("${const_sql_32_9}")
    qt_sql_32_10_non_strict "${const_sql_32_10}"
    testFoldConst("${const_sql_32_10}")
    qt_sql_32_11_non_strict "${const_sql_32_11}"
    testFoldConst("${const_sql_32_11}")
    qt_sql_32_12_non_strict "${const_sql_32_12}"
    testFoldConst("${const_sql_32_12}")
    qt_sql_32_13_non_strict "${const_sql_32_13}"
    testFoldConst("${const_sql_32_13}")
    qt_sql_32_14_non_strict "${const_sql_32_14}"
    testFoldConst("${const_sql_32_14}")
    qt_sql_32_15_non_strict "${const_sql_32_15}"
    testFoldConst("${const_sql_32_15}")
    qt_sql_32_16_non_strict "${const_sql_32_16}"
    testFoldConst("${const_sql_32_16}")
    qt_sql_32_17_non_strict "${const_sql_32_17}"
    testFoldConst("${const_sql_32_17}")
    qt_sql_32_18_non_strict "${const_sql_32_18}"
    testFoldConst("${const_sql_32_18}")
    qt_sql_32_19_non_strict "${const_sql_32_19}"
    testFoldConst("${const_sql_32_19}")
    qt_sql_32_20_non_strict "${const_sql_32_20}"
    testFoldConst("${const_sql_32_20}")
    qt_sql_32_21_non_strict "${const_sql_32_21}"
    testFoldConst("${const_sql_32_21}")
    qt_sql_32_22_non_strict "${const_sql_32_22}"
    testFoldConst("${const_sql_32_22}")
    qt_sql_32_23_non_strict "${const_sql_32_23}"
    testFoldConst("${const_sql_32_23}")
    qt_sql_32_24_non_strict "${const_sql_32_24}"
    testFoldConst("${const_sql_32_24}")
    qt_sql_32_25_non_strict "${const_sql_32_25}"
    testFoldConst("${const_sql_32_25}")
    qt_sql_32_26_non_strict "${const_sql_32_26}"
    testFoldConst("${const_sql_32_26}")
    qt_sql_32_27_non_strict "${const_sql_32_27}"
    testFoldConst("${const_sql_32_27}")
    qt_sql_32_28_non_strict "${const_sql_32_28}"
    testFoldConst("${const_sql_32_28}")
    qt_sql_32_29_non_strict "${const_sql_32_29}"
    testFoldConst("${const_sql_32_29}")
    qt_sql_32_30_non_strict "${const_sql_32_30}"
    testFoldConst("${const_sql_32_30}")
    qt_sql_32_31_non_strict "${const_sql_32_31}"
    testFoldConst("${const_sql_32_31}")
    qt_sql_32_32_non_strict "${const_sql_32_32}"
    testFoldConst("${const_sql_32_32}")
    qt_sql_32_33_non_strict "${const_sql_32_33}"
    testFoldConst("${const_sql_32_33}")
}