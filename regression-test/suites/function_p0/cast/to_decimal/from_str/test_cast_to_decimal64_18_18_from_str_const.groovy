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


suite("test_cast_to_decimal64_18_18_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_23_0 = """select "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647", cast(cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as string) as decimalv3(18, 18));"""
    qt_sql_23_0_strict "${const_sql_23_0}"
    testFoldConst("${const_sql_23_0}")
    def const_sql_23_1 = """select "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647", cast(cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as string) as decimalv3(18, 18));"""
    qt_sql_23_1_strict "${const_sql_23_1}"
    testFoldConst("${const_sql_23_1}")
    def const_sql_23_2 = """select ".0000000000000000004", cast(cast(".0000000000000000004" as string) as decimalv3(18, 18));"""
    qt_sql_23_2_strict "${const_sql_23_2}"
    testFoldConst("${const_sql_23_2}")
    def const_sql_23_3 = """select ".0000000000000000014", cast(cast(".0000000000000000014" as string) as decimalv3(18, 18));"""
    qt_sql_23_3_strict "${const_sql_23_3}"
    testFoldConst("${const_sql_23_3}")
    def const_sql_23_4 = """select ".0000000000000000094", cast(cast(".0000000000000000094" as string) as decimalv3(18, 18));"""
    qt_sql_23_4_strict "${const_sql_23_4}"
    testFoldConst("${const_sql_23_4}")
    def const_sql_23_5 = """select ".0999999999999999994", cast(cast(".0999999999999999994" as string) as decimalv3(18, 18));"""
    qt_sql_23_5_strict "${const_sql_23_5}"
    testFoldConst("${const_sql_23_5}")
    def const_sql_23_6 = """select ".9000000000000000004", cast(cast(".9000000000000000004" as string) as decimalv3(18, 18));"""
    qt_sql_23_6_strict "${const_sql_23_6}"
    testFoldConst("${const_sql_23_6}")
    def const_sql_23_7 = """select ".9000000000000000014", cast(cast(".9000000000000000014" as string) as decimalv3(18, 18));"""
    qt_sql_23_7_strict "${const_sql_23_7}"
    testFoldConst("${const_sql_23_7}")
    def const_sql_23_8 = """select ".9999999999999999984", cast(cast(".9999999999999999984" as string) as decimalv3(18, 18));"""
    qt_sql_23_8_strict "${const_sql_23_8}"
    testFoldConst("${const_sql_23_8}")
    def const_sql_23_9 = """select ".9999999999999999994", cast(cast(".9999999999999999994" as string) as decimalv3(18, 18));"""
    qt_sql_23_9_strict "${const_sql_23_9}"
    testFoldConst("${const_sql_23_9}")
    def const_sql_23_10 = """select ".0000000000000000005", cast(cast(".0000000000000000005" as string) as decimalv3(18, 18));"""
    qt_sql_23_10_strict "${const_sql_23_10}"
    testFoldConst("${const_sql_23_10}")
    def const_sql_23_11 = """select ".0000000000000000015", cast(cast(".0000000000000000015" as string) as decimalv3(18, 18));"""
    qt_sql_23_11_strict "${const_sql_23_11}"
    testFoldConst("${const_sql_23_11}")
    def const_sql_23_12 = """select ".0000000000000000095", cast(cast(".0000000000000000095" as string) as decimalv3(18, 18));"""
    qt_sql_23_12_strict "${const_sql_23_12}"
    testFoldConst("${const_sql_23_12}")
    def const_sql_23_13 = """select ".0999999999999999995", cast(cast(".0999999999999999995" as string) as decimalv3(18, 18));"""
    qt_sql_23_13_strict "${const_sql_23_13}"
    testFoldConst("${const_sql_23_13}")
    def const_sql_23_14 = """select ".9000000000000000005", cast(cast(".9000000000000000005" as string) as decimalv3(18, 18));"""
    qt_sql_23_14_strict "${const_sql_23_14}"
    testFoldConst("${const_sql_23_14}")
    def const_sql_23_15 = """select ".9000000000000000015", cast(cast(".9000000000000000015" as string) as decimalv3(18, 18));"""
    qt_sql_23_15_strict "${const_sql_23_15}"
    testFoldConst("${const_sql_23_15}")
    def const_sql_23_16 = """select ".9999999999999999985", cast(cast(".9999999999999999985" as string) as decimalv3(18, 18));"""
    qt_sql_23_16_strict "${const_sql_23_16}"
    testFoldConst("${const_sql_23_16}")
    def const_sql_23_17 = """select ".9999999999999999994", cast(cast(".9999999999999999994" as string) as decimalv3(18, 18));"""
    qt_sql_23_17_strict "${const_sql_23_17}"
    testFoldConst("${const_sql_23_17}")
    def const_sql_23_18 = """select "-.0000000000000000004", cast(cast("-.0000000000000000004" as string) as decimalv3(18, 18));"""
    qt_sql_23_18_strict "${const_sql_23_18}"
    testFoldConst("${const_sql_23_18}")
    def const_sql_23_19 = """select "-.0000000000000000014", cast(cast("-.0000000000000000014" as string) as decimalv3(18, 18));"""
    qt_sql_23_19_strict "${const_sql_23_19}"
    testFoldConst("${const_sql_23_19}")
    def const_sql_23_20 = """select "-.0000000000000000094", cast(cast("-.0000000000000000094" as string) as decimalv3(18, 18));"""
    qt_sql_23_20_strict "${const_sql_23_20}"
    testFoldConst("${const_sql_23_20}")
    def const_sql_23_21 = """select "-.0999999999999999994", cast(cast("-.0999999999999999994" as string) as decimalv3(18, 18));"""
    qt_sql_23_21_strict "${const_sql_23_21}"
    testFoldConst("${const_sql_23_21}")
    def const_sql_23_22 = """select "-.9000000000000000004", cast(cast("-.9000000000000000004" as string) as decimalv3(18, 18));"""
    qt_sql_23_22_strict "${const_sql_23_22}"
    testFoldConst("${const_sql_23_22}")
    def const_sql_23_23 = """select "-.9000000000000000014", cast(cast("-.9000000000000000014" as string) as decimalv3(18, 18));"""
    qt_sql_23_23_strict "${const_sql_23_23}"
    testFoldConst("${const_sql_23_23}")
    def const_sql_23_24 = """select "-.9999999999999999984", cast(cast("-.9999999999999999984" as string) as decimalv3(18, 18));"""
    qt_sql_23_24_strict "${const_sql_23_24}"
    testFoldConst("${const_sql_23_24}")
    def const_sql_23_25 = """select "-.9999999999999999994", cast(cast("-.9999999999999999994" as string) as decimalv3(18, 18));"""
    qt_sql_23_25_strict "${const_sql_23_25}"
    testFoldConst("${const_sql_23_25}")
    def const_sql_23_26 = """select "-.0000000000000000005", cast(cast("-.0000000000000000005" as string) as decimalv3(18, 18));"""
    qt_sql_23_26_strict "${const_sql_23_26}"
    testFoldConst("${const_sql_23_26}")
    def const_sql_23_27 = """select "-.0000000000000000015", cast(cast("-.0000000000000000015" as string) as decimalv3(18, 18));"""
    qt_sql_23_27_strict "${const_sql_23_27}"
    testFoldConst("${const_sql_23_27}")
    def const_sql_23_28 = """select "-.0000000000000000095", cast(cast("-.0000000000000000095" as string) as decimalv3(18, 18));"""
    qt_sql_23_28_strict "${const_sql_23_28}"
    testFoldConst("${const_sql_23_28}")
    def const_sql_23_29 = """select "-.0999999999999999995", cast(cast("-.0999999999999999995" as string) as decimalv3(18, 18));"""
    qt_sql_23_29_strict "${const_sql_23_29}"
    testFoldConst("${const_sql_23_29}")
    def const_sql_23_30 = """select "-.9000000000000000005", cast(cast("-.9000000000000000005" as string) as decimalv3(18, 18));"""
    qt_sql_23_30_strict "${const_sql_23_30}"
    testFoldConst("${const_sql_23_30}")
    def const_sql_23_31 = """select "-.9000000000000000015", cast(cast("-.9000000000000000015" as string) as decimalv3(18, 18));"""
    qt_sql_23_31_strict "${const_sql_23_31}"
    testFoldConst("${const_sql_23_31}")
    def const_sql_23_32 = """select "-.9999999999999999985", cast(cast("-.9999999999999999985" as string) as decimalv3(18, 18));"""
    qt_sql_23_32_strict "${const_sql_23_32}"
    testFoldConst("${const_sql_23_32}")
    def const_sql_23_33 = """select "-.9999999999999999994", cast(cast("-.9999999999999999994" as string) as decimalv3(18, 18));"""
    qt_sql_23_33_strict "${const_sql_23_33}"
    testFoldConst("${const_sql_23_33}")

    sql "set enable_strict_cast=false;"
    qt_sql_23_0_non_strict "${const_sql_23_0}"
    testFoldConst("${const_sql_23_0}")
    qt_sql_23_1_non_strict "${const_sql_23_1}"
    testFoldConst("${const_sql_23_1}")
    qt_sql_23_2_non_strict "${const_sql_23_2}"
    testFoldConst("${const_sql_23_2}")
    qt_sql_23_3_non_strict "${const_sql_23_3}"
    testFoldConst("${const_sql_23_3}")
    qt_sql_23_4_non_strict "${const_sql_23_4}"
    testFoldConst("${const_sql_23_4}")
    qt_sql_23_5_non_strict "${const_sql_23_5}"
    testFoldConst("${const_sql_23_5}")
    qt_sql_23_6_non_strict "${const_sql_23_6}"
    testFoldConst("${const_sql_23_6}")
    qt_sql_23_7_non_strict "${const_sql_23_7}"
    testFoldConst("${const_sql_23_7}")
    qt_sql_23_8_non_strict "${const_sql_23_8}"
    testFoldConst("${const_sql_23_8}")
    qt_sql_23_9_non_strict "${const_sql_23_9}"
    testFoldConst("${const_sql_23_9}")
    qt_sql_23_10_non_strict "${const_sql_23_10}"
    testFoldConst("${const_sql_23_10}")
    qt_sql_23_11_non_strict "${const_sql_23_11}"
    testFoldConst("${const_sql_23_11}")
    qt_sql_23_12_non_strict "${const_sql_23_12}"
    testFoldConst("${const_sql_23_12}")
    qt_sql_23_13_non_strict "${const_sql_23_13}"
    testFoldConst("${const_sql_23_13}")
    qt_sql_23_14_non_strict "${const_sql_23_14}"
    testFoldConst("${const_sql_23_14}")
    qt_sql_23_15_non_strict "${const_sql_23_15}"
    testFoldConst("${const_sql_23_15}")
    qt_sql_23_16_non_strict "${const_sql_23_16}"
    testFoldConst("${const_sql_23_16}")
    qt_sql_23_17_non_strict "${const_sql_23_17}"
    testFoldConst("${const_sql_23_17}")
    qt_sql_23_18_non_strict "${const_sql_23_18}"
    testFoldConst("${const_sql_23_18}")
    qt_sql_23_19_non_strict "${const_sql_23_19}"
    testFoldConst("${const_sql_23_19}")
    qt_sql_23_20_non_strict "${const_sql_23_20}"
    testFoldConst("${const_sql_23_20}")
    qt_sql_23_21_non_strict "${const_sql_23_21}"
    testFoldConst("${const_sql_23_21}")
    qt_sql_23_22_non_strict "${const_sql_23_22}"
    testFoldConst("${const_sql_23_22}")
    qt_sql_23_23_non_strict "${const_sql_23_23}"
    testFoldConst("${const_sql_23_23}")
    qt_sql_23_24_non_strict "${const_sql_23_24}"
    testFoldConst("${const_sql_23_24}")
    qt_sql_23_25_non_strict "${const_sql_23_25}"
    testFoldConst("${const_sql_23_25}")
    qt_sql_23_26_non_strict "${const_sql_23_26}"
    testFoldConst("${const_sql_23_26}")
    qt_sql_23_27_non_strict "${const_sql_23_27}"
    testFoldConst("${const_sql_23_27}")
    qt_sql_23_28_non_strict "${const_sql_23_28}"
    testFoldConst("${const_sql_23_28}")
    qt_sql_23_29_non_strict "${const_sql_23_29}"
    testFoldConst("${const_sql_23_29}")
    qt_sql_23_30_non_strict "${const_sql_23_30}"
    testFoldConst("${const_sql_23_30}")
    qt_sql_23_31_non_strict "${const_sql_23_31}"
    testFoldConst("${const_sql_23_31}")
    qt_sql_23_32_non_strict "${const_sql_23_32}"
    testFoldConst("${const_sql_23_32}")
    qt_sql_23_33_non_strict "${const_sql_23_33}"
    testFoldConst("${const_sql_23_33}")
}