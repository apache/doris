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


suite("test_cast_to_decimal128i_38_0_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_strict_cast=true;"
    def const_sql_0 = """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(38, 0));"""
    qt_sql_0_strict "${const_sql_0}"
    testFoldConst("${const_sql_0}")
    def const_sql_1 = """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(38, 0));"""
    qt_sql_1_strict "${const_sql_1}"
    testFoldConst("${const_sql_1}")
    def const_sql_2 = """select cast("0" as decimalv3(38, 0));"""
    qt_sql_2_strict "${const_sql_2}"
    testFoldConst("${const_sql_2}")
    def const_sql_3 = """select cast("1" as decimalv3(38, 0));"""
    qt_sql_3_strict "${const_sql_3}"
    testFoldConst("${const_sql_3}")
    def const_sql_4 = """select cast("9" as decimalv3(38, 0));"""
    qt_sql_4_strict "${const_sql_4}"
    testFoldConst("${const_sql_4}")
    def const_sql_5 = """select cast("9999999999999999999999999999999999999" as decimalv3(38, 0));"""
    qt_sql_5_strict "${const_sql_5}"
    testFoldConst("${const_sql_5}")
    def const_sql_6 = """select cast("90000000000000000000000000000000000000" as decimalv3(38, 0));"""
    qt_sql_6_strict "${const_sql_6}"
    testFoldConst("${const_sql_6}")
    def const_sql_7 = """select cast("90000000000000000000000000000000000001" as decimalv3(38, 0));"""
    qt_sql_7_strict "${const_sql_7}"
    testFoldConst("${const_sql_7}")
    def const_sql_8 = """select cast("99999999999999999999999999999999999998" as decimalv3(38, 0));"""
    qt_sql_8_strict "${const_sql_8}"
    testFoldConst("${const_sql_8}")
    def const_sql_9 = """select cast("99999999999999999999999999999999999999" as decimalv3(38, 0));"""
    qt_sql_9_strict "${const_sql_9}"
    testFoldConst("${const_sql_9}")
    def const_sql_10 = """select cast("0." as decimalv3(38, 0));"""
    qt_sql_10_strict "${const_sql_10}"
    testFoldConst("${const_sql_10}")
    def const_sql_11 = """select cast("1." as decimalv3(38, 0));"""
    qt_sql_11_strict "${const_sql_11}"
    testFoldConst("${const_sql_11}")
    def const_sql_12 = """select cast("9." as decimalv3(38, 0));"""
    qt_sql_12_strict "${const_sql_12}"
    testFoldConst("${const_sql_12}")
    def const_sql_13 = """select cast("9999999999999999999999999999999999999." as decimalv3(38, 0));"""
    qt_sql_13_strict "${const_sql_13}"
    testFoldConst("${const_sql_13}")
    def const_sql_14 = """select cast("90000000000000000000000000000000000000." as decimalv3(38, 0));"""
    qt_sql_14_strict "${const_sql_14}"
    testFoldConst("${const_sql_14}")
    def const_sql_15 = """select cast("90000000000000000000000000000000000001." as decimalv3(38, 0));"""
    qt_sql_15_strict "${const_sql_15}"
    testFoldConst("${const_sql_15}")
    def const_sql_16 = """select cast("99999999999999999999999999999999999998." as decimalv3(38, 0));"""
    qt_sql_16_strict "${const_sql_16}"
    testFoldConst("${const_sql_16}")
    def const_sql_17 = """select cast("99999999999999999999999999999999999999." as decimalv3(38, 0));"""
    qt_sql_17_strict "${const_sql_17}"
    testFoldConst("${const_sql_17}")
    def const_sql_18 = """select cast("-0" as decimalv3(38, 0));"""
    qt_sql_18_strict "${const_sql_18}"
    testFoldConst("${const_sql_18}")
    def const_sql_19 = """select cast("-1" as decimalv3(38, 0));"""
    qt_sql_19_strict "${const_sql_19}"
    testFoldConst("${const_sql_19}")
    def const_sql_20 = """select cast("-9" as decimalv3(38, 0));"""
    qt_sql_20_strict "${const_sql_20}"
    testFoldConst("${const_sql_20}")
    def const_sql_21 = """select cast("-9999999999999999999999999999999999999" as decimalv3(38, 0));"""
    qt_sql_21_strict "${const_sql_21}"
    testFoldConst("${const_sql_21}")
    def const_sql_22 = """select cast("-90000000000000000000000000000000000000" as decimalv3(38, 0));"""
    qt_sql_22_strict "${const_sql_22}"
    testFoldConst("${const_sql_22}")
    def const_sql_23 = """select cast("-90000000000000000000000000000000000001" as decimalv3(38, 0));"""
    qt_sql_23_strict "${const_sql_23}"
    testFoldConst("${const_sql_23}")
    def const_sql_24 = """select cast("-99999999999999999999999999999999999998" as decimalv3(38, 0));"""
    qt_sql_24_strict "${const_sql_24}"
    testFoldConst("${const_sql_24}")
    def const_sql_25 = """select cast("-99999999999999999999999999999999999999" as decimalv3(38, 0));"""
    qt_sql_25_strict "${const_sql_25}"
    testFoldConst("${const_sql_25}")
    def const_sql_26 = """select cast("-0." as decimalv3(38, 0));"""
    qt_sql_26_strict "${const_sql_26}"
    testFoldConst("${const_sql_26}")
    def const_sql_27 = """select cast("-1." as decimalv3(38, 0));"""
    qt_sql_27_strict "${const_sql_27}"
    testFoldConst("${const_sql_27}")
    def const_sql_28 = """select cast("-9." as decimalv3(38, 0));"""
    qt_sql_28_strict "${const_sql_28}"
    testFoldConst("${const_sql_28}")
    def const_sql_29 = """select cast("-9999999999999999999999999999999999999." as decimalv3(38, 0));"""
    qt_sql_29_strict "${const_sql_29}"
    testFoldConst("${const_sql_29}")
    def const_sql_30 = """select cast("-90000000000000000000000000000000000000." as decimalv3(38, 0));"""
    qt_sql_30_strict "${const_sql_30}"
    testFoldConst("${const_sql_30}")
    def const_sql_31 = """select cast("-90000000000000000000000000000000000001." as decimalv3(38, 0));"""
    qt_sql_31_strict "${const_sql_31}"
    testFoldConst("${const_sql_31}")
    def const_sql_32 = """select cast("-99999999999999999999999999999999999998." as decimalv3(38, 0));"""
    qt_sql_32_strict "${const_sql_32}"
    testFoldConst("${const_sql_32}")
    def const_sql_33 = """select cast("-99999999999999999999999999999999999999." as decimalv3(38, 0));"""
    qt_sql_33_strict "${const_sql_33}"
    testFoldConst("${const_sql_33}")
    def const_sql_34 = """select cast("0.49999" as decimalv3(38, 0));"""
    qt_sql_34_strict "${const_sql_34}"
    testFoldConst("${const_sql_34}")
    def const_sql_35 = """select cast("1.49999" as decimalv3(38, 0));"""
    qt_sql_35_strict "${const_sql_35}"
    testFoldConst("${const_sql_35}")
    def const_sql_36 = """select cast("9.49999" as decimalv3(38, 0));"""
    qt_sql_36_strict "${const_sql_36}"
    testFoldConst("${const_sql_36}")
    def const_sql_37 = """select cast("9999999999999999999999999999999999999.49999" as decimalv3(38, 0));"""
    qt_sql_37_strict "${const_sql_37}"
    testFoldConst("${const_sql_37}")
    def const_sql_38 = """select cast("90000000000000000000000000000000000000.49999" as decimalv3(38, 0));"""
    qt_sql_38_strict "${const_sql_38}"
    testFoldConst("${const_sql_38}")
    def const_sql_39 = """select cast("90000000000000000000000000000000000001.49999" as decimalv3(38, 0));"""
    qt_sql_39_strict "${const_sql_39}"
    testFoldConst("${const_sql_39}")
    def const_sql_40 = """select cast("99999999999999999999999999999999999998.49999" as decimalv3(38, 0));"""
    qt_sql_40_strict "${const_sql_40}"
    testFoldConst("${const_sql_40}")
    def const_sql_41 = """select cast("99999999999999999999999999999999999999.49999" as decimalv3(38, 0));"""
    qt_sql_41_strict "${const_sql_41}"
    testFoldConst("${const_sql_41}")
    def const_sql_42 = """select cast("0.5" as decimalv3(38, 0));"""
    qt_sql_42_strict "${const_sql_42}"
    testFoldConst("${const_sql_42}")
    def const_sql_43 = """select cast("1.5" as decimalv3(38, 0));"""
    qt_sql_43_strict "${const_sql_43}"
    testFoldConst("${const_sql_43}")
    def const_sql_44 = """select cast("9.5" as decimalv3(38, 0));"""
    qt_sql_44_strict "${const_sql_44}"
    testFoldConst("${const_sql_44}")
    def const_sql_45 = """select cast("9999999999999999999999999999999999999.5" as decimalv3(38, 0));"""
    qt_sql_45_strict "${const_sql_45}"
    testFoldConst("${const_sql_45}")
    def const_sql_46 = """select cast("90000000000000000000000000000000000000.5" as decimalv3(38, 0));"""
    qt_sql_46_strict "${const_sql_46}"
    testFoldConst("${const_sql_46}")
    def const_sql_47 = """select cast("90000000000000000000000000000000000001.5" as decimalv3(38, 0));"""
    qt_sql_47_strict "${const_sql_47}"
    testFoldConst("${const_sql_47}")
    def const_sql_48 = """select cast("99999999999999999999999999999999999998.5" as decimalv3(38, 0));"""
    qt_sql_48_strict "${const_sql_48}"
    testFoldConst("${const_sql_48}")
    def const_sql_49 = """select cast("99999999999999999999999999999999999999.49999" as decimalv3(38, 0));"""
    qt_sql_49_strict "${const_sql_49}"
    testFoldConst("${const_sql_49}")
    def const_sql_50 = """select cast("-0.49999" as decimalv3(38, 0));"""
    qt_sql_50_strict "${const_sql_50}"
    testFoldConst("${const_sql_50}")
    def const_sql_51 = """select cast("-1.49999" as decimalv3(38, 0));"""
    qt_sql_51_strict "${const_sql_51}"
    testFoldConst("${const_sql_51}")
    def const_sql_52 = """select cast("-9.49999" as decimalv3(38, 0));"""
    qt_sql_52_strict "${const_sql_52}"
    testFoldConst("${const_sql_52}")
    def const_sql_53 = """select cast("-9999999999999999999999999999999999999.49999" as decimalv3(38, 0));"""
    qt_sql_53_strict "${const_sql_53}"
    testFoldConst("${const_sql_53}")
    def const_sql_54 = """select cast("-90000000000000000000000000000000000000.49999" as decimalv3(38, 0));"""
    qt_sql_54_strict "${const_sql_54}"
    testFoldConst("${const_sql_54}")
    def const_sql_55 = """select cast("-90000000000000000000000000000000000001.49999" as decimalv3(38, 0));"""
    qt_sql_55_strict "${const_sql_55}"
    testFoldConst("${const_sql_55}")
    def const_sql_56 = """select cast("-99999999999999999999999999999999999998.49999" as decimalv3(38, 0));"""
    qt_sql_56_strict "${const_sql_56}"
    testFoldConst("${const_sql_56}")
    def const_sql_57 = """select cast("-99999999999999999999999999999999999999.49999" as decimalv3(38, 0));"""
    qt_sql_57_strict "${const_sql_57}"
    testFoldConst("${const_sql_57}")
    def const_sql_58 = """select cast("-0.5" as decimalv3(38, 0));"""
    qt_sql_58_strict "${const_sql_58}"
    testFoldConst("${const_sql_58}")
    def const_sql_59 = """select cast("-1.5" as decimalv3(38, 0));"""
    qt_sql_59_strict "${const_sql_59}"
    testFoldConst("${const_sql_59}")
    def const_sql_60 = """select cast("-9.5" as decimalv3(38, 0));"""
    qt_sql_60_strict "${const_sql_60}"
    testFoldConst("${const_sql_60}")
    def const_sql_61 = """select cast("-9999999999999999999999999999999999999.5" as decimalv3(38, 0));"""
    qt_sql_61_strict "${const_sql_61}"
    testFoldConst("${const_sql_61}")
    def const_sql_62 = """select cast("-90000000000000000000000000000000000000.5" as decimalv3(38, 0));"""
    qt_sql_62_strict "${const_sql_62}"
    testFoldConst("${const_sql_62}")
    def const_sql_63 = """select cast("-90000000000000000000000000000000000001.5" as decimalv3(38, 0));"""
    qt_sql_63_strict "${const_sql_63}"
    testFoldConst("${const_sql_63}")
    def const_sql_64 = """select cast("-99999999999999999999999999999999999998.5" as decimalv3(38, 0));"""
    qt_sql_64_strict "${const_sql_64}"
    testFoldConst("${const_sql_64}")
    def const_sql_65 = """select cast("-99999999999999999999999999999999999999.49999" as decimalv3(38, 0));"""
    qt_sql_65_strict "${const_sql_65}"
    testFoldConst("${const_sql_65}")
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
    qt_sql_34_non_strict "${const_sql_34}"
    testFoldConst("${const_sql_34}")
    qt_sql_35_non_strict "${const_sql_35}"
    testFoldConst("${const_sql_35}")
    qt_sql_36_non_strict "${const_sql_36}"
    testFoldConst("${const_sql_36}")
    qt_sql_37_non_strict "${const_sql_37}"
    testFoldConst("${const_sql_37}")
    qt_sql_38_non_strict "${const_sql_38}"
    testFoldConst("${const_sql_38}")
    qt_sql_39_non_strict "${const_sql_39}"
    testFoldConst("${const_sql_39}")
    qt_sql_40_non_strict "${const_sql_40}"
    testFoldConst("${const_sql_40}")
    qt_sql_41_non_strict "${const_sql_41}"
    testFoldConst("${const_sql_41}")
    qt_sql_42_non_strict "${const_sql_42}"
    testFoldConst("${const_sql_42}")
    qt_sql_43_non_strict "${const_sql_43}"
    testFoldConst("${const_sql_43}")
    qt_sql_44_non_strict "${const_sql_44}"
    testFoldConst("${const_sql_44}")
    qt_sql_45_non_strict "${const_sql_45}"
    testFoldConst("${const_sql_45}")
    qt_sql_46_non_strict "${const_sql_46}"
    testFoldConst("${const_sql_46}")
    qt_sql_47_non_strict "${const_sql_47}"
    testFoldConst("${const_sql_47}")
    qt_sql_48_non_strict "${const_sql_48}"
    testFoldConst("${const_sql_48}")
    qt_sql_49_non_strict "${const_sql_49}"
    testFoldConst("${const_sql_49}")
    qt_sql_50_non_strict "${const_sql_50}"
    testFoldConst("${const_sql_50}")
    qt_sql_51_non_strict "${const_sql_51}"
    testFoldConst("${const_sql_51}")
    qt_sql_52_non_strict "${const_sql_52}"
    testFoldConst("${const_sql_52}")
    qt_sql_53_non_strict "${const_sql_53}"
    testFoldConst("${const_sql_53}")
    qt_sql_54_non_strict "${const_sql_54}"
    testFoldConst("${const_sql_54}")
    qt_sql_55_non_strict "${const_sql_55}"
    testFoldConst("${const_sql_55}")
    qt_sql_56_non_strict "${const_sql_56}"
    testFoldConst("${const_sql_56}")
    qt_sql_57_non_strict "${const_sql_57}"
    testFoldConst("${const_sql_57}")
    qt_sql_58_non_strict "${const_sql_58}"
    testFoldConst("${const_sql_58}")
    qt_sql_59_non_strict "${const_sql_59}"
    testFoldConst("${const_sql_59}")
    qt_sql_60_non_strict "${const_sql_60}"
    testFoldConst("${const_sql_60}")
    qt_sql_61_non_strict "${const_sql_61}"
    testFoldConst("${const_sql_61}")
    qt_sql_62_non_strict "${const_sql_62}"
    testFoldConst("${const_sql_62}")
    qt_sql_63_non_strict "${const_sql_63}"
    testFoldConst("${const_sql_63}")
    qt_sql_64_non_strict "${const_sql_64}"
    testFoldConst("${const_sql_64}")
    qt_sql_65_non_strict "${const_sql_65}"
    testFoldConst("${const_sql_65}")
}