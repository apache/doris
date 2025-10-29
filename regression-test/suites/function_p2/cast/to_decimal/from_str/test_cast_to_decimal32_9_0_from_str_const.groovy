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


suite("test_cast_to_decimal32_9_0_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_2_0 = """select "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647", cast(cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as string) as decimalv3(9, 0));"""
    qt_sql_2_0_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    def const_sql_2_1 = """select "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647", cast(cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as string) as decimalv3(9, 0));"""
    qt_sql_2_1_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    def const_sql_2_2 = """select "0", cast(cast("0" as string) as decimalv3(9, 0));"""
    qt_sql_2_2_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    def const_sql_2_3 = """select "1", cast(cast("1" as string) as decimalv3(9, 0));"""
    qt_sql_2_3_strict "${const_sql_2_3}"
    testFoldConst("${const_sql_2_3}")
    def const_sql_2_4 = """select "9", cast(cast("9" as string) as decimalv3(9, 0));"""
    qt_sql_2_4_strict "${const_sql_2_4}"
    testFoldConst("${const_sql_2_4}")
    def const_sql_2_5 = """select "99999999", cast(cast("99999999" as string) as decimalv3(9, 0));"""
    qt_sql_2_5_strict "${const_sql_2_5}"
    testFoldConst("${const_sql_2_5}")
    def const_sql_2_6 = """select "900000000", cast(cast("900000000" as string) as decimalv3(9, 0));"""
    qt_sql_2_6_strict "${const_sql_2_6}"
    testFoldConst("${const_sql_2_6}")
    def const_sql_2_7 = """select "900000001", cast(cast("900000001" as string) as decimalv3(9, 0));"""
    qt_sql_2_7_strict "${const_sql_2_7}"
    testFoldConst("${const_sql_2_7}")
    def const_sql_2_8 = """select "999999998", cast(cast("999999998" as string) as decimalv3(9, 0));"""
    qt_sql_2_8_strict "${const_sql_2_8}"
    testFoldConst("${const_sql_2_8}")
    def const_sql_2_9 = """select "999999999", cast(cast("999999999" as string) as decimalv3(9, 0));"""
    qt_sql_2_9_strict "${const_sql_2_9}"
    testFoldConst("${const_sql_2_9}")
    def const_sql_2_10 = """select "0.", cast(cast("0." as string) as decimalv3(9, 0));"""
    qt_sql_2_10_strict "${const_sql_2_10}"
    testFoldConst("${const_sql_2_10}")
    def const_sql_2_11 = """select "1.", cast(cast("1." as string) as decimalv3(9, 0));"""
    qt_sql_2_11_strict "${const_sql_2_11}"
    testFoldConst("${const_sql_2_11}")
    def const_sql_2_12 = """select "9.", cast(cast("9." as string) as decimalv3(9, 0));"""
    qt_sql_2_12_strict "${const_sql_2_12}"
    testFoldConst("${const_sql_2_12}")
    def const_sql_2_13 = """select "99999999.", cast(cast("99999999." as string) as decimalv3(9, 0));"""
    qt_sql_2_13_strict "${const_sql_2_13}"
    testFoldConst("${const_sql_2_13}")
    def const_sql_2_14 = """select "900000000.", cast(cast("900000000." as string) as decimalv3(9, 0));"""
    qt_sql_2_14_strict "${const_sql_2_14}"
    testFoldConst("${const_sql_2_14}")
    def const_sql_2_15 = """select "900000001.", cast(cast("900000001." as string) as decimalv3(9, 0));"""
    qt_sql_2_15_strict "${const_sql_2_15}"
    testFoldConst("${const_sql_2_15}")
    def const_sql_2_16 = """select "999999998.", cast(cast("999999998." as string) as decimalv3(9, 0));"""
    qt_sql_2_16_strict "${const_sql_2_16}"
    testFoldConst("${const_sql_2_16}")
    def const_sql_2_17 = """select "999999999.", cast(cast("999999999." as string) as decimalv3(9, 0));"""
    qt_sql_2_17_strict "${const_sql_2_17}"
    testFoldConst("${const_sql_2_17}")
    def const_sql_2_18 = """select "-0", cast(cast("-0" as string) as decimalv3(9, 0));"""
    qt_sql_2_18_strict "${const_sql_2_18}"
    testFoldConst("${const_sql_2_18}")
    def const_sql_2_19 = """select "-1", cast(cast("-1" as string) as decimalv3(9, 0));"""
    qt_sql_2_19_strict "${const_sql_2_19}"
    testFoldConst("${const_sql_2_19}")
    def const_sql_2_20 = """select "-9", cast(cast("-9" as string) as decimalv3(9, 0));"""
    qt_sql_2_20_strict "${const_sql_2_20}"
    testFoldConst("${const_sql_2_20}")
    def const_sql_2_21 = """select "-99999999", cast(cast("-99999999" as string) as decimalv3(9, 0));"""
    qt_sql_2_21_strict "${const_sql_2_21}"
    testFoldConst("${const_sql_2_21}")
    def const_sql_2_22 = """select "-900000000", cast(cast("-900000000" as string) as decimalv3(9, 0));"""
    qt_sql_2_22_strict "${const_sql_2_22}"
    testFoldConst("${const_sql_2_22}")
    def const_sql_2_23 = """select "-900000001", cast(cast("-900000001" as string) as decimalv3(9, 0));"""
    qt_sql_2_23_strict "${const_sql_2_23}"
    testFoldConst("${const_sql_2_23}")
    def const_sql_2_24 = """select "-999999998", cast(cast("-999999998" as string) as decimalv3(9, 0));"""
    qt_sql_2_24_strict "${const_sql_2_24}"
    testFoldConst("${const_sql_2_24}")
    def const_sql_2_25 = """select "-999999999", cast(cast("-999999999" as string) as decimalv3(9, 0));"""
    qt_sql_2_25_strict "${const_sql_2_25}"
    testFoldConst("${const_sql_2_25}")
    def const_sql_2_26 = """select "-0.", cast(cast("-0." as string) as decimalv3(9, 0));"""
    qt_sql_2_26_strict "${const_sql_2_26}"
    testFoldConst("${const_sql_2_26}")
    def const_sql_2_27 = """select "-1.", cast(cast("-1." as string) as decimalv3(9, 0));"""
    qt_sql_2_27_strict "${const_sql_2_27}"
    testFoldConst("${const_sql_2_27}")
    def const_sql_2_28 = """select "-9.", cast(cast("-9." as string) as decimalv3(9, 0));"""
    qt_sql_2_28_strict "${const_sql_2_28}"
    testFoldConst("${const_sql_2_28}")
    def const_sql_2_29 = """select "-99999999.", cast(cast("-99999999." as string) as decimalv3(9, 0));"""
    qt_sql_2_29_strict "${const_sql_2_29}"
    testFoldConst("${const_sql_2_29}")
    def const_sql_2_30 = """select "-900000000.", cast(cast("-900000000." as string) as decimalv3(9, 0));"""
    qt_sql_2_30_strict "${const_sql_2_30}"
    testFoldConst("${const_sql_2_30}")
    def const_sql_2_31 = """select "-900000001.", cast(cast("-900000001." as string) as decimalv3(9, 0));"""
    qt_sql_2_31_strict "${const_sql_2_31}"
    testFoldConst("${const_sql_2_31}")
    def const_sql_2_32 = """select "-999999998.", cast(cast("-999999998." as string) as decimalv3(9, 0));"""
    qt_sql_2_32_strict "${const_sql_2_32}"
    testFoldConst("${const_sql_2_32}")
    def const_sql_2_33 = """select "-999999999.", cast(cast("-999999999." as string) as decimalv3(9, 0));"""
    qt_sql_2_33_strict "${const_sql_2_33}"
    testFoldConst("${const_sql_2_33}")
    def const_sql_2_34 = """select "0.49999", cast(cast("0.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_34_strict "${const_sql_2_34}"
    testFoldConst("${const_sql_2_34}")
    def const_sql_2_35 = """select "1.49999", cast(cast("1.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_35_strict "${const_sql_2_35}"
    testFoldConst("${const_sql_2_35}")
    def const_sql_2_36 = """select "9.49999", cast(cast("9.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_36_strict "${const_sql_2_36}"
    testFoldConst("${const_sql_2_36}")
    def const_sql_2_37 = """select "99999999.49999", cast(cast("99999999.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_37_strict "${const_sql_2_37}"
    testFoldConst("${const_sql_2_37}")
    def const_sql_2_38 = """select "900000000.49999", cast(cast("900000000.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_38_strict "${const_sql_2_38}"
    testFoldConst("${const_sql_2_38}")
    def const_sql_2_39 = """select "900000001.49999", cast(cast("900000001.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_39_strict "${const_sql_2_39}"
    testFoldConst("${const_sql_2_39}")
    def const_sql_2_40 = """select "999999998.49999", cast(cast("999999998.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_40_strict "${const_sql_2_40}"
    testFoldConst("${const_sql_2_40}")
    def const_sql_2_41 = """select "999999999.49999", cast(cast("999999999.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_41_strict "${const_sql_2_41}"
    testFoldConst("${const_sql_2_41}")
    def const_sql_2_42 = """select "0.5", cast(cast("0.5" as string) as decimalv3(9, 0));"""
    qt_sql_2_42_strict "${const_sql_2_42}"
    testFoldConst("${const_sql_2_42}")
    def const_sql_2_43 = """select "1.5", cast(cast("1.5" as string) as decimalv3(9, 0));"""
    qt_sql_2_43_strict "${const_sql_2_43}"
    testFoldConst("${const_sql_2_43}")
    def const_sql_2_44 = """select "9.5", cast(cast("9.5" as string) as decimalv3(9, 0));"""
    qt_sql_2_44_strict "${const_sql_2_44}"
    testFoldConst("${const_sql_2_44}")
    def const_sql_2_45 = """select "99999999.5", cast(cast("99999999.5" as string) as decimalv3(9, 0));"""
    qt_sql_2_45_strict "${const_sql_2_45}"
    testFoldConst("${const_sql_2_45}")
    def const_sql_2_46 = """select "900000000.5", cast(cast("900000000.5" as string) as decimalv3(9, 0));"""
    qt_sql_2_46_strict "${const_sql_2_46}"
    testFoldConst("${const_sql_2_46}")
    def const_sql_2_47 = """select "900000001.5", cast(cast("900000001.5" as string) as decimalv3(9, 0));"""
    qt_sql_2_47_strict "${const_sql_2_47}"
    testFoldConst("${const_sql_2_47}")
    def const_sql_2_48 = """select "999999998.5", cast(cast("999999998.5" as string) as decimalv3(9, 0));"""
    qt_sql_2_48_strict "${const_sql_2_48}"
    testFoldConst("${const_sql_2_48}")
    def const_sql_2_49 = """select "999999999.49999", cast(cast("999999999.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_49_strict "${const_sql_2_49}"
    testFoldConst("${const_sql_2_49}")
    def const_sql_2_50 = """select "-0.49999", cast(cast("-0.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_50_strict "${const_sql_2_50}"
    testFoldConst("${const_sql_2_50}")
    def const_sql_2_51 = """select "-1.49999", cast(cast("-1.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_51_strict "${const_sql_2_51}"
    testFoldConst("${const_sql_2_51}")
    def const_sql_2_52 = """select "-9.49999", cast(cast("-9.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_52_strict "${const_sql_2_52}"
    testFoldConst("${const_sql_2_52}")
    def const_sql_2_53 = """select "-99999999.49999", cast(cast("-99999999.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_53_strict "${const_sql_2_53}"
    testFoldConst("${const_sql_2_53}")
    def const_sql_2_54 = """select "-900000000.49999", cast(cast("-900000000.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_54_strict "${const_sql_2_54}"
    testFoldConst("${const_sql_2_54}")
    def const_sql_2_55 = """select "-900000001.49999", cast(cast("-900000001.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_55_strict "${const_sql_2_55}"
    testFoldConst("${const_sql_2_55}")
    def const_sql_2_56 = """select "-999999998.49999", cast(cast("-999999998.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_56_strict "${const_sql_2_56}"
    testFoldConst("${const_sql_2_56}")
    def const_sql_2_57 = """select "-999999999.49999", cast(cast("-999999999.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_57_strict "${const_sql_2_57}"
    testFoldConst("${const_sql_2_57}")
    def const_sql_2_58 = """select "-0.5", cast(cast("-0.5" as string) as decimalv3(9, 0));"""
    qt_sql_2_58_strict "${const_sql_2_58}"
    testFoldConst("${const_sql_2_58}")
    def const_sql_2_59 = """select "-1.5", cast(cast("-1.5" as string) as decimalv3(9, 0));"""
    qt_sql_2_59_strict "${const_sql_2_59}"
    testFoldConst("${const_sql_2_59}")
    def const_sql_2_60 = """select "-9.5", cast(cast("-9.5" as string) as decimalv3(9, 0));"""
    qt_sql_2_60_strict "${const_sql_2_60}"
    testFoldConst("${const_sql_2_60}")
    def const_sql_2_61 = """select "-99999999.5", cast(cast("-99999999.5" as string) as decimalv3(9, 0));"""
    qt_sql_2_61_strict "${const_sql_2_61}"
    testFoldConst("${const_sql_2_61}")
    def const_sql_2_62 = """select "-900000000.5", cast(cast("-900000000.5" as string) as decimalv3(9, 0));"""
    qt_sql_2_62_strict "${const_sql_2_62}"
    testFoldConst("${const_sql_2_62}")
    def const_sql_2_63 = """select "-900000001.5", cast(cast("-900000001.5" as string) as decimalv3(9, 0));"""
    qt_sql_2_63_strict "${const_sql_2_63}"
    testFoldConst("${const_sql_2_63}")
    def const_sql_2_64 = """select "-999999998.5", cast(cast("-999999998.5" as string) as decimalv3(9, 0));"""
    qt_sql_2_64_strict "${const_sql_2_64}"
    testFoldConst("${const_sql_2_64}")
    def const_sql_2_65 = """select "-999999999.49999", cast(cast("-999999999.49999" as string) as decimalv3(9, 0));"""
    qt_sql_2_65_strict "${const_sql_2_65}"
    testFoldConst("${const_sql_2_65}")

    sql "set enable_strict_cast=false;"
    qt_sql_2_0_non_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    qt_sql_2_1_non_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    qt_sql_2_2_non_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    qt_sql_2_3_non_strict "${const_sql_2_3}"
    testFoldConst("${const_sql_2_3}")
    qt_sql_2_4_non_strict "${const_sql_2_4}"
    testFoldConst("${const_sql_2_4}")
    qt_sql_2_5_non_strict "${const_sql_2_5}"
    testFoldConst("${const_sql_2_5}")
    qt_sql_2_6_non_strict "${const_sql_2_6}"
    testFoldConst("${const_sql_2_6}")
    qt_sql_2_7_non_strict "${const_sql_2_7}"
    testFoldConst("${const_sql_2_7}")
    qt_sql_2_8_non_strict "${const_sql_2_8}"
    testFoldConst("${const_sql_2_8}")
    qt_sql_2_9_non_strict "${const_sql_2_9}"
    testFoldConst("${const_sql_2_9}")
    qt_sql_2_10_non_strict "${const_sql_2_10}"
    testFoldConst("${const_sql_2_10}")
    qt_sql_2_11_non_strict "${const_sql_2_11}"
    testFoldConst("${const_sql_2_11}")
    qt_sql_2_12_non_strict "${const_sql_2_12}"
    testFoldConst("${const_sql_2_12}")
    qt_sql_2_13_non_strict "${const_sql_2_13}"
    testFoldConst("${const_sql_2_13}")
    qt_sql_2_14_non_strict "${const_sql_2_14}"
    testFoldConst("${const_sql_2_14}")
    qt_sql_2_15_non_strict "${const_sql_2_15}"
    testFoldConst("${const_sql_2_15}")
    qt_sql_2_16_non_strict "${const_sql_2_16}"
    testFoldConst("${const_sql_2_16}")
    qt_sql_2_17_non_strict "${const_sql_2_17}"
    testFoldConst("${const_sql_2_17}")
    qt_sql_2_18_non_strict "${const_sql_2_18}"
    testFoldConst("${const_sql_2_18}")
    qt_sql_2_19_non_strict "${const_sql_2_19}"
    testFoldConst("${const_sql_2_19}")
    qt_sql_2_20_non_strict "${const_sql_2_20}"
    testFoldConst("${const_sql_2_20}")
    qt_sql_2_21_non_strict "${const_sql_2_21}"
    testFoldConst("${const_sql_2_21}")
    qt_sql_2_22_non_strict "${const_sql_2_22}"
    testFoldConst("${const_sql_2_22}")
    qt_sql_2_23_non_strict "${const_sql_2_23}"
    testFoldConst("${const_sql_2_23}")
    qt_sql_2_24_non_strict "${const_sql_2_24}"
    testFoldConst("${const_sql_2_24}")
    qt_sql_2_25_non_strict "${const_sql_2_25}"
    testFoldConst("${const_sql_2_25}")
    qt_sql_2_26_non_strict "${const_sql_2_26}"
    testFoldConst("${const_sql_2_26}")
    qt_sql_2_27_non_strict "${const_sql_2_27}"
    testFoldConst("${const_sql_2_27}")
    qt_sql_2_28_non_strict "${const_sql_2_28}"
    testFoldConst("${const_sql_2_28}")
    qt_sql_2_29_non_strict "${const_sql_2_29}"
    testFoldConst("${const_sql_2_29}")
    qt_sql_2_30_non_strict "${const_sql_2_30}"
    testFoldConst("${const_sql_2_30}")
    qt_sql_2_31_non_strict "${const_sql_2_31}"
    testFoldConst("${const_sql_2_31}")
    qt_sql_2_32_non_strict "${const_sql_2_32}"
    testFoldConst("${const_sql_2_32}")
    qt_sql_2_33_non_strict "${const_sql_2_33}"
    testFoldConst("${const_sql_2_33}")
    qt_sql_2_34_non_strict "${const_sql_2_34}"
    testFoldConst("${const_sql_2_34}")
    qt_sql_2_35_non_strict "${const_sql_2_35}"
    testFoldConst("${const_sql_2_35}")
    qt_sql_2_36_non_strict "${const_sql_2_36}"
    testFoldConst("${const_sql_2_36}")
    qt_sql_2_37_non_strict "${const_sql_2_37}"
    testFoldConst("${const_sql_2_37}")
    qt_sql_2_38_non_strict "${const_sql_2_38}"
    testFoldConst("${const_sql_2_38}")
    qt_sql_2_39_non_strict "${const_sql_2_39}"
    testFoldConst("${const_sql_2_39}")
    qt_sql_2_40_non_strict "${const_sql_2_40}"
    testFoldConst("${const_sql_2_40}")
    qt_sql_2_41_non_strict "${const_sql_2_41}"
    testFoldConst("${const_sql_2_41}")
    qt_sql_2_42_non_strict "${const_sql_2_42}"
    testFoldConst("${const_sql_2_42}")
    qt_sql_2_43_non_strict "${const_sql_2_43}"
    testFoldConst("${const_sql_2_43}")
    qt_sql_2_44_non_strict "${const_sql_2_44}"
    testFoldConst("${const_sql_2_44}")
    qt_sql_2_45_non_strict "${const_sql_2_45}"
    testFoldConst("${const_sql_2_45}")
    qt_sql_2_46_non_strict "${const_sql_2_46}"
    testFoldConst("${const_sql_2_46}")
    qt_sql_2_47_non_strict "${const_sql_2_47}"
    testFoldConst("${const_sql_2_47}")
    qt_sql_2_48_non_strict "${const_sql_2_48}"
    testFoldConst("${const_sql_2_48}")
    qt_sql_2_49_non_strict "${const_sql_2_49}"
    testFoldConst("${const_sql_2_49}")
    qt_sql_2_50_non_strict "${const_sql_2_50}"
    testFoldConst("${const_sql_2_50}")
    qt_sql_2_51_non_strict "${const_sql_2_51}"
    testFoldConst("${const_sql_2_51}")
    qt_sql_2_52_non_strict "${const_sql_2_52}"
    testFoldConst("${const_sql_2_52}")
    qt_sql_2_53_non_strict "${const_sql_2_53}"
    testFoldConst("${const_sql_2_53}")
    qt_sql_2_54_non_strict "${const_sql_2_54}"
    testFoldConst("${const_sql_2_54}")
    qt_sql_2_55_non_strict "${const_sql_2_55}"
    testFoldConst("${const_sql_2_55}")
    qt_sql_2_56_non_strict "${const_sql_2_56}"
    testFoldConst("${const_sql_2_56}")
    qt_sql_2_57_non_strict "${const_sql_2_57}"
    testFoldConst("${const_sql_2_57}")
    qt_sql_2_58_non_strict "${const_sql_2_58}"
    testFoldConst("${const_sql_2_58}")
    qt_sql_2_59_non_strict "${const_sql_2_59}"
    testFoldConst("${const_sql_2_59}")
    qt_sql_2_60_non_strict "${const_sql_2_60}"
    testFoldConst("${const_sql_2_60}")
    qt_sql_2_61_non_strict "${const_sql_2_61}"
    testFoldConst("${const_sql_2_61}")
    qt_sql_2_62_non_strict "${const_sql_2_62}"
    testFoldConst("${const_sql_2_62}")
    qt_sql_2_63_non_strict "${const_sql_2_63}"
    testFoldConst("${const_sql_2_63}")
    qt_sql_2_64_non_strict "${const_sql_2_64}"
    testFoldConst("${const_sql_2_64}")
    qt_sql_2_65_non_strict "${const_sql_2_65}"
    testFoldConst("${const_sql_2_65}")
}