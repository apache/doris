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


suite("test_cast_to_decimal32_9_3_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_17_0 = """select "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647", cast(cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as string) as decimalv3(9, 3));"""
    qt_sql_17_0_strict "${const_sql_17_0}"
    testFoldConst("${const_sql_17_0}")
    def const_sql_17_1 = """select "-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647", cast(cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as string) as decimalv3(9, 3));"""
    qt_sql_17_1_strict "${const_sql_17_1}"
    testFoldConst("${const_sql_17_1}")
    def const_sql_17_2 = """select "0", cast(cast("0" as string) as decimalv3(9, 3));"""
    qt_sql_17_2_strict "${const_sql_17_2}"
    testFoldConst("${const_sql_17_2}")
    def const_sql_17_3 = """select "1", cast(cast("1" as string) as decimalv3(9, 3));"""
    qt_sql_17_3_strict "${const_sql_17_3}"
    testFoldConst("${const_sql_17_3}")
    def const_sql_17_4 = """select "9", cast(cast("9" as string) as decimalv3(9, 3));"""
    qt_sql_17_4_strict "${const_sql_17_4}"
    testFoldConst("${const_sql_17_4}")
    def const_sql_17_5 = """select "99999", cast(cast("99999" as string) as decimalv3(9, 3));"""
    qt_sql_17_5_strict "${const_sql_17_5}"
    testFoldConst("${const_sql_17_5}")
    def const_sql_17_6 = """select "900000", cast(cast("900000" as string) as decimalv3(9, 3));"""
    qt_sql_17_6_strict "${const_sql_17_6}"
    testFoldConst("${const_sql_17_6}")
    def const_sql_17_7 = """select "900001", cast(cast("900001" as string) as decimalv3(9, 3));"""
    qt_sql_17_7_strict "${const_sql_17_7}"
    testFoldConst("${const_sql_17_7}")
    def const_sql_17_8 = """select "999998", cast(cast("999998" as string) as decimalv3(9, 3));"""
    qt_sql_17_8_strict "${const_sql_17_8}"
    testFoldConst("${const_sql_17_8}")
    def const_sql_17_9 = """select "999999", cast(cast("999999" as string) as decimalv3(9, 3));"""
    qt_sql_17_9_strict "${const_sql_17_9}"
    testFoldConst("${const_sql_17_9}")
    def const_sql_17_10 = """select "0.", cast(cast("0." as string) as decimalv3(9, 3));"""
    qt_sql_17_10_strict "${const_sql_17_10}"
    testFoldConst("${const_sql_17_10}")
    def const_sql_17_11 = """select "1.", cast(cast("1." as string) as decimalv3(9, 3));"""
    qt_sql_17_11_strict "${const_sql_17_11}"
    testFoldConst("${const_sql_17_11}")
    def const_sql_17_12 = """select "9.", cast(cast("9." as string) as decimalv3(9, 3));"""
    qt_sql_17_12_strict "${const_sql_17_12}"
    testFoldConst("${const_sql_17_12}")
    def const_sql_17_13 = """select "99999.", cast(cast("99999." as string) as decimalv3(9, 3));"""
    qt_sql_17_13_strict "${const_sql_17_13}"
    testFoldConst("${const_sql_17_13}")
    def const_sql_17_14 = """select "900000.", cast(cast("900000." as string) as decimalv3(9, 3));"""
    qt_sql_17_14_strict "${const_sql_17_14}"
    testFoldConst("${const_sql_17_14}")
    def const_sql_17_15 = """select "900001.", cast(cast("900001." as string) as decimalv3(9, 3));"""
    qt_sql_17_15_strict "${const_sql_17_15}"
    testFoldConst("${const_sql_17_15}")
    def const_sql_17_16 = """select "999998.", cast(cast("999998." as string) as decimalv3(9, 3));"""
    qt_sql_17_16_strict "${const_sql_17_16}"
    testFoldConst("${const_sql_17_16}")
    def const_sql_17_17 = """select "999999.", cast(cast("999999." as string) as decimalv3(9, 3));"""
    qt_sql_17_17_strict "${const_sql_17_17}"
    testFoldConst("${const_sql_17_17}")
    def const_sql_17_18 = """select "-0", cast(cast("-0" as string) as decimalv3(9, 3));"""
    qt_sql_17_18_strict "${const_sql_17_18}"
    testFoldConst("${const_sql_17_18}")
    def const_sql_17_19 = """select "-1", cast(cast("-1" as string) as decimalv3(9, 3));"""
    qt_sql_17_19_strict "${const_sql_17_19}"
    testFoldConst("${const_sql_17_19}")
    def const_sql_17_20 = """select "-9", cast(cast("-9" as string) as decimalv3(9, 3));"""
    qt_sql_17_20_strict "${const_sql_17_20}"
    testFoldConst("${const_sql_17_20}")
    def const_sql_17_21 = """select "-99999", cast(cast("-99999" as string) as decimalv3(9, 3));"""
    qt_sql_17_21_strict "${const_sql_17_21}"
    testFoldConst("${const_sql_17_21}")
    def const_sql_17_22 = """select "-900000", cast(cast("-900000" as string) as decimalv3(9, 3));"""
    qt_sql_17_22_strict "${const_sql_17_22}"
    testFoldConst("${const_sql_17_22}")
    def const_sql_17_23 = """select "-900001", cast(cast("-900001" as string) as decimalv3(9, 3));"""
    qt_sql_17_23_strict "${const_sql_17_23}"
    testFoldConst("${const_sql_17_23}")
    def const_sql_17_24 = """select "-999998", cast(cast("-999998" as string) as decimalv3(9, 3));"""
    qt_sql_17_24_strict "${const_sql_17_24}"
    testFoldConst("${const_sql_17_24}")
    def const_sql_17_25 = """select "-999999", cast(cast("-999999" as string) as decimalv3(9, 3));"""
    qt_sql_17_25_strict "${const_sql_17_25}"
    testFoldConst("${const_sql_17_25}")
    def const_sql_17_26 = """select "-0.", cast(cast("-0." as string) as decimalv3(9, 3));"""
    qt_sql_17_26_strict "${const_sql_17_26}"
    testFoldConst("${const_sql_17_26}")
    def const_sql_17_27 = """select "-1.", cast(cast("-1." as string) as decimalv3(9, 3));"""
    qt_sql_17_27_strict "${const_sql_17_27}"
    testFoldConst("${const_sql_17_27}")
    def const_sql_17_28 = """select "-9.", cast(cast("-9." as string) as decimalv3(9, 3));"""
    qt_sql_17_28_strict "${const_sql_17_28}"
    testFoldConst("${const_sql_17_28}")
    def const_sql_17_29 = """select "-99999.", cast(cast("-99999." as string) as decimalv3(9, 3));"""
    qt_sql_17_29_strict "${const_sql_17_29}"
    testFoldConst("${const_sql_17_29}")
    def const_sql_17_30 = """select "-900000.", cast(cast("-900000." as string) as decimalv3(9, 3));"""
    qt_sql_17_30_strict "${const_sql_17_30}"
    testFoldConst("${const_sql_17_30}")
    def const_sql_17_31 = """select "-900001.", cast(cast("-900001." as string) as decimalv3(9, 3));"""
    qt_sql_17_31_strict "${const_sql_17_31}"
    testFoldConst("${const_sql_17_31}")
    def const_sql_17_32 = """select "-999998.", cast(cast("-999998." as string) as decimalv3(9, 3));"""
    qt_sql_17_32_strict "${const_sql_17_32}"
    testFoldConst("${const_sql_17_32}")
    def const_sql_17_33 = """select "-999999.", cast(cast("-999999." as string) as decimalv3(9, 3));"""
    qt_sql_17_33_strict "${const_sql_17_33}"
    testFoldConst("${const_sql_17_33}")
    def const_sql_17_34 = """select ".0004", cast(cast(".0004" as string) as decimalv3(9, 3));"""
    qt_sql_17_34_strict "${const_sql_17_34}"
    testFoldConst("${const_sql_17_34}")
    def const_sql_17_35 = """select ".0014", cast(cast(".0014" as string) as decimalv3(9, 3));"""
    qt_sql_17_35_strict "${const_sql_17_35}"
    testFoldConst("${const_sql_17_35}")
    def const_sql_17_36 = """select ".0094", cast(cast(".0094" as string) as decimalv3(9, 3));"""
    qt_sql_17_36_strict "${const_sql_17_36}"
    testFoldConst("${const_sql_17_36}")
    def const_sql_17_37 = """select ".0994", cast(cast(".0994" as string) as decimalv3(9, 3));"""
    qt_sql_17_37_strict "${const_sql_17_37}"
    testFoldConst("${const_sql_17_37}")
    def const_sql_17_38 = """select ".9004", cast(cast(".9004" as string) as decimalv3(9, 3));"""
    qt_sql_17_38_strict "${const_sql_17_38}"
    testFoldConst("${const_sql_17_38}")
    def const_sql_17_39 = """select ".9014", cast(cast(".9014" as string) as decimalv3(9, 3));"""
    qt_sql_17_39_strict "${const_sql_17_39}"
    testFoldConst("${const_sql_17_39}")
    def const_sql_17_40 = """select ".9984", cast(cast(".9984" as string) as decimalv3(9, 3));"""
    qt_sql_17_40_strict "${const_sql_17_40}"
    testFoldConst("${const_sql_17_40}")
    def const_sql_17_41 = """select ".9994", cast(cast(".9994" as string) as decimalv3(9, 3));"""
    qt_sql_17_41_strict "${const_sql_17_41}"
    testFoldConst("${const_sql_17_41}")
    def const_sql_17_42 = """select ".0005", cast(cast(".0005" as string) as decimalv3(9, 3));"""
    qt_sql_17_42_strict "${const_sql_17_42}"
    testFoldConst("${const_sql_17_42}")
    def const_sql_17_43 = """select ".0015", cast(cast(".0015" as string) as decimalv3(9, 3));"""
    qt_sql_17_43_strict "${const_sql_17_43}"
    testFoldConst("${const_sql_17_43}")
    def const_sql_17_44 = """select ".0095", cast(cast(".0095" as string) as decimalv3(9, 3));"""
    qt_sql_17_44_strict "${const_sql_17_44}"
    testFoldConst("${const_sql_17_44}")
    def const_sql_17_45 = """select ".0995", cast(cast(".0995" as string) as decimalv3(9, 3));"""
    qt_sql_17_45_strict "${const_sql_17_45}"
    testFoldConst("${const_sql_17_45}")
    def const_sql_17_46 = """select ".9005", cast(cast(".9005" as string) as decimalv3(9, 3));"""
    qt_sql_17_46_strict "${const_sql_17_46}"
    testFoldConst("${const_sql_17_46}")
    def const_sql_17_47 = """select ".9015", cast(cast(".9015" as string) as decimalv3(9, 3));"""
    qt_sql_17_47_strict "${const_sql_17_47}"
    testFoldConst("${const_sql_17_47}")
    def const_sql_17_48 = """select ".9985", cast(cast(".9985" as string) as decimalv3(9, 3));"""
    qt_sql_17_48_strict "${const_sql_17_48}"
    testFoldConst("${const_sql_17_48}")
    def const_sql_17_49 = """select ".9994", cast(cast(".9994" as string) as decimalv3(9, 3));"""
    qt_sql_17_49_strict "${const_sql_17_49}"
    testFoldConst("${const_sql_17_49}")
    def const_sql_17_50 = """select "-.0004", cast(cast("-.0004" as string) as decimalv3(9, 3));"""
    qt_sql_17_50_strict "${const_sql_17_50}"
    testFoldConst("${const_sql_17_50}")
    def const_sql_17_51 = """select "-.0014", cast(cast("-.0014" as string) as decimalv3(9, 3));"""
    qt_sql_17_51_strict "${const_sql_17_51}"
    testFoldConst("${const_sql_17_51}")
    def const_sql_17_52 = """select "-.0094", cast(cast("-.0094" as string) as decimalv3(9, 3));"""
    qt_sql_17_52_strict "${const_sql_17_52}"
    testFoldConst("${const_sql_17_52}")
    def const_sql_17_53 = """select "-.0994", cast(cast("-.0994" as string) as decimalv3(9, 3));"""
    qt_sql_17_53_strict "${const_sql_17_53}"
    testFoldConst("${const_sql_17_53}")
    def const_sql_17_54 = """select "-.9004", cast(cast("-.9004" as string) as decimalv3(9, 3));"""
    qt_sql_17_54_strict "${const_sql_17_54}"
    testFoldConst("${const_sql_17_54}")
    def const_sql_17_55 = """select "-.9014", cast(cast("-.9014" as string) as decimalv3(9, 3));"""
    qt_sql_17_55_strict "${const_sql_17_55}"
    testFoldConst("${const_sql_17_55}")
    def const_sql_17_56 = """select "-.9984", cast(cast("-.9984" as string) as decimalv3(9, 3));"""
    qt_sql_17_56_strict "${const_sql_17_56}"
    testFoldConst("${const_sql_17_56}")
    def const_sql_17_57 = """select "-.9994", cast(cast("-.9994" as string) as decimalv3(9, 3));"""
    qt_sql_17_57_strict "${const_sql_17_57}"
    testFoldConst("${const_sql_17_57}")
    def const_sql_17_58 = """select "-.0005", cast(cast("-.0005" as string) as decimalv3(9, 3));"""
    qt_sql_17_58_strict "${const_sql_17_58}"
    testFoldConst("${const_sql_17_58}")
    def const_sql_17_59 = """select "-.0015", cast(cast("-.0015" as string) as decimalv3(9, 3));"""
    qt_sql_17_59_strict "${const_sql_17_59}"
    testFoldConst("${const_sql_17_59}")
    def const_sql_17_60 = """select "-.0095", cast(cast("-.0095" as string) as decimalv3(9, 3));"""
    qt_sql_17_60_strict "${const_sql_17_60}"
    testFoldConst("${const_sql_17_60}")
    def const_sql_17_61 = """select "-.0995", cast(cast("-.0995" as string) as decimalv3(9, 3));"""
    qt_sql_17_61_strict "${const_sql_17_61}"
    testFoldConst("${const_sql_17_61}")
    def const_sql_17_62 = """select "-.9005", cast(cast("-.9005" as string) as decimalv3(9, 3));"""
    qt_sql_17_62_strict "${const_sql_17_62}"
    testFoldConst("${const_sql_17_62}")
    def const_sql_17_63 = """select "-.9015", cast(cast("-.9015" as string) as decimalv3(9, 3));"""
    qt_sql_17_63_strict "${const_sql_17_63}"
    testFoldConst("${const_sql_17_63}")
    def const_sql_17_64 = """select "-.9985", cast(cast("-.9985" as string) as decimalv3(9, 3));"""
    qt_sql_17_64_strict "${const_sql_17_64}"
    testFoldConst("${const_sql_17_64}")
    def const_sql_17_65 = """select "-.9994", cast(cast("-.9994" as string) as decimalv3(9, 3));"""
    qt_sql_17_65_strict "${const_sql_17_65}"
    testFoldConst("${const_sql_17_65}")
    def const_sql_17_66 = """select "00004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("00004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_66_strict "${const_sql_17_66}"
    testFoldConst("${const_sql_17_66}")
    def const_sql_17_67 = """select "00014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("00014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_67_strict "${const_sql_17_67}"
    testFoldConst("${const_sql_17_67}")
    def const_sql_17_68 = """select "00094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("00094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_68_strict "${const_sql_17_68}"
    testFoldConst("${const_sql_17_68}")
    def const_sql_17_69 = """select "00994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("00994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_69_strict "${const_sql_17_69}"
    testFoldConst("${const_sql_17_69}")
    def const_sql_17_70 = """select "09004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("09004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_70_strict "${const_sql_17_70}"
    testFoldConst("${const_sql_17_70}")
    def const_sql_17_71 = """select "09014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("09014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_71_strict "${const_sql_17_71}"
    testFoldConst("${const_sql_17_71}")
    def const_sql_17_72 = """select "09984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("09984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_72_strict "${const_sql_17_72}"
    testFoldConst("${const_sql_17_72}")
    def const_sql_17_73 = """select "09994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("09994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_73_strict "${const_sql_17_73}"
    testFoldConst("${const_sql_17_73}")
    def const_sql_17_74 = """select "10004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("10004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_74_strict "${const_sql_17_74}"
    testFoldConst("${const_sql_17_74}")
    def const_sql_17_75 = """select "10014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("10014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_75_strict "${const_sql_17_75}"
    testFoldConst("${const_sql_17_75}")
    def const_sql_17_76 = """select "10094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("10094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_76_strict "${const_sql_17_76}"
    testFoldConst("${const_sql_17_76}")
    def const_sql_17_77 = """select "10994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("10994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_77_strict "${const_sql_17_77}"
    testFoldConst("${const_sql_17_77}")
    def const_sql_17_78 = """select "19004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("19004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_78_strict "${const_sql_17_78}"
    testFoldConst("${const_sql_17_78}")
    def const_sql_17_79 = """select "19014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("19014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_79_strict "${const_sql_17_79}"
    testFoldConst("${const_sql_17_79}")
    def const_sql_17_80 = """select "19984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("19984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_80_strict "${const_sql_17_80}"
    testFoldConst("${const_sql_17_80}")
    def const_sql_17_81 = """select "19994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("19994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_81_strict "${const_sql_17_81}"
    testFoldConst("${const_sql_17_81}")
    def const_sql_17_82 = """select "90004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("90004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_82_strict "${const_sql_17_82}"
    testFoldConst("${const_sql_17_82}")
    def const_sql_17_83 = """select "90014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("90014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_83_strict "${const_sql_17_83}"
    testFoldConst("${const_sql_17_83}")
    def const_sql_17_84 = """select "90094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("90094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_84_strict "${const_sql_17_84}"
    testFoldConst("${const_sql_17_84}")
    def const_sql_17_85 = """select "90994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("90994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_85_strict "${const_sql_17_85}"
    testFoldConst("${const_sql_17_85}")
    def const_sql_17_86 = """select "99004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("99004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_86_strict "${const_sql_17_86}"
    testFoldConst("${const_sql_17_86}")
    def const_sql_17_87 = """select "99014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("99014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_87_strict "${const_sql_17_87}"
    testFoldConst("${const_sql_17_87}")
    def const_sql_17_88 = """select "99984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("99984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_88_strict "${const_sql_17_88}"
    testFoldConst("${const_sql_17_88}")
    def const_sql_17_89 = """select "99994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("99994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_89_strict "${const_sql_17_89}"
    testFoldConst("${const_sql_17_89}")
    def const_sql_17_90 = """select "999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_90_strict "${const_sql_17_90}"
    testFoldConst("${const_sql_17_90}")
    def const_sql_17_91 = """select "999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_91_strict "${const_sql_17_91}"
    testFoldConst("${const_sql_17_91}")
    def const_sql_17_92 = """select "999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_92_strict "${const_sql_17_92}"
    testFoldConst("${const_sql_17_92}")
    def const_sql_17_93 = """select "999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_93_strict "${const_sql_17_93}"
    testFoldConst("${const_sql_17_93}")
    def const_sql_17_94 = """select "999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_94_strict "${const_sql_17_94}"
    testFoldConst("${const_sql_17_94}")
    def const_sql_17_95 = """select "999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_95_strict "${const_sql_17_95}"
    testFoldConst("${const_sql_17_95}")
    def const_sql_17_96 = """select "999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_96_strict "${const_sql_17_96}"
    testFoldConst("${const_sql_17_96}")
    def const_sql_17_97 = """select "999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_97_strict "${const_sql_17_97}"
    testFoldConst("${const_sql_17_97}")
    def const_sql_17_98 = """select "9000000004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000000004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_98_strict "${const_sql_17_98}"
    testFoldConst("${const_sql_17_98}")
    def const_sql_17_99 = """select "9000000014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000000014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_99_strict "${const_sql_17_99}"
    testFoldConst("${const_sql_17_99}")
    def const_sql_17_100 = """select "9000000094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000000094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_100_strict "${const_sql_17_100}"
    testFoldConst("${const_sql_17_100}")
    def const_sql_17_101 = """select "9000000994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000000994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_101_strict "${const_sql_17_101}"
    testFoldConst("${const_sql_17_101}")
    def const_sql_17_102 = """select "9000009004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000009004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_102_strict "${const_sql_17_102}"
    testFoldConst("${const_sql_17_102}")
    def const_sql_17_103 = """select "9000009014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000009014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_103_strict "${const_sql_17_103}"
    testFoldConst("${const_sql_17_103}")
    def const_sql_17_104 = """select "9000009984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000009984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_104_strict "${const_sql_17_104}"
    testFoldConst("${const_sql_17_104}")
    def const_sql_17_105 = """select "9000009994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000009994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_105_strict "${const_sql_17_105}"
    testFoldConst("${const_sql_17_105}")
    def const_sql_17_106 = """select "9000010004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000010004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_106_strict "${const_sql_17_106}"
    testFoldConst("${const_sql_17_106}")
    def const_sql_17_107 = """select "9000010014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000010014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_107_strict "${const_sql_17_107}"
    testFoldConst("${const_sql_17_107}")
    def const_sql_17_108 = """select "9000010094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000010094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_108_strict "${const_sql_17_108}"
    testFoldConst("${const_sql_17_108}")
    def const_sql_17_109 = """select "9000010994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000010994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_109_strict "${const_sql_17_109}"
    testFoldConst("${const_sql_17_109}")
    def const_sql_17_110 = """select "9000019004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000019004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_110_strict "${const_sql_17_110}"
    testFoldConst("${const_sql_17_110}")
    def const_sql_17_111 = """select "9000019014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000019014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_111_strict "${const_sql_17_111}"
    testFoldConst("${const_sql_17_111}")
    def const_sql_17_112 = """select "9000019984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000019984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_112_strict "${const_sql_17_112}"
    testFoldConst("${const_sql_17_112}")
    def const_sql_17_113 = """select "9000019994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000019994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_113_strict "${const_sql_17_113}"
    testFoldConst("${const_sql_17_113}")
    def const_sql_17_114 = """select "9999980004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999980004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_114_strict "${const_sql_17_114}"
    testFoldConst("${const_sql_17_114}")
    def const_sql_17_115 = """select "9999980014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999980014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_115_strict "${const_sql_17_115}"
    testFoldConst("${const_sql_17_115}")
    def const_sql_17_116 = """select "9999980094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999980094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_116_strict "${const_sql_17_116}"
    testFoldConst("${const_sql_17_116}")
    def const_sql_17_117 = """select "9999980994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999980994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_117_strict "${const_sql_17_117}"
    testFoldConst("${const_sql_17_117}")
    def const_sql_17_118 = """select "9999989004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999989004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_118_strict "${const_sql_17_118}"
    testFoldConst("${const_sql_17_118}")
    def const_sql_17_119 = """select "9999989014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999989014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_119_strict "${const_sql_17_119}"
    testFoldConst("${const_sql_17_119}")
    def const_sql_17_120 = """select "9999989984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999989984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_120_strict "${const_sql_17_120}"
    testFoldConst("${const_sql_17_120}")
    def const_sql_17_121 = """select "9999989994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999989994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_121_strict "${const_sql_17_121}"
    testFoldConst("${const_sql_17_121}")
    def const_sql_17_122 = """select "9999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_122_strict "${const_sql_17_122}"
    testFoldConst("${const_sql_17_122}")
    def const_sql_17_123 = """select "9999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_123_strict "${const_sql_17_123}"
    testFoldConst("${const_sql_17_123}")
    def const_sql_17_124 = """select "9999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_124_strict "${const_sql_17_124}"
    testFoldConst("${const_sql_17_124}")
    def const_sql_17_125 = """select "9999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_125_strict "${const_sql_17_125}"
    testFoldConst("${const_sql_17_125}")
    def const_sql_17_126 = """select "9999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_126_strict "${const_sql_17_126}"
    testFoldConst("${const_sql_17_126}")
    def const_sql_17_127 = """select "9999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_127_strict "${const_sql_17_127}"
    testFoldConst("${const_sql_17_127}")
    def const_sql_17_128 = """select "9999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_128_strict "${const_sql_17_128}"
    testFoldConst("${const_sql_17_128}")
    def const_sql_17_129 = """select "9999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_129_strict "${const_sql_17_129}"
    testFoldConst("${const_sql_17_129}")
    def const_sql_17_130 = """select "00005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("00005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_130_strict "${const_sql_17_130}"
    testFoldConst("${const_sql_17_130}")
    def const_sql_17_131 = """select "00015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("00015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_131_strict "${const_sql_17_131}"
    testFoldConst("${const_sql_17_131}")
    def const_sql_17_132 = """select "00095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("00095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_132_strict "${const_sql_17_132}"
    testFoldConst("${const_sql_17_132}")
    def const_sql_17_133 = """select "00995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("00995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_133_strict "${const_sql_17_133}"
    testFoldConst("${const_sql_17_133}")
    def const_sql_17_134 = """select "09005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("09005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_134_strict "${const_sql_17_134}"
    testFoldConst("${const_sql_17_134}")
    def const_sql_17_135 = """select "09015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("09015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_135_strict "${const_sql_17_135}"
    testFoldConst("${const_sql_17_135}")
    def const_sql_17_136 = """select "09985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("09985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_136_strict "${const_sql_17_136}"
    testFoldConst("${const_sql_17_136}")
    def const_sql_17_137 = """select "09995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("09995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_137_strict "${const_sql_17_137}"
    testFoldConst("${const_sql_17_137}")
    def const_sql_17_138 = """select "10005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("10005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_138_strict "${const_sql_17_138}"
    testFoldConst("${const_sql_17_138}")
    def const_sql_17_139 = """select "10015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("10015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_139_strict "${const_sql_17_139}"
    testFoldConst("${const_sql_17_139}")
    def const_sql_17_140 = """select "10095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("10095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_140_strict "${const_sql_17_140}"
    testFoldConst("${const_sql_17_140}")
    def const_sql_17_141 = """select "10995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("10995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_141_strict "${const_sql_17_141}"
    testFoldConst("${const_sql_17_141}")
    def const_sql_17_142 = """select "19005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("19005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_142_strict "${const_sql_17_142}"
    testFoldConst("${const_sql_17_142}")
    def const_sql_17_143 = """select "19015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("19015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_143_strict "${const_sql_17_143}"
    testFoldConst("${const_sql_17_143}")
    def const_sql_17_144 = """select "19985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("19985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_144_strict "${const_sql_17_144}"
    testFoldConst("${const_sql_17_144}")
    def const_sql_17_145 = """select "19995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("19995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_145_strict "${const_sql_17_145}"
    testFoldConst("${const_sql_17_145}")
    def const_sql_17_146 = """select "90005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("90005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_146_strict "${const_sql_17_146}"
    testFoldConst("${const_sql_17_146}")
    def const_sql_17_147 = """select "90015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("90015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_147_strict "${const_sql_17_147}"
    testFoldConst("${const_sql_17_147}")
    def const_sql_17_148 = """select "90095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("90095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_148_strict "${const_sql_17_148}"
    testFoldConst("${const_sql_17_148}")
    def const_sql_17_149 = """select "90995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("90995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_149_strict "${const_sql_17_149}"
    testFoldConst("${const_sql_17_149}")
    def const_sql_17_150 = """select "99005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("99005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_150_strict "${const_sql_17_150}"
    testFoldConst("${const_sql_17_150}")
    def const_sql_17_151 = """select "99015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("99015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_151_strict "${const_sql_17_151}"
    testFoldConst("${const_sql_17_151}")
    def const_sql_17_152 = """select "99985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("99985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_152_strict "${const_sql_17_152}"
    testFoldConst("${const_sql_17_152}")
    def const_sql_17_153 = """select "99995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("99995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_153_strict "${const_sql_17_153}"
    testFoldConst("${const_sql_17_153}")
    def const_sql_17_154 = """select "999990005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999990005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_154_strict "${const_sql_17_154}"
    testFoldConst("${const_sql_17_154}")
    def const_sql_17_155 = """select "999990015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999990015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_155_strict "${const_sql_17_155}"
    testFoldConst("${const_sql_17_155}")
    def const_sql_17_156 = """select "999990095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999990095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_156_strict "${const_sql_17_156}"
    testFoldConst("${const_sql_17_156}")
    def const_sql_17_157 = """select "999990995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999990995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_157_strict "${const_sql_17_157}"
    testFoldConst("${const_sql_17_157}")
    def const_sql_17_158 = """select "999999005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999999005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_158_strict "${const_sql_17_158}"
    testFoldConst("${const_sql_17_158}")
    def const_sql_17_159 = """select "999999015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999999015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_159_strict "${const_sql_17_159}"
    testFoldConst("${const_sql_17_159}")
    def const_sql_17_160 = """select "999999985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999999985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_160_strict "${const_sql_17_160}"
    testFoldConst("${const_sql_17_160}")
    def const_sql_17_161 = """select "999999995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("999999995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_161_strict "${const_sql_17_161}"
    testFoldConst("${const_sql_17_161}")
    def const_sql_17_162 = """select "9000000005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000000005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_162_strict "${const_sql_17_162}"
    testFoldConst("${const_sql_17_162}")
    def const_sql_17_163 = """select "9000000015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000000015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_163_strict "${const_sql_17_163}"
    testFoldConst("${const_sql_17_163}")
    def const_sql_17_164 = """select "9000000095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000000095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_164_strict "${const_sql_17_164}"
    testFoldConst("${const_sql_17_164}")
    def const_sql_17_165 = """select "9000000995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000000995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_165_strict "${const_sql_17_165}"
    testFoldConst("${const_sql_17_165}")
    def const_sql_17_166 = """select "9000009005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000009005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_166_strict "${const_sql_17_166}"
    testFoldConst("${const_sql_17_166}")
    def const_sql_17_167 = """select "9000009015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000009015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_167_strict "${const_sql_17_167}"
    testFoldConst("${const_sql_17_167}")
    def const_sql_17_168 = """select "9000009985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000009985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_168_strict "${const_sql_17_168}"
    testFoldConst("${const_sql_17_168}")
    def const_sql_17_169 = """select "9000009995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000009995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_169_strict "${const_sql_17_169}"
    testFoldConst("${const_sql_17_169}")
    def const_sql_17_170 = """select "9000010005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000010005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_170_strict "${const_sql_17_170}"
    testFoldConst("${const_sql_17_170}")
    def const_sql_17_171 = """select "9000010015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000010015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_171_strict "${const_sql_17_171}"
    testFoldConst("${const_sql_17_171}")
    def const_sql_17_172 = """select "9000010095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000010095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_172_strict "${const_sql_17_172}"
    testFoldConst("${const_sql_17_172}")
    def const_sql_17_173 = """select "9000010995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000010995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_173_strict "${const_sql_17_173}"
    testFoldConst("${const_sql_17_173}")
    def const_sql_17_174 = """select "9000019005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000019005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_174_strict "${const_sql_17_174}"
    testFoldConst("${const_sql_17_174}")
    def const_sql_17_175 = """select "9000019015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000019015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_175_strict "${const_sql_17_175}"
    testFoldConst("${const_sql_17_175}")
    def const_sql_17_176 = """select "9000019985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000019985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_176_strict "${const_sql_17_176}"
    testFoldConst("${const_sql_17_176}")
    def const_sql_17_177 = """select "9000019995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9000019995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_177_strict "${const_sql_17_177}"
    testFoldConst("${const_sql_17_177}")
    def const_sql_17_178 = """select "9999980005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999980005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_178_strict "${const_sql_17_178}"
    testFoldConst("${const_sql_17_178}")
    def const_sql_17_179 = """select "9999980015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999980015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_179_strict "${const_sql_17_179}"
    testFoldConst("${const_sql_17_179}")
    def const_sql_17_180 = """select "9999980095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999980095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_180_strict "${const_sql_17_180}"
    testFoldConst("${const_sql_17_180}")
    def const_sql_17_181 = """select "9999980995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999980995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_181_strict "${const_sql_17_181}"
    testFoldConst("${const_sql_17_181}")
    def const_sql_17_182 = """select "9999989005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999989005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_182_strict "${const_sql_17_182}"
    testFoldConst("${const_sql_17_182}")
    def const_sql_17_183 = """select "9999989015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999989015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_183_strict "${const_sql_17_183}"
    testFoldConst("${const_sql_17_183}")
    def const_sql_17_184 = """select "9999989985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999989985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_184_strict "${const_sql_17_184}"
    testFoldConst("${const_sql_17_184}")
    def const_sql_17_185 = """select "9999989995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999989995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_185_strict "${const_sql_17_185}"
    testFoldConst("${const_sql_17_185}")
    def const_sql_17_186 = """select "9999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_186_strict "${const_sql_17_186}"
    testFoldConst("${const_sql_17_186}")
    def const_sql_17_187 = """select "9999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_187_strict "${const_sql_17_187}"
    testFoldConst("${const_sql_17_187}")
    def const_sql_17_188 = """select "9999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_188_strict "${const_sql_17_188}"
    testFoldConst("${const_sql_17_188}")
    def const_sql_17_189 = """select "9999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_189_strict "${const_sql_17_189}"
    testFoldConst("${const_sql_17_189}")
    def const_sql_17_190 = """select "9999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_190_strict "${const_sql_17_190}"
    testFoldConst("${const_sql_17_190}")
    def const_sql_17_191 = """select "9999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_191_strict "${const_sql_17_191}"
    testFoldConst("${const_sql_17_191}")
    def const_sql_17_192 = """select "9999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_192_strict "${const_sql_17_192}"
    testFoldConst("${const_sql_17_192}")
    def const_sql_17_193 = """select "9999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("9999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_193_strict "${const_sql_17_193}"
    testFoldConst("${const_sql_17_193}")
    def const_sql_17_194 = """select "-00004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-00004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_194_strict "${const_sql_17_194}"
    testFoldConst("${const_sql_17_194}")
    def const_sql_17_195 = """select "-00014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-00014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_195_strict "${const_sql_17_195}"
    testFoldConst("${const_sql_17_195}")
    def const_sql_17_196 = """select "-00094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-00094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_196_strict "${const_sql_17_196}"
    testFoldConst("${const_sql_17_196}")
    def const_sql_17_197 = """select "-00994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-00994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_197_strict "${const_sql_17_197}"
    testFoldConst("${const_sql_17_197}")
    def const_sql_17_198 = """select "-09004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-09004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_198_strict "${const_sql_17_198}"
    testFoldConst("${const_sql_17_198}")
    def const_sql_17_199 = """select "-09014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-09014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_199_strict "${const_sql_17_199}"
    testFoldConst("${const_sql_17_199}")
    def const_sql_17_200 = """select "-09984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-09984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_200_strict "${const_sql_17_200}"
    testFoldConst("${const_sql_17_200}")
    def const_sql_17_201 = """select "-09994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-09994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_201_strict "${const_sql_17_201}"
    testFoldConst("${const_sql_17_201}")
    def const_sql_17_202 = """select "-10004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-10004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_202_strict "${const_sql_17_202}"
    testFoldConst("${const_sql_17_202}")
    def const_sql_17_203 = """select "-10014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-10014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_203_strict "${const_sql_17_203}"
    testFoldConst("${const_sql_17_203}")
    def const_sql_17_204 = """select "-10094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-10094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_204_strict "${const_sql_17_204}"
    testFoldConst("${const_sql_17_204}")
    def const_sql_17_205 = """select "-10994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-10994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_205_strict "${const_sql_17_205}"
    testFoldConst("${const_sql_17_205}")
    def const_sql_17_206 = """select "-19004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-19004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_206_strict "${const_sql_17_206}"
    testFoldConst("${const_sql_17_206}")
    def const_sql_17_207 = """select "-19014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-19014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_207_strict "${const_sql_17_207}"
    testFoldConst("${const_sql_17_207}")
    def const_sql_17_208 = """select "-19984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-19984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_208_strict "${const_sql_17_208}"
    testFoldConst("${const_sql_17_208}")
    def const_sql_17_209 = """select "-19994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-19994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_209_strict "${const_sql_17_209}"
    testFoldConst("${const_sql_17_209}")
    def const_sql_17_210 = """select "-90004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-90004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_210_strict "${const_sql_17_210}"
    testFoldConst("${const_sql_17_210}")
    def const_sql_17_211 = """select "-90014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-90014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_211_strict "${const_sql_17_211}"
    testFoldConst("${const_sql_17_211}")
    def const_sql_17_212 = """select "-90094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-90094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_212_strict "${const_sql_17_212}"
    testFoldConst("${const_sql_17_212}")
    def const_sql_17_213 = """select "-90994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-90994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_213_strict "${const_sql_17_213}"
    testFoldConst("${const_sql_17_213}")
    def const_sql_17_214 = """select "-99004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-99004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_214_strict "${const_sql_17_214}"
    testFoldConst("${const_sql_17_214}")
    def const_sql_17_215 = """select "-99014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-99014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_215_strict "${const_sql_17_215}"
    testFoldConst("${const_sql_17_215}")
    def const_sql_17_216 = """select "-99984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-99984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_216_strict "${const_sql_17_216}"
    testFoldConst("${const_sql_17_216}")
    def const_sql_17_217 = """select "-99994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-99994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_217_strict "${const_sql_17_217}"
    testFoldConst("${const_sql_17_217}")
    def const_sql_17_218 = """select "-999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_218_strict "${const_sql_17_218}"
    testFoldConst("${const_sql_17_218}")
    def const_sql_17_219 = """select "-999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_219_strict "${const_sql_17_219}"
    testFoldConst("${const_sql_17_219}")
    def const_sql_17_220 = """select "-999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_220_strict "${const_sql_17_220}"
    testFoldConst("${const_sql_17_220}")
    def const_sql_17_221 = """select "-999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_221_strict "${const_sql_17_221}"
    testFoldConst("${const_sql_17_221}")
    def const_sql_17_222 = """select "-999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_222_strict "${const_sql_17_222}"
    testFoldConst("${const_sql_17_222}")
    def const_sql_17_223 = """select "-999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_223_strict "${const_sql_17_223}"
    testFoldConst("${const_sql_17_223}")
    def const_sql_17_224 = """select "-999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_224_strict "${const_sql_17_224}"
    testFoldConst("${const_sql_17_224}")
    def const_sql_17_225 = """select "-999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_225_strict "${const_sql_17_225}"
    testFoldConst("${const_sql_17_225}")
    def const_sql_17_226 = """select "-9000000004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000000004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_226_strict "${const_sql_17_226}"
    testFoldConst("${const_sql_17_226}")
    def const_sql_17_227 = """select "-9000000014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000000014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_227_strict "${const_sql_17_227}"
    testFoldConst("${const_sql_17_227}")
    def const_sql_17_228 = """select "-9000000094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000000094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_228_strict "${const_sql_17_228}"
    testFoldConst("${const_sql_17_228}")
    def const_sql_17_229 = """select "-9000000994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000000994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_229_strict "${const_sql_17_229}"
    testFoldConst("${const_sql_17_229}")
    def const_sql_17_230 = """select "-9000009004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000009004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_230_strict "${const_sql_17_230}"
    testFoldConst("${const_sql_17_230}")
    def const_sql_17_231 = """select "-9000009014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000009014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_231_strict "${const_sql_17_231}"
    testFoldConst("${const_sql_17_231}")
    def const_sql_17_232 = """select "-9000009984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000009984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_232_strict "${const_sql_17_232}"
    testFoldConst("${const_sql_17_232}")
    def const_sql_17_233 = """select "-9000009994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000009994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_233_strict "${const_sql_17_233}"
    testFoldConst("${const_sql_17_233}")
    def const_sql_17_234 = """select "-9000010004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000010004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_234_strict "${const_sql_17_234}"
    testFoldConst("${const_sql_17_234}")
    def const_sql_17_235 = """select "-9000010014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000010014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_235_strict "${const_sql_17_235}"
    testFoldConst("${const_sql_17_235}")
    def const_sql_17_236 = """select "-9000010094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000010094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_236_strict "${const_sql_17_236}"
    testFoldConst("${const_sql_17_236}")
    def const_sql_17_237 = """select "-9000010994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000010994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_237_strict "${const_sql_17_237}"
    testFoldConst("${const_sql_17_237}")
    def const_sql_17_238 = """select "-9000019004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000019004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_238_strict "${const_sql_17_238}"
    testFoldConst("${const_sql_17_238}")
    def const_sql_17_239 = """select "-9000019014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000019014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_239_strict "${const_sql_17_239}"
    testFoldConst("${const_sql_17_239}")
    def const_sql_17_240 = """select "-9000019984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000019984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_240_strict "${const_sql_17_240}"
    testFoldConst("${const_sql_17_240}")
    def const_sql_17_241 = """select "-9000019994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000019994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_241_strict "${const_sql_17_241}"
    testFoldConst("${const_sql_17_241}")
    def const_sql_17_242 = """select "-9999980004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999980004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_242_strict "${const_sql_17_242}"
    testFoldConst("${const_sql_17_242}")
    def const_sql_17_243 = """select "-9999980014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999980014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_243_strict "${const_sql_17_243}"
    testFoldConst("${const_sql_17_243}")
    def const_sql_17_244 = """select "-9999980094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999980094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_244_strict "${const_sql_17_244}"
    testFoldConst("${const_sql_17_244}")
    def const_sql_17_245 = """select "-9999980994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999980994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_245_strict "${const_sql_17_245}"
    testFoldConst("${const_sql_17_245}")
    def const_sql_17_246 = """select "-9999989004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999989004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_246_strict "${const_sql_17_246}"
    testFoldConst("${const_sql_17_246}")
    def const_sql_17_247 = """select "-9999989014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999989014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_247_strict "${const_sql_17_247}"
    testFoldConst("${const_sql_17_247}")
    def const_sql_17_248 = """select "-9999989984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999989984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_248_strict "${const_sql_17_248}"
    testFoldConst("${const_sql_17_248}")
    def const_sql_17_249 = """select "-9999989994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999989994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_249_strict "${const_sql_17_249}"
    testFoldConst("${const_sql_17_249}")
    def const_sql_17_250 = """select "-9999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_250_strict "${const_sql_17_250}"
    testFoldConst("${const_sql_17_250}")
    def const_sql_17_251 = """select "-9999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_251_strict "${const_sql_17_251}"
    testFoldConst("${const_sql_17_251}")
    def const_sql_17_252 = """select "-9999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_252_strict "${const_sql_17_252}"
    testFoldConst("${const_sql_17_252}")
    def const_sql_17_253 = """select "-9999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_253_strict "${const_sql_17_253}"
    testFoldConst("${const_sql_17_253}")
    def const_sql_17_254 = """select "-9999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_254_strict "${const_sql_17_254}"
    testFoldConst("${const_sql_17_254}")
    def const_sql_17_255 = """select "-9999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_255_strict "${const_sql_17_255}"
    testFoldConst("${const_sql_17_255}")
    def const_sql_17_256 = """select "-9999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_256_strict "${const_sql_17_256}"
    testFoldConst("${const_sql_17_256}")
    def const_sql_17_257 = """select "-9999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_257_strict "${const_sql_17_257}"
    testFoldConst("${const_sql_17_257}")
    def const_sql_17_258 = """select "-00005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-00005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_258_strict "${const_sql_17_258}"
    testFoldConst("${const_sql_17_258}")
    def const_sql_17_259 = """select "-00015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-00015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_259_strict "${const_sql_17_259}"
    testFoldConst("${const_sql_17_259}")
    def const_sql_17_260 = """select "-00095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-00095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_260_strict "${const_sql_17_260}"
    testFoldConst("${const_sql_17_260}")
    def const_sql_17_261 = """select "-00995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-00995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_261_strict "${const_sql_17_261}"
    testFoldConst("${const_sql_17_261}")
    def const_sql_17_262 = """select "-09005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-09005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_262_strict "${const_sql_17_262}"
    testFoldConst("${const_sql_17_262}")
    def const_sql_17_263 = """select "-09015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-09015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_263_strict "${const_sql_17_263}"
    testFoldConst("${const_sql_17_263}")
    def const_sql_17_264 = """select "-09985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-09985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_264_strict "${const_sql_17_264}"
    testFoldConst("${const_sql_17_264}")
    def const_sql_17_265 = """select "-09995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-09995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_265_strict "${const_sql_17_265}"
    testFoldConst("${const_sql_17_265}")
    def const_sql_17_266 = """select "-10005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-10005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_266_strict "${const_sql_17_266}"
    testFoldConst("${const_sql_17_266}")
    def const_sql_17_267 = """select "-10015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-10015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_267_strict "${const_sql_17_267}"
    testFoldConst("${const_sql_17_267}")
    def const_sql_17_268 = """select "-10095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-10095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_268_strict "${const_sql_17_268}"
    testFoldConst("${const_sql_17_268}")
    def const_sql_17_269 = """select "-10995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-10995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_269_strict "${const_sql_17_269}"
    testFoldConst("${const_sql_17_269}")
    def const_sql_17_270 = """select "-19005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-19005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_270_strict "${const_sql_17_270}"
    testFoldConst("${const_sql_17_270}")
    def const_sql_17_271 = """select "-19015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-19015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_271_strict "${const_sql_17_271}"
    testFoldConst("${const_sql_17_271}")
    def const_sql_17_272 = """select "-19985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-19985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_272_strict "${const_sql_17_272}"
    testFoldConst("${const_sql_17_272}")
    def const_sql_17_273 = """select "-19995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-19995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_273_strict "${const_sql_17_273}"
    testFoldConst("${const_sql_17_273}")
    def const_sql_17_274 = """select "-90005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-90005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_274_strict "${const_sql_17_274}"
    testFoldConst("${const_sql_17_274}")
    def const_sql_17_275 = """select "-90015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-90015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_275_strict "${const_sql_17_275}"
    testFoldConst("${const_sql_17_275}")
    def const_sql_17_276 = """select "-90095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-90095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_276_strict "${const_sql_17_276}"
    testFoldConst("${const_sql_17_276}")
    def const_sql_17_277 = """select "-90995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-90995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_277_strict "${const_sql_17_277}"
    testFoldConst("${const_sql_17_277}")
    def const_sql_17_278 = """select "-99005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-99005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_278_strict "${const_sql_17_278}"
    testFoldConst("${const_sql_17_278}")
    def const_sql_17_279 = """select "-99015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-99015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_279_strict "${const_sql_17_279}"
    testFoldConst("${const_sql_17_279}")
    def const_sql_17_280 = """select "-99985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-99985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_280_strict "${const_sql_17_280}"
    testFoldConst("${const_sql_17_280}")
    def const_sql_17_281 = """select "-99995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-99995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_281_strict "${const_sql_17_281}"
    testFoldConst("${const_sql_17_281}")
    def const_sql_17_282 = """select "-999990005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999990005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_282_strict "${const_sql_17_282}"
    testFoldConst("${const_sql_17_282}")
    def const_sql_17_283 = """select "-999990015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999990015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_283_strict "${const_sql_17_283}"
    testFoldConst("${const_sql_17_283}")
    def const_sql_17_284 = """select "-999990095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999990095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_284_strict "${const_sql_17_284}"
    testFoldConst("${const_sql_17_284}")
    def const_sql_17_285 = """select "-999990995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999990995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_285_strict "${const_sql_17_285}"
    testFoldConst("${const_sql_17_285}")
    def const_sql_17_286 = """select "-999999005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999999005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_286_strict "${const_sql_17_286}"
    testFoldConst("${const_sql_17_286}")
    def const_sql_17_287 = """select "-999999015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999999015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_287_strict "${const_sql_17_287}"
    testFoldConst("${const_sql_17_287}")
    def const_sql_17_288 = """select "-999999985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999999985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_288_strict "${const_sql_17_288}"
    testFoldConst("${const_sql_17_288}")
    def const_sql_17_289 = """select "-999999995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-999999995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_289_strict "${const_sql_17_289}"
    testFoldConst("${const_sql_17_289}")
    def const_sql_17_290 = """select "-9000000005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000000005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_290_strict "${const_sql_17_290}"
    testFoldConst("${const_sql_17_290}")
    def const_sql_17_291 = """select "-9000000015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000000015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_291_strict "${const_sql_17_291}"
    testFoldConst("${const_sql_17_291}")
    def const_sql_17_292 = """select "-9000000095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000000095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_292_strict "${const_sql_17_292}"
    testFoldConst("${const_sql_17_292}")
    def const_sql_17_293 = """select "-9000000995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000000995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_293_strict "${const_sql_17_293}"
    testFoldConst("${const_sql_17_293}")
    def const_sql_17_294 = """select "-9000009005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000009005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_294_strict "${const_sql_17_294}"
    testFoldConst("${const_sql_17_294}")
    def const_sql_17_295 = """select "-9000009015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000009015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_295_strict "${const_sql_17_295}"
    testFoldConst("${const_sql_17_295}")
    def const_sql_17_296 = """select "-9000009985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000009985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_296_strict "${const_sql_17_296}"
    testFoldConst("${const_sql_17_296}")
    def const_sql_17_297 = """select "-9000009995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000009995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_297_strict "${const_sql_17_297}"
    testFoldConst("${const_sql_17_297}")
    def const_sql_17_298 = """select "-9000010005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000010005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_298_strict "${const_sql_17_298}"
    testFoldConst("${const_sql_17_298}")
    def const_sql_17_299 = """select "-9000010015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000010015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_299_strict "${const_sql_17_299}"
    testFoldConst("${const_sql_17_299}")
    def const_sql_17_300 = """select "-9000010095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000010095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_300_strict "${const_sql_17_300}"
    testFoldConst("${const_sql_17_300}")
    def const_sql_17_301 = """select "-9000010995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000010995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_301_strict "${const_sql_17_301}"
    testFoldConst("${const_sql_17_301}")
    def const_sql_17_302 = """select "-9000019005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000019005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_302_strict "${const_sql_17_302}"
    testFoldConst("${const_sql_17_302}")
    def const_sql_17_303 = """select "-9000019015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000019015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_303_strict "${const_sql_17_303}"
    testFoldConst("${const_sql_17_303}")
    def const_sql_17_304 = """select "-9000019985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000019985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_304_strict "${const_sql_17_304}"
    testFoldConst("${const_sql_17_304}")
    def const_sql_17_305 = """select "-9000019995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9000019995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_305_strict "${const_sql_17_305}"
    testFoldConst("${const_sql_17_305}")
    def const_sql_17_306 = """select "-9999980005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999980005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_306_strict "${const_sql_17_306}"
    testFoldConst("${const_sql_17_306}")
    def const_sql_17_307 = """select "-9999980015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999980015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_307_strict "${const_sql_17_307}"
    testFoldConst("${const_sql_17_307}")
    def const_sql_17_308 = """select "-9999980095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999980095000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_308_strict "${const_sql_17_308}"
    testFoldConst("${const_sql_17_308}")
    def const_sql_17_309 = """select "-9999980995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999980995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_309_strict "${const_sql_17_309}"
    testFoldConst("${const_sql_17_309}")
    def const_sql_17_310 = """select "-9999989005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999989005000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_310_strict "${const_sql_17_310}"
    testFoldConst("${const_sql_17_310}")
    def const_sql_17_311 = """select "-9999989015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999989015000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_311_strict "${const_sql_17_311}"
    testFoldConst("${const_sql_17_311}")
    def const_sql_17_312 = """select "-9999989985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999989985000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_312_strict "${const_sql_17_312}"
    testFoldConst("${const_sql_17_312}")
    def const_sql_17_313 = """select "-9999989995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999989995000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_313_strict "${const_sql_17_313}"
    testFoldConst("${const_sql_17_313}")
    def const_sql_17_314 = """select "-9999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999990004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_314_strict "${const_sql_17_314}"
    testFoldConst("${const_sql_17_314}")
    def const_sql_17_315 = """select "-9999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999990014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_315_strict "${const_sql_17_315}"
    testFoldConst("${const_sql_17_315}")
    def const_sql_17_316 = """select "-9999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999990094000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_316_strict "${const_sql_17_316}"
    testFoldConst("${const_sql_17_316}")
    def const_sql_17_317 = """select "-9999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999990994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_317_strict "${const_sql_17_317}"
    testFoldConst("${const_sql_17_317}")
    def const_sql_17_318 = """select "-9999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999999004000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_318_strict "${const_sql_17_318}"
    testFoldConst("${const_sql_17_318}")
    def const_sql_17_319 = """select "-9999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999999014000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_319_strict "${const_sql_17_319}"
    testFoldConst("${const_sql_17_319}")
    def const_sql_17_320 = """select "-9999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999999984000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_320_strict "${const_sql_17_320}"
    testFoldConst("${const_sql_17_320}")
    def const_sql_17_321 = """select "-9999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100", cast(cast("-9999999994000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as string) as decimalv3(9, 3));"""
    qt_sql_17_321_strict "${const_sql_17_321}"
    testFoldConst("${const_sql_17_321}")

    sql "set enable_strict_cast=false;"
    qt_sql_17_0_non_strict "${const_sql_17_0}"
    testFoldConst("${const_sql_17_0}")
    qt_sql_17_1_non_strict "${const_sql_17_1}"
    testFoldConst("${const_sql_17_1}")
    qt_sql_17_2_non_strict "${const_sql_17_2}"
    testFoldConst("${const_sql_17_2}")
    qt_sql_17_3_non_strict "${const_sql_17_3}"
    testFoldConst("${const_sql_17_3}")
    qt_sql_17_4_non_strict "${const_sql_17_4}"
    testFoldConst("${const_sql_17_4}")
    qt_sql_17_5_non_strict "${const_sql_17_5}"
    testFoldConst("${const_sql_17_5}")
    qt_sql_17_6_non_strict "${const_sql_17_6}"
    testFoldConst("${const_sql_17_6}")
    qt_sql_17_7_non_strict "${const_sql_17_7}"
    testFoldConst("${const_sql_17_7}")
    qt_sql_17_8_non_strict "${const_sql_17_8}"
    testFoldConst("${const_sql_17_8}")
    qt_sql_17_9_non_strict "${const_sql_17_9}"
    testFoldConst("${const_sql_17_9}")
    qt_sql_17_10_non_strict "${const_sql_17_10}"
    testFoldConst("${const_sql_17_10}")
    qt_sql_17_11_non_strict "${const_sql_17_11}"
    testFoldConst("${const_sql_17_11}")
    qt_sql_17_12_non_strict "${const_sql_17_12}"
    testFoldConst("${const_sql_17_12}")
    qt_sql_17_13_non_strict "${const_sql_17_13}"
    testFoldConst("${const_sql_17_13}")
    qt_sql_17_14_non_strict "${const_sql_17_14}"
    testFoldConst("${const_sql_17_14}")
    qt_sql_17_15_non_strict "${const_sql_17_15}"
    testFoldConst("${const_sql_17_15}")
    qt_sql_17_16_non_strict "${const_sql_17_16}"
    testFoldConst("${const_sql_17_16}")
    qt_sql_17_17_non_strict "${const_sql_17_17}"
    testFoldConst("${const_sql_17_17}")
    qt_sql_17_18_non_strict "${const_sql_17_18}"
    testFoldConst("${const_sql_17_18}")
    qt_sql_17_19_non_strict "${const_sql_17_19}"
    testFoldConst("${const_sql_17_19}")
    qt_sql_17_20_non_strict "${const_sql_17_20}"
    testFoldConst("${const_sql_17_20}")
    qt_sql_17_21_non_strict "${const_sql_17_21}"
    testFoldConst("${const_sql_17_21}")
    qt_sql_17_22_non_strict "${const_sql_17_22}"
    testFoldConst("${const_sql_17_22}")
    qt_sql_17_23_non_strict "${const_sql_17_23}"
    testFoldConst("${const_sql_17_23}")
    qt_sql_17_24_non_strict "${const_sql_17_24}"
    testFoldConst("${const_sql_17_24}")
    qt_sql_17_25_non_strict "${const_sql_17_25}"
    testFoldConst("${const_sql_17_25}")
    qt_sql_17_26_non_strict "${const_sql_17_26}"
    testFoldConst("${const_sql_17_26}")
    qt_sql_17_27_non_strict "${const_sql_17_27}"
    testFoldConst("${const_sql_17_27}")
    qt_sql_17_28_non_strict "${const_sql_17_28}"
    testFoldConst("${const_sql_17_28}")
    qt_sql_17_29_non_strict "${const_sql_17_29}"
    testFoldConst("${const_sql_17_29}")
    qt_sql_17_30_non_strict "${const_sql_17_30}"
    testFoldConst("${const_sql_17_30}")
    qt_sql_17_31_non_strict "${const_sql_17_31}"
    testFoldConst("${const_sql_17_31}")
    qt_sql_17_32_non_strict "${const_sql_17_32}"
    testFoldConst("${const_sql_17_32}")
    qt_sql_17_33_non_strict "${const_sql_17_33}"
    testFoldConst("${const_sql_17_33}")
    qt_sql_17_34_non_strict "${const_sql_17_34}"
    testFoldConst("${const_sql_17_34}")
    qt_sql_17_35_non_strict "${const_sql_17_35}"
    testFoldConst("${const_sql_17_35}")
    qt_sql_17_36_non_strict "${const_sql_17_36}"
    testFoldConst("${const_sql_17_36}")
    qt_sql_17_37_non_strict "${const_sql_17_37}"
    testFoldConst("${const_sql_17_37}")
    qt_sql_17_38_non_strict "${const_sql_17_38}"
    testFoldConst("${const_sql_17_38}")
    qt_sql_17_39_non_strict "${const_sql_17_39}"
    testFoldConst("${const_sql_17_39}")
    qt_sql_17_40_non_strict "${const_sql_17_40}"
    testFoldConst("${const_sql_17_40}")
    qt_sql_17_41_non_strict "${const_sql_17_41}"
    testFoldConst("${const_sql_17_41}")
    qt_sql_17_42_non_strict "${const_sql_17_42}"
    testFoldConst("${const_sql_17_42}")
    qt_sql_17_43_non_strict "${const_sql_17_43}"
    testFoldConst("${const_sql_17_43}")
    qt_sql_17_44_non_strict "${const_sql_17_44}"
    testFoldConst("${const_sql_17_44}")
    qt_sql_17_45_non_strict "${const_sql_17_45}"
    testFoldConst("${const_sql_17_45}")
    qt_sql_17_46_non_strict "${const_sql_17_46}"
    testFoldConst("${const_sql_17_46}")
    qt_sql_17_47_non_strict "${const_sql_17_47}"
    testFoldConst("${const_sql_17_47}")
    qt_sql_17_48_non_strict "${const_sql_17_48}"
    testFoldConst("${const_sql_17_48}")
    qt_sql_17_49_non_strict "${const_sql_17_49}"
    testFoldConst("${const_sql_17_49}")
    qt_sql_17_50_non_strict "${const_sql_17_50}"
    testFoldConst("${const_sql_17_50}")
    qt_sql_17_51_non_strict "${const_sql_17_51}"
    testFoldConst("${const_sql_17_51}")
    qt_sql_17_52_non_strict "${const_sql_17_52}"
    testFoldConst("${const_sql_17_52}")
    qt_sql_17_53_non_strict "${const_sql_17_53}"
    testFoldConst("${const_sql_17_53}")
    qt_sql_17_54_non_strict "${const_sql_17_54}"
    testFoldConst("${const_sql_17_54}")
    qt_sql_17_55_non_strict "${const_sql_17_55}"
    testFoldConst("${const_sql_17_55}")
    qt_sql_17_56_non_strict "${const_sql_17_56}"
    testFoldConst("${const_sql_17_56}")
    qt_sql_17_57_non_strict "${const_sql_17_57}"
    testFoldConst("${const_sql_17_57}")
    qt_sql_17_58_non_strict "${const_sql_17_58}"
    testFoldConst("${const_sql_17_58}")
    qt_sql_17_59_non_strict "${const_sql_17_59}"
    testFoldConst("${const_sql_17_59}")
    qt_sql_17_60_non_strict "${const_sql_17_60}"
    testFoldConst("${const_sql_17_60}")
    qt_sql_17_61_non_strict "${const_sql_17_61}"
    testFoldConst("${const_sql_17_61}")
    qt_sql_17_62_non_strict "${const_sql_17_62}"
    testFoldConst("${const_sql_17_62}")
    qt_sql_17_63_non_strict "${const_sql_17_63}"
    testFoldConst("${const_sql_17_63}")
    qt_sql_17_64_non_strict "${const_sql_17_64}"
    testFoldConst("${const_sql_17_64}")
    qt_sql_17_65_non_strict "${const_sql_17_65}"
    testFoldConst("${const_sql_17_65}")
    qt_sql_17_66_non_strict "${const_sql_17_66}"
    testFoldConst("${const_sql_17_66}")
    qt_sql_17_67_non_strict "${const_sql_17_67}"
    testFoldConst("${const_sql_17_67}")
    qt_sql_17_68_non_strict "${const_sql_17_68}"
    testFoldConst("${const_sql_17_68}")
    qt_sql_17_69_non_strict "${const_sql_17_69}"
    testFoldConst("${const_sql_17_69}")
    qt_sql_17_70_non_strict "${const_sql_17_70}"
    testFoldConst("${const_sql_17_70}")
    qt_sql_17_71_non_strict "${const_sql_17_71}"
    testFoldConst("${const_sql_17_71}")
    qt_sql_17_72_non_strict "${const_sql_17_72}"
    testFoldConst("${const_sql_17_72}")
    qt_sql_17_73_non_strict "${const_sql_17_73}"
    testFoldConst("${const_sql_17_73}")
    qt_sql_17_74_non_strict "${const_sql_17_74}"
    testFoldConst("${const_sql_17_74}")
    qt_sql_17_75_non_strict "${const_sql_17_75}"
    testFoldConst("${const_sql_17_75}")
    qt_sql_17_76_non_strict "${const_sql_17_76}"
    testFoldConst("${const_sql_17_76}")
    qt_sql_17_77_non_strict "${const_sql_17_77}"
    testFoldConst("${const_sql_17_77}")
    qt_sql_17_78_non_strict "${const_sql_17_78}"
    testFoldConst("${const_sql_17_78}")
    qt_sql_17_79_non_strict "${const_sql_17_79}"
    testFoldConst("${const_sql_17_79}")
    qt_sql_17_80_non_strict "${const_sql_17_80}"
    testFoldConst("${const_sql_17_80}")
    qt_sql_17_81_non_strict "${const_sql_17_81}"
    testFoldConst("${const_sql_17_81}")
    qt_sql_17_82_non_strict "${const_sql_17_82}"
    testFoldConst("${const_sql_17_82}")
    qt_sql_17_83_non_strict "${const_sql_17_83}"
    testFoldConst("${const_sql_17_83}")
    qt_sql_17_84_non_strict "${const_sql_17_84}"
    testFoldConst("${const_sql_17_84}")
    qt_sql_17_85_non_strict "${const_sql_17_85}"
    testFoldConst("${const_sql_17_85}")
    qt_sql_17_86_non_strict "${const_sql_17_86}"
    testFoldConst("${const_sql_17_86}")
    qt_sql_17_87_non_strict "${const_sql_17_87}"
    testFoldConst("${const_sql_17_87}")
    qt_sql_17_88_non_strict "${const_sql_17_88}"
    testFoldConst("${const_sql_17_88}")
    qt_sql_17_89_non_strict "${const_sql_17_89}"
    testFoldConst("${const_sql_17_89}")
    qt_sql_17_90_non_strict "${const_sql_17_90}"
    testFoldConst("${const_sql_17_90}")
    qt_sql_17_91_non_strict "${const_sql_17_91}"
    testFoldConst("${const_sql_17_91}")
    qt_sql_17_92_non_strict "${const_sql_17_92}"
    testFoldConst("${const_sql_17_92}")
    qt_sql_17_93_non_strict "${const_sql_17_93}"
    testFoldConst("${const_sql_17_93}")
    qt_sql_17_94_non_strict "${const_sql_17_94}"
    testFoldConst("${const_sql_17_94}")
    qt_sql_17_95_non_strict "${const_sql_17_95}"
    testFoldConst("${const_sql_17_95}")
    qt_sql_17_96_non_strict "${const_sql_17_96}"
    testFoldConst("${const_sql_17_96}")
    qt_sql_17_97_non_strict "${const_sql_17_97}"
    testFoldConst("${const_sql_17_97}")
    qt_sql_17_98_non_strict "${const_sql_17_98}"
    testFoldConst("${const_sql_17_98}")
    qt_sql_17_99_non_strict "${const_sql_17_99}"
    testFoldConst("${const_sql_17_99}")
    qt_sql_17_100_non_strict "${const_sql_17_100}"
    testFoldConst("${const_sql_17_100}")
    qt_sql_17_101_non_strict "${const_sql_17_101}"
    testFoldConst("${const_sql_17_101}")
    qt_sql_17_102_non_strict "${const_sql_17_102}"
    testFoldConst("${const_sql_17_102}")
    qt_sql_17_103_non_strict "${const_sql_17_103}"
    testFoldConst("${const_sql_17_103}")
    qt_sql_17_104_non_strict "${const_sql_17_104}"
    testFoldConst("${const_sql_17_104}")
    qt_sql_17_105_non_strict "${const_sql_17_105}"
    testFoldConst("${const_sql_17_105}")
    qt_sql_17_106_non_strict "${const_sql_17_106}"
    testFoldConst("${const_sql_17_106}")
    qt_sql_17_107_non_strict "${const_sql_17_107}"
    testFoldConst("${const_sql_17_107}")
    qt_sql_17_108_non_strict "${const_sql_17_108}"
    testFoldConst("${const_sql_17_108}")
    qt_sql_17_109_non_strict "${const_sql_17_109}"
    testFoldConst("${const_sql_17_109}")
    qt_sql_17_110_non_strict "${const_sql_17_110}"
    testFoldConst("${const_sql_17_110}")
    qt_sql_17_111_non_strict "${const_sql_17_111}"
    testFoldConst("${const_sql_17_111}")
    qt_sql_17_112_non_strict "${const_sql_17_112}"
    testFoldConst("${const_sql_17_112}")
    qt_sql_17_113_non_strict "${const_sql_17_113}"
    testFoldConst("${const_sql_17_113}")
    qt_sql_17_114_non_strict "${const_sql_17_114}"
    testFoldConst("${const_sql_17_114}")
    qt_sql_17_115_non_strict "${const_sql_17_115}"
    testFoldConst("${const_sql_17_115}")
    qt_sql_17_116_non_strict "${const_sql_17_116}"
    testFoldConst("${const_sql_17_116}")
    qt_sql_17_117_non_strict "${const_sql_17_117}"
    testFoldConst("${const_sql_17_117}")
    qt_sql_17_118_non_strict "${const_sql_17_118}"
    testFoldConst("${const_sql_17_118}")
    qt_sql_17_119_non_strict "${const_sql_17_119}"
    testFoldConst("${const_sql_17_119}")
    qt_sql_17_120_non_strict "${const_sql_17_120}"
    testFoldConst("${const_sql_17_120}")
    qt_sql_17_121_non_strict "${const_sql_17_121}"
    testFoldConst("${const_sql_17_121}")
    qt_sql_17_122_non_strict "${const_sql_17_122}"
    testFoldConst("${const_sql_17_122}")
    qt_sql_17_123_non_strict "${const_sql_17_123}"
    testFoldConst("${const_sql_17_123}")
    qt_sql_17_124_non_strict "${const_sql_17_124}"
    testFoldConst("${const_sql_17_124}")
    qt_sql_17_125_non_strict "${const_sql_17_125}"
    testFoldConst("${const_sql_17_125}")
    qt_sql_17_126_non_strict "${const_sql_17_126}"
    testFoldConst("${const_sql_17_126}")
    qt_sql_17_127_non_strict "${const_sql_17_127}"
    testFoldConst("${const_sql_17_127}")
    qt_sql_17_128_non_strict "${const_sql_17_128}"
    testFoldConst("${const_sql_17_128}")
    qt_sql_17_129_non_strict "${const_sql_17_129}"
    testFoldConst("${const_sql_17_129}")
    qt_sql_17_130_non_strict "${const_sql_17_130}"
    testFoldConst("${const_sql_17_130}")
    qt_sql_17_131_non_strict "${const_sql_17_131}"
    testFoldConst("${const_sql_17_131}")
    qt_sql_17_132_non_strict "${const_sql_17_132}"
    testFoldConst("${const_sql_17_132}")
    qt_sql_17_133_non_strict "${const_sql_17_133}"
    testFoldConst("${const_sql_17_133}")
    qt_sql_17_134_non_strict "${const_sql_17_134}"
    testFoldConst("${const_sql_17_134}")
    qt_sql_17_135_non_strict "${const_sql_17_135}"
    testFoldConst("${const_sql_17_135}")
    qt_sql_17_136_non_strict "${const_sql_17_136}"
    testFoldConst("${const_sql_17_136}")
    qt_sql_17_137_non_strict "${const_sql_17_137}"
    testFoldConst("${const_sql_17_137}")
    qt_sql_17_138_non_strict "${const_sql_17_138}"
    testFoldConst("${const_sql_17_138}")
    qt_sql_17_139_non_strict "${const_sql_17_139}"
    testFoldConst("${const_sql_17_139}")
    qt_sql_17_140_non_strict "${const_sql_17_140}"
    testFoldConst("${const_sql_17_140}")
    qt_sql_17_141_non_strict "${const_sql_17_141}"
    testFoldConst("${const_sql_17_141}")
    qt_sql_17_142_non_strict "${const_sql_17_142}"
    testFoldConst("${const_sql_17_142}")
    qt_sql_17_143_non_strict "${const_sql_17_143}"
    testFoldConst("${const_sql_17_143}")
    qt_sql_17_144_non_strict "${const_sql_17_144}"
    testFoldConst("${const_sql_17_144}")
    qt_sql_17_145_non_strict "${const_sql_17_145}"
    testFoldConst("${const_sql_17_145}")
    qt_sql_17_146_non_strict "${const_sql_17_146}"
    testFoldConst("${const_sql_17_146}")
    qt_sql_17_147_non_strict "${const_sql_17_147}"
    testFoldConst("${const_sql_17_147}")
    qt_sql_17_148_non_strict "${const_sql_17_148}"
    testFoldConst("${const_sql_17_148}")
    qt_sql_17_149_non_strict "${const_sql_17_149}"
    testFoldConst("${const_sql_17_149}")
    qt_sql_17_150_non_strict "${const_sql_17_150}"
    testFoldConst("${const_sql_17_150}")
    qt_sql_17_151_non_strict "${const_sql_17_151}"
    testFoldConst("${const_sql_17_151}")
    qt_sql_17_152_non_strict "${const_sql_17_152}"
    testFoldConst("${const_sql_17_152}")
    qt_sql_17_153_non_strict "${const_sql_17_153}"
    testFoldConst("${const_sql_17_153}")
    qt_sql_17_154_non_strict "${const_sql_17_154}"
    testFoldConst("${const_sql_17_154}")
    qt_sql_17_155_non_strict "${const_sql_17_155}"
    testFoldConst("${const_sql_17_155}")
    qt_sql_17_156_non_strict "${const_sql_17_156}"
    testFoldConst("${const_sql_17_156}")
    qt_sql_17_157_non_strict "${const_sql_17_157}"
    testFoldConst("${const_sql_17_157}")
    qt_sql_17_158_non_strict "${const_sql_17_158}"
    testFoldConst("${const_sql_17_158}")
    qt_sql_17_159_non_strict "${const_sql_17_159}"
    testFoldConst("${const_sql_17_159}")
    qt_sql_17_160_non_strict "${const_sql_17_160}"
    testFoldConst("${const_sql_17_160}")
    qt_sql_17_161_non_strict "${const_sql_17_161}"
    testFoldConst("${const_sql_17_161}")
    qt_sql_17_162_non_strict "${const_sql_17_162}"
    testFoldConst("${const_sql_17_162}")
    qt_sql_17_163_non_strict "${const_sql_17_163}"
    testFoldConst("${const_sql_17_163}")
    qt_sql_17_164_non_strict "${const_sql_17_164}"
    testFoldConst("${const_sql_17_164}")
    qt_sql_17_165_non_strict "${const_sql_17_165}"
    testFoldConst("${const_sql_17_165}")
    qt_sql_17_166_non_strict "${const_sql_17_166}"
    testFoldConst("${const_sql_17_166}")
    qt_sql_17_167_non_strict "${const_sql_17_167}"
    testFoldConst("${const_sql_17_167}")
    qt_sql_17_168_non_strict "${const_sql_17_168}"
    testFoldConst("${const_sql_17_168}")
    qt_sql_17_169_non_strict "${const_sql_17_169}"
    testFoldConst("${const_sql_17_169}")
    qt_sql_17_170_non_strict "${const_sql_17_170}"
    testFoldConst("${const_sql_17_170}")
    qt_sql_17_171_non_strict "${const_sql_17_171}"
    testFoldConst("${const_sql_17_171}")
    qt_sql_17_172_non_strict "${const_sql_17_172}"
    testFoldConst("${const_sql_17_172}")
    qt_sql_17_173_non_strict "${const_sql_17_173}"
    testFoldConst("${const_sql_17_173}")
    qt_sql_17_174_non_strict "${const_sql_17_174}"
    testFoldConst("${const_sql_17_174}")
    qt_sql_17_175_non_strict "${const_sql_17_175}"
    testFoldConst("${const_sql_17_175}")
    qt_sql_17_176_non_strict "${const_sql_17_176}"
    testFoldConst("${const_sql_17_176}")
    qt_sql_17_177_non_strict "${const_sql_17_177}"
    testFoldConst("${const_sql_17_177}")
    qt_sql_17_178_non_strict "${const_sql_17_178}"
    testFoldConst("${const_sql_17_178}")
    qt_sql_17_179_non_strict "${const_sql_17_179}"
    testFoldConst("${const_sql_17_179}")
    qt_sql_17_180_non_strict "${const_sql_17_180}"
    testFoldConst("${const_sql_17_180}")
    qt_sql_17_181_non_strict "${const_sql_17_181}"
    testFoldConst("${const_sql_17_181}")
    qt_sql_17_182_non_strict "${const_sql_17_182}"
    testFoldConst("${const_sql_17_182}")
    qt_sql_17_183_non_strict "${const_sql_17_183}"
    testFoldConst("${const_sql_17_183}")
    qt_sql_17_184_non_strict "${const_sql_17_184}"
    testFoldConst("${const_sql_17_184}")
    qt_sql_17_185_non_strict "${const_sql_17_185}"
    testFoldConst("${const_sql_17_185}")
    qt_sql_17_186_non_strict "${const_sql_17_186}"
    testFoldConst("${const_sql_17_186}")
    qt_sql_17_187_non_strict "${const_sql_17_187}"
    testFoldConst("${const_sql_17_187}")
    qt_sql_17_188_non_strict "${const_sql_17_188}"
    testFoldConst("${const_sql_17_188}")
    qt_sql_17_189_non_strict "${const_sql_17_189}"
    testFoldConst("${const_sql_17_189}")
    qt_sql_17_190_non_strict "${const_sql_17_190}"
    testFoldConst("${const_sql_17_190}")
    qt_sql_17_191_non_strict "${const_sql_17_191}"
    testFoldConst("${const_sql_17_191}")
    qt_sql_17_192_non_strict "${const_sql_17_192}"
    testFoldConst("${const_sql_17_192}")
    qt_sql_17_193_non_strict "${const_sql_17_193}"
    testFoldConst("${const_sql_17_193}")
    qt_sql_17_194_non_strict "${const_sql_17_194}"
    testFoldConst("${const_sql_17_194}")
    qt_sql_17_195_non_strict "${const_sql_17_195}"
    testFoldConst("${const_sql_17_195}")
    qt_sql_17_196_non_strict "${const_sql_17_196}"
    testFoldConst("${const_sql_17_196}")
    qt_sql_17_197_non_strict "${const_sql_17_197}"
    testFoldConst("${const_sql_17_197}")
    qt_sql_17_198_non_strict "${const_sql_17_198}"
    testFoldConst("${const_sql_17_198}")
    qt_sql_17_199_non_strict "${const_sql_17_199}"
    testFoldConst("${const_sql_17_199}")
    qt_sql_17_200_non_strict "${const_sql_17_200}"
    testFoldConst("${const_sql_17_200}")
    qt_sql_17_201_non_strict "${const_sql_17_201}"
    testFoldConst("${const_sql_17_201}")
    qt_sql_17_202_non_strict "${const_sql_17_202}"
    testFoldConst("${const_sql_17_202}")
    qt_sql_17_203_non_strict "${const_sql_17_203}"
    testFoldConst("${const_sql_17_203}")
    qt_sql_17_204_non_strict "${const_sql_17_204}"
    testFoldConst("${const_sql_17_204}")
    qt_sql_17_205_non_strict "${const_sql_17_205}"
    testFoldConst("${const_sql_17_205}")
    qt_sql_17_206_non_strict "${const_sql_17_206}"
    testFoldConst("${const_sql_17_206}")
    qt_sql_17_207_non_strict "${const_sql_17_207}"
    testFoldConst("${const_sql_17_207}")
    qt_sql_17_208_non_strict "${const_sql_17_208}"
    testFoldConst("${const_sql_17_208}")
    qt_sql_17_209_non_strict "${const_sql_17_209}"
    testFoldConst("${const_sql_17_209}")
    qt_sql_17_210_non_strict "${const_sql_17_210}"
    testFoldConst("${const_sql_17_210}")
    qt_sql_17_211_non_strict "${const_sql_17_211}"
    testFoldConst("${const_sql_17_211}")
    qt_sql_17_212_non_strict "${const_sql_17_212}"
    testFoldConst("${const_sql_17_212}")
    qt_sql_17_213_non_strict "${const_sql_17_213}"
    testFoldConst("${const_sql_17_213}")
    qt_sql_17_214_non_strict "${const_sql_17_214}"
    testFoldConst("${const_sql_17_214}")
    qt_sql_17_215_non_strict "${const_sql_17_215}"
    testFoldConst("${const_sql_17_215}")
    qt_sql_17_216_non_strict "${const_sql_17_216}"
    testFoldConst("${const_sql_17_216}")
    qt_sql_17_217_non_strict "${const_sql_17_217}"
    testFoldConst("${const_sql_17_217}")
    qt_sql_17_218_non_strict "${const_sql_17_218}"
    testFoldConst("${const_sql_17_218}")
    qt_sql_17_219_non_strict "${const_sql_17_219}"
    testFoldConst("${const_sql_17_219}")
    qt_sql_17_220_non_strict "${const_sql_17_220}"
    testFoldConst("${const_sql_17_220}")
    qt_sql_17_221_non_strict "${const_sql_17_221}"
    testFoldConst("${const_sql_17_221}")
    qt_sql_17_222_non_strict "${const_sql_17_222}"
    testFoldConst("${const_sql_17_222}")
    qt_sql_17_223_non_strict "${const_sql_17_223}"
    testFoldConst("${const_sql_17_223}")
    qt_sql_17_224_non_strict "${const_sql_17_224}"
    testFoldConst("${const_sql_17_224}")
    qt_sql_17_225_non_strict "${const_sql_17_225}"
    testFoldConst("${const_sql_17_225}")
    qt_sql_17_226_non_strict "${const_sql_17_226}"
    testFoldConst("${const_sql_17_226}")
    qt_sql_17_227_non_strict "${const_sql_17_227}"
    testFoldConst("${const_sql_17_227}")
    qt_sql_17_228_non_strict "${const_sql_17_228}"
    testFoldConst("${const_sql_17_228}")
    qt_sql_17_229_non_strict "${const_sql_17_229}"
    testFoldConst("${const_sql_17_229}")
    qt_sql_17_230_non_strict "${const_sql_17_230}"
    testFoldConst("${const_sql_17_230}")
    qt_sql_17_231_non_strict "${const_sql_17_231}"
    testFoldConst("${const_sql_17_231}")
    qt_sql_17_232_non_strict "${const_sql_17_232}"
    testFoldConst("${const_sql_17_232}")
    qt_sql_17_233_non_strict "${const_sql_17_233}"
    testFoldConst("${const_sql_17_233}")
    qt_sql_17_234_non_strict "${const_sql_17_234}"
    testFoldConst("${const_sql_17_234}")
    qt_sql_17_235_non_strict "${const_sql_17_235}"
    testFoldConst("${const_sql_17_235}")
    qt_sql_17_236_non_strict "${const_sql_17_236}"
    testFoldConst("${const_sql_17_236}")
    qt_sql_17_237_non_strict "${const_sql_17_237}"
    testFoldConst("${const_sql_17_237}")
    qt_sql_17_238_non_strict "${const_sql_17_238}"
    testFoldConst("${const_sql_17_238}")
    qt_sql_17_239_non_strict "${const_sql_17_239}"
    testFoldConst("${const_sql_17_239}")
    qt_sql_17_240_non_strict "${const_sql_17_240}"
    testFoldConst("${const_sql_17_240}")
    qt_sql_17_241_non_strict "${const_sql_17_241}"
    testFoldConst("${const_sql_17_241}")
    qt_sql_17_242_non_strict "${const_sql_17_242}"
    testFoldConst("${const_sql_17_242}")
    qt_sql_17_243_non_strict "${const_sql_17_243}"
    testFoldConst("${const_sql_17_243}")
    qt_sql_17_244_non_strict "${const_sql_17_244}"
    testFoldConst("${const_sql_17_244}")
    qt_sql_17_245_non_strict "${const_sql_17_245}"
    testFoldConst("${const_sql_17_245}")
    qt_sql_17_246_non_strict "${const_sql_17_246}"
    testFoldConst("${const_sql_17_246}")
    qt_sql_17_247_non_strict "${const_sql_17_247}"
    testFoldConst("${const_sql_17_247}")
    qt_sql_17_248_non_strict "${const_sql_17_248}"
    testFoldConst("${const_sql_17_248}")
    qt_sql_17_249_non_strict "${const_sql_17_249}"
    testFoldConst("${const_sql_17_249}")
    qt_sql_17_250_non_strict "${const_sql_17_250}"
    testFoldConst("${const_sql_17_250}")
    qt_sql_17_251_non_strict "${const_sql_17_251}"
    testFoldConst("${const_sql_17_251}")
    qt_sql_17_252_non_strict "${const_sql_17_252}"
    testFoldConst("${const_sql_17_252}")
    qt_sql_17_253_non_strict "${const_sql_17_253}"
    testFoldConst("${const_sql_17_253}")
    qt_sql_17_254_non_strict "${const_sql_17_254}"
    testFoldConst("${const_sql_17_254}")
    qt_sql_17_255_non_strict "${const_sql_17_255}"
    testFoldConst("${const_sql_17_255}")
    qt_sql_17_256_non_strict "${const_sql_17_256}"
    testFoldConst("${const_sql_17_256}")
    qt_sql_17_257_non_strict "${const_sql_17_257}"
    testFoldConst("${const_sql_17_257}")
    qt_sql_17_258_non_strict "${const_sql_17_258}"
    testFoldConst("${const_sql_17_258}")
    qt_sql_17_259_non_strict "${const_sql_17_259}"
    testFoldConst("${const_sql_17_259}")
    qt_sql_17_260_non_strict "${const_sql_17_260}"
    testFoldConst("${const_sql_17_260}")
    qt_sql_17_261_non_strict "${const_sql_17_261}"
    testFoldConst("${const_sql_17_261}")
    qt_sql_17_262_non_strict "${const_sql_17_262}"
    testFoldConst("${const_sql_17_262}")
    qt_sql_17_263_non_strict "${const_sql_17_263}"
    testFoldConst("${const_sql_17_263}")
    qt_sql_17_264_non_strict "${const_sql_17_264}"
    testFoldConst("${const_sql_17_264}")
    qt_sql_17_265_non_strict "${const_sql_17_265}"
    testFoldConst("${const_sql_17_265}")
    qt_sql_17_266_non_strict "${const_sql_17_266}"
    testFoldConst("${const_sql_17_266}")
    qt_sql_17_267_non_strict "${const_sql_17_267}"
    testFoldConst("${const_sql_17_267}")
    qt_sql_17_268_non_strict "${const_sql_17_268}"
    testFoldConst("${const_sql_17_268}")
    qt_sql_17_269_non_strict "${const_sql_17_269}"
    testFoldConst("${const_sql_17_269}")
    qt_sql_17_270_non_strict "${const_sql_17_270}"
    testFoldConst("${const_sql_17_270}")
    qt_sql_17_271_non_strict "${const_sql_17_271}"
    testFoldConst("${const_sql_17_271}")
    qt_sql_17_272_non_strict "${const_sql_17_272}"
    testFoldConst("${const_sql_17_272}")
    qt_sql_17_273_non_strict "${const_sql_17_273}"
    testFoldConst("${const_sql_17_273}")
    qt_sql_17_274_non_strict "${const_sql_17_274}"
    testFoldConst("${const_sql_17_274}")
    qt_sql_17_275_non_strict "${const_sql_17_275}"
    testFoldConst("${const_sql_17_275}")
    qt_sql_17_276_non_strict "${const_sql_17_276}"
    testFoldConst("${const_sql_17_276}")
    qt_sql_17_277_non_strict "${const_sql_17_277}"
    testFoldConst("${const_sql_17_277}")
    qt_sql_17_278_non_strict "${const_sql_17_278}"
    testFoldConst("${const_sql_17_278}")
    qt_sql_17_279_non_strict "${const_sql_17_279}"
    testFoldConst("${const_sql_17_279}")
    qt_sql_17_280_non_strict "${const_sql_17_280}"
    testFoldConst("${const_sql_17_280}")
    qt_sql_17_281_non_strict "${const_sql_17_281}"
    testFoldConst("${const_sql_17_281}")
    qt_sql_17_282_non_strict "${const_sql_17_282}"
    testFoldConst("${const_sql_17_282}")
    qt_sql_17_283_non_strict "${const_sql_17_283}"
    testFoldConst("${const_sql_17_283}")
    qt_sql_17_284_non_strict "${const_sql_17_284}"
    testFoldConst("${const_sql_17_284}")
    qt_sql_17_285_non_strict "${const_sql_17_285}"
    testFoldConst("${const_sql_17_285}")
    qt_sql_17_286_non_strict "${const_sql_17_286}"
    testFoldConst("${const_sql_17_286}")
    qt_sql_17_287_non_strict "${const_sql_17_287}"
    testFoldConst("${const_sql_17_287}")
    qt_sql_17_288_non_strict "${const_sql_17_288}"
    testFoldConst("${const_sql_17_288}")
    qt_sql_17_289_non_strict "${const_sql_17_289}"
    testFoldConst("${const_sql_17_289}")
    qt_sql_17_290_non_strict "${const_sql_17_290}"
    testFoldConst("${const_sql_17_290}")
    qt_sql_17_291_non_strict "${const_sql_17_291}"
    testFoldConst("${const_sql_17_291}")
    qt_sql_17_292_non_strict "${const_sql_17_292}"
    testFoldConst("${const_sql_17_292}")
    qt_sql_17_293_non_strict "${const_sql_17_293}"
    testFoldConst("${const_sql_17_293}")
    qt_sql_17_294_non_strict "${const_sql_17_294}"
    testFoldConst("${const_sql_17_294}")
    qt_sql_17_295_non_strict "${const_sql_17_295}"
    testFoldConst("${const_sql_17_295}")
    qt_sql_17_296_non_strict "${const_sql_17_296}"
    testFoldConst("${const_sql_17_296}")
    qt_sql_17_297_non_strict "${const_sql_17_297}"
    testFoldConst("${const_sql_17_297}")
    qt_sql_17_298_non_strict "${const_sql_17_298}"
    testFoldConst("${const_sql_17_298}")
    qt_sql_17_299_non_strict "${const_sql_17_299}"
    testFoldConst("${const_sql_17_299}")
    qt_sql_17_300_non_strict "${const_sql_17_300}"
    testFoldConst("${const_sql_17_300}")
    qt_sql_17_301_non_strict "${const_sql_17_301}"
    testFoldConst("${const_sql_17_301}")
    qt_sql_17_302_non_strict "${const_sql_17_302}"
    testFoldConst("${const_sql_17_302}")
    qt_sql_17_303_non_strict "${const_sql_17_303}"
    testFoldConst("${const_sql_17_303}")
    qt_sql_17_304_non_strict "${const_sql_17_304}"
    testFoldConst("${const_sql_17_304}")
    qt_sql_17_305_non_strict "${const_sql_17_305}"
    testFoldConst("${const_sql_17_305}")
    qt_sql_17_306_non_strict "${const_sql_17_306}"
    testFoldConst("${const_sql_17_306}")
    qt_sql_17_307_non_strict "${const_sql_17_307}"
    testFoldConst("${const_sql_17_307}")
    qt_sql_17_308_non_strict "${const_sql_17_308}"
    testFoldConst("${const_sql_17_308}")
    qt_sql_17_309_non_strict "${const_sql_17_309}"
    testFoldConst("${const_sql_17_309}")
    qt_sql_17_310_non_strict "${const_sql_17_310}"
    testFoldConst("${const_sql_17_310}")
    qt_sql_17_311_non_strict "${const_sql_17_311}"
    testFoldConst("${const_sql_17_311}")
    qt_sql_17_312_non_strict "${const_sql_17_312}"
    testFoldConst("${const_sql_17_312}")
    qt_sql_17_313_non_strict "${const_sql_17_313}"
    testFoldConst("${const_sql_17_313}")
    qt_sql_17_314_non_strict "${const_sql_17_314}"
    testFoldConst("${const_sql_17_314}")
    qt_sql_17_315_non_strict "${const_sql_17_315}"
    testFoldConst("${const_sql_17_315}")
    qt_sql_17_316_non_strict "${const_sql_17_316}"
    testFoldConst("${const_sql_17_316}")
    qt_sql_17_317_non_strict "${const_sql_17_317}"
    testFoldConst("${const_sql_17_317}")
    qt_sql_17_318_non_strict "${const_sql_17_318}"
    testFoldConst("${const_sql_17_318}")
    qt_sql_17_319_non_strict "${const_sql_17_319}"
    testFoldConst("${const_sql_17_319}")
    qt_sql_17_320_non_strict "${const_sql_17_320}"
    testFoldConst("${const_sql_17_320}")
    qt_sql_17_321_non_strict "${const_sql_17_321}"
    testFoldConst("${const_sql_17_321}")
}