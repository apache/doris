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


suite("test_cast_to_decimal32_from_double_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_strict_cast=true;"
    def const_sql_0 = """select cast(cast("0.03" as double) as decimalv3(9, 0));"""
    qt_sql_0_strict "${const_sql_0}"
    testFoldConst("${const_sql_0}")
    def const_sql_1 = """select cast(cast("1.03" as double) as decimalv3(9, 0));"""
    qt_sql_1_strict "${const_sql_1}"
    testFoldConst("${const_sql_1}")
    def const_sql_2 = """select cast(cast("9.03" as double) as decimalv3(9, 0));"""
    qt_sql_2_strict "${const_sql_2}"
    testFoldConst("${const_sql_2}")
    def const_sql_3 = """select cast(cast("99999999.03" as double) as decimalv3(9, 0));"""
    qt_sql_3_strict "${const_sql_3}"
    testFoldConst("${const_sql_3}")
    def const_sql_4 = """select cast(cast("900000000.03" as double) as decimalv3(9, 0));"""
    qt_sql_4_strict "${const_sql_4}"
    testFoldConst("${const_sql_4}")
    def const_sql_5 = """select cast(cast("900000001.03" as double) as decimalv3(9, 0));"""
    qt_sql_5_strict "${const_sql_5}"
    testFoldConst("${const_sql_5}")
    def const_sql_6 = """select cast(cast("999999998.03" as double) as decimalv3(9, 0));"""
    qt_sql_6_strict "${const_sql_6}"
    testFoldConst("${const_sql_6}")
    def const_sql_7 = """select cast(cast("0.09" as double) as decimalv3(9, 0));"""
    qt_sql_7_strict "${const_sql_7}"
    testFoldConst("${const_sql_7}")
    def const_sql_8 = """select cast(cast("1.09" as double) as decimalv3(9, 0));"""
    qt_sql_8_strict "${const_sql_8}"
    testFoldConst("${const_sql_8}")
    def const_sql_9 = """select cast(cast("9.09" as double) as decimalv3(9, 0));"""
    qt_sql_9_strict "${const_sql_9}"
    testFoldConst("${const_sql_9}")
    def const_sql_10 = """select cast(cast("99999999.09" as double) as decimalv3(9, 0));"""
    qt_sql_10_strict "${const_sql_10}"
    testFoldConst("${const_sql_10}")
    def const_sql_11 = """select cast(cast("900000000.09" as double) as decimalv3(9, 0));"""
    qt_sql_11_strict "${const_sql_11}"
    testFoldConst("${const_sql_11}")
    def const_sql_12 = """select cast(cast("900000001.09" as double) as decimalv3(9, 0));"""
    qt_sql_12_strict "${const_sql_12}"
    testFoldConst("${const_sql_12}")
    def const_sql_13 = """select cast(cast("999999998.09" as double) as decimalv3(9, 0));"""
    qt_sql_13_strict "${const_sql_13}"
    testFoldConst("${const_sql_13}")
    def const_sql_14 = """select cast(cast("-0.03" as double) as decimalv3(9, 0));"""
    qt_sql_14_strict "${const_sql_14}"
    testFoldConst("${const_sql_14}")
    def const_sql_15 = """select cast(cast("-1.03" as double) as decimalv3(9, 0));"""
    qt_sql_15_strict "${const_sql_15}"
    testFoldConst("${const_sql_15}")
    def const_sql_16 = """select cast(cast("-9.03" as double) as decimalv3(9, 0));"""
    qt_sql_16_strict "${const_sql_16}"
    testFoldConst("${const_sql_16}")
    def const_sql_17 = """select cast(cast("-99999999.03" as double) as decimalv3(9, 0));"""
    qt_sql_17_strict "${const_sql_17}"
    testFoldConst("${const_sql_17}")
    def const_sql_18 = """select cast(cast("-900000000.03" as double) as decimalv3(9, 0));"""
    qt_sql_18_strict "${const_sql_18}"
    testFoldConst("${const_sql_18}")
    def const_sql_19 = """select cast(cast("-900000001.03" as double) as decimalv3(9, 0));"""
    qt_sql_19_strict "${const_sql_19}"
    testFoldConst("${const_sql_19}")
    def const_sql_20 = """select cast(cast("-999999998.03" as double) as decimalv3(9, 0));"""
    qt_sql_20_strict "${const_sql_20}"
    testFoldConst("${const_sql_20}")
    def const_sql_21 = """select cast(cast("-0.09" as double) as decimalv3(9, 0));"""
    qt_sql_21_strict "${const_sql_21}"
    testFoldConst("${const_sql_21}")
    def const_sql_22 = """select cast(cast("-1.09" as double) as decimalv3(9, 0));"""
    qt_sql_22_strict "${const_sql_22}"
    testFoldConst("${const_sql_22}")
    def const_sql_23 = """select cast(cast("-9.09" as double) as decimalv3(9, 0));"""
    qt_sql_23_strict "${const_sql_23}"
    testFoldConst("${const_sql_23}")
    def const_sql_24 = """select cast(cast("-99999999.09" as double) as decimalv3(9, 0));"""
    qt_sql_24_strict "${const_sql_24}"
    testFoldConst("${const_sql_24}")
    def const_sql_25 = """select cast(cast("-900000000.09" as double) as decimalv3(9, 0));"""
    qt_sql_25_strict "${const_sql_25}"
    testFoldConst("${const_sql_25}")
    def const_sql_26 = """select cast(cast("-900000001.09" as double) as decimalv3(9, 0));"""
    qt_sql_26_strict "${const_sql_26}"
    testFoldConst("${const_sql_26}")
    def const_sql_27 = """select cast(cast("-999999998.09" as double) as decimalv3(9, 0));"""
    qt_sql_27_strict "${const_sql_27}"
    testFoldConst("${const_sql_27}")
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
    sql "set enable_strict_cast=true;"
    def const_sql_28 = """select cast(cast("0.0003" as double) as decimalv3(9, 3));"""
    qt_sql_28_strict "${const_sql_28}"
    testFoldConst("${const_sql_28}")
    def const_sql_29 = """select cast(cast("0.0013" as double) as decimalv3(9, 3));"""
    qt_sql_29_strict "${const_sql_29}"
    testFoldConst("${const_sql_29}")
    def const_sql_30 = """select cast(cast("0.0093" as double) as decimalv3(9, 3));"""
    qt_sql_30_strict "${const_sql_30}"
    testFoldConst("${const_sql_30}")
    def const_sql_31 = """select cast(cast("0.0993" as double) as decimalv3(9, 3));"""
    qt_sql_31_strict "${const_sql_31}"
    testFoldConst("${const_sql_31}")
    def const_sql_32 = """select cast(cast("0.9003" as double) as decimalv3(9, 3));"""
    qt_sql_32_strict "${const_sql_32}"
    testFoldConst("${const_sql_32}")
    def const_sql_33 = """select cast(cast("0.9013" as double) as decimalv3(9, 3));"""
    qt_sql_33_strict "${const_sql_33}"
    testFoldConst("${const_sql_33}")
    def const_sql_34 = """select cast(cast("0.9983" as double) as decimalv3(9, 3));"""
    qt_sql_34_strict "${const_sql_34}"
    testFoldConst("${const_sql_34}")
    def const_sql_35 = """select cast(cast("0.9993" as double) as decimalv3(9, 3));"""
    qt_sql_35_strict "${const_sql_35}"
    testFoldConst("${const_sql_35}")
    def const_sql_36 = """select cast(cast("1.0003" as double) as decimalv3(9, 3));"""
    qt_sql_36_strict "${const_sql_36}"
    testFoldConst("${const_sql_36}")
    def const_sql_37 = """select cast(cast("1.0013" as double) as decimalv3(9, 3));"""
    qt_sql_37_strict "${const_sql_37}"
    testFoldConst("${const_sql_37}")
    def const_sql_38 = """select cast(cast("1.0093" as double) as decimalv3(9, 3));"""
    qt_sql_38_strict "${const_sql_38}"
    testFoldConst("${const_sql_38}")
    def const_sql_39 = """select cast(cast("1.0993" as double) as decimalv3(9, 3));"""
    qt_sql_39_strict "${const_sql_39}"
    testFoldConst("${const_sql_39}")
    def const_sql_40 = """select cast(cast("1.9003" as double) as decimalv3(9, 3));"""
    qt_sql_40_strict "${const_sql_40}"
    testFoldConst("${const_sql_40}")
    def const_sql_41 = """select cast(cast("1.9013" as double) as decimalv3(9, 3));"""
    qt_sql_41_strict "${const_sql_41}"
    testFoldConst("${const_sql_41}")
    def const_sql_42 = """select cast(cast("1.9983" as double) as decimalv3(9, 3));"""
    qt_sql_42_strict "${const_sql_42}"
    testFoldConst("${const_sql_42}")
    def const_sql_43 = """select cast(cast("1.9993" as double) as decimalv3(9, 3));"""
    qt_sql_43_strict "${const_sql_43}"
    testFoldConst("${const_sql_43}")
    def const_sql_44 = """select cast(cast("9.0003" as double) as decimalv3(9, 3));"""
    qt_sql_44_strict "${const_sql_44}"
    testFoldConst("${const_sql_44}")
    def const_sql_45 = """select cast(cast("9.0013" as double) as decimalv3(9, 3));"""
    qt_sql_45_strict "${const_sql_45}"
    testFoldConst("${const_sql_45}")
    def const_sql_46 = """select cast(cast("9.0093" as double) as decimalv3(9, 3));"""
    qt_sql_46_strict "${const_sql_46}"
    testFoldConst("${const_sql_46}")
    def const_sql_47 = """select cast(cast("9.0993" as double) as decimalv3(9, 3));"""
    qt_sql_47_strict "${const_sql_47}"
    testFoldConst("${const_sql_47}")
    def const_sql_48 = """select cast(cast("9.9003" as double) as decimalv3(9, 3));"""
    qt_sql_48_strict "${const_sql_48}"
    testFoldConst("${const_sql_48}")
    def const_sql_49 = """select cast(cast("9.9013" as double) as decimalv3(9, 3));"""
    qt_sql_49_strict "${const_sql_49}"
    testFoldConst("${const_sql_49}")
    def const_sql_50 = """select cast(cast("9.9983" as double) as decimalv3(9, 3));"""
    qt_sql_50_strict "${const_sql_50}"
    testFoldConst("${const_sql_50}")
    def const_sql_51 = """select cast(cast("9.9993" as double) as decimalv3(9, 3));"""
    qt_sql_51_strict "${const_sql_51}"
    testFoldConst("${const_sql_51}")
    def const_sql_52 = """select cast(cast("99999.0003" as double) as decimalv3(9, 3));"""
    qt_sql_52_strict "${const_sql_52}"
    testFoldConst("${const_sql_52}")
    def const_sql_53 = """select cast(cast("99999.0013" as double) as decimalv3(9, 3));"""
    qt_sql_53_strict "${const_sql_53}"
    testFoldConst("${const_sql_53}")
    def const_sql_54 = """select cast(cast("99999.0093" as double) as decimalv3(9, 3));"""
    qt_sql_54_strict "${const_sql_54}"
    testFoldConst("${const_sql_54}")
    def const_sql_55 = """select cast(cast("99999.0993" as double) as decimalv3(9, 3));"""
    qt_sql_55_strict "${const_sql_55}"
    testFoldConst("${const_sql_55}")
    def const_sql_56 = """select cast(cast("99999.9003" as double) as decimalv3(9, 3));"""
    qt_sql_56_strict "${const_sql_56}"
    testFoldConst("${const_sql_56}")
    def const_sql_57 = """select cast(cast("99999.9013" as double) as decimalv3(9, 3));"""
    qt_sql_57_strict "${const_sql_57}"
    testFoldConst("${const_sql_57}")
    def const_sql_58 = """select cast(cast("99999.9983" as double) as decimalv3(9, 3));"""
    qt_sql_58_strict "${const_sql_58}"
    testFoldConst("${const_sql_58}")
    def const_sql_59 = """select cast(cast("99999.9993" as double) as decimalv3(9, 3));"""
    qt_sql_59_strict "${const_sql_59}"
    testFoldConst("${const_sql_59}")
    def const_sql_60 = """select cast(cast("900000.0003" as double) as decimalv3(9, 3));"""
    qt_sql_60_strict "${const_sql_60}"
    testFoldConst("${const_sql_60}")
    def const_sql_61 = """select cast(cast("900000.0013" as double) as decimalv3(9, 3));"""
    qt_sql_61_strict "${const_sql_61}"
    testFoldConst("${const_sql_61}")
    def const_sql_62 = """select cast(cast("900000.0093" as double) as decimalv3(9, 3));"""
    qt_sql_62_strict "${const_sql_62}"
    testFoldConst("${const_sql_62}")
    def const_sql_63 = """select cast(cast("900000.0993" as double) as decimalv3(9, 3));"""
    qt_sql_63_strict "${const_sql_63}"
    testFoldConst("${const_sql_63}")
    def const_sql_64 = """select cast(cast("900000.9003" as double) as decimalv3(9, 3));"""
    qt_sql_64_strict "${const_sql_64}"
    testFoldConst("${const_sql_64}")
    def const_sql_65 = """select cast(cast("900000.9013" as double) as decimalv3(9, 3));"""
    qt_sql_65_strict "${const_sql_65}"
    testFoldConst("${const_sql_65}")
    def const_sql_66 = """select cast(cast("900000.9983" as double) as decimalv3(9, 3));"""
    qt_sql_66_strict "${const_sql_66}"
    testFoldConst("${const_sql_66}")
    def const_sql_67 = """select cast(cast("900000.9993" as double) as decimalv3(9, 3));"""
    qt_sql_67_strict "${const_sql_67}"
    testFoldConst("${const_sql_67}")
    def const_sql_68 = """select cast(cast("900001.0003" as double) as decimalv3(9, 3));"""
    qt_sql_68_strict "${const_sql_68}"
    testFoldConst("${const_sql_68}")
    def const_sql_69 = """select cast(cast("900001.0013" as double) as decimalv3(9, 3));"""
    qt_sql_69_strict "${const_sql_69}"
    testFoldConst("${const_sql_69}")
    def const_sql_70 = """select cast(cast("900001.0093" as double) as decimalv3(9, 3));"""
    qt_sql_70_strict "${const_sql_70}"
    testFoldConst("${const_sql_70}")
    def const_sql_71 = """select cast(cast("900001.0993" as double) as decimalv3(9, 3));"""
    qt_sql_71_strict "${const_sql_71}"
    testFoldConst("${const_sql_71}")
    def const_sql_72 = """select cast(cast("900001.9003" as double) as decimalv3(9, 3));"""
    qt_sql_72_strict "${const_sql_72}"
    testFoldConst("${const_sql_72}")
    def const_sql_73 = """select cast(cast("900001.9013" as double) as decimalv3(9, 3));"""
    qt_sql_73_strict "${const_sql_73}"
    testFoldConst("${const_sql_73}")
    def const_sql_74 = """select cast(cast("900001.9983" as double) as decimalv3(9, 3));"""
    qt_sql_74_strict "${const_sql_74}"
    testFoldConst("${const_sql_74}")
    def const_sql_75 = """select cast(cast("900001.9993" as double) as decimalv3(9, 3));"""
    qt_sql_75_strict "${const_sql_75}"
    testFoldConst("${const_sql_75}")
    def const_sql_76 = """select cast(cast("999998.0003" as double) as decimalv3(9, 3));"""
    qt_sql_76_strict "${const_sql_76}"
    testFoldConst("${const_sql_76}")
    def const_sql_77 = """select cast(cast("999998.0013" as double) as decimalv3(9, 3));"""
    qt_sql_77_strict "${const_sql_77}"
    testFoldConst("${const_sql_77}")
    def const_sql_78 = """select cast(cast("999998.0093" as double) as decimalv3(9, 3));"""
    qt_sql_78_strict "${const_sql_78}"
    testFoldConst("${const_sql_78}")
    def const_sql_79 = """select cast(cast("999998.0993" as double) as decimalv3(9, 3));"""
    qt_sql_79_strict "${const_sql_79}"
    testFoldConst("${const_sql_79}")
    def const_sql_80 = """select cast(cast("999998.9003" as double) as decimalv3(9, 3));"""
    qt_sql_80_strict "${const_sql_80}"
    testFoldConst("${const_sql_80}")
    def const_sql_81 = """select cast(cast("999998.9013" as double) as decimalv3(9, 3));"""
    qt_sql_81_strict "${const_sql_81}"
    testFoldConst("${const_sql_81}")
    def const_sql_82 = """select cast(cast("999998.9983" as double) as decimalv3(9, 3));"""
    qt_sql_82_strict "${const_sql_82}"
    testFoldConst("${const_sql_82}")
    def const_sql_83 = """select cast(cast("999998.9993" as double) as decimalv3(9, 3));"""
    qt_sql_83_strict "${const_sql_83}"
    testFoldConst("${const_sql_83}")
    def const_sql_84 = """select cast(cast("999999.0003" as double) as decimalv3(9, 3));"""
    qt_sql_84_strict "${const_sql_84}"
    testFoldConst("${const_sql_84}")
    def const_sql_85 = """select cast(cast("999999.0013" as double) as decimalv3(9, 3));"""
    qt_sql_85_strict "${const_sql_85}"
    testFoldConst("${const_sql_85}")
    def const_sql_86 = """select cast(cast("999999.0093" as double) as decimalv3(9, 3));"""
    qt_sql_86_strict "${const_sql_86}"
    testFoldConst("${const_sql_86}")
    def const_sql_87 = """select cast(cast("999999.0993" as double) as decimalv3(9, 3));"""
    qt_sql_87_strict "${const_sql_87}"
    testFoldConst("${const_sql_87}")
    def const_sql_88 = """select cast(cast("999999.9003" as double) as decimalv3(9, 3));"""
    qt_sql_88_strict "${const_sql_88}"
    testFoldConst("${const_sql_88}")
    def const_sql_89 = """select cast(cast("999999.9013" as double) as decimalv3(9, 3));"""
    qt_sql_89_strict "${const_sql_89}"
    testFoldConst("${const_sql_89}")
    def const_sql_90 = """select cast(cast("999999.9983" as double) as decimalv3(9, 3));"""
    qt_sql_90_strict "${const_sql_90}"
    testFoldConst("${const_sql_90}")
    def const_sql_91 = """select cast(cast("0.0009" as double) as decimalv3(9, 3));"""
    qt_sql_91_strict "${const_sql_91}"
    testFoldConst("${const_sql_91}")
    def const_sql_92 = """select cast(cast("0.0019" as double) as decimalv3(9, 3));"""
    qt_sql_92_strict "${const_sql_92}"
    testFoldConst("${const_sql_92}")
    def const_sql_93 = """select cast(cast("0.0099" as double) as decimalv3(9, 3));"""
    qt_sql_93_strict "${const_sql_93}"
    testFoldConst("${const_sql_93}")
    def const_sql_94 = """select cast(cast("0.0999" as double) as decimalv3(9, 3));"""
    qt_sql_94_strict "${const_sql_94}"
    testFoldConst("${const_sql_94}")
    def const_sql_95 = """select cast(cast("0.9009" as double) as decimalv3(9, 3));"""
    qt_sql_95_strict "${const_sql_95}"
    testFoldConst("${const_sql_95}")
    def const_sql_96 = """select cast(cast("0.9019" as double) as decimalv3(9, 3));"""
    qt_sql_96_strict "${const_sql_96}"
    testFoldConst("${const_sql_96}")
    def const_sql_97 = """select cast(cast("0.9989" as double) as decimalv3(9, 3));"""
    qt_sql_97_strict "${const_sql_97}"
    testFoldConst("${const_sql_97}")
    def const_sql_98 = """select cast(cast("0.9999" as double) as decimalv3(9, 3));"""
    qt_sql_98_strict "${const_sql_98}"
    testFoldConst("${const_sql_98}")
    def const_sql_99 = """select cast(cast("1.0009" as double) as decimalv3(9, 3));"""
    qt_sql_99_strict "${const_sql_99}"
    testFoldConst("${const_sql_99}")
    def const_sql_100 = """select cast(cast("1.0019" as double) as decimalv3(9, 3));"""
    qt_sql_100_strict "${const_sql_100}"
    testFoldConst("${const_sql_100}")
    def const_sql_101 = """select cast(cast("1.0099" as double) as decimalv3(9, 3));"""
    qt_sql_101_strict "${const_sql_101}"
    testFoldConst("${const_sql_101}")
    def const_sql_102 = """select cast(cast("1.0999" as double) as decimalv3(9, 3));"""
    qt_sql_102_strict "${const_sql_102}"
    testFoldConst("${const_sql_102}")
    def const_sql_103 = """select cast(cast("1.9009" as double) as decimalv3(9, 3));"""
    qt_sql_103_strict "${const_sql_103}"
    testFoldConst("${const_sql_103}")
    def const_sql_104 = """select cast(cast("1.9019" as double) as decimalv3(9, 3));"""
    qt_sql_104_strict "${const_sql_104}"
    testFoldConst("${const_sql_104}")
    def const_sql_105 = """select cast(cast("1.9989" as double) as decimalv3(9, 3));"""
    qt_sql_105_strict "${const_sql_105}"
    testFoldConst("${const_sql_105}")
    def const_sql_106 = """select cast(cast("1.9999" as double) as decimalv3(9, 3));"""
    qt_sql_106_strict "${const_sql_106}"
    testFoldConst("${const_sql_106}")
    def const_sql_107 = """select cast(cast("9.0009" as double) as decimalv3(9, 3));"""
    qt_sql_107_strict "${const_sql_107}"
    testFoldConst("${const_sql_107}")
    def const_sql_108 = """select cast(cast("9.0019" as double) as decimalv3(9, 3));"""
    qt_sql_108_strict "${const_sql_108}"
    testFoldConst("${const_sql_108}")
    def const_sql_109 = """select cast(cast("9.0099" as double) as decimalv3(9, 3));"""
    qt_sql_109_strict "${const_sql_109}"
    testFoldConst("${const_sql_109}")
    def const_sql_110 = """select cast(cast("9.0999" as double) as decimalv3(9, 3));"""
    qt_sql_110_strict "${const_sql_110}"
    testFoldConst("${const_sql_110}")
    def const_sql_111 = """select cast(cast("9.9009" as double) as decimalv3(9, 3));"""
    qt_sql_111_strict "${const_sql_111}"
    testFoldConst("${const_sql_111}")
    def const_sql_112 = """select cast(cast("9.9019" as double) as decimalv3(9, 3));"""
    qt_sql_112_strict "${const_sql_112}"
    testFoldConst("${const_sql_112}")
    def const_sql_113 = """select cast(cast("9.9989" as double) as decimalv3(9, 3));"""
    qt_sql_113_strict "${const_sql_113}"
    testFoldConst("${const_sql_113}")
    def const_sql_114 = """select cast(cast("9.9999" as double) as decimalv3(9, 3));"""
    qt_sql_114_strict "${const_sql_114}"
    testFoldConst("${const_sql_114}")
    def const_sql_115 = """select cast(cast("99999.0009" as double) as decimalv3(9, 3));"""
    qt_sql_115_strict "${const_sql_115}"
    testFoldConst("${const_sql_115}")
    def const_sql_116 = """select cast(cast("99999.0019" as double) as decimalv3(9, 3));"""
    qt_sql_116_strict "${const_sql_116}"
    testFoldConst("${const_sql_116}")
    def const_sql_117 = """select cast(cast("99999.0099" as double) as decimalv3(9, 3));"""
    qt_sql_117_strict "${const_sql_117}"
    testFoldConst("${const_sql_117}")
    def const_sql_118 = """select cast(cast("99999.0999" as double) as decimalv3(9, 3));"""
    qt_sql_118_strict "${const_sql_118}"
    testFoldConst("${const_sql_118}")
    def const_sql_119 = """select cast(cast("99999.9009" as double) as decimalv3(9, 3));"""
    qt_sql_119_strict "${const_sql_119}"
    testFoldConst("${const_sql_119}")
    def const_sql_120 = """select cast(cast("99999.9019" as double) as decimalv3(9, 3));"""
    qt_sql_120_strict "${const_sql_120}"
    testFoldConst("${const_sql_120}")
    def const_sql_121 = """select cast(cast("99999.9989" as double) as decimalv3(9, 3));"""
    qt_sql_121_strict "${const_sql_121}"
    testFoldConst("${const_sql_121}")
    def const_sql_122 = """select cast(cast("99999.9999" as double) as decimalv3(9, 3));"""
    qt_sql_122_strict "${const_sql_122}"
    testFoldConst("${const_sql_122}")
    def const_sql_123 = """select cast(cast("900000.0009" as double) as decimalv3(9, 3));"""
    qt_sql_123_strict "${const_sql_123}"
    testFoldConst("${const_sql_123}")
    def const_sql_124 = """select cast(cast("900000.0019" as double) as decimalv3(9, 3));"""
    qt_sql_124_strict "${const_sql_124}"
    testFoldConst("${const_sql_124}")
    def const_sql_125 = """select cast(cast("900000.0099" as double) as decimalv3(9, 3));"""
    qt_sql_125_strict "${const_sql_125}"
    testFoldConst("${const_sql_125}")
    def const_sql_126 = """select cast(cast("900000.0999" as double) as decimalv3(9, 3));"""
    qt_sql_126_strict "${const_sql_126}"
    testFoldConst("${const_sql_126}")
    def const_sql_127 = """select cast(cast("900000.9009" as double) as decimalv3(9, 3));"""
    qt_sql_127_strict "${const_sql_127}"
    testFoldConst("${const_sql_127}")
    def const_sql_128 = """select cast(cast("900000.9019" as double) as decimalv3(9, 3));"""
    qt_sql_128_strict "${const_sql_128}"
    testFoldConst("${const_sql_128}")
    def const_sql_129 = """select cast(cast("900000.9989" as double) as decimalv3(9, 3));"""
    qt_sql_129_strict "${const_sql_129}"
    testFoldConst("${const_sql_129}")
    def const_sql_130 = """select cast(cast("900000.9999" as double) as decimalv3(9, 3));"""
    qt_sql_130_strict "${const_sql_130}"
    testFoldConst("${const_sql_130}")
    def const_sql_131 = """select cast(cast("900001.0009" as double) as decimalv3(9, 3));"""
    qt_sql_131_strict "${const_sql_131}"
    testFoldConst("${const_sql_131}")
    def const_sql_132 = """select cast(cast("900001.0019" as double) as decimalv3(9, 3));"""
    qt_sql_132_strict "${const_sql_132}"
    testFoldConst("${const_sql_132}")
    def const_sql_133 = """select cast(cast("900001.0099" as double) as decimalv3(9, 3));"""
    qt_sql_133_strict "${const_sql_133}"
    testFoldConst("${const_sql_133}")
    def const_sql_134 = """select cast(cast("900001.0999" as double) as decimalv3(9, 3));"""
    qt_sql_134_strict "${const_sql_134}"
    testFoldConst("${const_sql_134}")
    def const_sql_135 = """select cast(cast("900001.9009" as double) as decimalv3(9, 3));"""
    qt_sql_135_strict "${const_sql_135}"
    testFoldConst("${const_sql_135}")
    def const_sql_136 = """select cast(cast("900001.9019" as double) as decimalv3(9, 3));"""
    qt_sql_136_strict "${const_sql_136}"
    testFoldConst("${const_sql_136}")
    def const_sql_137 = """select cast(cast("900001.9989" as double) as decimalv3(9, 3));"""
    qt_sql_137_strict "${const_sql_137}"
    testFoldConst("${const_sql_137}")
    def const_sql_138 = """select cast(cast("900001.9999" as double) as decimalv3(9, 3));"""
    qt_sql_138_strict "${const_sql_138}"
    testFoldConst("${const_sql_138}")
    def const_sql_139 = """select cast(cast("999998.0009" as double) as decimalv3(9, 3));"""
    qt_sql_139_strict "${const_sql_139}"
    testFoldConst("${const_sql_139}")
    def const_sql_140 = """select cast(cast("999998.0019" as double) as decimalv3(9, 3));"""
    qt_sql_140_strict "${const_sql_140}"
    testFoldConst("${const_sql_140}")
    def const_sql_141 = """select cast(cast("999998.0099" as double) as decimalv3(9, 3));"""
    qt_sql_141_strict "${const_sql_141}"
    testFoldConst("${const_sql_141}")
    def const_sql_142 = """select cast(cast("999998.0999" as double) as decimalv3(9, 3));"""
    qt_sql_142_strict "${const_sql_142}"
    testFoldConst("${const_sql_142}")
    def const_sql_143 = """select cast(cast("999998.9009" as double) as decimalv3(9, 3));"""
    qt_sql_143_strict "${const_sql_143}"
    testFoldConst("${const_sql_143}")
    def const_sql_144 = """select cast(cast("999998.9019" as double) as decimalv3(9, 3));"""
    qt_sql_144_strict "${const_sql_144}"
    testFoldConst("${const_sql_144}")
    def const_sql_145 = """select cast(cast("999998.9989" as double) as decimalv3(9, 3));"""
    qt_sql_145_strict "${const_sql_145}"
    testFoldConst("${const_sql_145}")
    def const_sql_146 = """select cast(cast("999998.9999" as double) as decimalv3(9, 3));"""
    qt_sql_146_strict "${const_sql_146}"
    testFoldConst("${const_sql_146}")
    def const_sql_147 = """select cast(cast("999999.0003" as double) as decimalv3(9, 3));"""
    qt_sql_147_strict "${const_sql_147}"
    testFoldConst("${const_sql_147}")
    def const_sql_148 = """select cast(cast("999999.0013" as double) as decimalv3(9, 3));"""
    qt_sql_148_strict "${const_sql_148}"
    testFoldConst("${const_sql_148}")
    def const_sql_149 = """select cast(cast("999999.0093" as double) as decimalv3(9, 3));"""
    qt_sql_149_strict "${const_sql_149}"
    testFoldConst("${const_sql_149}")
    def const_sql_150 = """select cast(cast("999999.0993" as double) as decimalv3(9, 3));"""
    qt_sql_150_strict "${const_sql_150}"
    testFoldConst("${const_sql_150}")
    def const_sql_151 = """select cast(cast("999999.9003" as double) as decimalv3(9, 3));"""
    qt_sql_151_strict "${const_sql_151}"
    testFoldConst("${const_sql_151}")
    def const_sql_152 = """select cast(cast("999999.9013" as double) as decimalv3(9, 3));"""
    qt_sql_152_strict "${const_sql_152}"
    testFoldConst("${const_sql_152}")
    def const_sql_153 = """select cast(cast("999999.9983" as double) as decimalv3(9, 3));"""
    qt_sql_153_strict "${const_sql_153}"
    testFoldConst("${const_sql_153}")
    def const_sql_154 = """select cast(cast("-0.0003" as double) as decimalv3(9, 3));"""
    qt_sql_154_strict "${const_sql_154}"
    testFoldConst("${const_sql_154}")
    def const_sql_155 = """select cast(cast("-0.0013" as double) as decimalv3(9, 3));"""
    qt_sql_155_strict "${const_sql_155}"
    testFoldConst("${const_sql_155}")
    def const_sql_156 = """select cast(cast("-0.0093" as double) as decimalv3(9, 3));"""
    qt_sql_156_strict "${const_sql_156}"
    testFoldConst("${const_sql_156}")
    def const_sql_157 = """select cast(cast("-0.0993" as double) as decimalv3(9, 3));"""
    qt_sql_157_strict "${const_sql_157}"
    testFoldConst("${const_sql_157}")
    def const_sql_158 = """select cast(cast("-0.9003" as double) as decimalv3(9, 3));"""
    qt_sql_158_strict "${const_sql_158}"
    testFoldConst("${const_sql_158}")
    def const_sql_159 = """select cast(cast("-0.9013" as double) as decimalv3(9, 3));"""
    qt_sql_159_strict "${const_sql_159}"
    testFoldConst("${const_sql_159}")
    def const_sql_160 = """select cast(cast("-0.9983" as double) as decimalv3(9, 3));"""
    qt_sql_160_strict "${const_sql_160}"
    testFoldConst("${const_sql_160}")
    def const_sql_161 = """select cast(cast("-0.9993" as double) as decimalv3(9, 3));"""
    qt_sql_161_strict "${const_sql_161}"
    testFoldConst("${const_sql_161}")
    def const_sql_162 = """select cast(cast("-1.0003" as double) as decimalv3(9, 3));"""
    qt_sql_162_strict "${const_sql_162}"
    testFoldConst("${const_sql_162}")
    def const_sql_163 = """select cast(cast("-1.0013" as double) as decimalv3(9, 3));"""
    qt_sql_163_strict "${const_sql_163}"
    testFoldConst("${const_sql_163}")
    def const_sql_164 = """select cast(cast("-1.0093" as double) as decimalv3(9, 3));"""
    qt_sql_164_strict "${const_sql_164}"
    testFoldConst("${const_sql_164}")
    def const_sql_165 = """select cast(cast("-1.0993" as double) as decimalv3(9, 3));"""
    qt_sql_165_strict "${const_sql_165}"
    testFoldConst("${const_sql_165}")
    def const_sql_166 = """select cast(cast("-1.9003" as double) as decimalv3(9, 3));"""
    qt_sql_166_strict "${const_sql_166}"
    testFoldConst("${const_sql_166}")
    def const_sql_167 = """select cast(cast("-1.9013" as double) as decimalv3(9, 3));"""
    qt_sql_167_strict "${const_sql_167}"
    testFoldConst("${const_sql_167}")
    def const_sql_168 = """select cast(cast("-1.9983" as double) as decimalv3(9, 3));"""
    qt_sql_168_strict "${const_sql_168}"
    testFoldConst("${const_sql_168}")
    def const_sql_169 = """select cast(cast("-1.9993" as double) as decimalv3(9, 3));"""
    qt_sql_169_strict "${const_sql_169}"
    testFoldConst("${const_sql_169}")
    def const_sql_170 = """select cast(cast("-9.0003" as double) as decimalv3(9, 3));"""
    qt_sql_170_strict "${const_sql_170}"
    testFoldConst("${const_sql_170}")
    def const_sql_171 = """select cast(cast("-9.0013" as double) as decimalv3(9, 3));"""
    qt_sql_171_strict "${const_sql_171}"
    testFoldConst("${const_sql_171}")
    def const_sql_172 = """select cast(cast("-9.0093" as double) as decimalv3(9, 3));"""
    qt_sql_172_strict "${const_sql_172}"
    testFoldConst("${const_sql_172}")
    def const_sql_173 = """select cast(cast("-9.0993" as double) as decimalv3(9, 3));"""
    qt_sql_173_strict "${const_sql_173}"
    testFoldConst("${const_sql_173}")
    def const_sql_174 = """select cast(cast("-9.9003" as double) as decimalv3(9, 3));"""
    qt_sql_174_strict "${const_sql_174}"
    testFoldConst("${const_sql_174}")
    def const_sql_175 = """select cast(cast("-9.9013" as double) as decimalv3(9, 3));"""
    qt_sql_175_strict "${const_sql_175}"
    testFoldConst("${const_sql_175}")
    def const_sql_176 = """select cast(cast("-9.9983" as double) as decimalv3(9, 3));"""
    qt_sql_176_strict "${const_sql_176}"
    testFoldConst("${const_sql_176}")
    def const_sql_177 = """select cast(cast("-9.9993" as double) as decimalv3(9, 3));"""
    qt_sql_177_strict "${const_sql_177}"
    testFoldConst("${const_sql_177}")
    def const_sql_178 = """select cast(cast("-99999.0003" as double) as decimalv3(9, 3));"""
    qt_sql_178_strict "${const_sql_178}"
    testFoldConst("${const_sql_178}")
    def const_sql_179 = """select cast(cast("-99999.0013" as double) as decimalv3(9, 3));"""
    qt_sql_179_strict "${const_sql_179}"
    testFoldConst("${const_sql_179}")
    def const_sql_180 = """select cast(cast("-99999.0093" as double) as decimalv3(9, 3));"""
    qt_sql_180_strict "${const_sql_180}"
    testFoldConst("${const_sql_180}")
    def const_sql_181 = """select cast(cast("-99999.0993" as double) as decimalv3(9, 3));"""
    qt_sql_181_strict "${const_sql_181}"
    testFoldConst("${const_sql_181}")
    def const_sql_182 = """select cast(cast("-99999.9003" as double) as decimalv3(9, 3));"""
    qt_sql_182_strict "${const_sql_182}"
    testFoldConst("${const_sql_182}")
    def const_sql_183 = """select cast(cast("-99999.9013" as double) as decimalv3(9, 3));"""
    qt_sql_183_strict "${const_sql_183}"
    testFoldConst("${const_sql_183}")
    def const_sql_184 = """select cast(cast("-99999.9983" as double) as decimalv3(9, 3));"""
    qt_sql_184_strict "${const_sql_184}"
    testFoldConst("${const_sql_184}")
    def const_sql_185 = """select cast(cast("-99999.9993" as double) as decimalv3(9, 3));"""
    qt_sql_185_strict "${const_sql_185}"
    testFoldConst("${const_sql_185}")
    def const_sql_186 = """select cast(cast("-900000.0003" as double) as decimalv3(9, 3));"""
    qt_sql_186_strict "${const_sql_186}"
    testFoldConst("${const_sql_186}")
    def const_sql_187 = """select cast(cast("-900000.0013" as double) as decimalv3(9, 3));"""
    qt_sql_187_strict "${const_sql_187}"
    testFoldConst("${const_sql_187}")
    def const_sql_188 = """select cast(cast("-900000.0093" as double) as decimalv3(9, 3));"""
    qt_sql_188_strict "${const_sql_188}"
    testFoldConst("${const_sql_188}")
    def const_sql_189 = """select cast(cast("-900000.0993" as double) as decimalv3(9, 3));"""
    qt_sql_189_strict "${const_sql_189}"
    testFoldConst("${const_sql_189}")
    def const_sql_190 = """select cast(cast("-900000.9003" as double) as decimalv3(9, 3));"""
    qt_sql_190_strict "${const_sql_190}"
    testFoldConst("${const_sql_190}")
    def const_sql_191 = """select cast(cast("-900000.9013" as double) as decimalv3(9, 3));"""
    qt_sql_191_strict "${const_sql_191}"
    testFoldConst("${const_sql_191}")
    def const_sql_192 = """select cast(cast("-900000.9983" as double) as decimalv3(9, 3));"""
    qt_sql_192_strict "${const_sql_192}"
    testFoldConst("${const_sql_192}")
    def const_sql_193 = """select cast(cast("-900000.9993" as double) as decimalv3(9, 3));"""
    qt_sql_193_strict "${const_sql_193}"
    testFoldConst("${const_sql_193}")
    def const_sql_194 = """select cast(cast("-900001.0003" as double) as decimalv3(9, 3));"""
    qt_sql_194_strict "${const_sql_194}"
    testFoldConst("${const_sql_194}")
    def const_sql_195 = """select cast(cast("-900001.0013" as double) as decimalv3(9, 3));"""
    qt_sql_195_strict "${const_sql_195}"
    testFoldConst("${const_sql_195}")
    def const_sql_196 = """select cast(cast("-900001.0093" as double) as decimalv3(9, 3));"""
    qt_sql_196_strict "${const_sql_196}"
    testFoldConst("${const_sql_196}")
    def const_sql_197 = """select cast(cast("-900001.0993" as double) as decimalv3(9, 3));"""
    qt_sql_197_strict "${const_sql_197}"
    testFoldConst("${const_sql_197}")
    def const_sql_198 = """select cast(cast("-900001.9003" as double) as decimalv3(9, 3));"""
    qt_sql_198_strict "${const_sql_198}"
    testFoldConst("${const_sql_198}")
    def const_sql_199 = """select cast(cast("-900001.9013" as double) as decimalv3(9, 3));"""
    qt_sql_199_strict "${const_sql_199}"
    testFoldConst("${const_sql_199}")
    def const_sql_200 = """select cast(cast("-900001.9983" as double) as decimalv3(9, 3));"""
    qt_sql_200_strict "${const_sql_200}"
    testFoldConst("${const_sql_200}")
    def const_sql_201 = """select cast(cast("-900001.9993" as double) as decimalv3(9, 3));"""
    qt_sql_201_strict "${const_sql_201}"
    testFoldConst("${const_sql_201}")
    def const_sql_202 = """select cast(cast("-999998.0003" as double) as decimalv3(9, 3));"""
    qt_sql_202_strict "${const_sql_202}"
    testFoldConst("${const_sql_202}")
    def const_sql_203 = """select cast(cast("-999998.0013" as double) as decimalv3(9, 3));"""
    qt_sql_203_strict "${const_sql_203}"
    testFoldConst("${const_sql_203}")
    def const_sql_204 = """select cast(cast("-999998.0093" as double) as decimalv3(9, 3));"""
    qt_sql_204_strict "${const_sql_204}"
    testFoldConst("${const_sql_204}")
    def const_sql_205 = """select cast(cast("-999998.0993" as double) as decimalv3(9, 3));"""
    qt_sql_205_strict "${const_sql_205}"
    testFoldConst("${const_sql_205}")
    def const_sql_206 = """select cast(cast("-999998.9003" as double) as decimalv3(9, 3));"""
    qt_sql_206_strict "${const_sql_206}"
    testFoldConst("${const_sql_206}")
    def const_sql_207 = """select cast(cast("-999998.9013" as double) as decimalv3(9, 3));"""
    qt_sql_207_strict "${const_sql_207}"
    testFoldConst("${const_sql_207}")
    def const_sql_208 = """select cast(cast("-999998.9983" as double) as decimalv3(9, 3));"""
    qt_sql_208_strict "${const_sql_208}"
    testFoldConst("${const_sql_208}")
    def const_sql_209 = """select cast(cast("-999998.9993" as double) as decimalv3(9, 3));"""
    qt_sql_209_strict "${const_sql_209}"
    testFoldConst("${const_sql_209}")
    def const_sql_210 = """select cast(cast("-999999.0003" as double) as decimalv3(9, 3));"""
    qt_sql_210_strict "${const_sql_210}"
    testFoldConst("${const_sql_210}")
    def const_sql_211 = """select cast(cast("-999999.0013" as double) as decimalv3(9, 3));"""
    qt_sql_211_strict "${const_sql_211}"
    testFoldConst("${const_sql_211}")
    def const_sql_212 = """select cast(cast("-999999.0093" as double) as decimalv3(9, 3));"""
    qt_sql_212_strict "${const_sql_212}"
    testFoldConst("${const_sql_212}")
    def const_sql_213 = """select cast(cast("-999999.0993" as double) as decimalv3(9, 3));"""
    qt_sql_213_strict "${const_sql_213}"
    testFoldConst("${const_sql_213}")
    def const_sql_214 = """select cast(cast("-999999.9003" as double) as decimalv3(9, 3));"""
    qt_sql_214_strict "${const_sql_214}"
    testFoldConst("${const_sql_214}")
    def const_sql_215 = """select cast(cast("-999999.9013" as double) as decimalv3(9, 3));"""
    qt_sql_215_strict "${const_sql_215}"
    testFoldConst("${const_sql_215}")
    def const_sql_216 = """select cast(cast("-999999.9983" as double) as decimalv3(9, 3));"""
    qt_sql_216_strict "${const_sql_216}"
    testFoldConst("${const_sql_216}")
    def const_sql_217 = """select cast(cast("-0.0009" as double) as decimalv3(9, 3));"""
    qt_sql_217_strict "${const_sql_217}"
    testFoldConst("${const_sql_217}")
    def const_sql_218 = """select cast(cast("-0.0019" as double) as decimalv3(9, 3));"""
    qt_sql_218_strict "${const_sql_218}"
    testFoldConst("${const_sql_218}")
    def const_sql_219 = """select cast(cast("-0.0099" as double) as decimalv3(9, 3));"""
    qt_sql_219_strict "${const_sql_219}"
    testFoldConst("${const_sql_219}")
    def const_sql_220 = """select cast(cast("-0.0999" as double) as decimalv3(9, 3));"""
    qt_sql_220_strict "${const_sql_220}"
    testFoldConst("${const_sql_220}")
    def const_sql_221 = """select cast(cast("-0.9009" as double) as decimalv3(9, 3));"""
    qt_sql_221_strict "${const_sql_221}"
    testFoldConst("${const_sql_221}")
    def const_sql_222 = """select cast(cast("-0.9019" as double) as decimalv3(9, 3));"""
    qt_sql_222_strict "${const_sql_222}"
    testFoldConst("${const_sql_222}")
    def const_sql_223 = """select cast(cast("-0.9989" as double) as decimalv3(9, 3));"""
    qt_sql_223_strict "${const_sql_223}"
    testFoldConst("${const_sql_223}")
    def const_sql_224 = """select cast(cast("-0.9999" as double) as decimalv3(9, 3));"""
    qt_sql_224_strict "${const_sql_224}"
    testFoldConst("${const_sql_224}")
    def const_sql_225 = """select cast(cast("-1.0009" as double) as decimalv3(9, 3));"""
    qt_sql_225_strict "${const_sql_225}"
    testFoldConst("${const_sql_225}")
    def const_sql_226 = """select cast(cast("-1.0019" as double) as decimalv3(9, 3));"""
    qt_sql_226_strict "${const_sql_226}"
    testFoldConst("${const_sql_226}")
    def const_sql_227 = """select cast(cast("-1.0099" as double) as decimalv3(9, 3));"""
    qt_sql_227_strict "${const_sql_227}"
    testFoldConst("${const_sql_227}")
    def const_sql_228 = """select cast(cast("-1.0999" as double) as decimalv3(9, 3));"""
    qt_sql_228_strict "${const_sql_228}"
    testFoldConst("${const_sql_228}")
    def const_sql_229 = """select cast(cast("-1.9009" as double) as decimalv3(9, 3));"""
    qt_sql_229_strict "${const_sql_229}"
    testFoldConst("${const_sql_229}")
    def const_sql_230 = """select cast(cast("-1.9019" as double) as decimalv3(9, 3));"""
    qt_sql_230_strict "${const_sql_230}"
    testFoldConst("${const_sql_230}")
    def const_sql_231 = """select cast(cast("-1.9989" as double) as decimalv3(9, 3));"""
    qt_sql_231_strict "${const_sql_231}"
    testFoldConst("${const_sql_231}")
    def const_sql_232 = """select cast(cast("-1.9999" as double) as decimalv3(9, 3));"""
    qt_sql_232_strict "${const_sql_232}"
    testFoldConst("${const_sql_232}")
    def const_sql_233 = """select cast(cast("-9.0009" as double) as decimalv3(9, 3));"""
    qt_sql_233_strict "${const_sql_233}"
    testFoldConst("${const_sql_233}")
    def const_sql_234 = """select cast(cast("-9.0019" as double) as decimalv3(9, 3));"""
    qt_sql_234_strict "${const_sql_234}"
    testFoldConst("${const_sql_234}")
    def const_sql_235 = """select cast(cast("-9.0099" as double) as decimalv3(9, 3));"""
    qt_sql_235_strict "${const_sql_235}"
    testFoldConst("${const_sql_235}")
    def const_sql_236 = """select cast(cast("-9.0999" as double) as decimalv3(9, 3));"""
    qt_sql_236_strict "${const_sql_236}"
    testFoldConst("${const_sql_236}")
    def const_sql_237 = """select cast(cast("-9.9009" as double) as decimalv3(9, 3));"""
    qt_sql_237_strict "${const_sql_237}"
    testFoldConst("${const_sql_237}")
    def const_sql_238 = """select cast(cast("-9.9019" as double) as decimalv3(9, 3));"""
    qt_sql_238_strict "${const_sql_238}"
    testFoldConst("${const_sql_238}")
    def const_sql_239 = """select cast(cast("-9.9989" as double) as decimalv3(9, 3));"""
    qt_sql_239_strict "${const_sql_239}"
    testFoldConst("${const_sql_239}")
    def const_sql_240 = """select cast(cast("-9.9999" as double) as decimalv3(9, 3));"""
    qt_sql_240_strict "${const_sql_240}"
    testFoldConst("${const_sql_240}")
    def const_sql_241 = """select cast(cast("-99999.0009" as double) as decimalv3(9, 3));"""
    qt_sql_241_strict "${const_sql_241}"
    testFoldConst("${const_sql_241}")
    def const_sql_242 = """select cast(cast("-99999.0019" as double) as decimalv3(9, 3));"""
    qt_sql_242_strict "${const_sql_242}"
    testFoldConst("${const_sql_242}")
    def const_sql_243 = """select cast(cast("-99999.0099" as double) as decimalv3(9, 3));"""
    qt_sql_243_strict "${const_sql_243}"
    testFoldConst("${const_sql_243}")
    def const_sql_244 = """select cast(cast("-99999.0999" as double) as decimalv3(9, 3));"""
    qt_sql_244_strict "${const_sql_244}"
    testFoldConst("${const_sql_244}")
    def const_sql_245 = """select cast(cast("-99999.9009" as double) as decimalv3(9, 3));"""
    qt_sql_245_strict "${const_sql_245}"
    testFoldConst("${const_sql_245}")
    def const_sql_246 = """select cast(cast("-99999.9019" as double) as decimalv3(9, 3));"""
    qt_sql_246_strict "${const_sql_246}"
    testFoldConst("${const_sql_246}")
    def const_sql_247 = """select cast(cast("-99999.9989" as double) as decimalv3(9, 3));"""
    qt_sql_247_strict "${const_sql_247}"
    testFoldConst("${const_sql_247}")
    def const_sql_248 = """select cast(cast("-99999.9999" as double) as decimalv3(9, 3));"""
    qt_sql_248_strict "${const_sql_248}"
    testFoldConst("${const_sql_248}")
    def const_sql_249 = """select cast(cast("-900000.0009" as double) as decimalv3(9, 3));"""
    qt_sql_249_strict "${const_sql_249}"
    testFoldConst("${const_sql_249}")
    def const_sql_250 = """select cast(cast("-900000.0019" as double) as decimalv3(9, 3));"""
    qt_sql_250_strict "${const_sql_250}"
    testFoldConst("${const_sql_250}")
    def const_sql_251 = """select cast(cast("-900000.0099" as double) as decimalv3(9, 3));"""
    qt_sql_251_strict "${const_sql_251}"
    testFoldConst("${const_sql_251}")
    def const_sql_252 = """select cast(cast("-900000.0999" as double) as decimalv3(9, 3));"""
    qt_sql_252_strict "${const_sql_252}"
    testFoldConst("${const_sql_252}")
    def const_sql_253 = """select cast(cast("-900000.9009" as double) as decimalv3(9, 3));"""
    qt_sql_253_strict "${const_sql_253}"
    testFoldConst("${const_sql_253}")
    def const_sql_254 = """select cast(cast("-900000.9019" as double) as decimalv3(9, 3));"""
    qt_sql_254_strict "${const_sql_254}"
    testFoldConst("${const_sql_254}")
    def const_sql_255 = """select cast(cast("-900000.9989" as double) as decimalv3(9, 3));"""
    qt_sql_255_strict "${const_sql_255}"
    testFoldConst("${const_sql_255}")
    def const_sql_256 = """select cast(cast("-900000.9999" as double) as decimalv3(9, 3));"""
    qt_sql_256_strict "${const_sql_256}"
    testFoldConst("${const_sql_256}")
    def const_sql_257 = """select cast(cast("-900001.0009" as double) as decimalv3(9, 3));"""
    qt_sql_257_strict "${const_sql_257}"
    testFoldConst("${const_sql_257}")
    def const_sql_258 = """select cast(cast("-900001.0019" as double) as decimalv3(9, 3));"""
    qt_sql_258_strict "${const_sql_258}"
    testFoldConst("${const_sql_258}")
    def const_sql_259 = """select cast(cast("-900001.0099" as double) as decimalv3(9, 3));"""
    qt_sql_259_strict "${const_sql_259}"
    testFoldConst("${const_sql_259}")
    def const_sql_260 = """select cast(cast("-900001.0999" as double) as decimalv3(9, 3));"""
    qt_sql_260_strict "${const_sql_260}"
    testFoldConst("${const_sql_260}")
    def const_sql_261 = """select cast(cast("-900001.9009" as double) as decimalv3(9, 3));"""
    qt_sql_261_strict "${const_sql_261}"
    testFoldConst("${const_sql_261}")
    def const_sql_262 = """select cast(cast("-900001.9019" as double) as decimalv3(9, 3));"""
    qt_sql_262_strict "${const_sql_262}"
    testFoldConst("${const_sql_262}")
    def const_sql_263 = """select cast(cast("-900001.9989" as double) as decimalv3(9, 3));"""
    qt_sql_263_strict "${const_sql_263}"
    testFoldConst("${const_sql_263}")
    def const_sql_264 = """select cast(cast("-900001.9999" as double) as decimalv3(9, 3));"""
    qt_sql_264_strict "${const_sql_264}"
    testFoldConst("${const_sql_264}")
    def const_sql_265 = """select cast(cast("-999998.0009" as double) as decimalv3(9, 3));"""
    qt_sql_265_strict "${const_sql_265}"
    testFoldConst("${const_sql_265}")
    def const_sql_266 = """select cast(cast("-999998.0019" as double) as decimalv3(9, 3));"""
    qt_sql_266_strict "${const_sql_266}"
    testFoldConst("${const_sql_266}")
    def const_sql_267 = """select cast(cast("-999998.0099" as double) as decimalv3(9, 3));"""
    qt_sql_267_strict "${const_sql_267}"
    testFoldConst("${const_sql_267}")
    def const_sql_268 = """select cast(cast("-999998.0999" as double) as decimalv3(9, 3));"""
    qt_sql_268_strict "${const_sql_268}"
    testFoldConst("${const_sql_268}")
    def const_sql_269 = """select cast(cast("-999998.9009" as double) as decimalv3(9, 3));"""
    qt_sql_269_strict "${const_sql_269}"
    testFoldConst("${const_sql_269}")
    def const_sql_270 = """select cast(cast("-999998.9019" as double) as decimalv3(9, 3));"""
    qt_sql_270_strict "${const_sql_270}"
    testFoldConst("${const_sql_270}")
    def const_sql_271 = """select cast(cast("-999998.9989" as double) as decimalv3(9, 3));"""
    qt_sql_271_strict "${const_sql_271}"
    testFoldConst("${const_sql_271}")
    def const_sql_272 = """select cast(cast("-999998.9999" as double) as decimalv3(9, 3));"""
    qt_sql_272_strict "${const_sql_272}"
    testFoldConst("${const_sql_272}")
    def const_sql_273 = """select cast(cast("-999999.0003" as double) as decimalv3(9, 3));"""
    qt_sql_273_strict "${const_sql_273}"
    testFoldConst("${const_sql_273}")
    def const_sql_274 = """select cast(cast("-999999.0013" as double) as decimalv3(9, 3));"""
    qt_sql_274_strict "${const_sql_274}"
    testFoldConst("${const_sql_274}")
    def const_sql_275 = """select cast(cast("-999999.0093" as double) as decimalv3(9, 3));"""
    qt_sql_275_strict "${const_sql_275}"
    testFoldConst("${const_sql_275}")
    def const_sql_276 = """select cast(cast("-999999.0993" as double) as decimalv3(9, 3));"""
    qt_sql_276_strict "${const_sql_276}"
    testFoldConst("${const_sql_276}")
    def const_sql_277 = """select cast(cast("-999999.9003" as double) as decimalv3(9, 3));"""
    qt_sql_277_strict "${const_sql_277}"
    testFoldConst("${const_sql_277}")
    def const_sql_278 = """select cast(cast("-999999.9013" as double) as decimalv3(9, 3));"""
    qt_sql_278_strict "${const_sql_278}"
    testFoldConst("${const_sql_278}")
    def const_sql_279 = """select cast(cast("-999999.9983" as double) as decimalv3(9, 3));"""
    qt_sql_279_strict "${const_sql_279}"
    testFoldConst("${const_sql_279}")
    sql "set enable_strict_cast=false;"
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
    qt_sql_66_non_strict "${const_sql_66}"
    testFoldConst("${const_sql_66}")
    qt_sql_67_non_strict "${const_sql_67}"
    testFoldConst("${const_sql_67}")
    qt_sql_68_non_strict "${const_sql_68}"
    testFoldConst("${const_sql_68}")
    qt_sql_69_non_strict "${const_sql_69}"
    testFoldConst("${const_sql_69}")
    qt_sql_70_non_strict "${const_sql_70}"
    testFoldConst("${const_sql_70}")
    qt_sql_71_non_strict "${const_sql_71}"
    testFoldConst("${const_sql_71}")
    qt_sql_72_non_strict "${const_sql_72}"
    testFoldConst("${const_sql_72}")
    qt_sql_73_non_strict "${const_sql_73}"
    testFoldConst("${const_sql_73}")
    qt_sql_74_non_strict "${const_sql_74}"
    testFoldConst("${const_sql_74}")
    qt_sql_75_non_strict "${const_sql_75}"
    testFoldConst("${const_sql_75}")
    qt_sql_76_non_strict "${const_sql_76}"
    testFoldConst("${const_sql_76}")
    qt_sql_77_non_strict "${const_sql_77}"
    testFoldConst("${const_sql_77}")
    qt_sql_78_non_strict "${const_sql_78}"
    testFoldConst("${const_sql_78}")
    qt_sql_79_non_strict "${const_sql_79}"
    testFoldConst("${const_sql_79}")
    qt_sql_80_non_strict "${const_sql_80}"
    testFoldConst("${const_sql_80}")
    qt_sql_81_non_strict "${const_sql_81}"
    testFoldConst("${const_sql_81}")
    qt_sql_82_non_strict "${const_sql_82}"
    testFoldConst("${const_sql_82}")
    qt_sql_83_non_strict "${const_sql_83}"
    testFoldConst("${const_sql_83}")
    qt_sql_84_non_strict "${const_sql_84}"
    testFoldConst("${const_sql_84}")
    qt_sql_85_non_strict "${const_sql_85}"
    testFoldConst("${const_sql_85}")
    qt_sql_86_non_strict "${const_sql_86}"
    testFoldConst("${const_sql_86}")
    qt_sql_87_non_strict "${const_sql_87}"
    testFoldConst("${const_sql_87}")
    qt_sql_88_non_strict "${const_sql_88}"
    testFoldConst("${const_sql_88}")
    qt_sql_89_non_strict "${const_sql_89}"
    testFoldConst("${const_sql_89}")
    qt_sql_90_non_strict "${const_sql_90}"
    testFoldConst("${const_sql_90}")
    qt_sql_91_non_strict "${const_sql_91}"
    testFoldConst("${const_sql_91}")
    qt_sql_92_non_strict "${const_sql_92}"
    testFoldConst("${const_sql_92}")
    qt_sql_93_non_strict "${const_sql_93}"
    testFoldConst("${const_sql_93}")
    qt_sql_94_non_strict "${const_sql_94}"
    testFoldConst("${const_sql_94}")
    qt_sql_95_non_strict "${const_sql_95}"
    testFoldConst("${const_sql_95}")
    qt_sql_96_non_strict "${const_sql_96}"
    testFoldConst("${const_sql_96}")
    qt_sql_97_non_strict "${const_sql_97}"
    testFoldConst("${const_sql_97}")
    qt_sql_98_non_strict "${const_sql_98}"
    testFoldConst("${const_sql_98}")
    qt_sql_99_non_strict "${const_sql_99}"
    testFoldConst("${const_sql_99}")
    qt_sql_100_non_strict "${const_sql_100}"
    testFoldConst("${const_sql_100}")
    qt_sql_101_non_strict "${const_sql_101}"
    testFoldConst("${const_sql_101}")
    qt_sql_102_non_strict "${const_sql_102}"
    testFoldConst("${const_sql_102}")
    qt_sql_103_non_strict "${const_sql_103}"
    testFoldConst("${const_sql_103}")
    qt_sql_104_non_strict "${const_sql_104}"
    testFoldConst("${const_sql_104}")
    qt_sql_105_non_strict "${const_sql_105}"
    testFoldConst("${const_sql_105}")
    qt_sql_106_non_strict "${const_sql_106}"
    testFoldConst("${const_sql_106}")
    qt_sql_107_non_strict "${const_sql_107}"
    testFoldConst("${const_sql_107}")
    qt_sql_108_non_strict "${const_sql_108}"
    testFoldConst("${const_sql_108}")
    qt_sql_109_non_strict "${const_sql_109}"
    testFoldConst("${const_sql_109}")
    qt_sql_110_non_strict "${const_sql_110}"
    testFoldConst("${const_sql_110}")
    qt_sql_111_non_strict "${const_sql_111}"
    testFoldConst("${const_sql_111}")
    qt_sql_112_non_strict "${const_sql_112}"
    testFoldConst("${const_sql_112}")
    qt_sql_113_non_strict "${const_sql_113}"
    testFoldConst("${const_sql_113}")
    qt_sql_114_non_strict "${const_sql_114}"
    testFoldConst("${const_sql_114}")
    qt_sql_115_non_strict "${const_sql_115}"
    testFoldConst("${const_sql_115}")
    qt_sql_116_non_strict "${const_sql_116}"
    testFoldConst("${const_sql_116}")
    qt_sql_117_non_strict "${const_sql_117}"
    testFoldConst("${const_sql_117}")
    qt_sql_118_non_strict "${const_sql_118}"
    testFoldConst("${const_sql_118}")
    qt_sql_119_non_strict "${const_sql_119}"
    testFoldConst("${const_sql_119}")
    qt_sql_120_non_strict "${const_sql_120}"
    testFoldConst("${const_sql_120}")
    qt_sql_121_non_strict "${const_sql_121}"
    testFoldConst("${const_sql_121}")
    qt_sql_122_non_strict "${const_sql_122}"
    testFoldConst("${const_sql_122}")
    qt_sql_123_non_strict "${const_sql_123}"
    testFoldConst("${const_sql_123}")
    qt_sql_124_non_strict "${const_sql_124}"
    testFoldConst("${const_sql_124}")
    qt_sql_125_non_strict "${const_sql_125}"
    testFoldConst("${const_sql_125}")
    qt_sql_126_non_strict "${const_sql_126}"
    testFoldConst("${const_sql_126}")
    qt_sql_127_non_strict "${const_sql_127}"
    testFoldConst("${const_sql_127}")
    qt_sql_128_non_strict "${const_sql_128}"
    testFoldConst("${const_sql_128}")
    qt_sql_129_non_strict "${const_sql_129}"
    testFoldConst("${const_sql_129}")
    qt_sql_130_non_strict "${const_sql_130}"
    testFoldConst("${const_sql_130}")
    qt_sql_131_non_strict "${const_sql_131}"
    testFoldConst("${const_sql_131}")
    qt_sql_132_non_strict "${const_sql_132}"
    testFoldConst("${const_sql_132}")
    qt_sql_133_non_strict "${const_sql_133}"
    testFoldConst("${const_sql_133}")
    qt_sql_134_non_strict "${const_sql_134}"
    testFoldConst("${const_sql_134}")
    qt_sql_135_non_strict "${const_sql_135}"
    testFoldConst("${const_sql_135}")
    qt_sql_136_non_strict "${const_sql_136}"
    testFoldConst("${const_sql_136}")
    qt_sql_137_non_strict "${const_sql_137}"
    testFoldConst("${const_sql_137}")
    qt_sql_138_non_strict "${const_sql_138}"
    testFoldConst("${const_sql_138}")
    qt_sql_139_non_strict "${const_sql_139}"
    testFoldConst("${const_sql_139}")
    qt_sql_140_non_strict "${const_sql_140}"
    testFoldConst("${const_sql_140}")
    qt_sql_141_non_strict "${const_sql_141}"
    testFoldConst("${const_sql_141}")
    qt_sql_142_non_strict "${const_sql_142}"
    testFoldConst("${const_sql_142}")
    qt_sql_143_non_strict "${const_sql_143}"
    testFoldConst("${const_sql_143}")
    qt_sql_144_non_strict "${const_sql_144}"
    testFoldConst("${const_sql_144}")
    qt_sql_145_non_strict "${const_sql_145}"
    testFoldConst("${const_sql_145}")
    qt_sql_146_non_strict "${const_sql_146}"
    testFoldConst("${const_sql_146}")
    qt_sql_147_non_strict "${const_sql_147}"
    testFoldConst("${const_sql_147}")
    qt_sql_148_non_strict "${const_sql_148}"
    testFoldConst("${const_sql_148}")
    qt_sql_149_non_strict "${const_sql_149}"
    testFoldConst("${const_sql_149}")
    qt_sql_150_non_strict "${const_sql_150}"
    testFoldConst("${const_sql_150}")
    qt_sql_151_non_strict "${const_sql_151}"
    testFoldConst("${const_sql_151}")
    qt_sql_152_non_strict "${const_sql_152}"
    testFoldConst("${const_sql_152}")
    qt_sql_153_non_strict "${const_sql_153}"
    testFoldConst("${const_sql_153}")
    qt_sql_154_non_strict "${const_sql_154}"
    testFoldConst("${const_sql_154}")
    qt_sql_155_non_strict "${const_sql_155}"
    testFoldConst("${const_sql_155}")
    qt_sql_156_non_strict "${const_sql_156}"
    testFoldConst("${const_sql_156}")
    qt_sql_157_non_strict "${const_sql_157}"
    testFoldConst("${const_sql_157}")
    qt_sql_158_non_strict "${const_sql_158}"
    testFoldConst("${const_sql_158}")
    qt_sql_159_non_strict "${const_sql_159}"
    testFoldConst("${const_sql_159}")
    qt_sql_160_non_strict "${const_sql_160}"
    testFoldConst("${const_sql_160}")
    qt_sql_161_non_strict "${const_sql_161}"
    testFoldConst("${const_sql_161}")
    qt_sql_162_non_strict "${const_sql_162}"
    testFoldConst("${const_sql_162}")
    qt_sql_163_non_strict "${const_sql_163}"
    testFoldConst("${const_sql_163}")
    qt_sql_164_non_strict "${const_sql_164}"
    testFoldConst("${const_sql_164}")
    qt_sql_165_non_strict "${const_sql_165}"
    testFoldConst("${const_sql_165}")
    qt_sql_166_non_strict "${const_sql_166}"
    testFoldConst("${const_sql_166}")
    qt_sql_167_non_strict "${const_sql_167}"
    testFoldConst("${const_sql_167}")
    qt_sql_168_non_strict "${const_sql_168}"
    testFoldConst("${const_sql_168}")
    qt_sql_169_non_strict "${const_sql_169}"
    testFoldConst("${const_sql_169}")
    qt_sql_170_non_strict "${const_sql_170}"
    testFoldConst("${const_sql_170}")
    qt_sql_171_non_strict "${const_sql_171}"
    testFoldConst("${const_sql_171}")
    qt_sql_172_non_strict "${const_sql_172}"
    testFoldConst("${const_sql_172}")
    qt_sql_173_non_strict "${const_sql_173}"
    testFoldConst("${const_sql_173}")
    qt_sql_174_non_strict "${const_sql_174}"
    testFoldConst("${const_sql_174}")
    qt_sql_175_non_strict "${const_sql_175}"
    testFoldConst("${const_sql_175}")
    qt_sql_176_non_strict "${const_sql_176}"
    testFoldConst("${const_sql_176}")
    qt_sql_177_non_strict "${const_sql_177}"
    testFoldConst("${const_sql_177}")
    qt_sql_178_non_strict "${const_sql_178}"
    testFoldConst("${const_sql_178}")
    qt_sql_179_non_strict "${const_sql_179}"
    testFoldConst("${const_sql_179}")
    qt_sql_180_non_strict "${const_sql_180}"
    testFoldConst("${const_sql_180}")
    qt_sql_181_non_strict "${const_sql_181}"
    testFoldConst("${const_sql_181}")
    qt_sql_182_non_strict "${const_sql_182}"
    testFoldConst("${const_sql_182}")
    qt_sql_183_non_strict "${const_sql_183}"
    testFoldConst("${const_sql_183}")
    qt_sql_184_non_strict "${const_sql_184}"
    testFoldConst("${const_sql_184}")
    qt_sql_185_non_strict "${const_sql_185}"
    testFoldConst("${const_sql_185}")
    qt_sql_186_non_strict "${const_sql_186}"
    testFoldConst("${const_sql_186}")
    qt_sql_187_non_strict "${const_sql_187}"
    testFoldConst("${const_sql_187}")
    qt_sql_188_non_strict "${const_sql_188}"
    testFoldConst("${const_sql_188}")
    qt_sql_189_non_strict "${const_sql_189}"
    testFoldConst("${const_sql_189}")
    qt_sql_190_non_strict "${const_sql_190}"
    testFoldConst("${const_sql_190}")
    qt_sql_191_non_strict "${const_sql_191}"
    testFoldConst("${const_sql_191}")
    qt_sql_192_non_strict "${const_sql_192}"
    testFoldConst("${const_sql_192}")
    qt_sql_193_non_strict "${const_sql_193}"
    testFoldConst("${const_sql_193}")
    qt_sql_194_non_strict "${const_sql_194}"
    testFoldConst("${const_sql_194}")
    qt_sql_195_non_strict "${const_sql_195}"
    testFoldConst("${const_sql_195}")
    qt_sql_196_non_strict "${const_sql_196}"
    testFoldConst("${const_sql_196}")
    qt_sql_197_non_strict "${const_sql_197}"
    testFoldConst("${const_sql_197}")
    qt_sql_198_non_strict "${const_sql_198}"
    testFoldConst("${const_sql_198}")
    qt_sql_199_non_strict "${const_sql_199}"
    testFoldConst("${const_sql_199}")
    qt_sql_200_non_strict "${const_sql_200}"
    testFoldConst("${const_sql_200}")
    qt_sql_201_non_strict "${const_sql_201}"
    testFoldConst("${const_sql_201}")
    qt_sql_202_non_strict "${const_sql_202}"
    testFoldConst("${const_sql_202}")
    qt_sql_203_non_strict "${const_sql_203}"
    testFoldConst("${const_sql_203}")
    qt_sql_204_non_strict "${const_sql_204}"
    testFoldConst("${const_sql_204}")
    qt_sql_205_non_strict "${const_sql_205}"
    testFoldConst("${const_sql_205}")
    qt_sql_206_non_strict "${const_sql_206}"
    testFoldConst("${const_sql_206}")
    qt_sql_207_non_strict "${const_sql_207}"
    testFoldConst("${const_sql_207}")
    qt_sql_208_non_strict "${const_sql_208}"
    testFoldConst("${const_sql_208}")
    qt_sql_209_non_strict "${const_sql_209}"
    testFoldConst("${const_sql_209}")
    qt_sql_210_non_strict "${const_sql_210}"
    testFoldConst("${const_sql_210}")
    qt_sql_211_non_strict "${const_sql_211}"
    testFoldConst("${const_sql_211}")
    qt_sql_212_non_strict "${const_sql_212}"
    testFoldConst("${const_sql_212}")
    qt_sql_213_non_strict "${const_sql_213}"
    testFoldConst("${const_sql_213}")
    qt_sql_214_non_strict "${const_sql_214}"
    testFoldConst("${const_sql_214}")
    qt_sql_215_non_strict "${const_sql_215}"
    testFoldConst("${const_sql_215}")
    qt_sql_216_non_strict "${const_sql_216}"
    testFoldConst("${const_sql_216}")
    qt_sql_217_non_strict "${const_sql_217}"
    testFoldConst("${const_sql_217}")
    qt_sql_218_non_strict "${const_sql_218}"
    testFoldConst("${const_sql_218}")
    qt_sql_219_non_strict "${const_sql_219}"
    testFoldConst("${const_sql_219}")
    qt_sql_220_non_strict "${const_sql_220}"
    testFoldConst("${const_sql_220}")
    qt_sql_221_non_strict "${const_sql_221}"
    testFoldConst("${const_sql_221}")
    qt_sql_222_non_strict "${const_sql_222}"
    testFoldConst("${const_sql_222}")
    qt_sql_223_non_strict "${const_sql_223}"
    testFoldConst("${const_sql_223}")
    qt_sql_224_non_strict "${const_sql_224}"
    testFoldConst("${const_sql_224}")
    qt_sql_225_non_strict "${const_sql_225}"
    testFoldConst("${const_sql_225}")
    qt_sql_226_non_strict "${const_sql_226}"
    testFoldConst("${const_sql_226}")
    qt_sql_227_non_strict "${const_sql_227}"
    testFoldConst("${const_sql_227}")
    qt_sql_228_non_strict "${const_sql_228}"
    testFoldConst("${const_sql_228}")
    qt_sql_229_non_strict "${const_sql_229}"
    testFoldConst("${const_sql_229}")
    qt_sql_230_non_strict "${const_sql_230}"
    testFoldConst("${const_sql_230}")
    qt_sql_231_non_strict "${const_sql_231}"
    testFoldConst("${const_sql_231}")
    qt_sql_232_non_strict "${const_sql_232}"
    testFoldConst("${const_sql_232}")
    qt_sql_233_non_strict "${const_sql_233}"
    testFoldConst("${const_sql_233}")
    qt_sql_234_non_strict "${const_sql_234}"
    testFoldConst("${const_sql_234}")
    qt_sql_235_non_strict "${const_sql_235}"
    testFoldConst("${const_sql_235}")
    qt_sql_236_non_strict "${const_sql_236}"
    testFoldConst("${const_sql_236}")
    qt_sql_237_non_strict "${const_sql_237}"
    testFoldConst("${const_sql_237}")
    qt_sql_238_non_strict "${const_sql_238}"
    testFoldConst("${const_sql_238}")
    qt_sql_239_non_strict "${const_sql_239}"
    testFoldConst("${const_sql_239}")
    qt_sql_240_non_strict "${const_sql_240}"
    testFoldConst("${const_sql_240}")
    qt_sql_241_non_strict "${const_sql_241}"
    testFoldConst("${const_sql_241}")
    qt_sql_242_non_strict "${const_sql_242}"
    testFoldConst("${const_sql_242}")
    qt_sql_243_non_strict "${const_sql_243}"
    testFoldConst("${const_sql_243}")
    qt_sql_244_non_strict "${const_sql_244}"
    testFoldConst("${const_sql_244}")
    qt_sql_245_non_strict "${const_sql_245}"
    testFoldConst("${const_sql_245}")
    qt_sql_246_non_strict "${const_sql_246}"
    testFoldConst("${const_sql_246}")
    qt_sql_247_non_strict "${const_sql_247}"
    testFoldConst("${const_sql_247}")
    qt_sql_248_non_strict "${const_sql_248}"
    testFoldConst("${const_sql_248}")
    qt_sql_249_non_strict "${const_sql_249}"
    testFoldConst("${const_sql_249}")
    qt_sql_250_non_strict "${const_sql_250}"
    testFoldConst("${const_sql_250}")
    qt_sql_251_non_strict "${const_sql_251}"
    testFoldConst("${const_sql_251}")
    qt_sql_252_non_strict "${const_sql_252}"
    testFoldConst("${const_sql_252}")
    qt_sql_253_non_strict "${const_sql_253}"
    testFoldConst("${const_sql_253}")
    qt_sql_254_non_strict "${const_sql_254}"
    testFoldConst("${const_sql_254}")
    qt_sql_255_non_strict "${const_sql_255}"
    testFoldConst("${const_sql_255}")
    qt_sql_256_non_strict "${const_sql_256}"
    testFoldConst("${const_sql_256}")
    qt_sql_257_non_strict "${const_sql_257}"
    testFoldConst("${const_sql_257}")
    qt_sql_258_non_strict "${const_sql_258}"
    testFoldConst("${const_sql_258}")
    qt_sql_259_non_strict "${const_sql_259}"
    testFoldConst("${const_sql_259}")
    qt_sql_260_non_strict "${const_sql_260}"
    testFoldConst("${const_sql_260}")
    qt_sql_261_non_strict "${const_sql_261}"
    testFoldConst("${const_sql_261}")
    qt_sql_262_non_strict "${const_sql_262}"
    testFoldConst("${const_sql_262}")
    qt_sql_263_non_strict "${const_sql_263}"
    testFoldConst("${const_sql_263}")
    qt_sql_264_non_strict "${const_sql_264}"
    testFoldConst("${const_sql_264}")
    qt_sql_265_non_strict "${const_sql_265}"
    testFoldConst("${const_sql_265}")
    qt_sql_266_non_strict "${const_sql_266}"
    testFoldConst("${const_sql_266}")
    qt_sql_267_non_strict "${const_sql_267}"
    testFoldConst("${const_sql_267}")
    qt_sql_268_non_strict "${const_sql_268}"
    testFoldConst("${const_sql_268}")
    qt_sql_269_non_strict "${const_sql_269}"
    testFoldConst("${const_sql_269}")
    qt_sql_270_non_strict "${const_sql_270}"
    testFoldConst("${const_sql_270}")
    qt_sql_271_non_strict "${const_sql_271}"
    testFoldConst("${const_sql_271}")
    qt_sql_272_non_strict "${const_sql_272}"
    testFoldConst("${const_sql_272}")
    qt_sql_273_non_strict "${const_sql_273}"
    testFoldConst("${const_sql_273}")
    qt_sql_274_non_strict "${const_sql_274}"
    testFoldConst("${const_sql_274}")
    qt_sql_275_non_strict "${const_sql_275}"
    testFoldConst("${const_sql_275}")
    qt_sql_276_non_strict "${const_sql_276}"
    testFoldConst("${const_sql_276}")
    qt_sql_277_non_strict "${const_sql_277}"
    testFoldConst("${const_sql_277}")
    qt_sql_278_non_strict "${const_sql_278}"
    testFoldConst("${const_sql_278}")
    qt_sql_279_non_strict "${const_sql_279}"
    testFoldConst("${const_sql_279}")
    sql "set enable_strict_cast=true;"
    def const_sql_280 = """select cast(cast("0.000000003" as double) as decimalv3(9, 8));"""
    qt_sql_280_strict "${const_sql_280}"
    testFoldConst("${const_sql_280}")
    def const_sql_281 = """select cast(cast("0.000000013" as double) as decimalv3(9, 8));"""
    qt_sql_281_strict "${const_sql_281}"
    testFoldConst("${const_sql_281}")
    def const_sql_282 = """select cast(cast("0.000000093" as double) as decimalv3(9, 8));"""
    qt_sql_282_strict "${const_sql_282}"
    testFoldConst("${const_sql_282}")
    def const_sql_283 = """select cast(cast("0.099999993" as double) as decimalv3(9, 8));"""
    qt_sql_283_strict "${const_sql_283}"
    testFoldConst("${const_sql_283}")
    def const_sql_284 = """select cast(cast("0.900000003" as double) as decimalv3(9, 8));"""
    qt_sql_284_strict "${const_sql_284}"
    testFoldConst("${const_sql_284}")
    def const_sql_285 = """select cast(cast("0.900000013" as double) as decimalv3(9, 8));"""
    qt_sql_285_strict "${const_sql_285}"
    testFoldConst("${const_sql_285}")
    def const_sql_286 = """select cast(cast("0.999999983" as double) as decimalv3(9, 8));"""
    qt_sql_286_strict "${const_sql_286}"
    testFoldConst("${const_sql_286}")
    def const_sql_287 = """select cast(cast("0.999999993" as double) as decimalv3(9, 8));"""
    qt_sql_287_strict "${const_sql_287}"
    testFoldConst("${const_sql_287}")
    def const_sql_288 = """select cast(cast("1.000000003" as double) as decimalv3(9, 8));"""
    qt_sql_288_strict "${const_sql_288}"
    testFoldConst("${const_sql_288}")
    def const_sql_289 = """select cast(cast("1.000000013" as double) as decimalv3(9, 8));"""
    qt_sql_289_strict "${const_sql_289}"
    testFoldConst("${const_sql_289}")
    def const_sql_290 = """select cast(cast("1.000000093" as double) as decimalv3(9, 8));"""
    qt_sql_290_strict "${const_sql_290}"
    testFoldConst("${const_sql_290}")
    def const_sql_291 = """select cast(cast("1.099999993" as double) as decimalv3(9, 8));"""
    qt_sql_291_strict "${const_sql_291}"
    testFoldConst("${const_sql_291}")
    def const_sql_292 = """select cast(cast("1.900000003" as double) as decimalv3(9, 8));"""
    qt_sql_292_strict "${const_sql_292}"
    testFoldConst("${const_sql_292}")
    def const_sql_293 = """select cast(cast("1.900000013" as double) as decimalv3(9, 8));"""
    qt_sql_293_strict "${const_sql_293}"
    testFoldConst("${const_sql_293}")
    def const_sql_294 = """select cast(cast("1.999999983" as double) as decimalv3(9, 8));"""
    qt_sql_294_strict "${const_sql_294}"
    testFoldConst("${const_sql_294}")
    def const_sql_295 = """select cast(cast("1.999999993" as double) as decimalv3(9, 8));"""
    qt_sql_295_strict "${const_sql_295}"
    testFoldConst("${const_sql_295}")
    def const_sql_296 = """select cast(cast("8.000000003" as double) as decimalv3(9, 8));"""
    qt_sql_296_strict "${const_sql_296}"
    testFoldConst("${const_sql_296}")
    def const_sql_297 = """select cast(cast("8.000000013" as double) as decimalv3(9, 8));"""
    qt_sql_297_strict "${const_sql_297}"
    testFoldConst("${const_sql_297}")
    def const_sql_298 = """select cast(cast("8.000000093" as double) as decimalv3(9, 8));"""
    qt_sql_298_strict "${const_sql_298}"
    testFoldConst("${const_sql_298}")
    def const_sql_299 = """select cast(cast("8.099999993" as double) as decimalv3(9, 8));"""
    qt_sql_299_strict "${const_sql_299}"
    testFoldConst("${const_sql_299}")
    def const_sql_300 = """select cast(cast("8.900000003" as double) as decimalv3(9, 8));"""
    qt_sql_300_strict "${const_sql_300}"
    testFoldConst("${const_sql_300}")
    def const_sql_301 = """select cast(cast("8.900000013" as double) as decimalv3(9, 8));"""
    qt_sql_301_strict "${const_sql_301}"
    testFoldConst("${const_sql_301}")
    def const_sql_302 = """select cast(cast("8.999999983" as double) as decimalv3(9, 8));"""
    qt_sql_302_strict "${const_sql_302}"
    testFoldConst("${const_sql_302}")
    def const_sql_303 = """select cast(cast("8.999999993" as double) as decimalv3(9, 8));"""
    qt_sql_303_strict "${const_sql_303}"
    testFoldConst("${const_sql_303}")
    def const_sql_304 = """select cast(cast("9.000000003" as double) as decimalv3(9, 8));"""
    qt_sql_304_strict "${const_sql_304}"
    testFoldConst("${const_sql_304}")
    def const_sql_305 = """select cast(cast("9.000000013" as double) as decimalv3(9, 8));"""
    qt_sql_305_strict "${const_sql_305}"
    testFoldConst("${const_sql_305}")
    def const_sql_306 = """select cast(cast("9.000000093" as double) as decimalv3(9, 8));"""
    qt_sql_306_strict "${const_sql_306}"
    testFoldConst("${const_sql_306}")
    def const_sql_307 = """select cast(cast("9.099999993" as double) as decimalv3(9, 8));"""
    qt_sql_307_strict "${const_sql_307}"
    testFoldConst("${const_sql_307}")
    def const_sql_308 = """select cast(cast("9.900000003" as double) as decimalv3(9, 8));"""
    qt_sql_308_strict "${const_sql_308}"
    testFoldConst("${const_sql_308}")
    def const_sql_309 = """select cast(cast("9.900000013" as double) as decimalv3(9, 8));"""
    qt_sql_309_strict "${const_sql_309}"
    testFoldConst("${const_sql_309}")
    def const_sql_310 = """select cast(cast("9.999999983" as double) as decimalv3(9, 8));"""
    qt_sql_310_strict "${const_sql_310}"
    testFoldConst("${const_sql_310}")
    def const_sql_311 = """select cast(cast("0.000000009" as double) as decimalv3(9, 8));"""
    qt_sql_311_strict "${const_sql_311}"
    testFoldConst("${const_sql_311}")
    def const_sql_312 = """select cast(cast("0.000000019" as double) as decimalv3(9, 8));"""
    qt_sql_312_strict "${const_sql_312}"
    testFoldConst("${const_sql_312}")
    def const_sql_313 = """select cast(cast("0.000000099" as double) as decimalv3(9, 8));"""
    qt_sql_313_strict "${const_sql_313}"
    testFoldConst("${const_sql_313}")
    def const_sql_314 = """select cast(cast("0.099999999" as double) as decimalv3(9, 8));"""
    qt_sql_314_strict "${const_sql_314}"
    testFoldConst("${const_sql_314}")
    def const_sql_315 = """select cast(cast("0.900000009" as double) as decimalv3(9, 8));"""
    qt_sql_315_strict "${const_sql_315}"
    testFoldConst("${const_sql_315}")
    def const_sql_316 = """select cast(cast("0.900000019" as double) as decimalv3(9, 8));"""
    qt_sql_316_strict "${const_sql_316}"
    testFoldConst("${const_sql_316}")
    def const_sql_317 = """select cast(cast("0.999999989" as double) as decimalv3(9, 8));"""
    qt_sql_317_strict "${const_sql_317}"
    testFoldConst("${const_sql_317}")
    def const_sql_318 = """select cast(cast("0.999999999" as double) as decimalv3(9, 8));"""
    qt_sql_318_strict "${const_sql_318}"
    testFoldConst("${const_sql_318}")
    def const_sql_319 = """select cast(cast("1.000000009" as double) as decimalv3(9, 8));"""
    qt_sql_319_strict "${const_sql_319}"
    testFoldConst("${const_sql_319}")
    def const_sql_320 = """select cast(cast("1.000000019" as double) as decimalv3(9, 8));"""
    qt_sql_320_strict "${const_sql_320}"
    testFoldConst("${const_sql_320}")
    def const_sql_321 = """select cast(cast("1.000000099" as double) as decimalv3(9, 8));"""
    qt_sql_321_strict "${const_sql_321}"
    testFoldConst("${const_sql_321}")
    def const_sql_322 = """select cast(cast("1.099999999" as double) as decimalv3(9, 8));"""
    qt_sql_322_strict "${const_sql_322}"
    testFoldConst("${const_sql_322}")
    def const_sql_323 = """select cast(cast("1.900000009" as double) as decimalv3(9, 8));"""
    qt_sql_323_strict "${const_sql_323}"
    testFoldConst("${const_sql_323}")
    def const_sql_324 = """select cast(cast("1.900000019" as double) as decimalv3(9, 8));"""
    qt_sql_324_strict "${const_sql_324}"
    testFoldConst("${const_sql_324}")
    def const_sql_325 = """select cast(cast("1.999999989" as double) as decimalv3(9, 8));"""
    qt_sql_325_strict "${const_sql_325}"
    testFoldConst("${const_sql_325}")
    def const_sql_326 = """select cast(cast("1.999999999" as double) as decimalv3(9, 8));"""
    qt_sql_326_strict "${const_sql_326}"
    testFoldConst("${const_sql_326}")
    def const_sql_327 = """select cast(cast("8.000000009" as double) as decimalv3(9, 8));"""
    qt_sql_327_strict "${const_sql_327}"
    testFoldConst("${const_sql_327}")
    def const_sql_328 = """select cast(cast("8.000000019" as double) as decimalv3(9, 8));"""
    qt_sql_328_strict "${const_sql_328}"
    testFoldConst("${const_sql_328}")
    def const_sql_329 = """select cast(cast("8.000000099" as double) as decimalv3(9, 8));"""
    qt_sql_329_strict "${const_sql_329}"
    testFoldConst("${const_sql_329}")
    def const_sql_330 = """select cast(cast("8.099999999" as double) as decimalv3(9, 8));"""
    qt_sql_330_strict "${const_sql_330}"
    testFoldConst("${const_sql_330}")
    def const_sql_331 = """select cast(cast("8.900000009" as double) as decimalv3(9, 8));"""
    qt_sql_331_strict "${const_sql_331}"
    testFoldConst("${const_sql_331}")
    def const_sql_332 = """select cast(cast("8.900000019" as double) as decimalv3(9, 8));"""
    qt_sql_332_strict "${const_sql_332}"
    testFoldConst("${const_sql_332}")
    def const_sql_333 = """select cast(cast("8.999999989" as double) as decimalv3(9, 8));"""
    qt_sql_333_strict "${const_sql_333}"
    testFoldConst("${const_sql_333}")
    def const_sql_334 = """select cast(cast("8.999999999" as double) as decimalv3(9, 8));"""
    qt_sql_334_strict "${const_sql_334}"
    testFoldConst("${const_sql_334}")
    def const_sql_335 = """select cast(cast("9.000000003" as double) as decimalv3(9, 8));"""
    qt_sql_335_strict "${const_sql_335}"
    testFoldConst("${const_sql_335}")
    def const_sql_336 = """select cast(cast("9.000000013" as double) as decimalv3(9, 8));"""
    qt_sql_336_strict "${const_sql_336}"
    testFoldConst("${const_sql_336}")
    def const_sql_337 = """select cast(cast("9.000000093" as double) as decimalv3(9, 8));"""
    qt_sql_337_strict "${const_sql_337}"
    testFoldConst("${const_sql_337}")
    def const_sql_338 = """select cast(cast("9.099999993" as double) as decimalv3(9, 8));"""
    qt_sql_338_strict "${const_sql_338}"
    testFoldConst("${const_sql_338}")
    def const_sql_339 = """select cast(cast("9.900000003" as double) as decimalv3(9, 8));"""
    qt_sql_339_strict "${const_sql_339}"
    testFoldConst("${const_sql_339}")
    def const_sql_340 = """select cast(cast("9.900000013" as double) as decimalv3(9, 8));"""
    qt_sql_340_strict "${const_sql_340}"
    testFoldConst("${const_sql_340}")
    def const_sql_341 = """select cast(cast("9.999999983" as double) as decimalv3(9, 8));"""
    qt_sql_341_strict "${const_sql_341}"
    testFoldConst("${const_sql_341}")
    def const_sql_342 = """select cast(cast("-0.000000003" as double) as decimalv3(9, 8));"""
    qt_sql_342_strict "${const_sql_342}"
    testFoldConst("${const_sql_342}")
    def const_sql_343 = """select cast(cast("-0.000000013" as double) as decimalv3(9, 8));"""
    qt_sql_343_strict "${const_sql_343}"
    testFoldConst("${const_sql_343}")
    def const_sql_344 = """select cast(cast("-0.000000093" as double) as decimalv3(9, 8));"""
    qt_sql_344_strict "${const_sql_344}"
    testFoldConst("${const_sql_344}")
    def const_sql_345 = """select cast(cast("-0.099999993" as double) as decimalv3(9, 8));"""
    qt_sql_345_strict "${const_sql_345}"
    testFoldConst("${const_sql_345}")
    def const_sql_346 = """select cast(cast("-0.900000003" as double) as decimalv3(9, 8));"""
    qt_sql_346_strict "${const_sql_346}"
    testFoldConst("${const_sql_346}")
    def const_sql_347 = """select cast(cast("-0.900000013" as double) as decimalv3(9, 8));"""
    qt_sql_347_strict "${const_sql_347}"
    testFoldConst("${const_sql_347}")
    def const_sql_348 = """select cast(cast("-0.999999983" as double) as decimalv3(9, 8));"""
    qt_sql_348_strict "${const_sql_348}"
    testFoldConst("${const_sql_348}")
    def const_sql_349 = """select cast(cast("-0.999999993" as double) as decimalv3(9, 8));"""
    qt_sql_349_strict "${const_sql_349}"
    testFoldConst("${const_sql_349}")
    def const_sql_350 = """select cast(cast("-1.000000003" as double) as decimalv3(9, 8));"""
    qt_sql_350_strict "${const_sql_350}"
    testFoldConst("${const_sql_350}")
    def const_sql_351 = """select cast(cast("-1.000000013" as double) as decimalv3(9, 8));"""
    qt_sql_351_strict "${const_sql_351}"
    testFoldConst("${const_sql_351}")
    def const_sql_352 = """select cast(cast("-1.000000093" as double) as decimalv3(9, 8));"""
    qt_sql_352_strict "${const_sql_352}"
    testFoldConst("${const_sql_352}")
    def const_sql_353 = """select cast(cast("-1.099999993" as double) as decimalv3(9, 8));"""
    qt_sql_353_strict "${const_sql_353}"
    testFoldConst("${const_sql_353}")
    def const_sql_354 = """select cast(cast("-1.900000003" as double) as decimalv3(9, 8));"""
    qt_sql_354_strict "${const_sql_354}"
    testFoldConst("${const_sql_354}")
    def const_sql_355 = """select cast(cast("-1.900000013" as double) as decimalv3(9, 8));"""
    qt_sql_355_strict "${const_sql_355}"
    testFoldConst("${const_sql_355}")
    def const_sql_356 = """select cast(cast("-1.999999983" as double) as decimalv3(9, 8));"""
    qt_sql_356_strict "${const_sql_356}"
    testFoldConst("${const_sql_356}")
    def const_sql_357 = """select cast(cast("-1.999999993" as double) as decimalv3(9, 8));"""
    qt_sql_357_strict "${const_sql_357}"
    testFoldConst("${const_sql_357}")
    def const_sql_358 = """select cast(cast("-8.000000003" as double) as decimalv3(9, 8));"""
    qt_sql_358_strict "${const_sql_358}"
    testFoldConst("${const_sql_358}")
    def const_sql_359 = """select cast(cast("-8.000000013" as double) as decimalv3(9, 8));"""
    qt_sql_359_strict "${const_sql_359}"
    testFoldConst("${const_sql_359}")
    def const_sql_360 = """select cast(cast("-8.000000093" as double) as decimalv3(9, 8));"""
    qt_sql_360_strict "${const_sql_360}"
    testFoldConst("${const_sql_360}")
    def const_sql_361 = """select cast(cast("-8.099999993" as double) as decimalv3(9, 8));"""
    qt_sql_361_strict "${const_sql_361}"
    testFoldConst("${const_sql_361}")
    def const_sql_362 = """select cast(cast("-8.900000003" as double) as decimalv3(9, 8));"""
    qt_sql_362_strict "${const_sql_362}"
    testFoldConst("${const_sql_362}")
    def const_sql_363 = """select cast(cast("-8.900000013" as double) as decimalv3(9, 8));"""
    qt_sql_363_strict "${const_sql_363}"
    testFoldConst("${const_sql_363}")
    def const_sql_364 = """select cast(cast("-8.999999983" as double) as decimalv3(9, 8));"""
    qt_sql_364_strict "${const_sql_364}"
    testFoldConst("${const_sql_364}")
    def const_sql_365 = """select cast(cast("-8.999999993" as double) as decimalv3(9, 8));"""
    qt_sql_365_strict "${const_sql_365}"
    testFoldConst("${const_sql_365}")
    def const_sql_366 = """select cast(cast("-9.000000003" as double) as decimalv3(9, 8));"""
    qt_sql_366_strict "${const_sql_366}"
    testFoldConst("${const_sql_366}")
    def const_sql_367 = """select cast(cast("-9.000000013" as double) as decimalv3(9, 8));"""
    qt_sql_367_strict "${const_sql_367}"
    testFoldConst("${const_sql_367}")
    def const_sql_368 = """select cast(cast("-9.000000093" as double) as decimalv3(9, 8));"""
    qt_sql_368_strict "${const_sql_368}"
    testFoldConst("${const_sql_368}")
    def const_sql_369 = """select cast(cast("-9.099999993" as double) as decimalv3(9, 8));"""
    qt_sql_369_strict "${const_sql_369}"
    testFoldConst("${const_sql_369}")
    def const_sql_370 = """select cast(cast("-9.900000003" as double) as decimalv3(9, 8));"""
    qt_sql_370_strict "${const_sql_370}"
    testFoldConst("${const_sql_370}")
    def const_sql_371 = """select cast(cast("-9.900000013" as double) as decimalv3(9, 8));"""
    qt_sql_371_strict "${const_sql_371}"
    testFoldConst("${const_sql_371}")
    def const_sql_372 = """select cast(cast("-9.999999983" as double) as decimalv3(9, 8));"""
    qt_sql_372_strict "${const_sql_372}"
    testFoldConst("${const_sql_372}")
    def const_sql_373 = """select cast(cast("-0.000000009" as double) as decimalv3(9, 8));"""
    qt_sql_373_strict "${const_sql_373}"
    testFoldConst("${const_sql_373}")
    def const_sql_374 = """select cast(cast("-0.000000019" as double) as decimalv3(9, 8));"""
    qt_sql_374_strict "${const_sql_374}"
    testFoldConst("${const_sql_374}")
    def const_sql_375 = """select cast(cast("-0.000000099" as double) as decimalv3(9, 8));"""
    qt_sql_375_strict "${const_sql_375}"
    testFoldConst("${const_sql_375}")
    def const_sql_376 = """select cast(cast("-0.099999999" as double) as decimalv3(9, 8));"""
    qt_sql_376_strict "${const_sql_376}"
    testFoldConst("${const_sql_376}")
    def const_sql_377 = """select cast(cast("-0.900000009" as double) as decimalv3(9, 8));"""
    qt_sql_377_strict "${const_sql_377}"
    testFoldConst("${const_sql_377}")
    def const_sql_378 = """select cast(cast("-0.900000019" as double) as decimalv3(9, 8));"""
    qt_sql_378_strict "${const_sql_378}"
    testFoldConst("${const_sql_378}")
    def const_sql_379 = """select cast(cast("-0.999999989" as double) as decimalv3(9, 8));"""
    qt_sql_379_strict "${const_sql_379}"
    testFoldConst("${const_sql_379}")
    def const_sql_380 = """select cast(cast("-0.999999999" as double) as decimalv3(9, 8));"""
    qt_sql_380_strict "${const_sql_380}"
    testFoldConst("${const_sql_380}")
    def const_sql_381 = """select cast(cast("-1.000000009" as double) as decimalv3(9, 8));"""
    qt_sql_381_strict "${const_sql_381}"
    testFoldConst("${const_sql_381}")
    def const_sql_382 = """select cast(cast("-1.000000019" as double) as decimalv3(9, 8));"""
    qt_sql_382_strict "${const_sql_382}"
    testFoldConst("${const_sql_382}")
    def const_sql_383 = """select cast(cast("-1.000000099" as double) as decimalv3(9, 8));"""
    qt_sql_383_strict "${const_sql_383}"
    testFoldConst("${const_sql_383}")
    def const_sql_384 = """select cast(cast("-1.099999999" as double) as decimalv3(9, 8));"""
    qt_sql_384_strict "${const_sql_384}"
    testFoldConst("${const_sql_384}")
    def const_sql_385 = """select cast(cast("-1.900000009" as double) as decimalv3(9, 8));"""
    qt_sql_385_strict "${const_sql_385}"
    testFoldConst("${const_sql_385}")
    def const_sql_386 = """select cast(cast("-1.900000019" as double) as decimalv3(9, 8));"""
    qt_sql_386_strict "${const_sql_386}"
    testFoldConst("${const_sql_386}")
    def const_sql_387 = """select cast(cast("-1.999999989" as double) as decimalv3(9, 8));"""
    qt_sql_387_strict "${const_sql_387}"
    testFoldConst("${const_sql_387}")
    def const_sql_388 = """select cast(cast("-1.999999999" as double) as decimalv3(9, 8));"""
    qt_sql_388_strict "${const_sql_388}"
    testFoldConst("${const_sql_388}")
    def const_sql_389 = """select cast(cast("-8.000000009" as double) as decimalv3(9, 8));"""
    qt_sql_389_strict "${const_sql_389}"
    testFoldConst("${const_sql_389}")
    def const_sql_390 = """select cast(cast("-8.000000019" as double) as decimalv3(9, 8));"""
    qt_sql_390_strict "${const_sql_390}"
    testFoldConst("${const_sql_390}")
    def const_sql_391 = """select cast(cast("-8.000000099" as double) as decimalv3(9, 8));"""
    qt_sql_391_strict "${const_sql_391}"
    testFoldConst("${const_sql_391}")
    def const_sql_392 = """select cast(cast("-8.099999999" as double) as decimalv3(9, 8));"""
    qt_sql_392_strict "${const_sql_392}"
    testFoldConst("${const_sql_392}")
    def const_sql_393 = """select cast(cast("-8.900000009" as double) as decimalv3(9, 8));"""
    qt_sql_393_strict "${const_sql_393}"
    testFoldConst("${const_sql_393}")
    def const_sql_394 = """select cast(cast("-8.900000019" as double) as decimalv3(9, 8));"""
    qt_sql_394_strict "${const_sql_394}"
    testFoldConst("${const_sql_394}")
    def const_sql_395 = """select cast(cast("-8.999999989" as double) as decimalv3(9, 8));"""
    qt_sql_395_strict "${const_sql_395}"
    testFoldConst("${const_sql_395}")
    def const_sql_396 = """select cast(cast("-8.999999999" as double) as decimalv3(9, 8));"""
    qt_sql_396_strict "${const_sql_396}"
    testFoldConst("${const_sql_396}")
    def const_sql_397 = """select cast(cast("-9.000000003" as double) as decimalv3(9, 8));"""
    qt_sql_397_strict "${const_sql_397}"
    testFoldConst("${const_sql_397}")
    def const_sql_398 = """select cast(cast("-9.000000013" as double) as decimalv3(9, 8));"""
    qt_sql_398_strict "${const_sql_398}"
    testFoldConst("${const_sql_398}")
    def const_sql_399 = """select cast(cast("-9.000000093" as double) as decimalv3(9, 8));"""
    qt_sql_399_strict "${const_sql_399}"
    testFoldConst("${const_sql_399}")
    def const_sql_400 = """select cast(cast("-9.099999993" as double) as decimalv3(9, 8));"""
    qt_sql_400_strict "${const_sql_400}"
    testFoldConst("${const_sql_400}")
    def const_sql_401 = """select cast(cast("-9.900000003" as double) as decimalv3(9, 8));"""
    qt_sql_401_strict "${const_sql_401}"
    testFoldConst("${const_sql_401}")
    def const_sql_402 = """select cast(cast("-9.900000013" as double) as decimalv3(9, 8));"""
    qt_sql_402_strict "${const_sql_402}"
    testFoldConst("${const_sql_402}")
    def const_sql_403 = """select cast(cast("-9.999999983" as double) as decimalv3(9, 8));"""
    qt_sql_403_strict "${const_sql_403}"
    testFoldConst("${const_sql_403}")
    sql "set enable_strict_cast=false;"
    qt_sql_280_non_strict "${const_sql_280}"
    testFoldConst("${const_sql_280}")
    qt_sql_281_non_strict "${const_sql_281}"
    testFoldConst("${const_sql_281}")
    qt_sql_282_non_strict "${const_sql_282}"
    testFoldConst("${const_sql_282}")
    qt_sql_283_non_strict "${const_sql_283}"
    testFoldConst("${const_sql_283}")
    qt_sql_284_non_strict "${const_sql_284}"
    testFoldConst("${const_sql_284}")
    qt_sql_285_non_strict "${const_sql_285}"
    testFoldConst("${const_sql_285}")
    qt_sql_286_non_strict "${const_sql_286}"
    testFoldConst("${const_sql_286}")
    qt_sql_287_non_strict "${const_sql_287}"
    testFoldConst("${const_sql_287}")
    qt_sql_288_non_strict "${const_sql_288}"
    testFoldConst("${const_sql_288}")
    qt_sql_289_non_strict "${const_sql_289}"
    testFoldConst("${const_sql_289}")
    qt_sql_290_non_strict "${const_sql_290}"
    testFoldConst("${const_sql_290}")
    qt_sql_291_non_strict "${const_sql_291}"
    testFoldConst("${const_sql_291}")
    qt_sql_292_non_strict "${const_sql_292}"
    testFoldConst("${const_sql_292}")
    qt_sql_293_non_strict "${const_sql_293}"
    testFoldConst("${const_sql_293}")
    qt_sql_294_non_strict "${const_sql_294}"
    testFoldConst("${const_sql_294}")
    qt_sql_295_non_strict "${const_sql_295}"
    testFoldConst("${const_sql_295}")
    qt_sql_296_non_strict "${const_sql_296}"
    testFoldConst("${const_sql_296}")
    qt_sql_297_non_strict "${const_sql_297}"
    testFoldConst("${const_sql_297}")
    qt_sql_298_non_strict "${const_sql_298}"
    testFoldConst("${const_sql_298}")
    qt_sql_299_non_strict "${const_sql_299}"
    testFoldConst("${const_sql_299}")
    qt_sql_300_non_strict "${const_sql_300}"
    testFoldConst("${const_sql_300}")
    qt_sql_301_non_strict "${const_sql_301}"
    testFoldConst("${const_sql_301}")
    qt_sql_302_non_strict "${const_sql_302}"
    testFoldConst("${const_sql_302}")
    qt_sql_303_non_strict "${const_sql_303}"
    testFoldConst("${const_sql_303}")
    qt_sql_304_non_strict "${const_sql_304}"
    testFoldConst("${const_sql_304}")
    qt_sql_305_non_strict "${const_sql_305}"
    testFoldConst("${const_sql_305}")
    qt_sql_306_non_strict "${const_sql_306}"
    testFoldConst("${const_sql_306}")
    qt_sql_307_non_strict "${const_sql_307}"
    testFoldConst("${const_sql_307}")
    qt_sql_308_non_strict "${const_sql_308}"
    testFoldConst("${const_sql_308}")
    qt_sql_309_non_strict "${const_sql_309}"
    testFoldConst("${const_sql_309}")
    qt_sql_310_non_strict "${const_sql_310}"
    testFoldConst("${const_sql_310}")
    qt_sql_311_non_strict "${const_sql_311}"
    testFoldConst("${const_sql_311}")
    qt_sql_312_non_strict "${const_sql_312}"
    testFoldConst("${const_sql_312}")
    qt_sql_313_non_strict "${const_sql_313}"
    testFoldConst("${const_sql_313}")
    qt_sql_314_non_strict "${const_sql_314}"
    testFoldConst("${const_sql_314}")
    qt_sql_315_non_strict "${const_sql_315}"
    testFoldConst("${const_sql_315}")
    qt_sql_316_non_strict "${const_sql_316}"
    testFoldConst("${const_sql_316}")
    qt_sql_317_non_strict "${const_sql_317}"
    testFoldConst("${const_sql_317}")
    qt_sql_318_non_strict "${const_sql_318}"
    testFoldConst("${const_sql_318}")
    qt_sql_319_non_strict "${const_sql_319}"
    testFoldConst("${const_sql_319}")
    qt_sql_320_non_strict "${const_sql_320}"
    testFoldConst("${const_sql_320}")
    qt_sql_321_non_strict "${const_sql_321}"
    testFoldConst("${const_sql_321}")
    qt_sql_322_non_strict "${const_sql_322}"
    testFoldConst("${const_sql_322}")
    qt_sql_323_non_strict "${const_sql_323}"
    testFoldConst("${const_sql_323}")
    qt_sql_324_non_strict "${const_sql_324}"
    testFoldConst("${const_sql_324}")
    qt_sql_325_non_strict "${const_sql_325}"
    testFoldConst("${const_sql_325}")
    qt_sql_326_non_strict "${const_sql_326}"
    testFoldConst("${const_sql_326}")
    qt_sql_327_non_strict "${const_sql_327}"
    testFoldConst("${const_sql_327}")
    qt_sql_328_non_strict "${const_sql_328}"
    testFoldConst("${const_sql_328}")
    qt_sql_329_non_strict "${const_sql_329}"
    testFoldConst("${const_sql_329}")
    qt_sql_330_non_strict "${const_sql_330}"
    testFoldConst("${const_sql_330}")
    qt_sql_331_non_strict "${const_sql_331}"
    testFoldConst("${const_sql_331}")
    qt_sql_332_non_strict "${const_sql_332}"
    testFoldConst("${const_sql_332}")
    qt_sql_333_non_strict "${const_sql_333}"
    testFoldConst("${const_sql_333}")
    qt_sql_334_non_strict "${const_sql_334}"
    testFoldConst("${const_sql_334}")
    qt_sql_335_non_strict "${const_sql_335}"
    testFoldConst("${const_sql_335}")
    qt_sql_336_non_strict "${const_sql_336}"
    testFoldConst("${const_sql_336}")
    qt_sql_337_non_strict "${const_sql_337}"
    testFoldConst("${const_sql_337}")
    qt_sql_338_non_strict "${const_sql_338}"
    testFoldConst("${const_sql_338}")
    qt_sql_339_non_strict "${const_sql_339}"
    testFoldConst("${const_sql_339}")
    qt_sql_340_non_strict "${const_sql_340}"
    testFoldConst("${const_sql_340}")
    qt_sql_341_non_strict "${const_sql_341}"
    testFoldConst("${const_sql_341}")
    qt_sql_342_non_strict "${const_sql_342}"
    testFoldConst("${const_sql_342}")
    qt_sql_343_non_strict "${const_sql_343}"
    testFoldConst("${const_sql_343}")
    qt_sql_344_non_strict "${const_sql_344}"
    testFoldConst("${const_sql_344}")
    qt_sql_345_non_strict "${const_sql_345}"
    testFoldConst("${const_sql_345}")
    qt_sql_346_non_strict "${const_sql_346}"
    testFoldConst("${const_sql_346}")
    qt_sql_347_non_strict "${const_sql_347}"
    testFoldConst("${const_sql_347}")
    qt_sql_348_non_strict "${const_sql_348}"
    testFoldConst("${const_sql_348}")
    qt_sql_349_non_strict "${const_sql_349}"
    testFoldConst("${const_sql_349}")
    qt_sql_350_non_strict "${const_sql_350}"
    testFoldConst("${const_sql_350}")
    qt_sql_351_non_strict "${const_sql_351}"
    testFoldConst("${const_sql_351}")
    qt_sql_352_non_strict "${const_sql_352}"
    testFoldConst("${const_sql_352}")
    qt_sql_353_non_strict "${const_sql_353}"
    testFoldConst("${const_sql_353}")
    qt_sql_354_non_strict "${const_sql_354}"
    testFoldConst("${const_sql_354}")
    qt_sql_355_non_strict "${const_sql_355}"
    testFoldConst("${const_sql_355}")
    qt_sql_356_non_strict "${const_sql_356}"
    testFoldConst("${const_sql_356}")
    qt_sql_357_non_strict "${const_sql_357}"
    testFoldConst("${const_sql_357}")
    qt_sql_358_non_strict "${const_sql_358}"
    testFoldConst("${const_sql_358}")
    qt_sql_359_non_strict "${const_sql_359}"
    testFoldConst("${const_sql_359}")
    qt_sql_360_non_strict "${const_sql_360}"
    testFoldConst("${const_sql_360}")
    qt_sql_361_non_strict "${const_sql_361}"
    testFoldConst("${const_sql_361}")
    qt_sql_362_non_strict "${const_sql_362}"
    testFoldConst("${const_sql_362}")
    qt_sql_363_non_strict "${const_sql_363}"
    testFoldConst("${const_sql_363}")
    qt_sql_364_non_strict "${const_sql_364}"
    testFoldConst("${const_sql_364}")
    qt_sql_365_non_strict "${const_sql_365}"
    testFoldConst("${const_sql_365}")
    qt_sql_366_non_strict "${const_sql_366}"
    testFoldConst("${const_sql_366}")
    qt_sql_367_non_strict "${const_sql_367}"
    testFoldConst("${const_sql_367}")
    qt_sql_368_non_strict "${const_sql_368}"
    testFoldConst("${const_sql_368}")
    qt_sql_369_non_strict "${const_sql_369}"
    testFoldConst("${const_sql_369}")
    qt_sql_370_non_strict "${const_sql_370}"
    testFoldConst("${const_sql_370}")
    qt_sql_371_non_strict "${const_sql_371}"
    testFoldConst("${const_sql_371}")
    qt_sql_372_non_strict "${const_sql_372}"
    testFoldConst("${const_sql_372}")
    qt_sql_373_non_strict "${const_sql_373}"
    testFoldConst("${const_sql_373}")
    qt_sql_374_non_strict "${const_sql_374}"
    testFoldConst("${const_sql_374}")
    qt_sql_375_non_strict "${const_sql_375}"
    testFoldConst("${const_sql_375}")
    qt_sql_376_non_strict "${const_sql_376}"
    testFoldConst("${const_sql_376}")
    qt_sql_377_non_strict "${const_sql_377}"
    testFoldConst("${const_sql_377}")
    qt_sql_378_non_strict "${const_sql_378}"
    testFoldConst("${const_sql_378}")
    qt_sql_379_non_strict "${const_sql_379}"
    testFoldConst("${const_sql_379}")
    qt_sql_380_non_strict "${const_sql_380}"
    testFoldConst("${const_sql_380}")
    qt_sql_381_non_strict "${const_sql_381}"
    testFoldConst("${const_sql_381}")
    qt_sql_382_non_strict "${const_sql_382}"
    testFoldConst("${const_sql_382}")
    qt_sql_383_non_strict "${const_sql_383}"
    testFoldConst("${const_sql_383}")
    qt_sql_384_non_strict "${const_sql_384}"
    testFoldConst("${const_sql_384}")
    qt_sql_385_non_strict "${const_sql_385}"
    testFoldConst("${const_sql_385}")
    qt_sql_386_non_strict "${const_sql_386}"
    testFoldConst("${const_sql_386}")
    qt_sql_387_non_strict "${const_sql_387}"
    testFoldConst("${const_sql_387}")
    qt_sql_388_non_strict "${const_sql_388}"
    testFoldConst("${const_sql_388}")
    qt_sql_389_non_strict "${const_sql_389}"
    testFoldConst("${const_sql_389}")
    qt_sql_390_non_strict "${const_sql_390}"
    testFoldConst("${const_sql_390}")
    qt_sql_391_non_strict "${const_sql_391}"
    testFoldConst("${const_sql_391}")
    qt_sql_392_non_strict "${const_sql_392}"
    testFoldConst("${const_sql_392}")
    qt_sql_393_non_strict "${const_sql_393}"
    testFoldConst("${const_sql_393}")
    qt_sql_394_non_strict "${const_sql_394}"
    testFoldConst("${const_sql_394}")
    qt_sql_395_non_strict "${const_sql_395}"
    testFoldConst("${const_sql_395}")
    qt_sql_396_non_strict "${const_sql_396}"
    testFoldConst("${const_sql_396}")
    qt_sql_397_non_strict "${const_sql_397}"
    testFoldConst("${const_sql_397}")
    qt_sql_398_non_strict "${const_sql_398}"
    testFoldConst("${const_sql_398}")
    qt_sql_399_non_strict "${const_sql_399}"
    testFoldConst("${const_sql_399}")
    qt_sql_400_non_strict "${const_sql_400}"
    testFoldConst("${const_sql_400}")
    qt_sql_401_non_strict "${const_sql_401}"
    testFoldConst("${const_sql_401}")
    qt_sql_402_non_strict "${const_sql_402}"
    testFoldConst("${const_sql_402}")
    qt_sql_403_non_strict "${const_sql_403}"
    testFoldConst("${const_sql_403}")
}