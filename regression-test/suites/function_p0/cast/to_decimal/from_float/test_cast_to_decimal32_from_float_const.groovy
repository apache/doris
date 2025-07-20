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
    def const_sql_0 = """select cast(cast("0.03" as float) as decimalv3(9, 0));"""
    qt_sql_0_strict "${const_sql_0}"
    testFoldConst("${const_sql_0}")
    def const_sql_1 = """select cast(cast("1.03" as float) as decimalv3(9, 0));"""
    qt_sql_1_strict "${const_sql_1}"
    testFoldConst("${const_sql_1}")
    def const_sql_2 = """select cast(cast("9.03" as float) as decimalv3(9, 0));"""
    qt_sql_2_strict "${const_sql_2}"
    testFoldConst("${const_sql_2}")
    def const_sql_3 = """select cast(cast("0.09" as float) as decimalv3(9, 0));"""
    qt_sql_3_strict "${const_sql_3}"
    testFoldConst("${const_sql_3}")
    def const_sql_4 = """select cast(cast("1.09" as float) as decimalv3(9, 0));"""
    qt_sql_4_strict "${const_sql_4}"
    testFoldConst("${const_sql_4}")
    def const_sql_5 = """select cast(cast("9.09" as float) as decimalv3(9, 0));"""
    qt_sql_5_strict "${const_sql_5}"
    testFoldConst("${const_sql_5}")
    def const_sql_6 = """select cast(cast("-0.03" as float) as decimalv3(9, 0));"""
    qt_sql_6_strict "${const_sql_6}"
    testFoldConst("${const_sql_6}")
    def const_sql_7 = """select cast(cast("-1.03" as float) as decimalv3(9, 0));"""
    qt_sql_7_strict "${const_sql_7}"
    testFoldConst("${const_sql_7}")
    def const_sql_8 = """select cast(cast("-9.03" as float) as decimalv3(9, 0));"""
    qt_sql_8_strict "${const_sql_8}"
    testFoldConst("${const_sql_8}")
    def const_sql_9 = """select cast(cast("-0.09" as float) as decimalv3(9, 0));"""
    qt_sql_9_strict "${const_sql_9}"
    testFoldConst("${const_sql_9}")
    def const_sql_10 = """select cast(cast("-1.09" as float) as decimalv3(9, 0));"""
    qt_sql_10_strict "${const_sql_10}"
    testFoldConst("${const_sql_10}")
    def const_sql_11 = """select cast(cast("-9.09" as float) as decimalv3(9, 0));"""
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
    def const_sql_24 = """select cast(cast("0.0003" as float) as decimalv3(9, 3));"""
    qt_sql_24_strict "${const_sql_24}"
    testFoldConst("${const_sql_24}")
    def const_sql_25 = """select cast(cast("0.0013" as float) as decimalv3(9, 3));"""
    qt_sql_25_strict "${const_sql_25}"
    testFoldConst("${const_sql_25}")
    def const_sql_26 = """select cast(cast("0.0093" as float) as decimalv3(9, 3));"""
    qt_sql_26_strict "${const_sql_26}"
    testFoldConst("${const_sql_26}")
    def const_sql_27 = """select cast(cast("0.0993" as float) as decimalv3(9, 3));"""
    qt_sql_27_strict "${const_sql_27}"
    testFoldConst("${const_sql_27}")
    def const_sql_28 = """select cast(cast("0.9003" as float) as decimalv3(9, 3));"""
    qt_sql_28_strict "${const_sql_28}"
    testFoldConst("${const_sql_28}")
    def const_sql_29 = """select cast(cast("0.9013" as float) as decimalv3(9, 3));"""
    qt_sql_29_strict "${const_sql_29}"
    testFoldConst("${const_sql_29}")
    def const_sql_30 = """select cast(cast("0.9983" as float) as decimalv3(9, 3));"""
    qt_sql_30_strict "${const_sql_30}"
    testFoldConst("${const_sql_30}")
    def const_sql_31 = """select cast(cast("0.9993" as float) as decimalv3(9, 3));"""
    qt_sql_31_strict "${const_sql_31}"
    testFoldConst("${const_sql_31}")
    def const_sql_32 = """select cast(cast("1.0003" as float) as decimalv3(9, 3));"""
    qt_sql_32_strict "${const_sql_32}"
    testFoldConst("${const_sql_32}")
    def const_sql_33 = """select cast(cast("1.0013" as float) as decimalv3(9, 3));"""
    qt_sql_33_strict "${const_sql_33}"
    testFoldConst("${const_sql_33}")
    def const_sql_34 = """select cast(cast("1.0093" as float) as decimalv3(9, 3));"""
    qt_sql_34_strict "${const_sql_34}"
    testFoldConst("${const_sql_34}")
    def const_sql_35 = """select cast(cast("1.0993" as float) as decimalv3(9, 3));"""
    qt_sql_35_strict "${const_sql_35}"
    testFoldConst("${const_sql_35}")
    def const_sql_36 = """select cast(cast("1.9003" as float) as decimalv3(9, 3));"""
    qt_sql_36_strict "${const_sql_36}"
    testFoldConst("${const_sql_36}")
    def const_sql_37 = """select cast(cast("1.9013" as float) as decimalv3(9, 3));"""
    qt_sql_37_strict "${const_sql_37}"
    testFoldConst("${const_sql_37}")
    def const_sql_38 = """select cast(cast("1.9983" as float) as decimalv3(9, 3));"""
    qt_sql_38_strict "${const_sql_38}"
    testFoldConst("${const_sql_38}")
    def const_sql_39 = """select cast(cast("1.9993" as float) as decimalv3(9, 3));"""
    qt_sql_39_strict "${const_sql_39}"
    testFoldConst("${const_sql_39}")
    def const_sql_40 = """select cast(cast("9.0003" as float) as decimalv3(9, 3));"""
    qt_sql_40_strict "${const_sql_40}"
    testFoldConst("${const_sql_40}")
    def const_sql_41 = """select cast(cast("9.0013" as float) as decimalv3(9, 3));"""
    qt_sql_41_strict "${const_sql_41}"
    testFoldConst("${const_sql_41}")
    def const_sql_42 = """select cast(cast("9.0093" as float) as decimalv3(9, 3));"""
    qt_sql_42_strict "${const_sql_42}"
    testFoldConst("${const_sql_42}")
    def const_sql_43 = """select cast(cast("9.0993" as float) as decimalv3(9, 3));"""
    qt_sql_43_strict "${const_sql_43}"
    testFoldConst("${const_sql_43}")
    def const_sql_44 = """select cast(cast("9.9003" as float) as decimalv3(9, 3));"""
    qt_sql_44_strict "${const_sql_44}"
    testFoldConst("${const_sql_44}")
    def const_sql_45 = """select cast(cast("9.9013" as float) as decimalv3(9, 3));"""
    qt_sql_45_strict "${const_sql_45}"
    testFoldConst("${const_sql_45}")
    def const_sql_46 = """select cast(cast("9.9983" as float) as decimalv3(9, 3));"""
    qt_sql_46_strict "${const_sql_46}"
    testFoldConst("${const_sql_46}")
    def const_sql_47 = """select cast(cast("9.9993" as float) as decimalv3(9, 3));"""
    qt_sql_47_strict "${const_sql_47}"
    testFoldConst("${const_sql_47}")
    def const_sql_48 = """select cast(cast("0.0009" as float) as decimalv3(9, 3));"""
    qt_sql_48_strict "${const_sql_48}"
    testFoldConst("${const_sql_48}")
    def const_sql_49 = """select cast(cast("0.0019" as float) as decimalv3(9, 3));"""
    qt_sql_49_strict "${const_sql_49}"
    testFoldConst("${const_sql_49}")
    def const_sql_50 = """select cast(cast("0.0099" as float) as decimalv3(9, 3));"""
    qt_sql_50_strict "${const_sql_50}"
    testFoldConst("${const_sql_50}")
    def const_sql_51 = """select cast(cast("0.0999" as float) as decimalv3(9, 3));"""
    qt_sql_51_strict "${const_sql_51}"
    testFoldConst("${const_sql_51}")
    def const_sql_52 = """select cast(cast("0.9009" as float) as decimalv3(9, 3));"""
    qt_sql_52_strict "${const_sql_52}"
    testFoldConst("${const_sql_52}")
    def const_sql_53 = """select cast(cast("0.9019" as float) as decimalv3(9, 3));"""
    qt_sql_53_strict "${const_sql_53}"
    testFoldConst("${const_sql_53}")
    def const_sql_54 = """select cast(cast("0.9989" as float) as decimalv3(9, 3));"""
    qt_sql_54_strict "${const_sql_54}"
    testFoldConst("${const_sql_54}")
    def const_sql_55 = """select cast(cast("0.9999" as float) as decimalv3(9, 3));"""
    qt_sql_55_strict "${const_sql_55}"
    testFoldConst("${const_sql_55}")
    def const_sql_56 = """select cast(cast("1.0009" as float) as decimalv3(9, 3));"""
    qt_sql_56_strict "${const_sql_56}"
    testFoldConst("${const_sql_56}")
    def const_sql_57 = """select cast(cast("1.0019" as float) as decimalv3(9, 3));"""
    qt_sql_57_strict "${const_sql_57}"
    testFoldConst("${const_sql_57}")
    def const_sql_58 = """select cast(cast("1.0099" as float) as decimalv3(9, 3));"""
    qt_sql_58_strict "${const_sql_58}"
    testFoldConst("${const_sql_58}")
    def const_sql_59 = """select cast(cast("1.0999" as float) as decimalv3(9, 3));"""
    qt_sql_59_strict "${const_sql_59}"
    testFoldConst("${const_sql_59}")
    def const_sql_60 = """select cast(cast("1.9009" as float) as decimalv3(9, 3));"""
    qt_sql_60_strict "${const_sql_60}"
    testFoldConst("${const_sql_60}")
    def const_sql_61 = """select cast(cast("1.9019" as float) as decimalv3(9, 3));"""
    qt_sql_61_strict "${const_sql_61}"
    testFoldConst("${const_sql_61}")
    def const_sql_62 = """select cast(cast("1.9989" as float) as decimalv3(9, 3));"""
    qt_sql_62_strict "${const_sql_62}"
    testFoldConst("${const_sql_62}")
    def const_sql_63 = """select cast(cast("1.9999" as float) as decimalv3(9, 3));"""
    qt_sql_63_strict "${const_sql_63}"
    testFoldConst("${const_sql_63}")
    def const_sql_64 = """select cast(cast("9.0009" as float) as decimalv3(9, 3));"""
    qt_sql_64_strict "${const_sql_64}"
    testFoldConst("${const_sql_64}")
    def const_sql_65 = """select cast(cast("9.0019" as float) as decimalv3(9, 3));"""
    qt_sql_65_strict "${const_sql_65}"
    testFoldConst("${const_sql_65}")
    def const_sql_66 = """select cast(cast("9.0099" as float) as decimalv3(9, 3));"""
    qt_sql_66_strict "${const_sql_66}"
    testFoldConst("${const_sql_66}")
    def const_sql_67 = """select cast(cast("9.0999" as float) as decimalv3(9, 3));"""
    qt_sql_67_strict "${const_sql_67}"
    testFoldConst("${const_sql_67}")
    def const_sql_68 = """select cast(cast("9.9009" as float) as decimalv3(9, 3));"""
    qt_sql_68_strict "${const_sql_68}"
    testFoldConst("${const_sql_68}")
    def const_sql_69 = """select cast(cast("9.9019" as float) as decimalv3(9, 3));"""
    qt_sql_69_strict "${const_sql_69}"
    testFoldConst("${const_sql_69}")
    def const_sql_70 = """select cast(cast("9.9989" as float) as decimalv3(9, 3));"""
    qt_sql_70_strict "${const_sql_70}"
    testFoldConst("${const_sql_70}")
    def const_sql_71 = """select cast(cast("9.9999" as float) as decimalv3(9, 3));"""
    qt_sql_71_strict "${const_sql_71}"
    testFoldConst("${const_sql_71}")
    def const_sql_72 = """select cast(cast("-0.0003" as float) as decimalv3(9, 3));"""
    qt_sql_72_strict "${const_sql_72}"
    testFoldConst("${const_sql_72}")
    def const_sql_73 = """select cast(cast("-0.0013" as float) as decimalv3(9, 3));"""
    qt_sql_73_strict "${const_sql_73}"
    testFoldConst("${const_sql_73}")
    def const_sql_74 = """select cast(cast("-0.0093" as float) as decimalv3(9, 3));"""
    qt_sql_74_strict "${const_sql_74}"
    testFoldConst("${const_sql_74}")
    def const_sql_75 = """select cast(cast("-0.0993" as float) as decimalv3(9, 3));"""
    qt_sql_75_strict "${const_sql_75}"
    testFoldConst("${const_sql_75}")
    def const_sql_76 = """select cast(cast("-0.9003" as float) as decimalv3(9, 3));"""
    qt_sql_76_strict "${const_sql_76}"
    testFoldConst("${const_sql_76}")
    def const_sql_77 = """select cast(cast("-0.9013" as float) as decimalv3(9, 3));"""
    qt_sql_77_strict "${const_sql_77}"
    testFoldConst("${const_sql_77}")
    def const_sql_78 = """select cast(cast("-0.9983" as float) as decimalv3(9, 3));"""
    qt_sql_78_strict "${const_sql_78}"
    testFoldConst("${const_sql_78}")
    def const_sql_79 = """select cast(cast("-0.9993" as float) as decimalv3(9, 3));"""
    qt_sql_79_strict "${const_sql_79}"
    testFoldConst("${const_sql_79}")
    def const_sql_80 = """select cast(cast("-1.0003" as float) as decimalv3(9, 3));"""
    qt_sql_80_strict "${const_sql_80}"
    testFoldConst("${const_sql_80}")
    def const_sql_81 = """select cast(cast("-1.0013" as float) as decimalv3(9, 3));"""
    qt_sql_81_strict "${const_sql_81}"
    testFoldConst("${const_sql_81}")
    def const_sql_82 = """select cast(cast("-1.0093" as float) as decimalv3(9, 3));"""
    qt_sql_82_strict "${const_sql_82}"
    testFoldConst("${const_sql_82}")
    def const_sql_83 = """select cast(cast("-1.0993" as float) as decimalv3(9, 3));"""
    qt_sql_83_strict "${const_sql_83}"
    testFoldConst("${const_sql_83}")
    def const_sql_84 = """select cast(cast("-1.9003" as float) as decimalv3(9, 3));"""
    qt_sql_84_strict "${const_sql_84}"
    testFoldConst("${const_sql_84}")
    def const_sql_85 = """select cast(cast("-1.9013" as float) as decimalv3(9, 3));"""
    qt_sql_85_strict "${const_sql_85}"
    testFoldConst("${const_sql_85}")
    def const_sql_86 = """select cast(cast("-1.9983" as float) as decimalv3(9, 3));"""
    qt_sql_86_strict "${const_sql_86}"
    testFoldConst("${const_sql_86}")
    def const_sql_87 = """select cast(cast("-1.9993" as float) as decimalv3(9, 3));"""
    qt_sql_87_strict "${const_sql_87}"
    testFoldConst("${const_sql_87}")
    def const_sql_88 = """select cast(cast("-9.0003" as float) as decimalv3(9, 3));"""
    qt_sql_88_strict "${const_sql_88}"
    testFoldConst("${const_sql_88}")
    def const_sql_89 = """select cast(cast("-9.0013" as float) as decimalv3(9, 3));"""
    qt_sql_89_strict "${const_sql_89}"
    testFoldConst("${const_sql_89}")
    def const_sql_90 = """select cast(cast("-9.0093" as float) as decimalv3(9, 3));"""
    qt_sql_90_strict "${const_sql_90}"
    testFoldConst("${const_sql_90}")
    def const_sql_91 = """select cast(cast("-9.0993" as float) as decimalv3(9, 3));"""
    qt_sql_91_strict "${const_sql_91}"
    testFoldConst("${const_sql_91}")
    def const_sql_92 = """select cast(cast("-9.9003" as float) as decimalv3(9, 3));"""
    qt_sql_92_strict "${const_sql_92}"
    testFoldConst("${const_sql_92}")
    def const_sql_93 = """select cast(cast("-9.9013" as float) as decimalv3(9, 3));"""
    qt_sql_93_strict "${const_sql_93}"
    testFoldConst("${const_sql_93}")
    def const_sql_94 = """select cast(cast("-9.9983" as float) as decimalv3(9, 3));"""
    qt_sql_94_strict "${const_sql_94}"
    testFoldConst("${const_sql_94}")
    def const_sql_95 = """select cast(cast("-9.9993" as float) as decimalv3(9, 3));"""
    qt_sql_95_strict "${const_sql_95}"
    testFoldConst("${const_sql_95}")
    def const_sql_96 = """select cast(cast("-0.0009" as float) as decimalv3(9, 3));"""
    qt_sql_96_strict "${const_sql_96}"
    testFoldConst("${const_sql_96}")
    def const_sql_97 = """select cast(cast("-0.0019" as float) as decimalv3(9, 3));"""
    qt_sql_97_strict "${const_sql_97}"
    testFoldConst("${const_sql_97}")
    def const_sql_98 = """select cast(cast("-0.0099" as float) as decimalv3(9, 3));"""
    qt_sql_98_strict "${const_sql_98}"
    testFoldConst("${const_sql_98}")
    def const_sql_99 = """select cast(cast("-0.0999" as float) as decimalv3(9, 3));"""
    qt_sql_99_strict "${const_sql_99}"
    testFoldConst("${const_sql_99}")
    def const_sql_100 = """select cast(cast("-0.9009" as float) as decimalv3(9, 3));"""
    qt_sql_100_strict "${const_sql_100}"
    testFoldConst("${const_sql_100}")
    def const_sql_101 = """select cast(cast("-0.9019" as float) as decimalv3(9, 3));"""
    qt_sql_101_strict "${const_sql_101}"
    testFoldConst("${const_sql_101}")
    def const_sql_102 = """select cast(cast("-0.9989" as float) as decimalv3(9, 3));"""
    qt_sql_102_strict "${const_sql_102}"
    testFoldConst("${const_sql_102}")
    def const_sql_103 = """select cast(cast("-0.9999" as float) as decimalv3(9, 3));"""
    qt_sql_103_strict "${const_sql_103}"
    testFoldConst("${const_sql_103}")
    def const_sql_104 = """select cast(cast("-1.0009" as float) as decimalv3(9, 3));"""
    qt_sql_104_strict "${const_sql_104}"
    testFoldConst("${const_sql_104}")
    def const_sql_105 = """select cast(cast("-1.0019" as float) as decimalv3(9, 3));"""
    qt_sql_105_strict "${const_sql_105}"
    testFoldConst("${const_sql_105}")
    def const_sql_106 = """select cast(cast("-1.0099" as float) as decimalv3(9, 3));"""
    qt_sql_106_strict "${const_sql_106}"
    testFoldConst("${const_sql_106}")
    def const_sql_107 = """select cast(cast("-1.0999" as float) as decimalv3(9, 3));"""
    qt_sql_107_strict "${const_sql_107}"
    testFoldConst("${const_sql_107}")
    def const_sql_108 = """select cast(cast("-1.9009" as float) as decimalv3(9, 3));"""
    qt_sql_108_strict "${const_sql_108}"
    testFoldConst("${const_sql_108}")
    def const_sql_109 = """select cast(cast("-1.9019" as float) as decimalv3(9, 3));"""
    qt_sql_109_strict "${const_sql_109}"
    testFoldConst("${const_sql_109}")
    def const_sql_110 = """select cast(cast("-1.9989" as float) as decimalv3(9, 3));"""
    qt_sql_110_strict "${const_sql_110}"
    testFoldConst("${const_sql_110}")
    def const_sql_111 = """select cast(cast("-1.9999" as float) as decimalv3(9, 3));"""
    qt_sql_111_strict "${const_sql_111}"
    testFoldConst("${const_sql_111}")
    def const_sql_112 = """select cast(cast("-9.0009" as float) as decimalv3(9, 3));"""
    qt_sql_112_strict "${const_sql_112}"
    testFoldConst("${const_sql_112}")
    def const_sql_113 = """select cast(cast("-9.0019" as float) as decimalv3(9, 3));"""
    qt_sql_113_strict "${const_sql_113}"
    testFoldConst("${const_sql_113}")
    def const_sql_114 = """select cast(cast("-9.0099" as float) as decimalv3(9, 3));"""
    qt_sql_114_strict "${const_sql_114}"
    testFoldConst("${const_sql_114}")
    def const_sql_115 = """select cast(cast("-9.0999" as float) as decimalv3(9, 3));"""
    qt_sql_115_strict "${const_sql_115}"
    testFoldConst("${const_sql_115}")
    def const_sql_116 = """select cast(cast("-9.9009" as float) as decimalv3(9, 3));"""
    qt_sql_116_strict "${const_sql_116}"
    testFoldConst("${const_sql_116}")
    def const_sql_117 = """select cast(cast("-9.9019" as float) as decimalv3(9, 3));"""
    qt_sql_117_strict "${const_sql_117}"
    testFoldConst("${const_sql_117}")
    def const_sql_118 = """select cast(cast("-9.9989" as float) as decimalv3(9, 3));"""
    qt_sql_118_strict "${const_sql_118}"
    testFoldConst("${const_sql_118}")
    def const_sql_119 = """select cast(cast("-9.9999" as float) as decimalv3(9, 3));"""
    qt_sql_119_strict "${const_sql_119}"
    testFoldConst("${const_sql_119}")
    sql "set enable_strict_cast=false;"
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
    sql "set enable_strict_cast=true;"
    sql "set enable_strict_cast=false;"
}