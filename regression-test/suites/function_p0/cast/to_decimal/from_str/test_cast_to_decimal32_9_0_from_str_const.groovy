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
    sql "set enable_strict_cast=true;"
    def const_sql_52 = """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 0));"""
    qt_sql_52_strict "${const_sql_52}"
    testFoldConst("${const_sql_52}")
    def const_sql_53 = """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 0));"""
    qt_sql_53_strict "${const_sql_53}"
    testFoldConst("${const_sql_53}")
    def const_sql_54 = """select cast("0" as decimalv3(9, 0));"""
    qt_sql_54_strict "${const_sql_54}"
    testFoldConst("${const_sql_54}")
    def const_sql_55 = """select cast("1" as decimalv3(9, 0));"""
    qt_sql_55_strict "${const_sql_55}"
    testFoldConst("${const_sql_55}")
    def const_sql_56 = """select cast("9" as decimalv3(9, 0));"""
    qt_sql_56_strict "${const_sql_56}"
    testFoldConst("${const_sql_56}")
    def const_sql_57 = """select cast("99999999" as decimalv3(9, 0));"""
    qt_sql_57_strict "${const_sql_57}"
    testFoldConst("${const_sql_57}")
    def const_sql_58 = """select cast("900000000" as decimalv3(9, 0));"""
    qt_sql_58_strict "${const_sql_58}"
    testFoldConst("${const_sql_58}")
    def const_sql_59 = """select cast("900000001" as decimalv3(9, 0));"""
    qt_sql_59_strict "${const_sql_59}"
    testFoldConst("${const_sql_59}")
    def const_sql_60 = """select cast("999999998" as decimalv3(9, 0));"""
    qt_sql_60_strict "${const_sql_60}"
    testFoldConst("${const_sql_60}")
    def const_sql_61 = """select cast("999999999" as decimalv3(9, 0));"""
    qt_sql_61_strict "${const_sql_61}"
    testFoldConst("${const_sql_61}")
    def const_sql_62 = """select cast("0." as decimalv3(9, 0));"""
    qt_sql_62_strict "${const_sql_62}"
    testFoldConst("${const_sql_62}")
    def const_sql_63 = """select cast("1." as decimalv3(9, 0));"""
    qt_sql_63_strict "${const_sql_63}"
    testFoldConst("${const_sql_63}")
    def const_sql_64 = """select cast("9." as decimalv3(9, 0));"""
    qt_sql_64_strict "${const_sql_64}"
    testFoldConst("${const_sql_64}")
    def const_sql_65 = """select cast("99999999." as decimalv3(9, 0));"""
    qt_sql_65_strict "${const_sql_65}"
    testFoldConst("${const_sql_65}")
    def const_sql_66 = """select cast("900000000." as decimalv3(9, 0));"""
    qt_sql_66_strict "${const_sql_66}"
    testFoldConst("${const_sql_66}")
    def const_sql_67 = """select cast("900000001." as decimalv3(9, 0));"""
    qt_sql_67_strict "${const_sql_67}"
    testFoldConst("${const_sql_67}")
    def const_sql_68 = """select cast("999999998." as decimalv3(9, 0));"""
    qt_sql_68_strict "${const_sql_68}"
    testFoldConst("${const_sql_68}")
    def const_sql_69 = """select cast("999999999." as decimalv3(9, 0));"""
    qt_sql_69_strict "${const_sql_69}"
    testFoldConst("${const_sql_69}")
    def const_sql_70 = """select cast("-0" as decimalv3(9, 0));"""
    qt_sql_70_strict "${const_sql_70}"
    testFoldConst("${const_sql_70}")
    def const_sql_71 = """select cast("-1" as decimalv3(9, 0));"""
    qt_sql_71_strict "${const_sql_71}"
    testFoldConst("${const_sql_71}")
    def const_sql_72 = """select cast("-9" as decimalv3(9, 0));"""
    qt_sql_72_strict "${const_sql_72}"
    testFoldConst("${const_sql_72}")
    def const_sql_73 = """select cast("-99999999" as decimalv3(9, 0));"""
    qt_sql_73_strict "${const_sql_73}"
    testFoldConst("${const_sql_73}")
    def const_sql_74 = """select cast("-900000000" as decimalv3(9, 0));"""
    qt_sql_74_strict "${const_sql_74}"
    testFoldConst("${const_sql_74}")
    def const_sql_75 = """select cast("-900000001" as decimalv3(9, 0));"""
    qt_sql_75_strict "${const_sql_75}"
    testFoldConst("${const_sql_75}")
    def const_sql_76 = """select cast("-999999998" as decimalv3(9, 0));"""
    qt_sql_76_strict "${const_sql_76}"
    testFoldConst("${const_sql_76}")
    def const_sql_77 = """select cast("-999999999" as decimalv3(9, 0));"""
    qt_sql_77_strict "${const_sql_77}"
    testFoldConst("${const_sql_77}")
    def const_sql_78 = """select cast("-0." as decimalv3(9, 0));"""
    qt_sql_78_strict "${const_sql_78}"
    testFoldConst("${const_sql_78}")
    def const_sql_79 = """select cast("-1." as decimalv3(9, 0));"""
    qt_sql_79_strict "${const_sql_79}"
    testFoldConst("${const_sql_79}")
    def const_sql_80 = """select cast("-9." as decimalv3(9, 0));"""
    qt_sql_80_strict "${const_sql_80}"
    testFoldConst("${const_sql_80}")
    def const_sql_81 = """select cast("-99999999." as decimalv3(9, 0));"""
    qt_sql_81_strict "${const_sql_81}"
    testFoldConst("${const_sql_81}")
    def const_sql_82 = """select cast("-900000000." as decimalv3(9, 0));"""
    qt_sql_82_strict "${const_sql_82}"
    testFoldConst("${const_sql_82}")
    def const_sql_83 = """select cast("-900000001." as decimalv3(9, 0));"""
    qt_sql_83_strict "${const_sql_83}"
    testFoldConst("${const_sql_83}")
    def const_sql_84 = """select cast("-999999998." as decimalv3(9, 0));"""
    qt_sql_84_strict "${const_sql_84}"
    testFoldConst("${const_sql_84}")
    def const_sql_85 = """select cast("-999999999." as decimalv3(9, 0));"""
    qt_sql_85_strict "${const_sql_85}"
    testFoldConst("${const_sql_85}")
    def const_sql_86 = """select cast("0.49999" as decimalv3(9, 0));"""
    qt_sql_86_strict "${const_sql_86}"
    testFoldConst("${const_sql_86}")
    def const_sql_87 = """select cast("1.49999" as decimalv3(9, 0));"""
    qt_sql_87_strict "${const_sql_87}"
    testFoldConst("${const_sql_87}")
    def const_sql_88 = """select cast("9.49999" as decimalv3(9, 0));"""
    qt_sql_88_strict "${const_sql_88}"
    testFoldConst("${const_sql_88}")
    def const_sql_89 = """select cast("99999999.49999" as decimalv3(9, 0));"""
    qt_sql_89_strict "${const_sql_89}"
    testFoldConst("${const_sql_89}")
    def const_sql_90 = """select cast("900000000.49999" as decimalv3(9, 0));"""
    qt_sql_90_strict "${const_sql_90}"
    testFoldConst("${const_sql_90}")
    def const_sql_91 = """select cast("900000001.49999" as decimalv3(9, 0));"""
    qt_sql_91_strict "${const_sql_91}"
    testFoldConst("${const_sql_91}")
    def const_sql_92 = """select cast("999999998.49999" as decimalv3(9, 0));"""
    qt_sql_92_strict "${const_sql_92}"
    testFoldConst("${const_sql_92}")
    def const_sql_93 = """select cast("999999999.49999" as decimalv3(9, 0));"""
    qt_sql_93_strict "${const_sql_93}"
    testFoldConst("${const_sql_93}")
    def const_sql_94 = """select cast("0.5" as decimalv3(9, 0));"""
    qt_sql_94_strict "${const_sql_94}"
    testFoldConst("${const_sql_94}")
    def const_sql_95 = """select cast("1.5" as decimalv3(9, 0));"""
    qt_sql_95_strict "${const_sql_95}"
    testFoldConst("${const_sql_95}")
    def const_sql_96 = """select cast("9.5" as decimalv3(9, 0));"""
    qt_sql_96_strict "${const_sql_96}"
    testFoldConst("${const_sql_96}")
    def const_sql_97 = """select cast("99999999.5" as decimalv3(9, 0));"""
    qt_sql_97_strict "${const_sql_97}"
    testFoldConst("${const_sql_97}")
    def const_sql_98 = """select cast("900000000.5" as decimalv3(9, 0));"""
    qt_sql_98_strict "${const_sql_98}"
    testFoldConst("${const_sql_98}")
    def const_sql_99 = """select cast("900000001.5" as decimalv3(9, 0));"""
    qt_sql_99_strict "${const_sql_99}"
    testFoldConst("${const_sql_99}")
    def const_sql_100 = """select cast("999999998.5" as decimalv3(9, 0));"""
    qt_sql_100_strict "${const_sql_100}"
    testFoldConst("${const_sql_100}")
    def const_sql_101 = """select cast("999999999.49999" as decimalv3(9, 0));"""
    qt_sql_101_strict "${const_sql_101}"
    testFoldConst("${const_sql_101}")
    def const_sql_102 = """select cast("-0.49999" as decimalv3(9, 0));"""
    qt_sql_102_strict "${const_sql_102}"
    testFoldConst("${const_sql_102}")
    def const_sql_103 = """select cast("-1.49999" as decimalv3(9, 0));"""
    qt_sql_103_strict "${const_sql_103}"
    testFoldConst("${const_sql_103}")
    def const_sql_104 = """select cast("-9.49999" as decimalv3(9, 0));"""
    qt_sql_104_strict "${const_sql_104}"
    testFoldConst("${const_sql_104}")
    def const_sql_105 = """select cast("-99999999.49999" as decimalv3(9, 0));"""
    qt_sql_105_strict "${const_sql_105}"
    testFoldConst("${const_sql_105}")
    def const_sql_106 = """select cast("-900000000.49999" as decimalv3(9, 0));"""
    qt_sql_106_strict "${const_sql_106}"
    testFoldConst("${const_sql_106}")
    def const_sql_107 = """select cast("-900000001.49999" as decimalv3(9, 0));"""
    qt_sql_107_strict "${const_sql_107}"
    testFoldConst("${const_sql_107}")
    def const_sql_108 = """select cast("-999999998.49999" as decimalv3(9, 0));"""
    qt_sql_108_strict "${const_sql_108}"
    testFoldConst("${const_sql_108}")
    def const_sql_109 = """select cast("-999999999.49999" as decimalv3(9, 0));"""
    qt_sql_109_strict "${const_sql_109}"
    testFoldConst("${const_sql_109}")
    def const_sql_110 = """select cast("-0.5" as decimalv3(9, 0));"""
    qt_sql_110_strict "${const_sql_110}"
    testFoldConst("${const_sql_110}")
    def const_sql_111 = """select cast("-1.5" as decimalv3(9, 0));"""
    qt_sql_111_strict "${const_sql_111}"
    testFoldConst("${const_sql_111}")
    def const_sql_112 = """select cast("-9.5" as decimalv3(9, 0));"""
    qt_sql_112_strict "${const_sql_112}"
    testFoldConst("${const_sql_112}")
    def const_sql_113 = """select cast("-99999999.5" as decimalv3(9, 0));"""
    qt_sql_113_strict "${const_sql_113}"
    testFoldConst("${const_sql_113}")
    def const_sql_114 = """select cast("-900000000.5" as decimalv3(9, 0));"""
    qt_sql_114_strict "${const_sql_114}"
    testFoldConst("${const_sql_114}")
    def const_sql_115 = """select cast("-900000001.5" as decimalv3(9, 0));"""
    qt_sql_115_strict "${const_sql_115}"
    testFoldConst("${const_sql_115}")
    def const_sql_116 = """select cast("-999999998.5" as decimalv3(9, 0));"""
    qt_sql_116_strict "${const_sql_116}"
    testFoldConst("${const_sql_116}")
    def const_sql_117 = """select cast("-999999999.49999" as decimalv3(9, 0));"""
    qt_sql_117_strict "${const_sql_117}"
    testFoldConst("${const_sql_117}")
    sql "set enable_strict_cast=false;"
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
}