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


suite("test_cast_str_to_int_with_fraction_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    def test_cast_str_to_int_with_fraction_strs = ["""+0000.4""","""+0000.5""","""+0001.4""","""+0001.5""","""+0009.4""","""+0009.5""","""+000123.4""","""+000123.5""","""+0002147483647.4""","""+0002147483647.5""","""-0001.4""","""-0001.5""","""-0009.4""","""-0009.5""","""-000123.4""","""-000123.5""","""-0002147483648.4""","""-0002147483648.5""","""+0002147483646.4""","""+0002147483646.5""",
        """-0002147483647.4""","""-0002147483647.5""","""+0.4""","""+0.5""","""+1.4""","""+1.5""","""+9.4""","""+9.5""","""+123.4""","""+123.5""","""+2147483647.4""","""+2147483647.5""","""-1.4""","""-1.5""","""-9.4""","""-9.5""","""-123.4""","""-123.5""","""-2147483648.4""","""-2147483648.5""",
        """+2147483646.4""","""+2147483646.5""","""-2147483647.4""","""-2147483647.5""","""0000.4""","""0000.5""","""0001.4""","""0001.5""","""0009.4""","""0009.5""","""000123.4""","""000123.5""","""0002147483647.4""","""0002147483647.5""","""-0001.4""","""-0001.5""","""-0009.4""","""-0009.5""","""-000123.4""","""-000123.5""",
        """-0002147483648.4""","""-0002147483648.5""","""0002147483646.4""","""0002147483646.5""","""-0002147483647.4""","""-0002147483647.5""","""0.4""","""0.5""","""1.4""","""1.5""","""9.4""","""9.5""","""123.4""","""123.5""","""2147483647.4""","""2147483647.5""","""-1.4""","""-1.5""","""-9.4""","""-9.5""",
        """-123.4""","""-123.5""","""-2147483648.4""","""-2147483648.5""","""2147483646.4""","""2147483646.5""","""-2147483647.4""","""-2147483647.5"""]
    sql "set enable_strict_cast=true;"
    for (test_str in test_cast_str_to_int_with_fraction_strs) {
        test {
            sql """select cast("${test_str}" as int);"""
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict """select "+0000.4", cast("+0000.4" as int);"""
    qt_sql_1_non_strict """select "+0000.5", cast("+0000.5" as int);"""
    qt_sql_2_non_strict """select "+0001.4", cast("+0001.4" as int);"""
    qt_sql_3_non_strict """select "+0001.5", cast("+0001.5" as int);"""
    qt_sql_4_non_strict """select "+0009.4", cast("+0009.4" as int);"""
    qt_sql_5_non_strict """select "+0009.5", cast("+0009.5" as int);"""
    qt_sql_6_non_strict """select "+000123.4", cast("+000123.4" as int);"""
    qt_sql_7_non_strict """select "+000123.5", cast("+000123.5" as int);"""
    qt_sql_8_non_strict """select "+0002147483647.4", cast("+0002147483647.4" as int);"""
    qt_sql_9_non_strict """select "+0002147483647.5", cast("+0002147483647.5" as int);"""
    qt_sql_10_non_strict """select "-0001.4", cast("-0001.4" as int);"""
    qt_sql_11_non_strict """select "-0001.5", cast("-0001.5" as int);"""
    qt_sql_12_non_strict """select "-0009.4", cast("-0009.4" as int);"""
    qt_sql_13_non_strict """select "-0009.5", cast("-0009.5" as int);"""
    qt_sql_14_non_strict """select "-000123.4", cast("-000123.4" as int);"""
    qt_sql_15_non_strict """select "-000123.5", cast("-000123.5" as int);"""
    qt_sql_16_non_strict """select "-0002147483648.4", cast("-0002147483648.4" as int);"""
    qt_sql_17_non_strict """select "-0002147483648.5", cast("-0002147483648.5" as int);"""
    qt_sql_18_non_strict """select "+0002147483646.4", cast("+0002147483646.4" as int);"""
    qt_sql_19_non_strict """select "+0002147483646.5", cast("+0002147483646.5" as int);"""
    qt_sql_20_non_strict """select "-0002147483647.4", cast("-0002147483647.4" as int);"""
    qt_sql_21_non_strict """select "-0002147483647.5", cast("-0002147483647.5" as int);"""
    qt_sql_22_non_strict """select "+0.4", cast("+0.4" as int);"""
    qt_sql_23_non_strict """select "+0.5", cast("+0.5" as int);"""
    qt_sql_24_non_strict """select "+1.4", cast("+1.4" as int);"""
    qt_sql_25_non_strict """select "+1.5", cast("+1.5" as int);"""
    qt_sql_26_non_strict """select "+9.4", cast("+9.4" as int);"""
    qt_sql_27_non_strict """select "+9.5", cast("+9.5" as int);"""
    qt_sql_28_non_strict """select "+123.4", cast("+123.4" as int);"""
    qt_sql_29_non_strict """select "+123.5", cast("+123.5" as int);"""
    qt_sql_30_non_strict """select "+2147483647.4", cast("+2147483647.4" as int);"""
    qt_sql_31_non_strict """select "+2147483647.5", cast("+2147483647.5" as int);"""
    qt_sql_32_non_strict """select "-1.4", cast("-1.4" as int);"""
    qt_sql_33_non_strict """select "-1.5", cast("-1.5" as int);"""
    qt_sql_34_non_strict """select "-9.4", cast("-9.4" as int);"""
    qt_sql_35_non_strict """select "-9.5", cast("-9.5" as int);"""
    qt_sql_36_non_strict """select "-123.4", cast("-123.4" as int);"""
    qt_sql_37_non_strict """select "-123.5", cast("-123.5" as int);"""
    qt_sql_38_non_strict """select "-2147483648.4", cast("-2147483648.4" as int);"""
    qt_sql_39_non_strict """select "-2147483648.5", cast("-2147483648.5" as int);"""
    qt_sql_40_non_strict """select "+2147483646.4", cast("+2147483646.4" as int);"""
    qt_sql_41_non_strict """select "+2147483646.5", cast("+2147483646.5" as int);"""
    qt_sql_42_non_strict """select "-2147483647.4", cast("-2147483647.4" as int);"""
    qt_sql_43_non_strict """select "-2147483647.5", cast("-2147483647.5" as int);"""
    qt_sql_44_non_strict """select "0000.4", cast("0000.4" as int);"""
    qt_sql_45_non_strict """select "0000.5", cast("0000.5" as int);"""
    qt_sql_46_non_strict """select "0001.4", cast("0001.4" as int);"""
    qt_sql_47_non_strict """select "0001.5", cast("0001.5" as int);"""
    qt_sql_48_non_strict """select "0009.4", cast("0009.4" as int);"""
    qt_sql_49_non_strict """select "0009.5", cast("0009.5" as int);"""
    qt_sql_50_non_strict """select "000123.4", cast("000123.4" as int);"""
    qt_sql_51_non_strict """select "000123.5", cast("000123.5" as int);"""
    qt_sql_52_non_strict """select "0002147483647.4", cast("0002147483647.4" as int);"""
    qt_sql_53_non_strict """select "0002147483647.5", cast("0002147483647.5" as int);"""
    qt_sql_54_non_strict """select "-0001.4", cast("-0001.4" as int);"""
    qt_sql_55_non_strict """select "-0001.5", cast("-0001.5" as int);"""
    qt_sql_56_non_strict """select "-0009.4", cast("-0009.4" as int);"""
    qt_sql_57_non_strict """select "-0009.5", cast("-0009.5" as int);"""
    qt_sql_58_non_strict """select "-000123.4", cast("-000123.4" as int);"""
    qt_sql_59_non_strict """select "-000123.5", cast("-000123.5" as int);"""
    qt_sql_60_non_strict """select "-0002147483648.4", cast("-0002147483648.4" as int);"""
    qt_sql_61_non_strict """select "-0002147483648.5", cast("-0002147483648.5" as int);"""
    qt_sql_62_non_strict """select "0002147483646.4", cast("0002147483646.4" as int);"""
    qt_sql_63_non_strict """select "0002147483646.5", cast("0002147483646.5" as int);"""
    qt_sql_64_non_strict """select "-0002147483647.4", cast("-0002147483647.4" as int);"""
    qt_sql_65_non_strict """select "-0002147483647.5", cast("-0002147483647.5" as int);"""
    qt_sql_66_non_strict """select "0.4", cast("0.4" as int);"""
    qt_sql_67_non_strict """select "0.5", cast("0.5" as int);"""
    qt_sql_68_non_strict """select "1.4", cast("1.4" as int);"""
    qt_sql_69_non_strict """select "1.5", cast("1.5" as int);"""
    qt_sql_70_non_strict """select "9.4", cast("9.4" as int);"""
    qt_sql_71_non_strict """select "9.5", cast("9.5" as int);"""
    qt_sql_72_non_strict """select "123.4", cast("123.4" as int);"""
    qt_sql_73_non_strict """select "123.5", cast("123.5" as int);"""
    qt_sql_74_non_strict """select "2147483647.4", cast("2147483647.4" as int);"""
    qt_sql_75_non_strict """select "2147483647.5", cast("2147483647.5" as int);"""
    qt_sql_76_non_strict """select "-1.4", cast("-1.4" as int);"""
    qt_sql_77_non_strict """select "-1.5", cast("-1.5" as int);"""
    qt_sql_78_non_strict """select "-9.4", cast("-9.4" as int);"""
    qt_sql_79_non_strict """select "-9.5", cast("-9.5" as int);"""
    qt_sql_80_non_strict """select "-123.4", cast("-123.4" as int);"""
    qt_sql_81_non_strict """select "-123.5", cast("-123.5" as int);"""
    qt_sql_82_non_strict """select "-2147483648.4", cast("-2147483648.4" as int);"""
    qt_sql_83_non_strict """select "-2147483648.5", cast("-2147483648.5" as int);"""
    qt_sql_84_non_strict """select "2147483646.4", cast("2147483646.4" as int);"""
    qt_sql_85_non_strict """select "2147483646.5", cast("2147483646.5" as int);"""
    qt_sql_86_non_strict """select "-2147483647.4", cast("-2147483647.4" as int);"""
    qt_sql_87_non_strict """select "-2147483647.5", cast("-2147483647.5" as int);"""
}