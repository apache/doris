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


suite("test_cast_to_smallint_from_string_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "+0000", cast("+0000" as smallint);"""
    qt_sql_0_0_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    def const_sql_0_1 = """select "+0001", cast("+0001" as smallint);"""
    qt_sql_0_1_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    def const_sql_0_2 = """select "+0009", cast("+0009" as smallint);"""
    qt_sql_0_2_strict "${const_sql_0_2}"
    testFoldConst("${const_sql_0_2}")
    def const_sql_0_3 = """select "+000123", cast("+000123" as smallint);"""
    qt_sql_0_3_strict "${const_sql_0_3}"
    testFoldConst("${const_sql_0_3}")
    def const_sql_0_4 = """select "+00032767", cast("+00032767" as smallint);"""
    qt_sql_0_4_strict "${const_sql_0_4}"
    testFoldConst("${const_sql_0_4}")
    def const_sql_0_5 = """select "-0001", cast("-0001" as smallint);"""
    qt_sql_0_5_strict "${const_sql_0_5}"
    testFoldConst("${const_sql_0_5}")
    def const_sql_0_6 = """select "-0009", cast("-0009" as smallint);"""
    qt_sql_0_6_strict "${const_sql_0_6}"
    testFoldConst("${const_sql_0_6}")
    def const_sql_0_7 = """select "-000123", cast("-000123" as smallint);"""
    qt_sql_0_7_strict "${const_sql_0_7}"
    testFoldConst("${const_sql_0_7}")
    def const_sql_0_8 = """select "-00032768", cast("-00032768" as smallint);"""
    qt_sql_0_8_strict "${const_sql_0_8}"
    testFoldConst("${const_sql_0_8}")
    def const_sql_0_9 = """select "+00032766", cast("+00032766" as smallint);"""
    qt_sql_0_9_strict "${const_sql_0_9}"
    testFoldConst("${const_sql_0_9}")
    def const_sql_0_10 = """select "-00032767", cast("-00032767" as smallint);"""
    qt_sql_0_10_strict "${const_sql_0_10}"
    testFoldConst("${const_sql_0_10}")
    def const_sql_0_11 = """select "+0", cast("+0" as smallint);"""
    qt_sql_0_11_strict "${const_sql_0_11}"
    testFoldConst("${const_sql_0_11}")
    def const_sql_0_12 = """select "+1", cast("+1" as smallint);"""
    qt_sql_0_12_strict "${const_sql_0_12}"
    testFoldConst("${const_sql_0_12}")
    def const_sql_0_13 = """select "+9", cast("+9" as smallint);"""
    qt_sql_0_13_strict "${const_sql_0_13}"
    testFoldConst("${const_sql_0_13}")
    def const_sql_0_14 = """select "+123", cast("+123" as smallint);"""
    qt_sql_0_14_strict "${const_sql_0_14}"
    testFoldConst("${const_sql_0_14}")
    def const_sql_0_15 = """select "+32767", cast("+32767" as smallint);"""
    qt_sql_0_15_strict "${const_sql_0_15}"
    testFoldConst("${const_sql_0_15}")
    def const_sql_0_16 = """select "-1", cast("-1" as smallint);"""
    qt_sql_0_16_strict "${const_sql_0_16}"
    testFoldConst("${const_sql_0_16}")
    def const_sql_0_17 = """select "-9", cast("-9" as smallint);"""
    qt_sql_0_17_strict "${const_sql_0_17}"
    testFoldConst("${const_sql_0_17}")
    def const_sql_0_18 = """select "-123", cast("-123" as smallint);"""
    qt_sql_0_18_strict "${const_sql_0_18}"
    testFoldConst("${const_sql_0_18}")
    def const_sql_0_19 = """select "-32768", cast("-32768" as smallint);"""
    qt_sql_0_19_strict "${const_sql_0_19}"
    testFoldConst("${const_sql_0_19}")
    def const_sql_0_20 = """select "+32766", cast("+32766" as smallint);"""
    qt_sql_0_20_strict "${const_sql_0_20}"
    testFoldConst("${const_sql_0_20}")
    def const_sql_0_21 = """select "-32767", cast("-32767" as smallint);"""
    qt_sql_0_21_strict "${const_sql_0_21}"
    testFoldConst("${const_sql_0_21}")
    def const_sql_0_22 = """select "0000", cast("0000" as smallint);"""
    qt_sql_0_22_strict "${const_sql_0_22}"
    testFoldConst("${const_sql_0_22}")
    def const_sql_0_23 = """select "0001", cast("0001" as smallint);"""
    qt_sql_0_23_strict "${const_sql_0_23}"
    testFoldConst("${const_sql_0_23}")
    def const_sql_0_24 = """select "0009", cast("0009" as smallint);"""
    qt_sql_0_24_strict "${const_sql_0_24}"
    testFoldConst("${const_sql_0_24}")
    def const_sql_0_25 = """select "000123", cast("000123" as smallint);"""
    qt_sql_0_25_strict "${const_sql_0_25}"
    testFoldConst("${const_sql_0_25}")
    def const_sql_0_26 = """select "00032767", cast("00032767" as smallint);"""
    qt_sql_0_26_strict "${const_sql_0_26}"
    testFoldConst("${const_sql_0_26}")
    def const_sql_0_27 = """select "-0001", cast("-0001" as smallint);"""
    qt_sql_0_27_strict "${const_sql_0_27}"
    testFoldConst("${const_sql_0_27}")
    def const_sql_0_28 = """select "-0009", cast("-0009" as smallint);"""
    qt_sql_0_28_strict "${const_sql_0_28}"
    testFoldConst("${const_sql_0_28}")
    def const_sql_0_29 = """select "-000123", cast("-000123" as smallint);"""
    qt_sql_0_29_strict "${const_sql_0_29}"
    testFoldConst("${const_sql_0_29}")
    def const_sql_0_30 = """select "-00032768", cast("-00032768" as smallint);"""
    qt_sql_0_30_strict "${const_sql_0_30}"
    testFoldConst("${const_sql_0_30}")
    def const_sql_0_31 = """select "00032766", cast("00032766" as smallint);"""
    qt_sql_0_31_strict "${const_sql_0_31}"
    testFoldConst("${const_sql_0_31}")
    def const_sql_0_32 = """select "-00032767", cast("-00032767" as smallint);"""
    qt_sql_0_32_strict "${const_sql_0_32}"
    testFoldConst("${const_sql_0_32}")
    def const_sql_0_33 = """select "0", cast("0" as smallint);"""
    qt_sql_0_33_strict "${const_sql_0_33}"
    testFoldConst("${const_sql_0_33}")
    def const_sql_0_34 = """select "1", cast("1" as smallint);"""
    qt_sql_0_34_strict "${const_sql_0_34}"
    testFoldConst("${const_sql_0_34}")
    def const_sql_0_35 = """select "9", cast("9" as smallint);"""
    qt_sql_0_35_strict "${const_sql_0_35}"
    testFoldConst("${const_sql_0_35}")
    def const_sql_0_36 = """select "123", cast("123" as smallint);"""
    qt_sql_0_36_strict "${const_sql_0_36}"
    testFoldConst("${const_sql_0_36}")
    def const_sql_0_37 = """select "32767", cast("32767" as smallint);"""
    qt_sql_0_37_strict "${const_sql_0_37}"
    testFoldConst("${const_sql_0_37}")
    def const_sql_0_38 = """select "-1", cast("-1" as smallint);"""
    qt_sql_0_38_strict "${const_sql_0_38}"
    testFoldConst("${const_sql_0_38}")
    def const_sql_0_39 = """select "-9", cast("-9" as smallint);"""
    qt_sql_0_39_strict "${const_sql_0_39}"
    testFoldConst("${const_sql_0_39}")
    def const_sql_0_40 = """select "-123", cast("-123" as smallint);"""
    qt_sql_0_40_strict "${const_sql_0_40}"
    testFoldConst("${const_sql_0_40}")
    def const_sql_0_41 = """select "-32768", cast("-32768" as smallint);"""
    qt_sql_0_41_strict "${const_sql_0_41}"
    testFoldConst("${const_sql_0_41}")
    def const_sql_0_42 = """select "32766", cast("32766" as smallint);"""
    qt_sql_0_42_strict "${const_sql_0_42}"
    testFoldConst("${const_sql_0_42}")
    def const_sql_0_43 = """select "-32767", cast("-32767" as smallint);"""
    qt_sql_0_43_strict "${const_sql_0_43}"
    testFoldConst("${const_sql_0_43}")

    sql "set enable_strict_cast=false;"
    qt_sql_0_0_non_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    qt_sql_0_1_non_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    qt_sql_0_2_non_strict "${const_sql_0_2}"
    testFoldConst("${const_sql_0_2}")
    qt_sql_0_3_non_strict "${const_sql_0_3}"
    testFoldConst("${const_sql_0_3}")
    qt_sql_0_4_non_strict "${const_sql_0_4}"
    testFoldConst("${const_sql_0_4}")
    qt_sql_0_5_non_strict "${const_sql_0_5}"
    testFoldConst("${const_sql_0_5}")
    qt_sql_0_6_non_strict "${const_sql_0_6}"
    testFoldConst("${const_sql_0_6}")
    qt_sql_0_7_non_strict "${const_sql_0_7}"
    testFoldConst("${const_sql_0_7}")
    qt_sql_0_8_non_strict "${const_sql_0_8}"
    testFoldConst("${const_sql_0_8}")
    qt_sql_0_9_non_strict "${const_sql_0_9}"
    testFoldConst("${const_sql_0_9}")
    qt_sql_0_10_non_strict "${const_sql_0_10}"
    testFoldConst("${const_sql_0_10}")
    qt_sql_0_11_non_strict "${const_sql_0_11}"
    testFoldConst("${const_sql_0_11}")
    qt_sql_0_12_non_strict "${const_sql_0_12}"
    testFoldConst("${const_sql_0_12}")
    qt_sql_0_13_non_strict "${const_sql_0_13}"
    testFoldConst("${const_sql_0_13}")
    qt_sql_0_14_non_strict "${const_sql_0_14}"
    testFoldConst("${const_sql_0_14}")
    qt_sql_0_15_non_strict "${const_sql_0_15}"
    testFoldConst("${const_sql_0_15}")
    qt_sql_0_16_non_strict "${const_sql_0_16}"
    testFoldConst("${const_sql_0_16}")
    qt_sql_0_17_non_strict "${const_sql_0_17}"
    testFoldConst("${const_sql_0_17}")
    qt_sql_0_18_non_strict "${const_sql_0_18}"
    testFoldConst("${const_sql_0_18}")
    qt_sql_0_19_non_strict "${const_sql_0_19}"
    testFoldConst("${const_sql_0_19}")
    qt_sql_0_20_non_strict "${const_sql_0_20}"
    testFoldConst("${const_sql_0_20}")
    qt_sql_0_21_non_strict "${const_sql_0_21}"
    testFoldConst("${const_sql_0_21}")
    qt_sql_0_22_non_strict "${const_sql_0_22}"
    testFoldConst("${const_sql_0_22}")
    qt_sql_0_23_non_strict "${const_sql_0_23}"
    testFoldConst("${const_sql_0_23}")
    qt_sql_0_24_non_strict "${const_sql_0_24}"
    testFoldConst("${const_sql_0_24}")
    qt_sql_0_25_non_strict "${const_sql_0_25}"
    testFoldConst("${const_sql_0_25}")
    qt_sql_0_26_non_strict "${const_sql_0_26}"
    testFoldConst("${const_sql_0_26}")
    qt_sql_0_27_non_strict "${const_sql_0_27}"
    testFoldConst("${const_sql_0_27}")
    qt_sql_0_28_non_strict "${const_sql_0_28}"
    testFoldConst("${const_sql_0_28}")
    qt_sql_0_29_non_strict "${const_sql_0_29}"
    testFoldConst("${const_sql_0_29}")
    qt_sql_0_30_non_strict "${const_sql_0_30}"
    testFoldConst("${const_sql_0_30}")
    qt_sql_0_31_non_strict "${const_sql_0_31}"
    testFoldConst("${const_sql_0_31}")
    qt_sql_0_32_non_strict "${const_sql_0_32}"
    testFoldConst("${const_sql_0_32}")
    qt_sql_0_33_non_strict "${const_sql_0_33}"
    testFoldConst("${const_sql_0_33}")
    qt_sql_0_34_non_strict "${const_sql_0_34}"
    testFoldConst("${const_sql_0_34}")
    qt_sql_0_35_non_strict "${const_sql_0_35}"
    testFoldConst("${const_sql_0_35}")
    qt_sql_0_36_non_strict "${const_sql_0_36}"
    testFoldConst("${const_sql_0_36}")
    qt_sql_0_37_non_strict "${const_sql_0_37}"
    testFoldConst("${const_sql_0_37}")
    qt_sql_0_38_non_strict "${const_sql_0_38}"
    testFoldConst("${const_sql_0_38}")
    qt_sql_0_39_non_strict "${const_sql_0_39}"
    testFoldConst("${const_sql_0_39}")
    qt_sql_0_40_non_strict "${const_sql_0_40}"
    testFoldConst("${const_sql_0_40}")
    qt_sql_0_41_non_strict "${const_sql_0_41}"
    testFoldConst("${const_sql_0_41}")
    qt_sql_0_42_non_strict "${const_sql_0_42}"
    testFoldConst("${const_sql_0_42}")
    qt_sql_0_43_non_strict "${const_sql_0_43}"
    testFoldConst("${const_sql_0_43}")
}