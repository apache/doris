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


suite("test_cast_str_to_int_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "set enable_strict_cast=true;"
    qt_sql_0_strict """select "+0000", cast("+0000" as int);"""
    qt_sql_1_strict """select "+0001", cast("+0001" as int);"""
    qt_sql_2_strict """select "+0009", cast("+0009" as int);"""
    qt_sql_3_strict """select "+000123", cast("+000123" as int);"""
    qt_sql_4_strict """select "+0002147483647", cast("+0002147483647" as int);"""
    qt_sql_5_strict """select "-0001", cast("-0001" as int);"""
    qt_sql_6_strict """select "-0009", cast("-0009" as int);"""
    qt_sql_7_strict """select "-000123", cast("-000123" as int);"""
    qt_sql_8_strict """select "-0002147483648", cast("-0002147483648" as int);"""
    qt_sql_9_strict """select "+0002147483646", cast("+0002147483646" as int);"""
    qt_sql_10_strict """select "-0002147483647", cast("-0002147483647" as int);"""
    qt_sql_11_strict """select "+0", cast("+0" as int);"""
    qt_sql_12_strict """select "+1", cast("+1" as int);"""
    qt_sql_13_strict """select "+9", cast("+9" as int);"""
    qt_sql_14_strict """select "+123", cast("+123" as int);"""
    qt_sql_15_strict """select "+2147483647", cast("+2147483647" as int);"""
    qt_sql_16_strict """select "-1", cast("-1" as int);"""
    qt_sql_17_strict """select "-9", cast("-9" as int);"""
    qt_sql_18_strict """select "-123", cast("-123" as int);"""
    qt_sql_19_strict """select "-2147483648", cast("-2147483648" as int);"""
    qt_sql_20_strict """select "+2147483646", cast("+2147483646" as int);"""
    qt_sql_21_strict """select "-2147483647", cast("-2147483647" as int);"""
    qt_sql_22_strict """select "0000", cast("0000" as int);"""
    qt_sql_23_strict """select "0001", cast("0001" as int);"""
    qt_sql_24_strict """select "0009", cast("0009" as int);"""
    qt_sql_25_strict """select "000123", cast("000123" as int);"""
    qt_sql_26_strict """select "0002147483647", cast("0002147483647" as int);"""
    qt_sql_27_strict """select "-0001", cast("-0001" as int);"""
    qt_sql_28_strict """select "-0009", cast("-0009" as int);"""
    qt_sql_29_strict """select "-000123", cast("-000123" as int);"""
    qt_sql_30_strict """select "-0002147483648", cast("-0002147483648" as int);"""
    qt_sql_31_strict """select "0002147483646", cast("0002147483646" as int);"""
    qt_sql_32_strict """select "-0002147483647", cast("-0002147483647" as int);"""
    qt_sql_33_strict """select "0", cast("0" as int);"""
    qt_sql_34_strict """select "1", cast("1" as int);"""
    qt_sql_35_strict """select "9", cast("9" as int);"""
    qt_sql_36_strict """select "123", cast("123" as int);"""
    qt_sql_37_strict """select "2147483647", cast("2147483647" as int);"""
    qt_sql_38_strict """select "-1", cast("-1" as int);"""
    qt_sql_39_strict """select "-9", cast("-9" as int);"""
    qt_sql_40_strict """select "-123", cast("-123" as int);"""
    qt_sql_41_strict """select "-2147483648", cast("-2147483648" as int);"""
    qt_sql_42_strict """select "2147483646", cast("2147483646" as int);"""
    qt_sql_43_strict """select "-2147483647", cast("-2147483647" as int);"""
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict """select "+0000", cast("+0000" as int);"""
    qt_sql_1_non_strict """select "+0001", cast("+0001" as int);"""
    qt_sql_2_non_strict """select "+0009", cast("+0009" as int);"""
    qt_sql_3_non_strict """select "+000123", cast("+000123" as int);"""
    qt_sql_4_non_strict """select "+0002147483647", cast("+0002147483647" as int);"""
    qt_sql_5_non_strict """select "-0001", cast("-0001" as int);"""
    qt_sql_6_non_strict """select "-0009", cast("-0009" as int);"""
    qt_sql_7_non_strict """select "-000123", cast("-000123" as int);"""
    qt_sql_8_non_strict """select "-0002147483648", cast("-0002147483648" as int);"""
    qt_sql_9_non_strict """select "+0002147483646", cast("+0002147483646" as int);"""
    qt_sql_10_non_strict """select "-0002147483647", cast("-0002147483647" as int);"""
    qt_sql_11_non_strict """select "+0", cast("+0" as int);"""
    qt_sql_12_non_strict """select "+1", cast("+1" as int);"""
    qt_sql_13_non_strict """select "+9", cast("+9" as int);"""
    qt_sql_14_non_strict """select "+123", cast("+123" as int);"""
    qt_sql_15_non_strict """select "+2147483647", cast("+2147483647" as int);"""
    qt_sql_16_non_strict """select "-1", cast("-1" as int);"""
    qt_sql_17_non_strict """select "-9", cast("-9" as int);"""
    qt_sql_18_non_strict """select "-123", cast("-123" as int);"""
    qt_sql_19_non_strict """select "-2147483648", cast("-2147483648" as int);"""
    qt_sql_20_non_strict """select "+2147483646", cast("+2147483646" as int);"""
    qt_sql_21_non_strict """select "-2147483647", cast("-2147483647" as int);"""
    qt_sql_22_non_strict """select "0000", cast("0000" as int);"""
    qt_sql_23_non_strict """select "0001", cast("0001" as int);"""
    qt_sql_24_non_strict """select "0009", cast("0009" as int);"""
    qt_sql_25_non_strict """select "000123", cast("000123" as int);"""
    qt_sql_26_non_strict """select "0002147483647", cast("0002147483647" as int);"""
    qt_sql_27_non_strict """select "-0001", cast("-0001" as int);"""
    qt_sql_28_non_strict """select "-0009", cast("-0009" as int);"""
    qt_sql_29_non_strict """select "-000123", cast("-000123" as int);"""
    qt_sql_30_non_strict """select "-0002147483648", cast("-0002147483648" as int);"""
    qt_sql_31_non_strict """select "0002147483646", cast("0002147483646" as int);"""
    qt_sql_32_non_strict """select "-0002147483647", cast("-0002147483647" as int);"""
    qt_sql_33_non_strict """select "0", cast("0" as int);"""
    qt_sql_34_non_strict """select "1", cast("1" as int);"""
    qt_sql_35_non_strict """select "9", cast("9" as int);"""
    qt_sql_36_non_strict """select "123", cast("123" as int);"""
    qt_sql_37_non_strict """select "2147483647", cast("2147483647" as int);"""
    qt_sql_38_non_strict """select "-1", cast("-1" as int);"""
    qt_sql_39_non_strict """select "-9", cast("-9" as int);"""
    qt_sql_40_non_strict """select "-123", cast("-123" as int);"""
    qt_sql_41_non_strict """select "-2147483648", cast("-2147483648" as int);"""
    qt_sql_42_non_strict """select "2147483646", cast("2147483646" as int);"""
    qt_sql_43_non_strict """select "-2147483647", cast("-2147483647" as int);"""
}