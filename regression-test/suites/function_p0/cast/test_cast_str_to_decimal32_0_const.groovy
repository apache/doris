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


suite("test_cast_str_to_decimal32_0_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "set enable_strict_cast=true;"
    qt_sql_0_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(1, 0));"""
    qt_sql_1_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(1, 0));"""
    qt_sql_2_strict """select cast("0" as decimalv3(1, 0));"""
    qt_sql_3_strict """select cast("1" as decimalv3(1, 0));"""
    qt_sql_4_strict """select cast("8" as decimalv3(1, 0));"""
    qt_sql_5_strict """select cast("9" as decimalv3(1, 0));"""
    qt_sql_6_strict """select cast("0." as decimalv3(1, 0));"""
    qt_sql_7_strict """select cast("1." as decimalv3(1, 0));"""
    qt_sql_8_strict """select cast("8." as decimalv3(1, 0));"""
    qt_sql_9_strict """select cast("9." as decimalv3(1, 0));"""
    qt_sql_10_strict """select cast("-0" as decimalv3(1, 0));"""
    qt_sql_11_strict """select cast("-1" as decimalv3(1, 0));"""
    qt_sql_12_strict """select cast("-8" as decimalv3(1, 0));"""
    qt_sql_13_strict """select cast("-9" as decimalv3(1, 0));"""
    qt_sql_14_strict """select cast("-0." as decimalv3(1, 0));"""
    qt_sql_15_strict """select cast("-1." as decimalv3(1, 0));"""
    qt_sql_16_strict """select cast("-8." as decimalv3(1, 0));"""
    qt_sql_17_strict """select cast("-9." as decimalv3(1, 0));"""
    qt_sql_18_strict """select cast("0.49999" as decimalv3(1, 0));"""
    qt_sql_19_strict """select cast("1.49999" as decimalv3(1, 0));"""
    qt_sql_20_strict """select cast("8.49999" as decimalv3(1, 0));"""
    qt_sql_21_strict """select cast("9.49999" as decimalv3(1, 0));"""
    qt_sql_22_strict """select cast("0.5" as decimalv3(1, 0));"""
    qt_sql_23_strict """select cast("1.5" as decimalv3(1, 0));"""
    qt_sql_24_strict """select cast("8.5" as decimalv3(1, 0));"""
    qt_sql_25_strict """select cast("9.49999" as decimalv3(1, 0));"""
    qt_sql_26_strict """select cast("-0.49999" as decimalv3(1, 0));"""
    qt_sql_27_strict """select cast("-1.49999" as decimalv3(1, 0));"""
    qt_sql_28_strict """select cast("-8.49999" as decimalv3(1, 0));"""
    qt_sql_29_strict """select cast("-9.49999" as decimalv3(1, 0));"""
    qt_sql_30_strict """select cast("-0.5" as decimalv3(1, 0));"""
    qt_sql_31_strict """select cast("-1.5" as decimalv3(1, 0));"""
    qt_sql_32_strict """select cast("-8.5" as decimalv3(1, 0));"""
    qt_sql_33_strict """select cast("-9.49999" as decimalv3(1, 0));"""
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(1, 0));"""
    qt_sql_1_non_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(1, 0));"""
    qt_sql_2_non_strict """select cast("0" as decimalv3(1, 0));"""
    qt_sql_3_non_strict """select cast("1" as decimalv3(1, 0));"""
    qt_sql_4_non_strict """select cast("8" as decimalv3(1, 0));"""
    qt_sql_5_non_strict """select cast("9" as decimalv3(1, 0));"""
    qt_sql_6_non_strict """select cast("0." as decimalv3(1, 0));"""
    qt_sql_7_non_strict """select cast("1." as decimalv3(1, 0));"""
    qt_sql_8_non_strict """select cast("8." as decimalv3(1, 0));"""
    qt_sql_9_non_strict """select cast("9." as decimalv3(1, 0));"""
    qt_sql_10_non_strict """select cast("-0" as decimalv3(1, 0));"""
    qt_sql_11_non_strict """select cast("-1" as decimalv3(1, 0));"""
    qt_sql_12_non_strict """select cast("-8" as decimalv3(1, 0));"""
    qt_sql_13_non_strict """select cast("-9" as decimalv3(1, 0));"""
    qt_sql_14_non_strict """select cast("-0." as decimalv3(1, 0));"""
    qt_sql_15_non_strict """select cast("-1." as decimalv3(1, 0));"""
    qt_sql_16_non_strict """select cast("-8." as decimalv3(1, 0));"""
    qt_sql_17_non_strict """select cast("-9." as decimalv3(1, 0));"""
    qt_sql_18_non_strict """select cast("0.49999" as decimalv3(1, 0));"""
    qt_sql_19_non_strict """select cast("1.49999" as decimalv3(1, 0));"""
    qt_sql_20_non_strict """select cast("8.49999" as decimalv3(1, 0));"""
    qt_sql_21_non_strict """select cast("9.49999" as decimalv3(1, 0));"""
    qt_sql_22_non_strict """select cast("0.5" as decimalv3(1, 0));"""
    qt_sql_23_non_strict """select cast("1.5" as decimalv3(1, 0));"""
    qt_sql_24_non_strict """select cast("8.5" as decimalv3(1, 0));"""
    qt_sql_25_non_strict """select cast("9.49999" as decimalv3(1, 0));"""
    qt_sql_26_non_strict """select cast("-0.49999" as decimalv3(1, 0));"""
    qt_sql_27_non_strict """select cast("-1.49999" as decimalv3(1, 0));"""
    qt_sql_28_non_strict """select cast("-8.49999" as decimalv3(1, 0));"""
    qt_sql_29_non_strict """select cast("-9.49999" as decimalv3(1, 0));"""
    qt_sql_30_non_strict """select cast("-0.5" as decimalv3(1, 0));"""
    qt_sql_31_non_strict """select cast("-1.5" as decimalv3(1, 0));"""
    qt_sql_32_non_strict """select cast("-8.5" as decimalv3(1, 0));"""
    qt_sql_33_non_strict """select cast("-9.49999" as decimalv3(1, 0));"""
}