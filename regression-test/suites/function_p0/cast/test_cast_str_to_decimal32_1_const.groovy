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


suite("test_cast_str_to_decimal32_1_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "set enable_strict_cast=true;"
    qt_sql_34_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(1, 1));"""
    qt_sql_35_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(1, 1));"""
    qt_sql_36_strict """select cast(".04" as decimalv3(1, 1));"""
    qt_sql_37_strict """select cast(".14" as decimalv3(1, 1));"""
    qt_sql_38_strict """select cast(".84" as decimalv3(1, 1));"""
    qt_sql_39_strict """select cast(".94" as decimalv3(1, 1));"""
    qt_sql_40_strict """select cast(".05" as decimalv3(1, 1));"""
    qt_sql_41_strict """select cast(".15" as decimalv3(1, 1));"""
    qt_sql_42_strict """select cast(".85" as decimalv3(1, 1));"""
    qt_sql_43_strict """select cast(".94" as decimalv3(1, 1));"""
    qt_sql_44_strict """select cast("-.04" as decimalv3(1, 1));"""
    qt_sql_45_strict """select cast("-.14" as decimalv3(1, 1));"""
    qt_sql_46_strict """select cast("-.84" as decimalv3(1, 1));"""
    qt_sql_47_strict """select cast("-.94" as decimalv3(1, 1));"""
    qt_sql_48_strict """select cast("-.05" as decimalv3(1, 1));"""
    qt_sql_49_strict """select cast("-.15" as decimalv3(1, 1));"""
    qt_sql_50_strict """select cast("-.85" as decimalv3(1, 1));"""
    qt_sql_51_strict """select cast("-.94" as decimalv3(1, 1));"""
    sql "set enable_strict_cast=false;"
    qt_sql_34_non_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(1, 1));"""
    qt_sql_35_non_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(1, 1));"""
    qt_sql_36_non_strict """select cast(".04" as decimalv3(1, 1));"""
    qt_sql_37_non_strict """select cast(".14" as decimalv3(1, 1));"""
    qt_sql_38_non_strict """select cast(".84" as decimalv3(1, 1));"""
    qt_sql_39_non_strict """select cast(".94" as decimalv3(1, 1));"""
    qt_sql_40_non_strict """select cast(".05" as decimalv3(1, 1));"""
    qt_sql_41_non_strict """select cast(".15" as decimalv3(1, 1));"""
    qt_sql_42_non_strict """select cast(".85" as decimalv3(1, 1));"""
    qt_sql_43_non_strict """select cast(".94" as decimalv3(1, 1));"""
    qt_sql_44_non_strict """select cast("-.04" as decimalv3(1, 1));"""
    qt_sql_45_non_strict """select cast("-.14" as decimalv3(1, 1));"""
    qt_sql_46_non_strict """select cast("-.84" as decimalv3(1, 1));"""
    qt_sql_47_non_strict """select cast("-.94" as decimalv3(1, 1));"""
    qt_sql_48_non_strict """select cast("-.05" as decimalv3(1, 1));"""
    qt_sql_49_non_strict """select cast("-.15" as decimalv3(1, 1));"""
    qt_sql_50_non_strict """select cast("-.85" as decimalv3(1, 1));"""
    qt_sql_51_non_strict """select cast("-.94" as decimalv3(1, 1));"""
}