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


suite("test_cast_str_to_decimal128v3_23_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "set enable_strict_cast=true;"
    qt_sql_5134_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(38, 38));"""
    qt_sql_5135_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(38, 38));"""
    qt_sql_5136_strict """select cast(".000000000000000000000000000000000000004" as decimalv3(38, 38));"""
    qt_sql_5137_strict """select cast(".000000000000000000000000000000000000014" as decimalv3(38, 38));"""
    qt_sql_5138_strict """select cast(".000000000000000000000000000000000000094" as decimalv3(38, 38));"""
    qt_sql_5139_strict """select cast(".099999999999999999999999999999999999994" as decimalv3(38, 38));"""
    qt_sql_5140_strict """select cast(".900000000000000000000000000000000000004" as decimalv3(38, 38));"""
    qt_sql_5141_strict """select cast(".900000000000000000000000000000000000014" as decimalv3(38, 38));"""
    qt_sql_5142_strict """select cast(".999999999999999999999999999999999999984" as decimalv3(38, 38));"""
    qt_sql_5143_strict """select cast(".999999999999999999999999999999999999994" as decimalv3(38, 38));"""
    qt_sql_5144_strict """select cast(".000000000000000000000000000000000000005" as decimalv3(38, 38));"""
    qt_sql_5145_strict """select cast(".000000000000000000000000000000000000015" as decimalv3(38, 38));"""
    qt_sql_5146_strict """select cast(".000000000000000000000000000000000000095" as decimalv3(38, 38));"""
    qt_sql_5147_strict """select cast(".099999999999999999999999999999999999995" as decimalv3(38, 38));"""
    qt_sql_5148_strict """select cast(".900000000000000000000000000000000000005" as decimalv3(38, 38));"""
    qt_sql_5149_strict """select cast(".900000000000000000000000000000000000015" as decimalv3(38, 38));"""
    qt_sql_5150_strict """select cast(".999999999999999999999999999999999999985" as decimalv3(38, 38));"""
    qt_sql_5151_strict """select cast(".999999999999999999999999999999999999994" as decimalv3(38, 38));"""
    qt_sql_5152_strict """select cast("-.000000000000000000000000000000000000004" as decimalv3(38, 38));"""
    qt_sql_5153_strict """select cast("-.000000000000000000000000000000000000014" as decimalv3(38, 38));"""
    qt_sql_5154_strict """select cast("-.000000000000000000000000000000000000094" as decimalv3(38, 38));"""
    qt_sql_5155_strict """select cast("-.099999999999999999999999999999999999994" as decimalv3(38, 38));"""
    qt_sql_5156_strict """select cast("-.900000000000000000000000000000000000004" as decimalv3(38, 38));"""
    qt_sql_5157_strict """select cast("-.900000000000000000000000000000000000014" as decimalv3(38, 38));"""
    qt_sql_5158_strict """select cast("-.999999999999999999999999999999999999984" as decimalv3(38, 38));"""
    qt_sql_5159_strict """select cast("-.999999999999999999999999999999999999994" as decimalv3(38, 38));"""
    qt_sql_5160_strict """select cast("-.000000000000000000000000000000000000005" as decimalv3(38, 38));"""
    qt_sql_5161_strict """select cast("-.000000000000000000000000000000000000015" as decimalv3(38, 38));"""
    qt_sql_5162_strict """select cast("-.000000000000000000000000000000000000095" as decimalv3(38, 38));"""
    qt_sql_5163_strict """select cast("-.099999999999999999999999999999999999995" as decimalv3(38, 38));"""
    qt_sql_5164_strict """select cast("-.900000000000000000000000000000000000005" as decimalv3(38, 38));"""
    qt_sql_5165_strict """select cast("-.900000000000000000000000000000000000015" as decimalv3(38, 38));"""
    qt_sql_5166_strict """select cast("-.999999999999999999999999999999999999985" as decimalv3(38, 38));"""
    qt_sql_5167_strict """select cast("-.999999999999999999999999999999999999994" as decimalv3(38, 38));"""
    sql "set enable_strict_cast=false;"
    qt_sql_5134_non_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(38, 38));"""
    qt_sql_5135_non_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(38, 38));"""
    qt_sql_5136_non_strict """select cast(".000000000000000000000000000000000000004" as decimalv3(38, 38));"""
    qt_sql_5137_non_strict """select cast(".000000000000000000000000000000000000014" as decimalv3(38, 38));"""
    qt_sql_5138_non_strict """select cast(".000000000000000000000000000000000000094" as decimalv3(38, 38));"""
    qt_sql_5139_non_strict """select cast(".099999999999999999999999999999999999994" as decimalv3(38, 38));"""
    qt_sql_5140_non_strict """select cast(".900000000000000000000000000000000000004" as decimalv3(38, 38));"""
    qt_sql_5141_non_strict """select cast(".900000000000000000000000000000000000014" as decimalv3(38, 38));"""
    qt_sql_5142_non_strict """select cast(".999999999999999999999999999999999999984" as decimalv3(38, 38));"""
    qt_sql_5143_non_strict """select cast(".999999999999999999999999999999999999994" as decimalv3(38, 38));"""
    qt_sql_5144_non_strict """select cast(".000000000000000000000000000000000000005" as decimalv3(38, 38));"""
    qt_sql_5145_non_strict """select cast(".000000000000000000000000000000000000015" as decimalv3(38, 38));"""
    qt_sql_5146_non_strict """select cast(".000000000000000000000000000000000000095" as decimalv3(38, 38));"""
    qt_sql_5147_non_strict """select cast(".099999999999999999999999999999999999995" as decimalv3(38, 38));"""
    qt_sql_5148_non_strict """select cast(".900000000000000000000000000000000000005" as decimalv3(38, 38));"""
    qt_sql_5149_non_strict """select cast(".900000000000000000000000000000000000015" as decimalv3(38, 38));"""
    qt_sql_5150_non_strict """select cast(".999999999999999999999999999999999999985" as decimalv3(38, 38));"""
    qt_sql_5151_non_strict """select cast(".999999999999999999999999999999999999994" as decimalv3(38, 38));"""
    qt_sql_5152_non_strict """select cast("-.000000000000000000000000000000000000004" as decimalv3(38, 38));"""
    qt_sql_5153_non_strict """select cast("-.000000000000000000000000000000000000014" as decimalv3(38, 38));"""
    qt_sql_5154_non_strict """select cast("-.000000000000000000000000000000000000094" as decimalv3(38, 38));"""
    qt_sql_5155_non_strict """select cast("-.099999999999999999999999999999999999994" as decimalv3(38, 38));"""
    qt_sql_5156_non_strict """select cast("-.900000000000000000000000000000000000004" as decimalv3(38, 38));"""
    qt_sql_5157_non_strict """select cast("-.900000000000000000000000000000000000014" as decimalv3(38, 38));"""
    qt_sql_5158_non_strict """select cast("-.999999999999999999999999999999999999984" as decimalv3(38, 38));"""
    qt_sql_5159_non_strict """select cast("-.999999999999999999999999999999999999994" as decimalv3(38, 38));"""
    qt_sql_5160_non_strict """select cast("-.000000000000000000000000000000000000005" as decimalv3(38, 38));"""
    qt_sql_5161_non_strict """select cast("-.000000000000000000000000000000000000015" as decimalv3(38, 38));"""
    qt_sql_5162_non_strict """select cast("-.000000000000000000000000000000000000095" as decimalv3(38, 38));"""
    qt_sql_5163_non_strict """select cast("-.099999999999999999999999999999999999995" as decimalv3(38, 38));"""
    qt_sql_5164_non_strict """select cast("-.900000000000000000000000000000000000005" as decimalv3(38, 38));"""
    qt_sql_5165_non_strict """select cast("-.900000000000000000000000000000000000015" as decimalv3(38, 38));"""
    qt_sql_5166_non_strict """select cast("-.999999999999999999999999999999999999985" as decimalv3(38, 38));"""
    qt_sql_5167_non_strict """select cast("-.999999999999999999999999999999999999994" as decimalv3(38, 38));"""
}