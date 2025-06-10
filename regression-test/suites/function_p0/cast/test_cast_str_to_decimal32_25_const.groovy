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


suite("test_cast_str_to_decimal32_25_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "set enable_strict_cast=true;"
    qt_sql_5186_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5187_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5188_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5189_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5190_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5191_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5192_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5193_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5194_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5195_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5196_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5197_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5198_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5199_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5200_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5201_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5202_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5203_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5204_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5205_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5206_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5207_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5208_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5209_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5210_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5211_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5212_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5213_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5214_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5215_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5216_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5217_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5218_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5219_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    sql "set enable_strict_cast=false;"
    qt_sql_5186_non_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5187_non_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5188_non_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5189_non_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5190_non_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5191_non_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5192_non_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5193_non_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5194_non_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5195_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5196_non_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5197_non_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5198_non_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5199_non_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5200_non_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5201_non_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5202_non_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5203_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5204_non_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5205_non_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5206_non_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5207_non_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5208_non_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5209_non_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5210_non_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5211_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5212_non_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5213_non_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5214_non_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5215_non_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5216_non_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5217_non_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5218_non_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5219_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
}