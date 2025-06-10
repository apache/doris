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


suite("test_cast_str_to_decimal32_27_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "set enable_strict_cast=true;"
    qt_sql_5254_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5255_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5256_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5257_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5258_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5259_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5260_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5261_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5262_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5263_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5264_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5265_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5266_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5267_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5268_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5269_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5270_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5271_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5272_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5273_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5274_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5275_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5276_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5277_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5278_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5279_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5280_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5281_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5282_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5283_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5284_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5285_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5286_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5287_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    sql "set enable_strict_cast=false;"
    qt_sql_5254_non_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5255_non_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5256_non_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5257_non_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5258_non_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5259_non_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5260_non_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5261_non_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5262_non_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5263_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5264_non_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5265_non_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5266_non_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5267_non_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5268_non_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5269_non_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5270_non_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5271_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5272_non_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5273_non_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5274_non_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5275_non_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5276_non_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5277_non_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5278_non_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5279_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5280_non_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5281_non_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5282_non_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5283_non_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5284_non_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5285_non_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5286_non_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5287_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
}