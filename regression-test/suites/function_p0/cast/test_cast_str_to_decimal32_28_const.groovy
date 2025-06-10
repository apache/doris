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


suite("test_cast_str_to_decimal32_28_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "set enable_strict_cast=true;"
    qt_sql_5288_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5289_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5290_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5291_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5292_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5293_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5294_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5295_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5296_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5297_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5298_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5299_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5300_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5301_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5302_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5303_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5304_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5305_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5306_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5307_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5308_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5309_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5310_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5311_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5312_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5313_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5314_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5315_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5316_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5317_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5318_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5319_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5320_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5321_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    sql "set enable_strict_cast=false;"
    qt_sql_5288_non_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5289_non_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5290_non_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5291_non_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5292_non_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5293_non_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5294_non_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5295_non_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5296_non_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5297_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5298_non_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5299_non_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5300_non_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5301_non_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5302_non_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5303_non_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5304_non_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5305_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5306_non_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5307_non_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5308_non_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5309_non_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5310_non_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5311_non_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5312_non_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5313_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5314_non_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5315_non_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5316_non_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5317_non_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5318_non_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5319_non_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5320_non_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5321_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
}