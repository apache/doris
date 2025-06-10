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


suite("test_cast_str_to_decimal32_26_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "set enable_strict_cast=true;"
    qt_sql_5220_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5221_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5222_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5223_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5224_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5225_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5226_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5227_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5228_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5229_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5230_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5231_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5232_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5233_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5234_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5235_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5236_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5237_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5238_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5239_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5240_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5241_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5242_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5243_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5244_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5245_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5246_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5247_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5248_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5249_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5250_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5251_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5252_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5253_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    sql "set enable_strict_cast=false;"
    qt_sql_5220_non_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5221_non_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5222_non_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5223_non_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5224_non_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5225_non_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5226_non_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5227_non_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5228_non_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5229_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5230_non_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5231_non_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5232_non_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5233_non_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5234_non_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5235_non_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5236_non_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5237_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5238_non_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5239_non_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5240_non_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5241_non_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5242_non_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5243_non_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5244_non_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5245_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5246_non_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5247_non_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5248_non_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5249_non_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5250_non_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5251_non_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5252_non_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5253_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
}