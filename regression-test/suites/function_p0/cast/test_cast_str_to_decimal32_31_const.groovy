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


suite("test_cast_str_to_decimal32_31_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "set enable_strict_cast=true;"
    qt_sql_5390_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5391_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5392_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5393_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5394_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5395_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5396_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5397_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5398_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5399_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5400_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5401_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5402_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5403_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5404_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5405_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5406_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5407_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5408_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5409_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5410_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5411_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5412_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5413_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5414_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5415_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5416_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5417_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5418_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5419_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5420_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5421_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5422_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5423_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    sql "set enable_strict_cast=false;"
    qt_sql_5390_non_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5391_non_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5392_non_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5393_non_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5394_non_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5395_non_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5396_non_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5397_non_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5398_non_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5399_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5400_non_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5401_non_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5402_non_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5403_non_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5404_non_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5405_non_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5406_non_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5407_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5408_non_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5409_non_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5410_non_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5411_non_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5412_non_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5413_non_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5414_non_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5415_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5416_non_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5417_non_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5418_non_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5419_non_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5420_non_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5421_non_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5422_non_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5423_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
}