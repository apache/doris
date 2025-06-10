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


suite("test_cast_str_to_decimal32_29_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "set enable_strict_cast=true;"
    qt_sql_5322_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5323_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5324_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5325_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5326_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5327_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5328_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5329_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5330_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5331_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5332_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5333_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5334_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5335_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5336_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5337_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5338_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5339_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5340_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5341_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5342_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5343_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5344_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5345_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5346_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5347_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5348_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5349_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5350_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5351_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5352_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5353_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5354_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5355_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    sql "set enable_strict_cast=false;"
    qt_sql_5322_non_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5323_non_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5324_non_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5325_non_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5326_non_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5327_non_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5328_non_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5329_non_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5330_non_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5331_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5332_non_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5333_non_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5334_non_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5335_non_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5336_non_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5337_non_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5338_non_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5339_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5340_non_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5341_non_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5342_non_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5343_non_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5344_non_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5345_non_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5346_non_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5347_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5348_non_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5349_non_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5350_non_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5351_non_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5352_non_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5353_non_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5354_non_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5355_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
}