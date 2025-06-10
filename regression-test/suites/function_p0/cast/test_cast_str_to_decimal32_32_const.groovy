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


suite("test_cast_str_to_decimal32_32_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "set enable_strict_cast=true;"
    qt_sql_5424_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5425_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5426_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5427_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5428_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5429_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5430_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5431_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5432_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5433_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5434_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5435_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5436_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5437_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5438_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5439_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5440_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5441_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5442_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5443_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5444_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5445_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5446_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5447_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5448_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5449_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5450_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5451_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5452_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5453_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5454_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5455_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5456_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5457_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    sql "set enable_strict_cast=false;"
    qt_sql_5424_non_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5425_non_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5426_non_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5427_non_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5428_non_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5429_non_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5430_non_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5431_non_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5432_non_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5433_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5434_non_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5435_non_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5436_non_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5437_non_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5438_non_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5439_non_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5440_non_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5441_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5442_non_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5443_non_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5444_non_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5445_non_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5446_non_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5447_non_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5448_non_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5449_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5450_non_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5451_non_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5452_non_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5453_non_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5454_non_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5455_non_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5456_non_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5457_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
}