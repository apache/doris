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


suite("test_cast_str_to_decimal32_30_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    sql "set enable_strict_cast=true;"
    qt_sql_5356_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5357_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5358_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5359_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5360_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5361_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5362_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5363_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5364_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5365_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5366_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5367_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5368_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5369_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5370_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5371_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5372_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5373_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5374_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5375_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5376_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5377_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5378_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5379_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5380_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5381_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5382_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5383_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5384_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5385_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5386_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5387_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5388_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5389_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    sql "set enable_strict_cast=false;"
    qt_sql_5356_non_strict """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5357_non_strict """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5358_non_strict """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5359_non_strict """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5360_non_strict """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5361_non_strict """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5362_non_strict """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5363_non_strict """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5364_non_strict """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5365_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5366_non_strict """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5367_non_strict """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5368_non_strict """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5369_non_strict """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5370_non_strict """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5371_non_strict """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5372_non_strict """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5373_non_strict """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5374_non_strict """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5375_non_strict """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5376_non_strict """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5377_non_strict """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5378_non_strict """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5379_non_strict """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5380_non_strict """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5381_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5382_non_strict """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5383_non_strict """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5384_non_strict """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5385_non_strict """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5386_non_strict """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5387_non_strict """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5388_non_strict """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5389_non_strict """select cast("-.9999999994" as decimalv3(9, 9));"""
}