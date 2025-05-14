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


suite("test_cast_to_decimal32_9_9_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_strict_cast=true;"
    def const_sql_5424 = """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5424_strict "${const_sql_5424}"
    testFoldConst("${const_sql_5424}")
    def const_sql_5425 = """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 9));"""
    qt_sql_5425_strict "${const_sql_5425}"
    testFoldConst("${const_sql_5425}")
    def const_sql_5426 = """select cast(".0000000004" as decimalv3(9, 9));"""
    qt_sql_5426_strict "${const_sql_5426}"
    testFoldConst("${const_sql_5426}")
    def const_sql_5427 = """select cast(".0000000014" as decimalv3(9, 9));"""
    qt_sql_5427_strict "${const_sql_5427}"
    testFoldConst("${const_sql_5427}")
    def const_sql_5428 = """select cast(".0000000094" as decimalv3(9, 9));"""
    qt_sql_5428_strict "${const_sql_5428}"
    testFoldConst("${const_sql_5428}")
    def const_sql_5429 = """select cast(".0999999994" as decimalv3(9, 9));"""
    qt_sql_5429_strict "${const_sql_5429}"
    testFoldConst("${const_sql_5429}")
    def const_sql_5430 = """select cast(".9000000004" as decimalv3(9, 9));"""
    qt_sql_5430_strict "${const_sql_5430}"
    testFoldConst("${const_sql_5430}")
    def const_sql_5431 = """select cast(".9000000014" as decimalv3(9, 9));"""
    qt_sql_5431_strict "${const_sql_5431}"
    testFoldConst("${const_sql_5431}")
    def const_sql_5432 = """select cast(".9999999984" as decimalv3(9, 9));"""
    qt_sql_5432_strict "${const_sql_5432}"
    testFoldConst("${const_sql_5432}")
    def const_sql_5433 = """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5433_strict "${const_sql_5433}"
    testFoldConst("${const_sql_5433}")
    def const_sql_5434 = """select cast(".0000000005" as decimalv3(9, 9));"""
    qt_sql_5434_strict "${const_sql_5434}"
    testFoldConst("${const_sql_5434}")
    def const_sql_5435 = """select cast(".0000000015" as decimalv3(9, 9));"""
    qt_sql_5435_strict "${const_sql_5435}"
    testFoldConst("${const_sql_5435}")
    def const_sql_5436 = """select cast(".0000000095" as decimalv3(9, 9));"""
    qt_sql_5436_strict "${const_sql_5436}"
    testFoldConst("${const_sql_5436}")
    def const_sql_5437 = """select cast(".0999999995" as decimalv3(9, 9));"""
    qt_sql_5437_strict "${const_sql_5437}"
    testFoldConst("${const_sql_5437}")
    def const_sql_5438 = """select cast(".9000000005" as decimalv3(9, 9));"""
    qt_sql_5438_strict "${const_sql_5438}"
    testFoldConst("${const_sql_5438}")
    def const_sql_5439 = """select cast(".9000000015" as decimalv3(9, 9));"""
    qt_sql_5439_strict "${const_sql_5439}"
    testFoldConst("${const_sql_5439}")
    def const_sql_5440 = """select cast(".9999999985" as decimalv3(9, 9));"""
    qt_sql_5440_strict "${const_sql_5440}"
    testFoldConst("${const_sql_5440}")
    def const_sql_5441 = """select cast(".9999999994" as decimalv3(9, 9));"""
    qt_sql_5441_strict "${const_sql_5441}"
    testFoldConst("${const_sql_5441}")
    def const_sql_5442 = """select cast("-.0000000004" as decimalv3(9, 9));"""
    qt_sql_5442_strict "${const_sql_5442}"
    testFoldConst("${const_sql_5442}")
    def const_sql_5443 = """select cast("-.0000000014" as decimalv3(9, 9));"""
    qt_sql_5443_strict "${const_sql_5443}"
    testFoldConst("${const_sql_5443}")
    def const_sql_5444 = """select cast("-.0000000094" as decimalv3(9, 9));"""
    qt_sql_5444_strict "${const_sql_5444}"
    testFoldConst("${const_sql_5444}")
    def const_sql_5445 = """select cast("-.0999999994" as decimalv3(9, 9));"""
    qt_sql_5445_strict "${const_sql_5445}"
    testFoldConst("${const_sql_5445}")
    def const_sql_5446 = """select cast("-.9000000004" as decimalv3(9, 9));"""
    qt_sql_5446_strict "${const_sql_5446}"
    testFoldConst("${const_sql_5446}")
    def const_sql_5447 = """select cast("-.9000000014" as decimalv3(9, 9));"""
    qt_sql_5447_strict "${const_sql_5447}"
    testFoldConst("${const_sql_5447}")
    def const_sql_5448 = """select cast("-.9999999984" as decimalv3(9, 9));"""
    qt_sql_5448_strict "${const_sql_5448}"
    testFoldConst("${const_sql_5448}")
    def const_sql_5449 = """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5449_strict "${const_sql_5449}"
    testFoldConst("${const_sql_5449}")
    def const_sql_5450 = """select cast("-.0000000005" as decimalv3(9, 9));"""
    qt_sql_5450_strict "${const_sql_5450}"
    testFoldConst("${const_sql_5450}")
    def const_sql_5451 = """select cast("-.0000000015" as decimalv3(9, 9));"""
    qt_sql_5451_strict "${const_sql_5451}"
    testFoldConst("${const_sql_5451}")
    def const_sql_5452 = """select cast("-.0000000095" as decimalv3(9, 9));"""
    qt_sql_5452_strict "${const_sql_5452}"
    testFoldConst("${const_sql_5452}")
    def const_sql_5453 = """select cast("-.0999999995" as decimalv3(9, 9));"""
    qt_sql_5453_strict "${const_sql_5453}"
    testFoldConst("${const_sql_5453}")
    def const_sql_5454 = """select cast("-.9000000005" as decimalv3(9, 9));"""
    qt_sql_5454_strict "${const_sql_5454}"
    testFoldConst("${const_sql_5454}")
    def const_sql_5455 = """select cast("-.9000000015" as decimalv3(9, 9));"""
    qt_sql_5455_strict "${const_sql_5455}"
    testFoldConst("${const_sql_5455}")
    def const_sql_5456 = """select cast("-.9999999985" as decimalv3(9, 9));"""
    qt_sql_5456_strict "${const_sql_5456}"
    testFoldConst("${const_sql_5456}")
    def const_sql_5457 = """select cast("-.9999999994" as decimalv3(9, 9));"""
    qt_sql_5457_strict "${const_sql_5457}"
    testFoldConst("${const_sql_5457}")
    sql "set enable_strict_cast=false;"
    qt_sql_5424_non_strict "${const_sql_5424}"
    testFoldConst("${const_sql_5424}")
    qt_sql_5425_non_strict "${const_sql_5425}"
    testFoldConst("${const_sql_5425}")
    qt_sql_5426_non_strict "${const_sql_5426}"
    testFoldConst("${const_sql_5426}")
    qt_sql_5427_non_strict "${const_sql_5427}"
    testFoldConst("${const_sql_5427}")
    qt_sql_5428_non_strict "${const_sql_5428}"
    testFoldConst("${const_sql_5428}")
    qt_sql_5429_non_strict "${const_sql_5429}"
    testFoldConst("${const_sql_5429}")
    qt_sql_5430_non_strict "${const_sql_5430}"
    testFoldConst("${const_sql_5430}")
    qt_sql_5431_non_strict "${const_sql_5431}"
    testFoldConst("${const_sql_5431}")
    qt_sql_5432_non_strict "${const_sql_5432}"
    testFoldConst("${const_sql_5432}")
    qt_sql_5433_non_strict "${const_sql_5433}"
    testFoldConst("${const_sql_5433}")
    qt_sql_5434_non_strict "${const_sql_5434}"
    testFoldConst("${const_sql_5434}")
    qt_sql_5435_non_strict "${const_sql_5435}"
    testFoldConst("${const_sql_5435}")
    qt_sql_5436_non_strict "${const_sql_5436}"
    testFoldConst("${const_sql_5436}")
    qt_sql_5437_non_strict "${const_sql_5437}"
    testFoldConst("${const_sql_5437}")
    qt_sql_5438_non_strict "${const_sql_5438}"
    testFoldConst("${const_sql_5438}")
    qt_sql_5439_non_strict "${const_sql_5439}"
    testFoldConst("${const_sql_5439}")
    qt_sql_5440_non_strict "${const_sql_5440}"
    testFoldConst("${const_sql_5440}")
    qt_sql_5441_non_strict "${const_sql_5441}"
    testFoldConst("${const_sql_5441}")
    qt_sql_5442_non_strict "${const_sql_5442}"
    testFoldConst("${const_sql_5442}")
    qt_sql_5443_non_strict "${const_sql_5443}"
    testFoldConst("${const_sql_5443}")
    qt_sql_5444_non_strict "${const_sql_5444}"
    testFoldConst("${const_sql_5444}")
    qt_sql_5445_non_strict "${const_sql_5445}"
    testFoldConst("${const_sql_5445}")
    qt_sql_5446_non_strict "${const_sql_5446}"
    testFoldConst("${const_sql_5446}")
    qt_sql_5447_non_strict "${const_sql_5447}"
    testFoldConst("${const_sql_5447}")
    qt_sql_5448_non_strict "${const_sql_5448}"
    testFoldConst("${const_sql_5448}")
    qt_sql_5449_non_strict "${const_sql_5449}"
    testFoldConst("${const_sql_5449}")
    qt_sql_5450_non_strict "${const_sql_5450}"
    testFoldConst("${const_sql_5450}")
    qt_sql_5451_non_strict "${const_sql_5451}"
    testFoldConst("${const_sql_5451}")
    qt_sql_5452_non_strict "${const_sql_5452}"
    testFoldConst("${const_sql_5452}")
    qt_sql_5453_non_strict "${const_sql_5453}"
    testFoldConst("${const_sql_5453}")
    qt_sql_5454_non_strict "${const_sql_5454}"
    testFoldConst("${const_sql_5454}")
    qt_sql_5455_non_strict "${const_sql_5455}"
    testFoldConst("${const_sql_5455}")
    qt_sql_5456_non_strict "${const_sql_5456}"
    testFoldConst("${const_sql_5456}")
    qt_sql_5457_non_strict "${const_sql_5457}"
    testFoldConst("${const_sql_5457}")
}