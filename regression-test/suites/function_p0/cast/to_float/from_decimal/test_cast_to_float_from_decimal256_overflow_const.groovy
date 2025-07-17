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


suite("test_cast_to_float_from_decimal256_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_decimal256 = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "900000000000000000000000000000000000000", cast(cast("900000000000000000000000000000000000000" as decimalv3(39, 0)) as float);"""
    qt_sql_0_0_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    def const_sql_0_1 = """select "-900000000000000000000000000000000000000", cast(cast("-900000000000000000000000000000000000000" as decimalv3(39, 0)) as float);"""
    qt_sql_0_1_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    def const_sql_0_2 = """select "900000000000000000000000000000000000001", cast(cast("900000000000000000000000000000000000001" as decimalv3(39, 0)) as float);"""
    qt_sql_0_2_strict "${const_sql_0_2}"
    testFoldConst("${const_sql_0_2}")
    def const_sql_0_3 = """select "-900000000000000000000000000000000000001", cast(cast("-900000000000000000000000000000000000001" as decimalv3(39, 0)) as float);"""
    qt_sql_0_3_strict "${const_sql_0_3}"
    testFoldConst("${const_sql_0_3}")
    def const_sql_0_4 = """select "999999999999999999999999999999999999999", cast(cast("999999999999999999999999999999999999999" as decimalv3(39, 0)) as float);"""
    qt_sql_0_4_strict "${const_sql_0_4}"
    testFoldConst("${const_sql_0_4}")
    def const_sql_0_5 = """select "-999999999999999999999999999999999999999", cast(cast("-999999999999999999999999999999999999999" as decimalv3(39, 0)) as float);"""
    qt_sql_0_5_strict "${const_sql_0_5}"
    testFoldConst("${const_sql_0_5}")

    sql "set enable_strict_cast=false;"
    qt_sql_0_0_non_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    qt_sql_0_1_non_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    qt_sql_0_2_non_strict "${const_sql_0_2}"
    testFoldConst("${const_sql_0_2}")
    qt_sql_0_3_non_strict "${const_sql_0_3}"
    testFoldConst("${const_sql_0_3}")
    qt_sql_0_4_non_strict "${const_sql_0_4}"
    testFoldConst("${const_sql_0_4}")
    qt_sql_0_5_non_strict "${const_sql_0_5}"
    testFoldConst("${const_sql_0_5}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_1_0 = """select "9000000000000000000000000000000000000000", cast(cast("9000000000000000000000000000000000000000" as decimalv3(40, 0)) as float);"""
    qt_sql_1_0_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    def const_sql_1_1 = """select "-9000000000000000000000000000000000000000", cast(cast("-9000000000000000000000000000000000000000" as decimalv3(40, 0)) as float);"""
    qt_sql_1_1_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    def const_sql_1_2 = """select "9000000000000000000000000000000000000001", cast(cast("9000000000000000000000000000000000000001" as decimalv3(40, 0)) as float);"""
    qt_sql_1_2_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    def const_sql_1_3 = """select "-9000000000000000000000000000000000000001", cast(cast("-9000000000000000000000000000000000000001" as decimalv3(40, 0)) as float);"""
    qt_sql_1_3_strict "${const_sql_1_3}"
    testFoldConst("${const_sql_1_3}")
    def const_sql_1_4 = """select "9999999999999999999999999999999999999999", cast(cast("9999999999999999999999999999999999999999" as decimalv3(40, 0)) as float);"""
    qt_sql_1_4_strict "${const_sql_1_4}"
    testFoldConst("${const_sql_1_4}")
    def const_sql_1_5 = """select "-9999999999999999999999999999999999999999", cast(cast("-9999999999999999999999999999999999999999" as decimalv3(40, 0)) as float);"""
    qt_sql_1_5_strict "${const_sql_1_5}"
    testFoldConst("${const_sql_1_5}")

    sql "set enable_strict_cast=false;"
    qt_sql_1_0_non_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    qt_sql_1_1_non_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    qt_sql_1_2_non_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    qt_sql_1_3_non_strict "${const_sql_1_3}"
    testFoldConst("${const_sql_1_3}")
    qt_sql_1_4_non_strict "${const_sql_1_4}"
    testFoldConst("${const_sql_1_4}")
    qt_sql_1_5_non_strict "${const_sql_1_5}"
    testFoldConst("${const_sql_1_5}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_2_0 = """select "900000000000000000000000000000000000000.0", cast(cast("900000000000000000000000000000000000000.0" as decimalv3(40, 1)) as float);"""
    qt_sql_2_0_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    def const_sql_2_1 = """select "-900000000000000000000000000000000000000.0", cast(cast("-900000000000000000000000000000000000000.0" as decimalv3(40, 1)) as float);"""
    qt_sql_2_1_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    def const_sql_2_2 = """select "900000000000000000000000000000000000000.8", cast(cast("900000000000000000000000000000000000000.8" as decimalv3(40, 1)) as float);"""
    qt_sql_2_2_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    def const_sql_2_3 = """select "-900000000000000000000000000000000000000.8", cast(cast("-900000000000000000000000000000000000000.8" as decimalv3(40, 1)) as float);"""
    qt_sql_2_3_strict "${const_sql_2_3}"
    testFoldConst("${const_sql_2_3}")
    def const_sql_2_4 = """select "900000000000000000000000000000000000000.9", cast(cast("900000000000000000000000000000000000000.9" as decimalv3(40, 1)) as float);"""
    qt_sql_2_4_strict "${const_sql_2_4}"
    testFoldConst("${const_sql_2_4}")
    def const_sql_2_5 = """select "-900000000000000000000000000000000000000.9", cast(cast("-900000000000000000000000000000000000000.9" as decimalv3(40, 1)) as float);"""
    qt_sql_2_5_strict "${const_sql_2_5}"
    testFoldConst("${const_sql_2_5}")
    def const_sql_2_6 = """select "900000000000000000000000000000000000001.0", cast(cast("900000000000000000000000000000000000001.0" as decimalv3(40, 1)) as float);"""
    qt_sql_2_6_strict "${const_sql_2_6}"
    testFoldConst("${const_sql_2_6}")
    def const_sql_2_7 = """select "-900000000000000000000000000000000000001.0", cast(cast("-900000000000000000000000000000000000001.0" as decimalv3(40, 1)) as float);"""
    qt_sql_2_7_strict "${const_sql_2_7}"
    testFoldConst("${const_sql_2_7}")
    def const_sql_2_8 = """select "900000000000000000000000000000000000001.8", cast(cast("900000000000000000000000000000000000001.8" as decimalv3(40, 1)) as float);"""
    qt_sql_2_8_strict "${const_sql_2_8}"
    testFoldConst("${const_sql_2_8}")
    def const_sql_2_9 = """select "-900000000000000000000000000000000000001.8", cast(cast("-900000000000000000000000000000000000001.8" as decimalv3(40, 1)) as float);"""
    qt_sql_2_9_strict "${const_sql_2_9}"
    testFoldConst("${const_sql_2_9}")
    def const_sql_2_10 = """select "900000000000000000000000000000000000001.9", cast(cast("900000000000000000000000000000000000001.9" as decimalv3(40, 1)) as float);"""
    qt_sql_2_10_strict "${const_sql_2_10}"
    testFoldConst("${const_sql_2_10}")
    def const_sql_2_11 = """select "-900000000000000000000000000000000000001.9", cast(cast("-900000000000000000000000000000000000001.9" as decimalv3(40, 1)) as float);"""
    qt_sql_2_11_strict "${const_sql_2_11}"
    testFoldConst("${const_sql_2_11}")
    def const_sql_2_12 = """select "999999999999999999999999999999999999999.0", cast(cast("999999999999999999999999999999999999999.0" as decimalv3(40, 1)) as float);"""
    qt_sql_2_12_strict "${const_sql_2_12}"
    testFoldConst("${const_sql_2_12}")
    def const_sql_2_13 = """select "-999999999999999999999999999999999999999.0", cast(cast("-999999999999999999999999999999999999999.0" as decimalv3(40, 1)) as float);"""
    qt_sql_2_13_strict "${const_sql_2_13}"
    testFoldConst("${const_sql_2_13}")
    def const_sql_2_14 = """select "999999999999999999999999999999999999999.8", cast(cast("999999999999999999999999999999999999999.8" as decimalv3(40, 1)) as float);"""
    qt_sql_2_14_strict "${const_sql_2_14}"
    testFoldConst("${const_sql_2_14}")
    def const_sql_2_15 = """select "-999999999999999999999999999999999999999.8", cast(cast("-999999999999999999999999999999999999999.8" as decimalv3(40, 1)) as float);"""
    qt_sql_2_15_strict "${const_sql_2_15}"
    testFoldConst("${const_sql_2_15}")
    def const_sql_2_16 = """select "999999999999999999999999999999999999999.9", cast(cast("999999999999999999999999999999999999999.9" as decimalv3(40, 1)) as float);"""
    qt_sql_2_16_strict "${const_sql_2_16}"
    testFoldConst("${const_sql_2_16}")
    def const_sql_2_17 = """select "-999999999999999999999999999999999999999.9", cast(cast("-999999999999999999999999999999999999999.9" as decimalv3(40, 1)) as float);"""
    qt_sql_2_17_strict "${const_sql_2_17}"
    testFoldConst("${const_sql_2_17}")

    sql "set enable_strict_cast=false;"
    qt_sql_2_0_non_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    qt_sql_2_1_non_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    qt_sql_2_2_non_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    qt_sql_2_3_non_strict "${const_sql_2_3}"
    testFoldConst("${const_sql_2_3}")
    qt_sql_2_4_non_strict "${const_sql_2_4}"
    testFoldConst("${const_sql_2_4}")
    qt_sql_2_5_non_strict "${const_sql_2_5}"
    testFoldConst("${const_sql_2_5}")
    qt_sql_2_6_non_strict "${const_sql_2_6}"
    testFoldConst("${const_sql_2_6}")
    qt_sql_2_7_non_strict "${const_sql_2_7}"
    testFoldConst("${const_sql_2_7}")
    qt_sql_2_8_non_strict "${const_sql_2_8}"
    testFoldConst("${const_sql_2_8}")
    qt_sql_2_9_non_strict "${const_sql_2_9}"
    testFoldConst("${const_sql_2_9}")
    qt_sql_2_10_non_strict "${const_sql_2_10}"
    testFoldConst("${const_sql_2_10}")
    qt_sql_2_11_non_strict "${const_sql_2_11}"
    testFoldConst("${const_sql_2_11}")
    qt_sql_2_12_non_strict "${const_sql_2_12}"
    testFoldConst("${const_sql_2_12}")
    qt_sql_2_13_non_strict "${const_sql_2_13}"
    testFoldConst("${const_sql_2_13}")
    qt_sql_2_14_non_strict "${const_sql_2_14}"
    testFoldConst("${const_sql_2_14}")
    qt_sql_2_15_non_strict "${const_sql_2_15}"
    testFoldConst("${const_sql_2_15}")
    qt_sql_2_16_non_strict "${const_sql_2_16}"
    testFoldConst("${const_sql_2_16}")
    qt_sql_2_17_non_strict "${const_sql_2_17}"
    testFoldConst("${const_sql_2_17}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_3_0 = """select "900000000000000000000000000000000000000000000000000000000000", cast(cast("900000000000000000000000000000000000000000000000000000000000" as decimalv3(60, 0)) as float);"""
    qt_sql_3_0_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    def const_sql_3_1 = """select "-900000000000000000000000000000000000000000000000000000000000", cast(cast("-900000000000000000000000000000000000000000000000000000000000" as decimalv3(60, 0)) as float);"""
    qt_sql_3_1_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    def const_sql_3_2 = """select "900000000000000000000000000000000000000000000000000000000001", cast(cast("900000000000000000000000000000000000000000000000000000000001" as decimalv3(60, 0)) as float);"""
    qt_sql_3_2_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    def const_sql_3_3 = """select "-900000000000000000000000000000000000000000000000000000000001", cast(cast("-900000000000000000000000000000000000000000000000000000000001" as decimalv3(60, 0)) as float);"""
    qt_sql_3_3_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    def const_sql_3_4 = """select "999999999999999999999999999999999999999999999999999999999999", cast(cast("999999999999999999999999999999999999999999999999999999999999" as decimalv3(60, 0)) as float);"""
    qt_sql_3_4_strict "${const_sql_3_4}"
    testFoldConst("${const_sql_3_4}")
    def const_sql_3_5 = """select "-999999999999999999999999999999999999999999999999999999999999", cast(cast("-999999999999999999999999999999999999999999999999999999999999" as decimalv3(60, 0)) as float);"""
    qt_sql_3_5_strict "${const_sql_3_5}"
    testFoldConst("${const_sql_3_5}")

    sql "set enable_strict_cast=false;"
    qt_sql_3_0_non_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    qt_sql_3_1_non_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    qt_sql_3_2_non_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    qt_sql_3_3_non_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    qt_sql_3_4_non_strict "${const_sql_3_4}"
    testFoldConst("${const_sql_3_4}")
    qt_sql_3_5_non_strict "${const_sql_3_5}"
    testFoldConst("${const_sql_3_5}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_4_0 = """select "9000000000000000000000000000000000000000.00000000000000000000", cast(cast("9000000000000000000000000000000000000000.00000000000000000000" as decimalv3(60, 20)) as float);"""
    qt_sql_4_0_strict "${const_sql_4_0}"
    testFoldConst("${const_sql_4_0}")
    def const_sql_4_1 = """select "-9000000000000000000000000000000000000000.00000000000000000000", cast(cast("-9000000000000000000000000000000000000000.00000000000000000000" as decimalv3(60, 20)) as float);"""
    qt_sql_4_1_strict "${const_sql_4_1}"
    testFoldConst("${const_sql_4_1}")
    def const_sql_4_2 = """select "9000000000000000000000000000000000000000.09999999999999999999", cast(cast("9000000000000000000000000000000000000000.09999999999999999999" as decimalv3(60, 20)) as float);"""
    qt_sql_4_2_strict "${const_sql_4_2}"
    testFoldConst("${const_sql_4_2}")
    def const_sql_4_3 = """select "-9000000000000000000000000000000000000000.09999999999999999999", cast(cast("-9000000000000000000000000000000000000000.09999999999999999999" as decimalv3(60, 20)) as float);"""
    qt_sql_4_3_strict "${const_sql_4_3}"
    testFoldConst("${const_sql_4_3}")
    def const_sql_4_4 = """select "9000000000000000000000000000000000000000.90000000000000000000", cast(cast("9000000000000000000000000000000000000000.90000000000000000000" as decimalv3(60, 20)) as float);"""
    qt_sql_4_4_strict "${const_sql_4_4}"
    testFoldConst("${const_sql_4_4}")
    def const_sql_4_5 = """select "-9000000000000000000000000000000000000000.90000000000000000000", cast(cast("-9000000000000000000000000000000000000000.90000000000000000000" as decimalv3(60, 20)) as float);"""
    qt_sql_4_5_strict "${const_sql_4_5}"
    testFoldConst("${const_sql_4_5}")
    def const_sql_4_6 = """select "9000000000000000000000000000000000000000.90000000000000000001", cast(cast("9000000000000000000000000000000000000000.90000000000000000001" as decimalv3(60, 20)) as float);"""
    qt_sql_4_6_strict "${const_sql_4_6}"
    testFoldConst("${const_sql_4_6}")
    def const_sql_4_7 = """select "-9000000000000000000000000000000000000000.90000000000000000001", cast(cast("-9000000000000000000000000000000000000000.90000000000000000001" as decimalv3(60, 20)) as float);"""
    qt_sql_4_7_strict "${const_sql_4_7}"
    testFoldConst("${const_sql_4_7}")
    def const_sql_4_8 = """select "9000000000000000000000000000000000000000.99999999999999999999", cast(cast("9000000000000000000000000000000000000000.99999999999999999999" as decimalv3(60, 20)) as float);"""
    qt_sql_4_8_strict "${const_sql_4_8}"
    testFoldConst("${const_sql_4_8}")
    def const_sql_4_9 = """select "-9000000000000000000000000000000000000000.99999999999999999999", cast(cast("-9000000000000000000000000000000000000000.99999999999999999999" as decimalv3(60, 20)) as float);"""
    qt_sql_4_9_strict "${const_sql_4_9}"
    testFoldConst("${const_sql_4_9}")
    def const_sql_4_10 = """select "9000000000000000000000000000000000000001.00000000000000000000", cast(cast("9000000000000000000000000000000000000001.00000000000000000000" as decimalv3(60, 20)) as float);"""
    qt_sql_4_10_strict "${const_sql_4_10}"
    testFoldConst("${const_sql_4_10}")
    def const_sql_4_11 = """select "-9000000000000000000000000000000000000001.00000000000000000000", cast(cast("-9000000000000000000000000000000000000001.00000000000000000000" as decimalv3(60, 20)) as float);"""
    qt_sql_4_11_strict "${const_sql_4_11}"
    testFoldConst("${const_sql_4_11}")
    def const_sql_4_12 = """select "9000000000000000000000000000000000000001.09999999999999999999", cast(cast("9000000000000000000000000000000000000001.09999999999999999999" as decimalv3(60, 20)) as float);"""
    qt_sql_4_12_strict "${const_sql_4_12}"
    testFoldConst("${const_sql_4_12}")
    def const_sql_4_13 = """select "-9000000000000000000000000000000000000001.09999999999999999999", cast(cast("-9000000000000000000000000000000000000001.09999999999999999999" as decimalv3(60, 20)) as float);"""
    qt_sql_4_13_strict "${const_sql_4_13}"
    testFoldConst("${const_sql_4_13}")
    def const_sql_4_14 = """select "9000000000000000000000000000000000000001.90000000000000000000", cast(cast("9000000000000000000000000000000000000001.90000000000000000000" as decimalv3(60, 20)) as float);"""
    qt_sql_4_14_strict "${const_sql_4_14}"
    testFoldConst("${const_sql_4_14}")
    def const_sql_4_15 = """select "-9000000000000000000000000000000000000001.90000000000000000000", cast(cast("-9000000000000000000000000000000000000001.90000000000000000000" as decimalv3(60, 20)) as float);"""
    qt_sql_4_15_strict "${const_sql_4_15}"
    testFoldConst("${const_sql_4_15}")
    def const_sql_4_16 = """select "9000000000000000000000000000000000000001.90000000000000000001", cast(cast("9000000000000000000000000000000000000001.90000000000000000001" as decimalv3(60, 20)) as float);"""
    qt_sql_4_16_strict "${const_sql_4_16}"
    testFoldConst("${const_sql_4_16}")
    def const_sql_4_17 = """select "-9000000000000000000000000000000000000001.90000000000000000001", cast(cast("-9000000000000000000000000000000000000001.90000000000000000001" as decimalv3(60, 20)) as float);"""
    qt_sql_4_17_strict "${const_sql_4_17}"
    testFoldConst("${const_sql_4_17}")
    def const_sql_4_18 = """select "9000000000000000000000000000000000000001.99999999999999999999", cast(cast("9000000000000000000000000000000000000001.99999999999999999999" as decimalv3(60, 20)) as float);"""
    qt_sql_4_18_strict "${const_sql_4_18}"
    testFoldConst("${const_sql_4_18}")
    def const_sql_4_19 = """select "-9000000000000000000000000000000000000001.99999999999999999999", cast(cast("-9000000000000000000000000000000000000001.99999999999999999999" as decimalv3(60, 20)) as float);"""
    qt_sql_4_19_strict "${const_sql_4_19}"
    testFoldConst("${const_sql_4_19}")
    def const_sql_4_20 = """select "9999999999999999999999999999999999999999.00000000000000000000", cast(cast("9999999999999999999999999999999999999999.00000000000000000000" as decimalv3(60, 20)) as float);"""
    qt_sql_4_20_strict "${const_sql_4_20}"
    testFoldConst("${const_sql_4_20}")
    def const_sql_4_21 = """select "-9999999999999999999999999999999999999999.00000000000000000000", cast(cast("-9999999999999999999999999999999999999999.00000000000000000000" as decimalv3(60, 20)) as float);"""
    qt_sql_4_21_strict "${const_sql_4_21}"
    testFoldConst("${const_sql_4_21}")
    def const_sql_4_22 = """select "9999999999999999999999999999999999999999.09999999999999999999", cast(cast("9999999999999999999999999999999999999999.09999999999999999999" as decimalv3(60, 20)) as float);"""
    qt_sql_4_22_strict "${const_sql_4_22}"
    testFoldConst("${const_sql_4_22}")
    def const_sql_4_23 = """select "-9999999999999999999999999999999999999999.09999999999999999999", cast(cast("-9999999999999999999999999999999999999999.09999999999999999999" as decimalv3(60, 20)) as float);"""
    qt_sql_4_23_strict "${const_sql_4_23}"
    testFoldConst("${const_sql_4_23}")
    def const_sql_4_24 = """select "9999999999999999999999999999999999999999.90000000000000000000", cast(cast("9999999999999999999999999999999999999999.90000000000000000000" as decimalv3(60, 20)) as float);"""
    qt_sql_4_24_strict "${const_sql_4_24}"
    testFoldConst("${const_sql_4_24}")
    def const_sql_4_25 = """select "-9999999999999999999999999999999999999999.90000000000000000000", cast(cast("-9999999999999999999999999999999999999999.90000000000000000000" as decimalv3(60, 20)) as float);"""
    qt_sql_4_25_strict "${const_sql_4_25}"
    testFoldConst("${const_sql_4_25}")
    def const_sql_4_26 = """select "9999999999999999999999999999999999999999.90000000000000000001", cast(cast("9999999999999999999999999999999999999999.90000000000000000001" as decimalv3(60, 20)) as float);"""
    qt_sql_4_26_strict "${const_sql_4_26}"
    testFoldConst("${const_sql_4_26}")
    def const_sql_4_27 = """select "-9999999999999999999999999999999999999999.90000000000000000001", cast(cast("-9999999999999999999999999999999999999999.90000000000000000001" as decimalv3(60, 20)) as float);"""
    qt_sql_4_27_strict "${const_sql_4_27}"
    testFoldConst("${const_sql_4_27}")
    def const_sql_4_28 = """select "9999999999999999999999999999999999999999.99999999999999999999", cast(cast("9999999999999999999999999999999999999999.99999999999999999999" as decimalv3(60, 20)) as float);"""
    qt_sql_4_28_strict "${const_sql_4_28}"
    testFoldConst("${const_sql_4_28}")
    def const_sql_4_29 = """select "-9999999999999999999999999999999999999999.99999999999999999999", cast(cast("-9999999999999999999999999999999999999999.99999999999999999999" as decimalv3(60, 20)) as float);"""
    qt_sql_4_29_strict "${const_sql_4_29}"
    testFoldConst("${const_sql_4_29}")

    sql "set enable_strict_cast=false;"
    qt_sql_4_0_non_strict "${const_sql_4_0}"
    testFoldConst("${const_sql_4_0}")
    qt_sql_4_1_non_strict "${const_sql_4_1}"
    testFoldConst("${const_sql_4_1}")
    qt_sql_4_2_non_strict "${const_sql_4_2}"
    testFoldConst("${const_sql_4_2}")
    qt_sql_4_3_non_strict "${const_sql_4_3}"
    testFoldConst("${const_sql_4_3}")
    qt_sql_4_4_non_strict "${const_sql_4_4}"
    testFoldConst("${const_sql_4_4}")
    qt_sql_4_5_non_strict "${const_sql_4_5}"
    testFoldConst("${const_sql_4_5}")
    qt_sql_4_6_non_strict "${const_sql_4_6}"
    testFoldConst("${const_sql_4_6}")
    qt_sql_4_7_non_strict "${const_sql_4_7}"
    testFoldConst("${const_sql_4_7}")
    qt_sql_4_8_non_strict "${const_sql_4_8}"
    testFoldConst("${const_sql_4_8}")
    qt_sql_4_9_non_strict "${const_sql_4_9}"
    testFoldConst("${const_sql_4_9}")
    qt_sql_4_10_non_strict "${const_sql_4_10}"
    testFoldConst("${const_sql_4_10}")
    qt_sql_4_11_non_strict "${const_sql_4_11}"
    testFoldConst("${const_sql_4_11}")
    qt_sql_4_12_non_strict "${const_sql_4_12}"
    testFoldConst("${const_sql_4_12}")
    qt_sql_4_13_non_strict "${const_sql_4_13}"
    testFoldConst("${const_sql_4_13}")
    qt_sql_4_14_non_strict "${const_sql_4_14}"
    testFoldConst("${const_sql_4_14}")
    qt_sql_4_15_non_strict "${const_sql_4_15}"
    testFoldConst("${const_sql_4_15}")
    qt_sql_4_16_non_strict "${const_sql_4_16}"
    testFoldConst("${const_sql_4_16}")
    qt_sql_4_17_non_strict "${const_sql_4_17}"
    testFoldConst("${const_sql_4_17}")
    qt_sql_4_18_non_strict "${const_sql_4_18}"
    testFoldConst("${const_sql_4_18}")
    qt_sql_4_19_non_strict "${const_sql_4_19}"
    testFoldConst("${const_sql_4_19}")
    qt_sql_4_20_non_strict "${const_sql_4_20}"
    testFoldConst("${const_sql_4_20}")
    qt_sql_4_21_non_strict "${const_sql_4_21}"
    testFoldConst("${const_sql_4_21}")
    qt_sql_4_22_non_strict "${const_sql_4_22}"
    testFoldConst("${const_sql_4_22}")
    qt_sql_4_23_non_strict "${const_sql_4_23}"
    testFoldConst("${const_sql_4_23}")
    qt_sql_4_24_non_strict "${const_sql_4_24}"
    testFoldConst("${const_sql_4_24}")
    qt_sql_4_25_non_strict "${const_sql_4_25}"
    testFoldConst("${const_sql_4_25}")
    qt_sql_4_26_non_strict "${const_sql_4_26}"
    testFoldConst("${const_sql_4_26}")
    qt_sql_4_27_non_strict "${const_sql_4_27}"
    testFoldConst("${const_sql_4_27}")
    qt_sql_4_28_non_strict "${const_sql_4_28}"
    testFoldConst("${const_sql_4_28}")
    qt_sql_4_29_non_strict "${const_sql_4_29}"
    testFoldConst("${const_sql_4_29}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_5_0 = """select "9000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("9000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 0)) as float);"""
    qt_sql_5_0_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")
    def const_sql_5_1 = """select "-9000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("-9000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 0)) as float);"""
    qt_sql_5_1_strict "${const_sql_5_1}"
    testFoldConst("${const_sql_5_1}")
    def const_sql_5_2 = """select "9000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("9000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 0)) as float);"""
    qt_sql_5_2_strict "${const_sql_5_2}"
    testFoldConst("${const_sql_5_2}")
    def const_sql_5_3 = """select "-9000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("-9000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 0)) as float);"""
    qt_sql_5_3_strict "${const_sql_5_3}"
    testFoldConst("${const_sql_5_3}")
    def const_sql_5_4 = """select "9999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("9999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 0)) as float);"""
    qt_sql_5_4_strict "${const_sql_5_4}"
    testFoldConst("${const_sql_5_4}")
    def const_sql_5_5 = """select "-9999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("-9999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 0)) as float);"""
    qt_sql_5_5_strict "${const_sql_5_5}"
    testFoldConst("${const_sql_5_5}")

    sql "set enable_strict_cast=false;"
    qt_sql_5_0_non_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")
    qt_sql_5_1_non_strict "${const_sql_5_1}"
    testFoldConst("${const_sql_5_1}")
    qt_sql_5_2_non_strict "${const_sql_5_2}"
    testFoldConst("${const_sql_5_2}")
    qt_sql_5_3_non_strict "${const_sql_5_3}"
    testFoldConst("${const_sql_5_3}")
    qt_sql_5_4_non_strict "${const_sql_5_4}"
    testFoldConst("${const_sql_5_4}")
    qt_sql_5_5_non_strict "${const_sql_5_5}"
    testFoldConst("${const_sql_5_5}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_6_0 = """select "900000000000000000000000000000000000000000000000000000000000000000000000000.0", cast(cast("900000000000000000000000000000000000000000000000000000000000000000000000000.0" as decimalv3(76, 1)) as float);"""
    qt_sql_6_0_strict "${const_sql_6_0}"
    testFoldConst("${const_sql_6_0}")
    def const_sql_6_1 = """select "-900000000000000000000000000000000000000000000000000000000000000000000000000.0", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000000000000.0" as decimalv3(76, 1)) as float);"""
    qt_sql_6_1_strict "${const_sql_6_1}"
    testFoldConst("${const_sql_6_1}")
    def const_sql_6_2 = """select "900000000000000000000000000000000000000000000000000000000000000000000000000.8", cast(cast("900000000000000000000000000000000000000000000000000000000000000000000000000.8" as decimalv3(76, 1)) as float);"""
    qt_sql_6_2_strict "${const_sql_6_2}"
    testFoldConst("${const_sql_6_2}")
    def const_sql_6_3 = """select "-900000000000000000000000000000000000000000000000000000000000000000000000000.8", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000000000000.8" as decimalv3(76, 1)) as float);"""
    qt_sql_6_3_strict "${const_sql_6_3}"
    testFoldConst("${const_sql_6_3}")
    def const_sql_6_4 = """select "900000000000000000000000000000000000000000000000000000000000000000000000000.9", cast(cast("900000000000000000000000000000000000000000000000000000000000000000000000000.9" as decimalv3(76, 1)) as float);"""
    qt_sql_6_4_strict "${const_sql_6_4}"
    testFoldConst("${const_sql_6_4}")
    def const_sql_6_5 = """select "-900000000000000000000000000000000000000000000000000000000000000000000000000.9", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000000000000.9" as decimalv3(76, 1)) as float);"""
    qt_sql_6_5_strict "${const_sql_6_5}"
    testFoldConst("${const_sql_6_5}")
    def const_sql_6_6 = """select "900000000000000000000000000000000000000000000000000000000000000000000000001.0", cast(cast("900000000000000000000000000000000000000000000000000000000000000000000000001.0" as decimalv3(76, 1)) as float);"""
    qt_sql_6_6_strict "${const_sql_6_6}"
    testFoldConst("${const_sql_6_6}")
    def const_sql_6_7 = """select "-900000000000000000000000000000000000000000000000000000000000000000000000001.0", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000000000001.0" as decimalv3(76, 1)) as float);"""
    qt_sql_6_7_strict "${const_sql_6_7}"
    testFoldConst("${const_sql_6_7}")
    def const_sql_6_8 = """select "900000000000000000000000000000000000000000000000000000000000000000000000001.8", cast(cast("900000000000000000000000000000000000000000000000000000000000000000000000001.8" as decimalv3(76, 1)) as float);"""
    qt_sql_6_8_strict "${const_sql_6_8}"
    testFoldConst("${const_sql_6_8}")
    def const_sql_6_9 = """select "-900000000000000000000000000000000000000000000000000000000000000000000000001.8", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000000000001.8" as decimalv3(76, 1)) as float);"""
    qt_sql_6_9_strict "${const_sql_6_9}"
    testFoldConst("${const_sql_6_9}")
    def const_sql_6_10 = """select "900000000000000000000000000000000000000000000000000000000000000000000000001.9", cast(cast("900000000000000000000000000000000000000000000000000000000000000000000000001.9" as decimalv3(76, 1)) as float);"""
    qt_sql_6_10_strict "${const_sql_6_10}"
    testFoldConst("${const_sql_6_10}")
    def const_sql_6_11 = """select "-900000000000000000000000000000000000000000000000000000000000000000000000001.9", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000000000001.9" as decimalv3(76, 1)) as float);"""
    qt_sql_6_11_strict "${const_sql_6_11}"
    testFoldConst("${const_sql_6_11}")
    def const_sql_6_12 = """select "999999999999999999999999999999999999999999999999999999999999999999999999999.0", cast(cast("999999999999999999999999999999999999999999999999999999999999999999999999999.0" as decimalv3(76, 1)) as float);"""
    qt_sql_6_12_strict "${const_sql_6_12}"
    testFoldConst("${const_sql_6_12}")
    def const_sql_6_13 = """select "-999999999999999999999999999999999999999999999999999999999999999999999999999.0", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999999999999.0" as decimalv3(76, 1)) as float);"""
    qt_sql_6_13_strict "${const_sql_6_13}"
    testFoldConst("${const_sql_6_13}")
    def const_sql_6_14 = """select "999999999999999999999999999999999999999999999999999999999999999999999999999.8", cast(cast("999999999999999999999999999999999999999999999999999999999999999999999999999.8" as decimalv3(76, 1)) as float);"""
    qt_sql_6_14_strict "${const_sql_6_14}"
    testFoldConst("${const_sql_6_14}")
    def const_sql_6_15 = """select "-999999999999999999999999999999999999999999999999999999999999999999999999999.8", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999999999999.8" as decimalv3(76, 1)) as float);"""
    qt_sql_6_15_strict "${const_sql_6_15}"
    testFoldConst("${const_sql_6_15}")
    def const_sql_6_16 = """select "999999999999999999999999999999999999999999999999999999999999999999999999999.9", cast(cast("999999999999999999999999999999999999999999999999999999999999999999999999999.9" as decimalv3(76, 1)) as float);"""
    qt_sql_6_16_strict "${const_sql_6_16}"
    testFoldConst("${const_sql_6_16}")
    def const_sql_6_17 = """select "-999999999999999999999999999999999999999999999999999999999999999999999999999.9", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999999999999.9" as decimalv3(76, 1)) as float);"""
    qt_sql_6_17_strict "${const_sql_6_17}"
    testFoldConst("${const_sql_6_17}")

    sql "set enable_strict_cast=false;"
    qt_sql_6_0_non_strict "${const_sql_6_0}"
    testFoldConst("${const_sql_6_0}")
    qt_sql_6_1_non_strict "${const_sql_6_1}"
    testFoldConst("${const_sql_6_1}")
    qt_sql_6_2_non_strict "${const_sql_6_2}"
    testFoldConst("${const_sql_6_2}")
    qt_sql_6_3_non_strict "${const_sql_6_3}"
    testFoldConst("${const_sql_6_3}")
    qt_sql_6_4_non_strict "${const_sql_6_4}"
    testFoldConst("${const_sql_6_4}")
    qt_sql_6_5_non_strict "${const_sql_6_5}"
    testFoldConst("${const_sql_6_5}")
    qt_sql_6_6_non_strict "${const_sql_6_6}"
    testFoldConst("${const_sql_6_6}")
    qt_sql_6_7_non_strict "${const_sql_6_7}"
    testFoldConst("${const_sql_6_7}")
    qt_sql_6_8_non_strict "${const_sql_6_8}"
    testFoldConst("${const_sql_6_8}")
    qt_sql_6_9_non_strict "${const_sql_6_9}"
    testFoldConst("${const_sql_6_9}")
    qt_sql_6_10_non_strict "${const_sql_6_10}"
    testFoldConst("${const_sql_6_10}")
    qt_sql_6_11_non_strict "${const_sql_6_11}"
    testFoldConst("${const_sql_6_11}")
    qt_sql_6_12_non_strict "${const_sql_6_12}"
    testFoldConst("${const_sql_6_12}")
    qt_sql_6_13_non_strict "${const_sql_6_13}"
    testFoldConst("${const_sql_6_13}")
    qt_sql_6_14_non_strict "${const_sql_6_14}"
    testFoldConst("${const_sql_6_14}")
    qt_sql_6_15_non_strict "${const_sql_6_15}"
    testFoldConst("${const_sql_6_15}")
    qt_sql_6_16_non_strict "${const_sql_6_16}"
    testFoldConst("${const_sql_6_16}")
    qt_sql_6_17_non_strict "${const_sql_6_17}"
    testFoldConst("${const_sql_6_17}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_7_0 = """select "900000000000000000000000000000000000000000000000000000000000000000.0000000000", cast(cast("900000000000000000000000000000000000000000000000000000000000000000.0000000000" as decimalv3(76, 10)) as float);"""
    qt_sql_7_0_strict "${const_sql_7_0}"
    testFoldConst("${const_sql_7_0}")
    def const_sql_7_1 = """select "-900000000000000000000000000000000000000000000000000000000000000000.0000000000", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000.0000000000" as decimalv3(76, 10)) as float);"""
    qt_sql_7_1_strict "${const_sql_7_1}"
    testFoldConst("${const_sql_7_1}")
    def const_sql_7_2 = """select "900000000000000000000000000000000000000000000000000000000000000000.0999999999", cast(cast("900000000000000000000000000000000000000000000000000000000000000000.0999999999" as decimalv3(76, 10)) as float);"""
    qt_sql_7_2_strict "${const_sql_7_2}"
    testFoldConst("${const_sql_7_2}")
    def const_sql_7_3 = """select "-900000000000000000000000000000000000000000000000000000000000000000.0999999999", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000.0999999999" as decimalv3(76, 10)) as float);"""
    qt_sql_7_3_strict "${const_sql_7_3}"
    testFoldConst("${const_sql_7_3}")
    def const_sql_7_4 = """select "900000000000000000000000000000000000000000000000000000000000000000.9000000000", cast(cast("900000000000000000000000000000000000000000000000000000000000000000.9000000000" as decimalv3(76, 10)) as float);"""
    qt_sql_7_4_strict "${const_sql_7_4}"
    testFoldConst("${const_sql_7_4}")
    def const_sql_7_5 = """select "-900000000000000000000000000000000000000000000000000000000000000000.9000000000", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000.9000000000" as decimalv3(76, 10)) as float);"""
    qt_sql_7_5_strict "${const_sql_7_5}"
    testFoldConst("${const_sql_7_5}")
    def const_sql_7_6 = """select "900000000000000000000000000000000000000000000000000000000000000000.9000000001", cast(cast("900000000000000000000000000000000000000000000000000000000000000000.9000000001" as decimalv3(76, 10)) as float);"""
    qt_sql_7_6_strict "${const_sql_7_6}"
    testFoldConst("${const_sql_7_6}")
    def const_sql_7_7 = """select "-900000000000000000000000000000000000000000000000000000000000000000.9000000001", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000.9000000001" as decimalv3(76, 10)) as float);"""
    qt_sql_7_7_strict "${const_sql_7_7}"
    testFoldConst("${const_sql_7_7}")
    def const_sql_7_8 = """select "900000000000000000000000000000000000000000000000000000000000000000.9999999999", cast(cast("900000000000000000000000000000000000000000000000000000000000000000.9999999999" as decimalv3(76, 10)) as float);"""
    qt_sql_7_8_strict "${const_sql_7_8}"
    testFoldConst("${const_sql_7_8}")
    def const_sql_7_9 = """select "-900000000000000000000000000000000000000000000000000000000000000000.9999999999", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000.9999999999" as decimalv3(76, 10)) as float);"""
    qt_sql_7_9_strict "${const_sql_7_9}"
    testFoldConst("${const_sql_7_9}")
    def const_sql_7_10 = """select "900000000000000000000000000000000000000000000000000000000000000001.0000000000", cast(cast("900000000000000000000000000000000000000000000000000000000000000001.0000000000" as decimalv3(76, 10)) as float);"""
    qt_sql_7_10_strict "${const_sql_7_10}"
    testFoldConst("${const_sql_7_10}")
    def const_sql_7_11 = """select "-900000000000000000000000000000000000000000000000000000000000000001.0000000000", cast(cast("-900000000000000000000000000000000000000000000000000000000000000001.0000000000" as decimalv3(76, 10)) as float);"""
    qt_sql_7_11_strict "${const_sql_7_11}"
    testFoldConst("${const_sql_7_11}")
    def const_sql_7_12 = """select "900000000000000000000000000000000000000000000000000000000000000001.0999999999", cast(cast("900000000000000000000000000000000000000000000000000000000000000001.0999999999" as decimalv3(76, 10)) as float);"""
    qt_sql_7_12_strict "${const_sql_7_12}"
    testFoldConst("${const_sql_7_12}")
    def const_sql_7_13 = """select "-900000000000000000000000000000000000000000000000000000000000000001.0999999999", cast(cast("-900000000000000000000000000000000000000000000000000000000000000001.0999999999" as decimalv3(76, 10)) as float);"""
    qt_sql_7_13_strict "${const_sql_7_13}"
    testFoldConst("${const_sql_7_13}")
    def const_sql_7_14 = """select "900000000000000000000000000000000000000000000000000000000000000001.9000000000", cast(cast("900000000000000000000000000000000000000000000000000000000000000001.9000000000" as decimalv3(76, 10)) as float);"""
    qt_sql_7_14_strict "${const_sql_7_14}"
    testFoldConst("${const_sql_7_14}")
    def const_sql_7_15 = """select "-900000000000000000000000000000000000000000000000000000000000000001.9000000000", cast(cast("-900000000000000000000000000000000000000000000000000000000000000001.9000000000" as decimalv3(76, 10)) as float);"""
    qt_sql_7_15_strict "${const_sql_7_15}"
    testFoldConst("${const_sql_7_15}")
    def const_sql_7_16 = """select "900000000000000000000000000000000000000000000000000000000000000001.9000000001", cast(cast("900000000000000000000000000000000000000000000000000000000000000001.9000000001" as decimalv3(76, 10)) as float);"""
    qt_sql_7_16_strict "${const_sql_7_16}"
    testFoldConst("${const_sql_7_16}")
    def const_sql_7_17 = """select "-900000000000000000000000000000000000000000000000000000000000000001.9000000001", cast(cast("-900000000000000000000000000000000000000000000000000000000000000001.9000000001" as decimalv3(76, 10)) as float);"""
    qt_sql_7_17_strict "${const_sql_7_17}"
    testFoldConst("${const_sql_7_17}")
    def const_sql_7_18 = """select "900000000000000000000000000000000000000000000000000000000000000001.9999999999", cast(cast("900000000000000000000000000000000000000000000000000000000000000001.9999999999" as decimalv3(76, 10)) as float);"""
    qt_sql_7_18_strict "${const_sql_7_18}"
    testFoldConst("${const_sql_7_18}")
    def const_sql_7_19 = """select "-900000000000000000000000000000000000000000000000000000000000000001.9999999999", cast(cast("-900000000000000000000000000000000000000000000000000000000000000001.9999999999" as decimalv3(76, 10)) as float);"""
    qt_sql_7_19_strict "${const_sql_7_19}"
    testFoldConst("${const_sql_7_19}")
    def const_sql_7_20 = """select "999999999999999999999999999999999999999999999999999999999999999999.0000000000", cast(cast("999999999999999999999999999999999999999999999999999999999999999999.0000000000" as decimalv3(76, 10)) as float);"""
    qt_sql_7_20_strict "${const_sql_7_20}"
    testFoldConst("${const_sql_7_20}")
    def const_sql_7_21 = """select "-999999999999999999999999999999999999999999999999999999999999999999.0000000000", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999.0000000000" as decimalv3(76, 10)) as float);"""
    qt_sql_7_21_strict "${const_sql_7_21}"
    testFoldConst("${const_sql_7_21}")
    def const_sql_7_22 = """select "999999999999999999999999999999999999999999999999999999999999999999.0999999999", cast(cast("999999999999999999999999999999999999999999999999999999999999999999.0999999999" as decimalv3(76, 10)) as float);"""
    qt_sql_7_22_strict "${const_sql_7_22}"
    testFoldConst("${const_sql_7_22}")
    def const_sql_7_23 = """select "-999999999999999999999999999999999999999999999999999999999999999999.0999999999", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999.0999999999" as decimalv3(76, 10)) as float);"""
    qt_sql_7_23_strict "${const_sql_7_23}"
    testFoldConst("${const_sql_7_23}")
    def const_sql_7_24 = """select "999999999999999999999999999999999999999999999999999999999999999999.9000000000", cast(cast("999999999999999999999999999999999999999999999999999999999999999999.9000000000" as decimalv3(76, 10)) as float);"""
    qt_sql_7_24_strict "${const_sql_7_24}"
    testFoldConst("${const_sql_7_24}")
    def const_sql_7_25 = """select "-999999999999999999999999999999999999999999999999999999999999999999.9000000000", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999.9000000000" as decimalv3(76, 10)) as float);"""
    qt_sql_7_25_strict "${const_sql_7_25}"
    testFoldConst("${const_sql_7_25}")
    def const_sql_7_26 = """select "999999999999999999999999999999999999999999999999999999999999999999.9000000001", cast(cast("999999999999999999999999999999999999999999999999999999999999999999.9000000001" as decimalv3(76, 10)) as float);"""
    qt_sql_7_26_strict "${const_sql_7_26}"
    testFoldConst("${const_sql_7_26}")
    def const_sql_7_27 = """select "-999999999999999999999999999999999999999999999999999999999999999999.9000000001", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999.9000000001" as decimalv3(76, 10)) as float);"""
    qt_sql_7_27_strict "${const_sql_7_27}"
    testFoldConst("${const_sql_7_27}")
    def const_sql_7_28 = """select "999999999999999999999999999999999999999999999999999999999999999999.9999999999", cast(cast("999999999999999999999999999999999999999999999999999999999999999999.9999999999" as decimalv3(76, 10)) as float);"""
    qt_sql_7_28_strict "${const_sql_7_28}"
    testFoldConst("${const_sql_7_28}")
    def const_sql_7_29 = """select "-999999999999999999999999999999999999999999999999999999999999999999.9999999999", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999.9999999999" as decimalv3(76, 10)) as float);"""
    qt_sql_7_29_strict "${const_sql_7_29}"
    testFoldConst("${const_sql_7_29}")

    sql "set enable_strict_cast=false;"
    qt_sql_7_0_non_strict "${const_sql_7_0}"
    testFoldConst("${const_sql_7_0}")
    qt_sql_7_1_non_strict "${const_sql_7_1}"
    testFoldConst("${const_sql_7_1}")
    qt_sql_7_2_non_strict "${const_sql_7_2}"
    testFoldConst("${const_sql_7_2}")
    qt_sql_7_3_non_strict "${const_sql_7_3}"
    testFoldConst("${const_sql_7_3}")
    qt_sql_7_4_non_strict "${const_sql_7_4}"
    testFoldConst("${const_sql_7_4}")
    qt_sql_7_5_non_strict "${const_sql_7_5}"
    testFoldConst("${const_sql_7_5}")
    qt_sql_7_6_non_strict "${const_sql_7_6}"
    testFoldConst("${const_sql_7_6}")
    qt_sql_7_7_non_strict "${const_sql_7_7}"
    testFoldConst("${const_sql_7_7}")
    qt_sql_7_8_non_strict "${const_sql_7_8}"
    testFoldConst("${const_sql_7_8}")
    qt_sql_7_9_non_strict "${const_sql_7_9}"
    testFoldConst("${const_sql_7_9}")
    qt_sql_7_10_non_strict "${const_sql_7_10}"
    testFoldConst("${const_sql_7_10}")
    qt_sql_7_11_non_strict "${const_sql_7_11}"
    testFoldConst("${const_sql_7_11}")
    qt_sql_7_12_non_strict "${const_sql_7_12}"
    testFoldConst("${const_sql_7_12}")
    qt_sql_7_13_non_strict "${const_sql_7_13}"
    testFoldConst("${const_sql_7_13}")
    qt_sql_7_14_non_strict "${const_sql_7_14}"
    testFoldConst("${const_sql_7_14}")
    qt_sql_7_15_non_strict "${const_sql_7_15}"
    testFoldConst("${const_sql_7_15}")
    qt_sql_7_16_non_strict "${const_sql_7_16}"
    testFoldConst("${const_sql_7_16}")
    qt_sql_7_17_non_strict "${const_sql_7_17}"
    testFoldConst("${const_sql_7_17}")
    qt_sql_7_18_non_strict "${const_sql_7_18}"
    testFoldConst("${const_sql_7_18}")
    qt_sql_7_19_non_strict "${const_sql_7_19}"
    testFoldConst("${const_sql_7_19}")
    qt_sql_7_20_non_strict "${const_sql_7_20}"
    testFoldConst("${const_sql_7_20}")
    qt_sql_7_21_non_strict "${const_sql_7_21}"
    testFoldConst("${const_sql_7_21}")
    qt_sql_7_22_non_strict "${const_sql_7_22}"
    testFoldConst("${const_sql_7_22}")
    qt_sql_7_23_non_strict "${const_sql_7_23}"
    testFoldConst("${const_sql_7_23}")
    qt_sql_7_24_non_strict "${const_sql_7_24}"
    testFoldConst("${const_sql_7_24}")
    qt_sql_7_25_non_strict "${const_sql_7_25}"
    testFoldConst("${const_sql_7_25}")
    qt_sql_7_26_non_strict "${const_sql_7_26}"
    testFoldConst("${const_sql_7_26}")
    qt_sql_7_27_non_strict "${const_sql_7_27}"
    testFoldConst("${const_sql_7_27}")
    qt_sql_7_28_non_strict "${const_sql_7_28}"
    testFoldConst("${const_sql_7_28}")
    qt_sql_7_29_non_strict "${const_sql_7_29}"
    testFoldConst("${const_sql_7_29}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_8_0 = """select "900000000000000000000000000000000000000.0000000000000000000000000000000000000", cast(cast("900000000000000000000000000000000000000.0000000000000000000000000000000000000" as decimalv3(76, 37)) as float);"""
    qt_sql_8_0_strict "${const_sql_8_0}"
    testFoldConst("${const_sql_8_0}")
    def const_sql_8_1 = """select "-900000000000000000000000000000000000000.0000000000000000000000000000000000000", cast(cast("-900000000000000000000000000000000000000.0000000000000000000000000000000000000" as decimalv3(76, 37)) as float);"""
    qt_sql_8_1_strict "${const_sql_8_1}"
    testFoldConst("${const_sql_8_1}")
    def const_sql_8_2 = """select "900000000000000000000000000000000000000.0999999999999999999999999999999999999", cast(cast("900000000000000000000000000000000000000.0999999999999999999999999999999999999" as decimalv3(76, 37)) as float);"""
    qt_sql_8_2_strict "${const_sql_8_2}"
    testFoldConst("${const_sql_8_2}")
    def const_sql_8_3 = """select "-900000000000000000000000000000000000000.0999999999999999999999999999999999999", cast(cast("-900000000000000000000000000000000000000.0999999999999999999999999999999999999" as decimalv3(76, 37)) as float);"""
    qt_sql_8_3_strict "${const_sql_8_3}"
    testFoldConst("${const_sql_8_3}")
    def const_sql_8_4 = """select "900000000000000000000000000000000000000.9000000000000000000000000000000000000", cast(cast("900000000000000000000000000000000000000.9000000000000000000000000000000000000" as decimalv3(76, 37)) as float);"""
    qt_sql_8_4_strict "${const_sql_8_4}"
    testFoldConst("${const_sql_8_4}")
    def const_sql_8_5 = """select "-900000000000000000000000000000000000000.9000000000000000000000000000000000000", cast(cast("-900000000000000000000000000000000000000.9000000000000000000000000000000000000" as decimalv3(76, 37)) as float);"""
    qt_sql_8_5_strict "${const_sql_8_5}"
    testFoldConst("${const_sql_8_5}")
    def const_sql_8_6 = """select "900000000000000000000000000000000000000.9000000000000000000000000000000000001", cast(cast("900000000000000000000000000000000000000.9000000000000000000000000000000000001" as decimalv3(76, 37)) as float);"""
    qt_sql_8_6_strict "${const_sql_8_6}"
    testFoldConst("${const_sql_8_6}")
    def const_sql_8_7 = """select "-900000000000000000000000000000000000000.9000000000000000000000000000000000001", cast(cast("-900000000000000000000000000000000000000.9000000000000000000000000000000000001" as decimalv3(76, 37)) as float);"""
    qt_sql_8_7_strict "${const_sql_8_7}"
    testFoldConst("${const_sql_8_7}")
    def const_sql_8_8 = """select "900000000000000000000000000000000000000.9999999999999999999999999999999999999", cast(cast("900000000000000000000000000000000000000.9999999999999999999999999999999999999" as decimalv3(76, 37)) as float);"""
    qt_sql_8_8_strict "${const_sql_8_8}"
    testFoldConst("${const_sql_8_8}")
    def const_sql_8_9 = """select "-900000000000000000000000000000000000000.9999999999999999999999999999999999999", cast(cast("-900000000000000000000000000000000000000.9999999999999999999999999999999999999" as decimalv3(76, 37)) as float);"""
    qt_sql_8_9_strict "${const_sql_8_9}"
    testFoldConst("${const_sql_8_9}")
    def const_sql_8_10 = """select "900000000000000000000000000000000000001.0000000000000000000000000000000000000", cast(cast("900000000000000000000000000000000000001.0000000000000000000000000000000000000" as decimalv3(76, 37)) as float);"""
    qt_sql_8_10_strict "${const_sql_8_10}"
    testFoldConst("${const_sql_8_10}")
    def const_sql_8_11 = """select "-900000000000000000000000000000000000001.0000000000000000000000000000000000000", cast(cast("-900000000000000000000000000000000000001.0000000000000000000000000000000000000" as decimalv3(76, 37)) as float);"""
    qt_sql_8_11_strict "${const_sql_8_11}"
    testFoldConst("${const_sql_8_11}")
    def const_sql_8_12 = """select "900000000000000000000000000000000000001.0999999999999999999999999999999999999", cast(cast("900000000000000000000000000000000000001.0999999999999999999999999999999999999" as decimalv3(76, 37)) as float);"""
    qt_sql_8_12_strict "${const_sql_8_12}"
    testFoldConst("${const_sql_8_12}")
    def const_sql_8_13 = """select "-900000000000000000000000000000000000001.0999999999999999999999999999999999999", cast(cast("-900000000000000000000000000000000000001.0999999999999999999999999999999999999" as decimalv3(76, 37)) as float);"""
    qt_sql_8_13_strict "${const_sql_8_13}"
    testFoldConst("${const_sql_8_13}")
    def const_sql_8_14 = """select "900000000000000000000000000000000000001.9000000000000000000000000000000000000", cast(cast("900000000000000000000000000000000000001.9000000000000000000000000000000000000" as decimalv3(76, 37)) as float);"""
    qt_sql_8_14_strict "${const_sql_8_14}"
    testFoldConst("${const_sql_8_14}")
    def const_sql_8_15 = """select "-900000000000000000000000000000000000001.9000000000000000000000000000000000000", cast(cast("-900000000000000000000000000000000000001.9000000000000000000000000000000000000" as decimalv3(76, 37)) as float);"""
    qt_sql_8_15_strict "${const_sql_8_15}"
    testFoldConst("${const_sql_8_15}")
    def const_sql_8_16 = """select "900000000000000000000000000000000000001.9000000000000000000000000000000000001", cast(cast("900000000000000000000000000000000000001.9000000000000000000000000000000000001" as decimalv3(76, 37)) as float);"""
    qt_sql_8_16_strict "${const_sql_8_16}"
    testFoldConst("${const_sql_8_16}")
    def const_sql_8_17 = """select "-900000000000000000000000000000000000001.9000000000000000000000000000000000001", cast(cast("-900000000000000000000000000000000000001.9000000000000000000000000000000000001" as decimalv3(76, 37)) as float);"""
    qt_sql_8_17_strict "${const_sql_8_17}"
    testFoldConst("${const_sql_8_17}")
    def const_sql_8_18 = """select "900000000000000000000000000000000000001.9999999999999999999999999999999999999", cast(cast("900000000000000000000000000000000000001.9999999999999999999999999999999999999" as decimalv3(76, 37)) as float);"""
    qt_sql_8_18_strict "${const_sql_8_18}"
    testFoldConst("${const_sql_8_18}")
    def const_sql_8_19 = """select "-900000000000000000000000000000000000001.9999999999999999999999999999999999999", cast(cast("-900000000000000000000000000000000000001.9999999999999999999999999999999999999" as decimalv3(76, 37)) as float);"""
    qt_sql_8_19_strict "${const_sql_8_19}"
    testFoldConst("${const_sql_8_19}")
    def const_sql_8_20 = """select "999999999999999999999999999999999999999.0000000000000000000000000000000000000", cast(cast("999999999999999999999999999999999999999.0000000000000000000000000000000000000" as decimalv3(76, 37)) as float);"""
    qt_sql_8_20_strict "${const_sql_8_20}"
    testFoldConst("${const_sql_8_20}")
    def const_sql_8_21 = """select "-999999999999999999999999999999999999999.0000000000000000000000000000000000000", cast(cast("-999999999999999999999999999999999999999.0000000000000000000000000000000000000" as decimalv3(76, 37)) as float);"""
    qt_sql_8_21_strict "${const_sql_8_21}"
    testFoldConst("${const_sql_8_21}")
    def const_sql_8_22 = """select "999999999999999999999999999999999999999.0999999999999999999999999999999999999", cast(cast("999999999999999999999999999999999999999.0999999999999999999999999999999999999" as decimalv3(76, 37)) as float);"""
    qt_sql_8_22_strict "${const_sql_8_22}"
    testFoldConst("${const_sql_8_22}")
    def const_sql_8_23 = """select "-999999999999999999999999999999999999999.0999999999999999999999999999999999999", cast(cast("-999999999999999999999999999999999999999.0999999999999999999999999999999999999" as decimalv3(76, 37)) as float);"""
    qt_sql_8_23_strict "${const_sql_8_23}"
    testFoldConst("${const_sql_8_23}")
    def const_sql_8_24 = """select "999999999999999999999999999999999999999.9000000000000000000000000000000000000", cast(cast("999999999999999999999999999999999999999.9000000000000000000000000000000000000" as decimalv3(76, 37)) as float);"""
    qt_sql_8_24_strict "${const_sql_8_24}"
    testFoldConst("${const_sql_8_24}")
    def const_sql_8_25 = """select "-999999999999999999999999999999999999999.9000000000000000000000000000000000000", cast(cast("-999999999999999999999999999999999999999.9000000000000000000000000000000000000" as decimalv3(76, 37)) as float);"""
    qt_sql_8_25_strict "${const_sql_8_25}"
    testFoldConst("${const_sql_8_25}")
    def const_sql_8_26 = """select "999999999999999999999999999999999999999.9000000000000000000000000000000000001", cast(cast("999999999999999999999999999999999999999.9000000000000000000000000000000000001" as decimalv3(76, 37)) as float);"""
    qt_sql_8_26_strict "${const_sql_8_26}"
    testFoldConst("${const_sql_8_26}")
    def const_sql_8_27 = """select "-999999999999999999999999999999999999999.9000000000000000000000000000000000001", cast(cast("-999999999999999999999999999999999999999.9000000000000000000000000000000000001" as decimalv3(76, 37)) as float);"""
    qt_sql_8_27_strict "${const_sql_8_27}"
    testFoldConst("${const_sql_8_27}")
    def const_sql_8_28 = """select "999999999999999999999999999999999999999.9999999999999999999999999999999999999", cast(cast("999999999999999999999999999999999999999.9999999999999999999999999999999999999" as decimalv3(76, 37)) as float);"""
    qt_sql_8_28_strict "${const_sql_8_28}"
    testFoldConst("${const_sql_8_28}")
    def const_sql_8_29 = """select "-999999999999999999999999999999999999999.9999999999999999999999999999999999999", cast(cast("-999999999999999999999999999999999999999.9999999999999999999999999999999999999" as decimalv3(76, 37)) as float);"""
    qt_sql_8_29_strict "${const_sql_8_29}"
    testFoldConst("${const_sql_8_29}")

    sql "set enable_strict_cast=false;"
    qt_sql_8_0_non_strict "${const_sql_8_0}"
    testFoldConst("${const_sql_8_0}")
    qt_sql_8_1_non_strict "${const_sql_8_1}"
    testFoldConst("${const_sql_8_1}")
    qt_sql_8_2_non_strict "${const_sql_8_2}"
    testFoldConst("${const_sql_8_2}")
    qt_sql_8_3_non_strict "${const_sql_8_3}"
    testFoldConst("${const_sql_8_3}")
    qt_sql_8_4_non_strict "${const_sql_8_4}"
    testFoldConst("${const_sql_8_4}")
    qt_sql_8_5_non_strict "${const_sql_8_5}"
    testFoldConst("${const_sql_8_5}")
    qt_sql_8_6_non_strict "${const_sql_8_6}"
    testFoldConst("${const_sql_8_6}")
    qt_sql_8_7_non_strict "${const_sql_8_7}"
    testFoldConst("${const_sql_8_7}")
    qt_sql_8_8_non_strict "${const_sql_8_8}"
    testFoldConst("${const_sql_8_8}")
    qt_sql_8_9_non_strict "${const_sql_8_9}"
    testFoldConst("${const_sql_8_9}")
    qt_sql_8_10_non_strict "${const_sql_8_10}"
    testFoldConst("${const_sql_8_10}")
    qt_sql_8_11_non_strict "${const_sql_8_11}"
    testFoldConst("${const_sql_8_11}")
    qt_sql_8_12_non_strict "${const_sql_8_12}"
    testFoldConst("${const_sql_8_12}")
    qt_sql_8_13_non_strict "${const_sql_8_13}"
    testFoldConst("${const_sql_8_13}")
    qt_sql_8_14_non_strict "${const_sql_8_14}"
    testFoldConst("${const_sql_8_14}")
    qt_sql_8_15_non_strict "${const_sql_8_15}"
    testFoldConst("${const_sql_8_15}")
    qt_sql_8_16_non_strict "${const_sql_8_16}"
    testFoldConst("${const_sql_8_16}")
    qt_sql_8_17_non_strict "${const_sql_8_17}"
    testFoldConst("${const_sql_8_17}")
    qt_sql_8_18_non_strict "${const_sql_8_18}"
    testFoldConst("${const_sql_8_18}")
    qt_sql_8_19_non_strict "${const_sql_8_19}"
    testFoldConst("${const_sql_8_19}")
    qt_sql_8_20_non_strict "${const_sql_8_20}"
    testFoldConst("${const_sql_8_20}")
    qt_sql_8_21_non_strict "${const_sql_8_21}"
    testFoldConst("${const_sql_8_21}")
    qt_sql_8_22_non_strict "${const_sql_8_22}"
    testFoldConst("${const_sql_8_22}")
    qt_sql_8_23_non_strict "${const_sql_8_23}"
    testFoldConst("${const_sql_8_23}")
    qt_sql_8_24_non_strict "${const_sql_8_24}"
    testFoldConst("${const_sql_8_24}")
    qt_sql_8_25_non_strict "${const_sql_8_25}"
    testFoldConst("${const_sql_8_25}")
    qt_sql_8_26_non_strict "${const_sql_8_26}"
    testFoldConst("${const_sql_8_26}")
    qt_sql_8_27_non_strict "${const_sql_8_27}"
    testFoldConst("${const_sql_8_27}")
    qt_sql_8_28_non_strict "${const_sql_8_28}"
    testFoldConst("${const_sql_8_28}")
    qt_sql_8_29_non_strict "${const_sql_8_29}"
    testFoldConst("${const_sql_8_29}")
}