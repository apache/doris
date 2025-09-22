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


suite("test_cast_to_decimal256_76_75_from_decimalv2_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_decimal256 = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "0", cast(cast("0" as decimalv2(1, 0)) as decimalv3(76, 75));"""
    qt_sql_0_0_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    def const_sql_0_1 = """select "8", cast(cast("8" as decimalv2(1, 0)) as decimalv3(76, 75));"""
    qt_sql_0_1_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    def const_sql_0_2 = """select "9", cast(cast("9" as decimalv2(1, 0)) as decimalv3(76, 75));"""
    qt_sql_0_2_strict "${const_sql_0_2}"
    testFoldConst("${const_sql_0_2}")

    sql "set enable_strict_cast=false;"
    qt_sql_0_0_non_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    qt_sql_0_1_non_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    qt_sql_0_2_non_strict "${const_sql_0_2}"
    testFoldConst("${const_sql_0_2}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_1_0 = """select "0.0", cast(cast("0.0" as decimalv2(1, 1)) as decimalv3(76, 75));"""
    qt_sql_1_0_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    def const_sql_1_1 = """select "0.1", cast(cast("0.1" as decimalv2(1, 1)) as decimalv3(76, 75));"""
    qt_sql_1_1_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    def const_sql_1_2 = """select "0.8", cast(cast("0.8" as decimalv2(1, 1)) as decimalv3(76, 75));"""
    qt_sql_1_2_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    def const_sql_1_3 = """select "0.9", cast(cast("0.9" as decimalv2(1, 1)) as decimalv3(76, 75));"""
    qt_sql_1_3_strict "${const_sql_1_3}"
    testFoldConst("${const_sql_1_3}")

    sql "set enable_strict_cast=false;"
    qt_sql_1_0_non_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    qt_sql_1_1_non_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    qt_sql_1_2_non_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    qt_sql_1_3_non_strict "${const_sql_1_3}"
    testFoldConst("${const_sql_1_3}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_2_0 = """select "0.000000000", cast(cast("0.000000000" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_0_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    def const_sql_2_1 = """select "0.000000001", cast(cast("0.000000001" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_1_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    def const_sql_2_2 = """select "0.000000009", cast(cast("0.000000009" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_2_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    def const_sql_2_3 = """select "0.099999999", cast(cast("0.099999999" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_3_strict "${const_sql_2_3}"
    testFoldConst("${const_sql_2_3}")
    def const_sql_2_4 = """select "0.900000000", cast(cast("0.900000000" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_4_strict "${const_sql_2_4}"
    testFoldConst("${const_sql_2_4}")
    def const_sql_2_5 = """select "0.900000001", cast(cast("0.900000001" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_5_strict "${const_sql_2_5}"
    testFoldConst("${const_sql_2_5}")
    def const_sql_2_6 = """select "0.999999998", cast(cast("0.999999998" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_6_strict "${const_sql_2_6}"
    testFoldConst("${const_sql_2_6}")
    def const_sql_2_7 = """select "0.999999999", cast(cast("0.999999999" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_7_strict "${const_sql_2_7}"
    testFoldConst("${const_sql_2_7}")
    def const_sql_2_8 = """select "8.000000000", cast(cast("8.000000000" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_8_strict "${const_sql_2_8}"
    testFoldConst("${const_sql_2_8}")
    def const_sql_2_9 = """select "8.000000001", cast(cast("8.000000001" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_9_strict "${const_sql_2_9}"
    testFoldConst("${const_sql_2_9}")
    def const_sql_2_10 = """select "8.000000009", cast(cast("8.000000009" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_10_strict "${const_sql_2_10}"
    testFoldConst("${const_sql_2_10}")
    def const_sql_2_11 = """select "8.099999999", cast(cast("8.099999999" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_11_strict "${const_sql_2_11}"
    testFoldConst("${const_sql_2_11}")
    def const_sql_2_12 = """select "8.900000000", cast(cast("8.900000000" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_12_strict "${const_sql_2_12}"
    testFoldConst("${const_sql_2_12}")
    def const_sql_2_13 = """select "8.900000001", cast(cast("8.900000001" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_13_strict "${const_sql_2_13}"
    testFoldConst("${const_sql_2_13}")
    def const_sql_2_14 = """select "8.999999998", cast(cast("8.999999998" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_14_strict "${const_sql_2_14}"
    testFoldConst("${const_sql_2_14}")
    def const_sql_2_15 = """select "8.999999999", cast(cast("8.999999999" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_15_strict "${const_sql_2_15}"
    testFoldConst("${const_sql_2_15}")
    def const_sql_2_16 = """select "9.000000000", cast(cast("9.000000000" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_16_strict "${const_sql_2_16}"
    testFoldConst("${const_sql_2_16}")
    def const_sql_2_17 = """select "9.000000001", cast(cast("9.000000001" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_17_strict "${const_sql_2_17}"
    testFoldConst("${const_sql_2_17}")
    def const_sql_2_18 = """select "9.000000009", cast(cast("9.000000009" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_18_strict "${const_sql_2_18}"
    testFoldConst("${const_sql_2_18}")
    def const_sql_2_19 = """select "9.099999999", cast(cast("9.099999999" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_19_strict "${const_sql_2_19}"
    testFoldConst("${const_sql_2_19}")
    def const_sql_2_20 = """select "9.900000000", cast(cast("9.900000000" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_20_strict "${const_sql_2_20}"
    testFoldConst("${const_sql_2_20}")
    def const_sql_2_21 = """select "9.900000001", cast(cast("9.900000001" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_21_strict "${const_sql_2_21}"
    testFoldConst("${const_sql_2_21}")
    def const_sql_2_22 = """select "9.999999998", cast(cast("9.999999998" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_22_strict "${const_sql_2_22}"
    testFoldConst("${const_sql_2_22}")
    def const_sql_2_23 = """select "9.999999999", cast(cast("9.999999999" as decimalv2(27, 9)) as decimalv3(76, 75));"""
    qt_sql_2_23_strict "${const_sql_2_23}"
    testFoldConst("${const_sql_2_23}")

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
    qt_sql_2_18_non_strict "${const_sql_2_18}"
    testFoldConst("${const_sql_2_18}")
    qt_sql_2_19_non_strict "${const_sql_2_19}"
    testFoldConst("${const_sql_2_19}")
    qt_sql_2_20_non_strict "${const_sql_2_20}"
    testFoldConst("${const_sql_2_20}")
    qt_sql_2_21_non_strict "${const_sql_2_21}"
    testFoldConst("${const_sql_2_21}")
    qt_sql_2_22_non_strict "${const_sql_2_22}"
    testFoldConst("${const_sql_2_22}")
    qt_sql_2_23_non_strict "${const_sql_2_23}"
    testFoldConst("${const_sql_2_23}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_3_0 = """select "0.00000", cast(cast("0.00000" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_0_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    def const_sql_3_1 = """select "0.00001", cast(cast("0.00001" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_1_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    def const_sql_3_2 = """select "0.00009", cast(cast("0.00009" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_2_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    def const_sql_3_3 = """select "0.09999", cast(cast("0.09999" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_3_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    def const_sql_3_4 = """select "0.90000", cast(cast("0.90000" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_4_strict "${const_sql_3_4}"
    testFoldConst("${const_sql_3_4}")
    def const_sql_3_5 = """select "0.90001", cast(cast("0.90001" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_5_strict "${const_sql_3_5}"
    testFoldConst("${const_sql_3_5}")
    def const_sql_3_6 = """select "0.99998", cast(cast("0.99998" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_6_strict "${const_sql_3_6}"
    testFoldConst("${const_sql_3_6}")
    def const_sql_3_7 = """select "0.99999", cast(cast("0.99999" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_7_strict "${const_sql_3_7}"
    testFoldConst("${const_sql_3_7}")
    def const_sql_3_8 = """select "8.00000", cast(cast("8.00000" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_8_strict "${const_sql_3_8}"
    testFoldConst("${const_sql_3_8}")
    def const_sql_3_9 = """select "8.00001", cast(cast("8.00001" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_9_strict "${const_sql_3_9}"
    testFoldConst("${const_sql_3_9}")
    def const_sql_3_10 = """select "8.00009", cast(cast("8.00009" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_10_strict "${const_sql_3_10}"
    testFoldConst("${const_sql_3_10}")
    def const_sql_3_11 = """select "8.09999", cast(cast("8.09999" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_11_strict "${const_sql_3_11}"
    testFoldConst("${const_sql_3_11}")
    def const_sql_3_12 = """select "8.90000", cast(cast("8.90000" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_12_strict "${const_sql_3_12}"
    testFoldConst("${const_sql_3_12}")
    def const_sql_3_13 = """select "8.90001", cast(cast("8.90001" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_13_strict "${const_sql_3_13}"
    testFoldConst("${const_sql_3_13}")
    def const_sql_3_14 = """select "8.99998", cast(cast("8.99998" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_14_strict "${const_sql_3_14}"
    testFoldConst("${const_sql_3_14}")
    def const_sql_3_15 = """select "8.99999", cast(cast("8.99999" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_15_strict "${const_sql_3_15}"
    testFoldConst("${const_sql_3_15}")
    def const_sql_3_16 = """select "9.00000", cast(cast("9.00000" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_16_strict "${const_sql_3_16}"
    testFoldConst("${const_sql_3_16}")
    def const_sql_3_17 = """select "9.00001", cast(cast("9.00001" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_17_strict "${const_sql_3_17}"
    testFoldConst("${const_sql_3_17}")
    def const_sql_3_18 = """select "9.00009", cast(cast("9.00009" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_18_strict "${const_sql_3_18}"
    testFoldConst("${const_sql_3_18}")
    def const_sql_3_19 = """select "9.09999", cast(cast("9.09999" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_19_strict "${const_sql_3_19}"
    testFoldConst("${const_sql_3_19}")
    def const_sql_3_20 = """select "9.90000", cast(cast("9.90000" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_20_strict "${const_sql_3_20}"
    testFoldConst("${const_sql_3_20}")
    def const_sql_3_21 = """select "9.90001", cast(cast("9.90001" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_21_strict "${const_sql_3_21}"
    testFoldConst("${const_sql_3_21}")
    def const_sql_3_22 = """select "9.99998", cast(cast("9.99998" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_22_strict "${const_sql_3_22}"
    testFoldConst("${const_sql_3_22}")
    def const_sql_3_23 = """select "9.99999", cast(cast("9.99999" as decimalv2(20, 5)) as decimalv3(76, 75));"""
    qt_sql_3_23_strict "${const_sql_3_23}"
    testFoldConst("${const_sql_3_23}")

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
    qt_sql_3_6_non_strict "${const_sql_3_6}"
    testFoldConst("${const_sql_3_6}")
    qt_sql_3_7_non_strict "${const_sql_3_7}"
    testFoldConst("${const_sql_3_7}")
    qt_sql_3_8_non_strict "${const_sql_3_8}"
    testFoldConst("${const_sql_3_8}")
    qt_sql_3_9_non_strict "${const_sql_3_9}"
    testFoldConst("${const_sql_3_9}")
    qt_sql_3_10_non_strict "${const_sql_3_10}"
    testFoldConst("${const_sql_3_10}")
    qt_sql_3_11_non_strict "${const_sql_3_11}"
    testFoldConst("${const_sql_3_11}")
    qt_sql_3_12_non_strict "${const_sql_3_12}"
    testFoldConst("${const_sql_3_12}")
    qt_sql_3_13_non_strict "${const_sql_3_13}"
    testFoldConst("${const_sql_3_13}")
    qt_sql_3_14_non_strict "${const_sql_3_14}"
    testFoldConst("${const_sql_3_14}")
    qt_sql_3_15_non_strict "${const_sql_3_15}"
    testFoldConst("${const_sql_3_15}")
    qt_sql_3_16_non_strict "${const_sql_3_16}"
    testFoldConst("${const_sql_3_16}")
    qt_sql_3_17_non_strict "${const_sql_3_17}"
    testFoldConst("${const_sql_3_17}")
    qt_sql_3_18_non_strict "${const_sql_3_18}"
    testFoldConst("${const_sql_3_18}")
    qt_sql_3_19_non_strict "${const_sql_3_19}"
    testFoldConst("${const_sql_3_19}")
    qt_sql_3_20_non_strict "${const_sql_3_20}"
    testFoldConst("${const_sql_3_20}")
    qt_sql_3_21_non_strict "${const_sql_3_21}"
    testFoldConst("${const_sql_3_21}")
    qt_sql_3_22_non_strict "${const_sql_3_22}"
    testFoldConst("${const_sql_3_22}")
    qt_sql_3_23_non_strict "${const_sql_3_23}"
    testFoldConst("${const_sql_3_23}")
}