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


suite("test_cast_to_decimal256_75_74_from_decimal128i_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_decimal256 = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "0", cast(cast("0" as decimalv3(19, 0)) as decimalv3(75, 74));"""
    qt_sql_0_0_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    def const_sql_0_1 = """select "8", cast(cast("8" as decimalv3(19, 0)) as decimalv3(75, 74));"""
    qt_sql_0_1_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    def const_sql_0_2 = """select "9", cast(cast("9" as decimalv3(19, 0)) as decimalv3(75, 74));"""
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
    def const_sql_1_0 = """select "0.0", cast(cast("0.0" as decimalv3(19, 1)) as decimalv3(75, 74));"""
    qt_sql_1_0_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    def const_sql_1_1 = """select "0.1", cast(cast("0.1" as decimalv3(19, 1)) as decimalv3(75, 74));"""
    qt_sql_1_1_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    def const_sql_1_2 = """select "0.8", cast(cast("0.8" as decimalv3(19, 1)) as decimalv3(75, 74));"""
    qt_sql_1_2_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    def const_sql_1_3 = """select "0.9", cast(cast("0.9" as decimalv3(19, 1)) as decimalv3(75, 74));"""
    qt_sql_1_3_strict "${const_sql_1_3}"
    testFoldConst("${const_sql_1_3}")
    def const_sql_1_4 = """select "8.0", cast(cast("8.0" as decimalv3(19, 1)) as decimalv3(75, 74));"""
    qt_sql_1_4_strict "${const_sql_1_4}"
    testFoldConst("${const_sql_1_4}")
    def const_sql_1_5 = """select "8.1", cast(cast("8.1" as decimalv3(19, 1)) as decimalv3(75, 74));"""
    qt_sql_1_5_strict "${const_sql_1_5}"
    testFoldConst("${const_sql_1_5}")
    def const_sql_1_6 = """select "8.8", cast(cast("8.8" as decimalv3(19, 1)) as decimalv3(75, 74));"""
    qt_sql_1_6_strict "${const_sql_1_6}"
    testFoldConst("${const_sql_1_6}")
    def const_sql_1_7 = """select "8.9", cast(cast("8.9" as decimalv3(19, 1)) as decimalv3(75, 74));"""
    qt_sql_1_7_strict "${const_sql_1_7}"
    testFoldConst("${const_sql_1_7}")
    def const_sql_1_8 = """select "9.0", cast(cast("9.0" as decimalv3(19, 1)) as decimalv3(75, 74));"""
    qt_sql_1_8_strict "${const_sql_1_8}"
    testFoldConst("${const_sql_1_8}")
    def const_sql_1_9 = """select "9.1", cast(cast("9.1" as decimalv3(19, 1)) as decimalv3(75, 74));"""
    qt_sql_1_9_strict "${const_sql_1_9}"
    testFoldConst("${const_sql_1_9}")
    def const_sql_1_10 = """select "9.8", cast(cast("9.8" as decimalv3(19, 1)) as decimalv3(75, 74));"""
    qt_sql_1_10_strict "${const_sql_1_10}"
    testFoldConst("${const_sql_1_10}")
    def const_sql_1_11 = """select "9.9", cast(cast("9.9" as decimalv3(19, 1)) as decimalv3(75, 74));"""
    qt_sql_1_11_strict "${const_sql_1_11}"
    testFoldConst("${const_sql_1_11}")

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
    qt_sql_1_6_non_strict "${const_sql_1_6}"
    testFoldConst("${const_sql_1_6}")
    qt_sql_1_7_non_strict "${const_sql_1_7}"
    testFoldConst("${const_sql_1_7}")
    qt_sql_1_8_non_strict "${const_sql_1_8}"
    testFoldConst("${const_sql_1_8}")
    qt_sql_1_9_non_strict "${const_sql_1_9}"
    testFoldConst("${const_sql_1_9}")
    qt_sql_1_10_non_strict "${const_sql_1_10}"
    testFoldConst("${const_sql_1_10}")
    qt_sql_1_11_non_strict "${const_sql_1_11}"
    testFoldConst("${const_sql_1_11}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_2_0 = """select "0.000000000", cast(cast("0.000000000" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_0_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    def const_sql_2_1 = """select "0.000000001", cast(cast("0.000000001" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_1_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    def const_sql_2_2 = """select "0.000000009", cast(cast("0.000000009" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_2_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    def const_sql_2_3 = """select "0.099999999", cast(cast("0.099999999" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_3_strict "${const_sql_2_3}"
    testFoldConst("${const_sql_2_3}")
    def const_sql_2_4 = """select "0.900000000", cast(cast("0.900000000" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_4_strict "${const_sql_2_4}"
    testFoldConst("${const_sql_2_4}")
    def const_sql_2_5 = """select "0.900000001", cast(cast("0.900000001" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_5_strict "${const_sql_2_5}"
    testFoldConst("${const_sql_2_5}")
    def const_sql_2_6 = """select "0.999999998", cast(cast("0.999999998" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_6_strict "${const_sql_2_6}"
    testFoldConst("${const_sql_2_6}")
    def const_sql_2_7 = """select "0.999999999", cast(cast("0.999999999" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_7_strict "${const_sql_2_7}"
    testFoldConst("${const_sql_2_7}")
    def const_sql_2_8 = """select "8.000000000", cast(cast("8.000000000" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_8_strict "${const_sql_2_8}"
    testFoldConst("${const_sql_2_8}")
    def const_sql_2_9 = """select "8.000000001", cast(cast("8.000000001" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_9_strict "${const_sql_2_9}"
    testFoldConst("${const_sql_2_9}")
    def const_sql_2_10 = """select "8.000000009", cast(cast("8.000000009" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_10_strict "${const_sql_2_10}"
    testFoldConst("${const_sql_2_10}")
    def const_sql_2_11 = """select "8.099999999", cast(cast("8.099999999" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_11_strict "${const_sql_2_11}"
    testFoldConst("${const_sql_2_11}")
    def const_sql_2_12 = """select "8.900000000", cast(cast("8.900000000" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_12_strict "${const_sql_2_12}"
    testFoldConst("${const_sql_2_12}")
    def const_sql_2_13 = """select "8.900000001", cast(cast("8.900000001" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_13_strict "${const_sql_2_13}"
    testFoldConst("${const_sql_2_13}")
    def const_sql_2_14 = """select "8.999999998", cast(cast("8.999999998" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_14_strict "${const_sql_2_14}"
    testFoldConst("${const_sql_2_14}")
    def const_sql_2_15 = """select "8.999999999", cast(cast("8.999999999" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_15_strict "${const_sql_2_15}"
    testFoldConst("${const_sql_2_15}")
    def const_sql_2_16 = """select "9.000000000", cast(cast("9.000000000" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_16_strict "${const_sql_2_16}"
    testFoldConst("${const_sql_2_16}")
    def const_sql_2_17 = """select "9.000000001", cast(cast("9.000000001" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_17_strict "${const_sql_2_17}"
    testFoldConst("${const_sql_2_17}")
    def const_sql_2_18 = """select "9.000000009", cast(cast("9.000000009" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_18_strict "${const_sql_2_18}"
    testFoldConst("${const_sql_2_18}")
    def const_sql_2_19 = """select "9.099999999", cast(cast("9.099999999" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_19_strict "${const_sql_2_19}"
    testFoldConst("${const_sql_2_19}")
    def const_sql_2_20 = """select "9.900000000", cast(cast("9.900000000" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_20_strict "${const_sql_2_20}"
    testFoldConst("${const_sql_2_20}")
    def const_sql_2_21 = """select "9.900000001", cast(cast("9.900000001" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_21_strict "${const_sql_2_21}"
    testFoldConst("${const_sql_2_21}")
    def const_sql_2_22 = """select "9.999999998", cast(cast("9.999999998" as decimalv3(19, 9)) as decimalv3(75, 74));"""
    qt_sql_2_22_strict "${const_sql_2_22}"
    testFoldConst("${const_sql_2_22}")
    def const_sql_2_23 = """select "9.999999999", cast(cast("9.999999999" as decimalv3(19, 9)) as decimalv3(75, 74));"""
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
    def const_sql_3_0 = """select "0.000000000000000000", cast(cast("0.000000000000000000" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_0_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    def const_sql_3_1 = """select "0.000000000000000001", cast(cast("0.000000000000000001" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_1_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    def const_sql_3_2 = """select "0.000000000000000009", cast(cast("0.000000000000000009" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_2_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    def const_sql_3_3 = """select "0.099999999999999999", cast(cast("0.099999999999999999" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_3_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    def const_sql_3_4 = """select "0.900000000000000000", cast(cast("0.900000000000000000" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_4_strict "${const_sql_3_4}"
    testFoldConst("${const_sql_3_4}")
    def const_sql_3_5 = """select "0.900000000000000001", cast(cast("0.900000000000000001" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_5_strict "${const_sql_3_5}"
    testFoldConst("${const_sql_3_5}")
    def const_sql_3_6 = """select "0.999999999999999998", cast(cast("0.999999999999999998" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_6_strict "${const_sql_3_6}"
    testFoldConst("${const_sql_3_6}")
    def const_sql_3_7 = """select "0.999999999999999999", cast(cast("0.999999999999999999" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_7_strict "${const_sql_3_7}"
    testFoldConst("${const_sql_3_7}")
    def const_sql_3_8 = """select "8.000000000000000000", cast(cast("8.000000000000000000" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_8_strict "${const_sql_3_8}"
    testFoldConst("${const_sql_3_8}")
    def const_sql_3_9 = """select "8.000000000000000001", cast(cast("8.000000000000000001" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_9_strict "${const_sql_3_9}"
    testFoldConst("${const_sql_3_9}")
    def const_sql_3_10 = """select "8.000000000000000009", cast(cast("8.000000000000000009" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_10_strict "${const_sql_3_10}"
    testFoldConst("${const_sql_3_10}")
    def const_sql_3_11 = """select "8.099999999999999999", cast(cast("8.099999999999999999" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_11_strict "${const_sql_3_11}"
    testFoldConst("${const_sql_3_11}")
    def const_sql_3_12 = """select "8.900000000000000000", cast(cast("8.900000000000000000" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_12_strict "${const_sql_3_12}"
    testFoldConst("${const_sql_3_12}")
    def const_sql_3_13 = """select "8.900000000000000001", cast(cast("8.900000000000000001" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_13_strict "${const_sql_3_13}"
    testFoldConst("${const_sql_3_13}")
    def const_sql_3_14 = """select "8.999999999999999998", cast(cast("8.999999999999999998" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_14_strict "${const_sql_3_14}"
    testFoldConst("${const_sql_3_14}")
    def const_sql_3_15 = """select "8.999999999999999999", cast(cast("8.999999999999999999" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_15_strict "${const_sql_3_15}"
    testFoldConst("${const_sql_3_15}")
    def const_sql_3_16 = """select "9.000000000000000000", cast(cast("9.000000000000000000" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_16_strict "${const_sql_3_16}"
    testFoldConst("${const_sql_3_16}")
    def const_sql_3_17 = """select "9.000000000000000001", cast(cast("9.000000000000000001" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_17_strict "${const_sql_3_17}"
    testFoldConst("${const_sql_3_17}")
    def const_sql_3_18 = """select "9.000000000000000009", cast(cast("9.000000000000000009" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_18_strict "${const_sql_3_18}"
    testFoldConst("${const_sql_3_18}")
    def const_sql_3_19 = """select "9.099999999999999999", cast(cast("9.099999999999999999" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_19_strict "${const_sql_3_19}"
    testFoldConst("${const_sql_3_19}")
    def const_sql_3_20 = """select "9.900000000000000000", cast(cast("9.900000000000000000" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_20_strict "${const_sql_3_20}"
    testFoldConst("${const_sql_3_20}")
    def const_sql_3_21 = """select "9.900000000000000001", cast(cast("9.900000000000000001" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_21_strict "${const_sql_3_21}"
    testFoldConst("${const_sql_3_21}")
    def const_sql_3_22 = """select "9.999999999999999998", cast(cast("9.999999999999999998" as decimalv3(19, 18)) as decimalv3(75, 74));"""
    qt_sql_3_22_strict "${const_sql_3_22}"
    testFoldConst("${const_sql_3_22}")
    def const_sql_3_23 = """select "9.999999999999999999", cast(cast("9.999999999999999999" as decimalv3(19, 18)) as decimalv3(75, 74));"""
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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_4_0 = """select "0.0000000000000000000", cast(cast("0.0000000000000000000" as decimalv3(19, 19)) as decimalv3(75, 74));"""
    qt_sql_4_0_strict "${const_sql_4_0}"
    testFoldConst("${const_sql_4_0}")
    def const_sql_4_1 = """select "0.0000000000000000001", cast(cast("0.0000000000000000001" as decimalv3(19, 19)) as decimalv3(75, 74));"""
    qt_sql_4_1_strict "${const_sql_4_1}"
    testFoldConst("${const_sql_4_1}")
    def const_sql_4_2 = """select "0.0000000000000000009", cast(cast("0.0000000000000000009" as decimalv3(19, 19)) as decimalv3(75, 74));"""
    qt_sql_4_2_strict "${const_sql_4_2}"
    testFoldConst("${const_sql_4_2}")
    def const_sql_4_3 = """select "0.0999999999999999999", cast(cast("0.0999999999999999999" as decimalv3(19, 19)) as decimalv3(75, 74));"""
    qt_sql_4_3_strict "${const_sql_4_3}"
    testFoldConst("${const_sql_4_3}")
    def const_sql_4_4 = """select "0.9000000000000000000", cast(cast("0.9000000000000000000" as decimalv3(19, 19)) as decimalv3(75, 74));"""
    qt_sql_4_4_strict "${const_sql_4_4}"
    testFoldConst("${const_sql_4_4}")
    def const_sql_4_5 = """select "0.9000000000000000001", cast(cast("0.9000000000000000001" as decimalv3(19, 19)) as decimalv3(75, 74));"""
    qt_sql_4_5_strict "${const_sql_4_5}"
    testFoldConst("${const_sql_4_5}")
    def const_sql_4_6 = """select "0.9999999999999999998", cast(cast("0.9999999999999999998" as decimalv3(19, 19)) as decimalv3(75, 74));"""
    qt_sql_4_6_strict "${const_sql_4_6}"
    testFoldConst("${const_sql_4_6}")
    def const_sql_4_7 = """select "0.9999999999999999999", cast(cast("0.9999999999999999999" as decimalv3(19, 19)) as decimalv3(75, 74));"""
    qt_sql_4_7_strict "${const_sql_4_7}"
    testFoldConst("${const_sql_4_7}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_5_0 = """select "0", cast(cast("0" as decimalv3(37, 0)) as decimalv3(75, 74));"""
    qt_sql_5_0_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")
    def const_sql_5_1 = """select "8", cast(cast("8" as decimalv3(37, 0)) as decimalv3(75, 74));"""
    qt_sql_5_1_strict "${const_sql_5_1}"
    testFoldConst("${const_sql_5_1}")
    def const_sql_5_2 = """select "9", cast(cast("9" as decimalv3(37, 0)) as decimalv3(75, 74));"""
    qt_sql_5_2_strict "${const_sql_5_2}"
    testFoldConst("${const_sql_5_2}")

    sql "set enable_strict_cast=false;"
    qt_sql_5_0_non_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")
    qt_sql_5_1_non_strict "${const_sql_5_1}"
    testFoldConst("${const_sql_5_1}")
    qt_sql_5_2_non_strict "${const_sql_5_2}"
    testFoldConst("${const_sql_5_2}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_6_0 = """select "0.0", cast(cast("0.0" as decimalv3(37, 1)) as decimalv3(75, 74));"""
    qt_sql_6_0_strict "${const_sql_6_0}"
    testFoldConst("${const_sql_6_0}")
    def const_sql_6_1 = """select "0.1", cast(cast("0.1" as decimalv3(37, 1)) as decimalv3(75, 74));"""
    qt_sql_6_1_strict "${const_sql_6_1}"
    testFoldConst("${const_sql_6_1}")
    def const_sql_6_2 = """select "0.8", cast(cast("0.8" as decimalv3(37, 1)) as decimalv3(75, 74));"""
    qt_sql_6_2_strict "${const_sql_6_2}"
    testFoldConst("${const_sql_6_2}")
    def const_sql_6_3 = """select "0.9", cast(cast("0.9" as decimalv3(37, 1)) as decimalv3(75, 74));"""
    qt_sql_6_3_strict "${const_sql_6_3}"
    testFoldConst("${const_sql_6_3}")
    def const_sql_6_4 = """select "8.0", cast(cast("8.0" as decimalv3(37, 1)) as decimalv3(75, 74));"""
    qt_sql_6_4_strict "${const_sql_6_4}"
    testFoldConst("${const_sql_6_4}")
    def const_sql_6_5 = """select "8.1", cast(cast("8.1" as decimalv3(37, 1)) as decimalv3(75, 74));"""
    qt_sql_6_5_strict "${const_sql_6_5}"
    testFoldConst("${const_sql_6_5}")
    def const_sql_6_6 = """select "8.8", cast(cast("8.8" as decimalv3(37, 1)) as decimalv3(75, 74));"""
    qt_sql_6_6_strict "${const_sql_6_6}"
    testFoldConst("${const_sql_6_6}")
    def const_sql_6_7 = """select "8.9", cast(cast("8.9" as decimalv3(37, 1)) as decimalv3(75, 74));"""
    qt_sql_6_7_strict "${const_sql_6_7}"
    testFoldConst("${const_sql_6_7}")
    def const_sql_6_8 = """select "9.0", cast(cast("9.0" as decimalv3(37, 1)) as decimalv3(75, 74));"""
    qt_sql_6_8_strict "${const_sql_6_8}"
    testFoldConst("${const_sql_6_8}")
    def const_sql_6_9 = """select "9.1", cast(cast("9.1" as decimalv3(37, 1)) as decimalv3(75, 74));"""
    qt_sql_6_9_strict "${const_sql_6_9}"
    testFoldConst("${const_sql_6_9}")
    def const_sql_6_10 = """select "9.8", cast(cast("9.8" as decimalv3(37, 1)) as decimalv3(75, 74));"""
    qt_sql_6_10_strict "${const_sql_6_10}"
    testFoldConst("${const_sql_6_10}")
    def const_sql_6_11 = """select "9.9", cast(cast("9.9" as decimalv3(37, 1)) as decimalv3(75, 74));"""
    qt_sql_6_11_strict "${const_sql_6_11}"
    testFoldConst("${const_sql_6_11}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_7_0 = """select "0.000000000000000000", cast(cast("0.000000000000000000" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_0_strict "${const_sql_7_0}"
    testFoldConst("${const_sql_7_0}")
    def const_sql_7_1 = """select "0.000000000000000001", cast(cast("0.000000000000000001" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_1_strict "${const_sql_7_1}"
    testFoldConst("${const_sql_7_1}")
    def const_sql_7_2 = """select "0.000000000000000009", cast(cast("0.000000000000000009" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_2_strict "${const_sql_7_2}"
    testFoldConst("${const_sql_7_2}")
    def const_sql_7_3 = """select "0.099999999999999999", cast(cast("0.099999999999999999" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_3_strict "${const_sql_7_3}"
    testFoldConst("${const_sql_7_3}")
    def const_sql_7_4 = """select "0.900000000000000000", cast(cast("0.900000000000000000" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_4_strict "${const_sql_7_4}"
    testFoldConst("${const_sql_7_4}")
    def const_sql_7_5 = """select "0.900000000000000001", cast(cast("0.900000000000000001" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_5_strict "${const_sql_7_5}"
    testFoldConst("${const_sql_7_5}")
    def const_sql_7_6 = """select "0.999999999999999998", cast(cast("0.999999999999999998" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_6_strict "${const_sql_7_6}"
    testFoldConst("${const_sql_7_6}")
    def const_sql_7_7 = """select "0.999999999999999999", cast(cast("0.999999999999999999" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_7_strict "${const_sql_7_7}"
    testFoldConst("${const_sql_7_7}")
    def const_sql_7_8 = """select "8.000000000000000000", cast(cast("8.000000000000000000" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_8_strict "${const_sql_7_8}"
    testFoldConst("${const_sql_7_8}")
    def const_sql_7_9 = """select "8.000000000000000001", cast(cast("8.000000000000000001" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_9_strict "${const_sql_7_9}"
    testFoldConst("${const_sql_7_9}")
    def const_sql_7_10 = """select "8.000000000000000009", cast(cast("8.000000000000000009" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_10_strict "${const_sql_7_10}"
    testFoldConst("${const_sql_7_10}")
    def const_sql_7_11 = """select "8.099999999999999999", cast(cast("8.099999999999999999" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_11_strict "${const_sql_7_11}"
    testFoldConst("${const_sql_7_11}")
    def const_sql_7_12 = """select "8.900000000000000000", cast(cast("8.900000000000000000" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_12_strict "${const_sql_7_12}"
    testFoldConst("${const_sql_7_12}")
    def const_sql_7_13 = """select "8.900000000000000001", cast(cast("8.900000000000000001" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_13_strict "${const_sql_7_13}"
    testFoldConst("${const_sql_7_13}")
    def const_sql_7_14 = """select "8.999999999999999998", cast(cast("8.999999999999999998" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_14_strict "${const_sql_7_14}"
    testFoldConst("${const_sql_7_14}")
    def const_sql_7_15 = """select "8.999999999999999999", cast(cast("8.999999999999999999" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_15_strict "${const_sql_7_15}"
    testFoldConst("${const_sql_7_15}")
    def const_sql_7_16 = """select "9.000000000000000000", cast(cast("9.000000000000000000" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_16_strict "${const_sql_7_16}"
    testFoldConst("${const_sql_7_16}")
    def const_sql_7_17 = """select "9.000000000000000001", cast(cast("9.000000000000000001" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_17_strict "${const_sql_7_17}"
    testFoldConst("${const_sql_7_17}")
    def const_sql_7_18 = """select "9.000000000000000009", cast(cast("9.000000000000000009" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_18_strict "${const_sql_7_18}"
    testFoldConst("${const_sql_7_18}")
    def const_sql_7_19 = """select "9.099999999999999999", cast(cast("9.099999999999999999" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_19_strict "${const_sql_7_19}"
    testFoldConst("${const_sql_7_19}")
    def const_sql_7_20 = """select "9.900000000000000000", cast(cast("9.900000000000000000" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_20_strict "${const_sql_7_20}"
    testFoldConst("${const_sql_7_20}")
    def const_sql_7_21 = """select "9.900000000000000001", cast(cast("9.900000000000000001" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_21_strict "${const_sql_7_21}"
    testFoldConst("${const_sql_7_21}")
    def const_sql_7_22 = """select "9.999999999999999998", cast(cast("9.999999999999999998" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_22_strict "${const_sql_7_22}"
    testFoldConst("${const_sql_7_22}")
    def const_sql_7_23 = """select "9.999999999999999999", cast(cast("9.999999999999999999" as decimalv3(37, 18)) as decimalv3(75, 74));"""
    qt_sql_7_23_strict "${const_sql_7_23}"
    testFoldConst("${const_sql_7_23}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_8_0 = """select "0.000000000000000000000000000000000000", cast(cast("0.000000000000000000000000000000000000" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_0_strict "${const_sql_8_0}"
    testFoldConst("${const_sql_8_0}")
    def const_sql_8_1 = """select "0.000000000000000000000000000000000001", cast(cast("0.000000000000000000000000000000000001" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_1_strict "${const_sql_8_1}"
    testFoldConst("${const_sql_8_1}")
    def const_sql_8_2 = """select "0.000000000000000000000000000000000009", cast(cast("0.000000000000000000000000000000000009" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_2_strict "${const_sql_8_2}"
    testFoldConst("${const_sql_8_2}")
    def const_sql_8_3 = """select "0.099999999999999999999999999999999999", cast(cast("0.099999999999999999999999999999999999" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_3_strict "${const_sql_8_3}"
    testFoldConst("${const_sql_8_3}")
    def const_sql_8_4 = """select "0.900000000000000000000000000000000000", cast(cast("0.900000000000000000000000000000000000" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_4_strict "${const_sql_8_4}"
    testFoldConst("${const_sql_8_4}")
    def const_sql_8_5 = """select "0.900000000000000000000000000000000001", cast(cast("0.900000000000000000000000000000000001" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_5_strict "${const_sql_8_5}"
    testFoldConst("${const_sql_8_5}")
    def const_sql_8_6 = """select "0.999999999999999999999999999999999998", cast(cast("0.999999999999999999999999999999999998" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_6_strict "${const_sql_8_6}"
    testFoldConst("${const_sql_8_6}")
    def const_sql_8_7 = """select "0.999999999999999999999999999999999999", cast(cast("0.999999999999999999999999999999999999" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_7_strict "${const_sql_8_7}"
    testFoldConst("${const_sql_8_7}")
    def const_sql_8_8 = """select "8.000000000000000000000000000000000000", cast(cast("8.000000000000000000000000000000000000" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_8_strict "${const_sql_8_8}"
    testFoldConst("${const_sql_8_8}")
    def const_sql_8_9 = """select "8.000000000000000000000000000000000001", cast(cast("8.000000000000000000000000000000000001" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_9_strict "${const_sql_8_9}"
    testFoldConst("${const_sql_8_9}")
    def const_sql_8_10 = """select "8.000000000000000000000000000000000009", cast(cast("8.000000000000000000000000000000000009" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_10_strict "${const_sql_8_10}"
    testFoldConst("${const_sql_8_10}")
    def const_sql_8_11 = """select "8.099999999999999999999999999999999999", cast(cast("8.099999999999999999999999999999999999" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_11_strict "${const_sql_8_11}"
    testFoldConst("${const_sql_8_11}")
    def const_sql_8_12 = """select "8.900000000000000000000000000000000000", cast(cast("8.900000000000000000000000000000000000" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_12_strict "${const_sql_8_12}"
    testFoldConst("${const_sql_8_12}")
    def const_sql_8_13 = """select "8.900000000000000000000000000000000001", cast(cast("8.900000000000000000000000000000000001" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_13_strict "${const_sql_8_13}"
    testFoldConst("${const_sql_8_13}")
    def const_sql_8_14 = """select "8.999999999999999999999999999999999998", cast(cast("8.999999999999999999999999999999999998" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_14_strict "${const_sql_8_14}"
    testFoldConst("${const_sql_8_14}")
    def const_sql_8_15 = """select "8.999999999999999999999999999999999999", cast(cast("8.999999999999999999999999999999999999" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_15_strict "${const_sql_8_15}"
    testFoldConst("${const_sql_8_15}")
    def const_sql_8_16 = """select "9.000000000000000000000000000000000000", cast(cast("9.000000000000000000000000000000000000" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_16_strict "${const_sql_8_16}"
    testFoldConst("${const_sql_8_16}")
    def const_sql_8_17 = """select "9.000000000000000000000000000000000001", cast(cast("9.000000000000000000000000000000000001" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_17_strict "${const_sql_8_17}"
    testFoldConst("${const_sql_8_17}")
    def const_sql_8_18 = """select "9.000000000000000000000000000000000009", cast(cast("9.000000000000000000000000000000000009" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_18_strict "${const_sql_8_18}"
    testFoldConst("${const_sql_8_18}")
    def const_sql_8_19 = """select "9.099999999999999999999999999999999999", cast(cast("9.099999999999999999999999999999999999" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_19_strict "${const_sql_8_19}"
    testFoldConst("${const_sql_8_19}")
    def const_sql_8_20 = """select "9.900000000000000000000000000000000000", cast(cast("9.900000000000000000000000000000000000" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_20_strict "${const_sql_8_20}"
    testFoldConst("${const_sql_8_20}")
    def const_sql_8_21 = """select "9.900000000000000000000000000000000001", cast(cast("9.900000000000000000000000000000000001" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_21_strict "${const_sql_8_21}"
    testFoldConst("${const_sql_8_21}")
    def const_sql_8_22 = """select "9.999999999999999999999999999999999998", cast(cast("9.999999999999999999999999999999999998" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_22_strict "${const_sql_8_22}"
    testFoldConst("${const_sql_8_22}")
    def const_sql_8_23 = """select "9.999999999999999999999999999999999999", cast(cast("9.999999999999999999999999999999999999" as decimalv3(37, 36)) as decimalv3(75, 74));"""
    qt_sql_8_23_strict "${const_sql_8_23}"
    testFoldConst("${const_sql_8_23}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_9_0 = """select "0.0000000000000000000000000000000000000", cast(cast("0.0000000000000000000000000000000000000" as decimalv3(37, 37)) as decimalv3(75, 74));"""
    qt_sql_9_0_strict "${const_sql_9_0}"
    testFoldConst("${const_sql_9_0}")
    def const_sql_9_1 = """select "0.0000000000000000000000000000000000001", cast(cast("0.0000000000000000000000000000000000001" as decimalv3(37, 37)) as decimalv3(75, 74));"""
    qt_sql_9_1_strict "${const_sql_9_1}"
    testFoldConst("${const_sql_9_1}")
    def const_sql_9_2 = """select "0.0000000000000000000000000000000000009", cast(cast("0.0000000000000000000000000000000000009" as decimalv3(37, 37)) as decimalv3(75, 74));"""
    qt_sql_9_2_strict "${const_sql_9_2}"
    testFoldConst("${const_sql_9_2}")
    def const_sql_9_3 = """select "0.0999999999999999999999999999999999999", cast(cast("0.0999999999999999999999999999999999999" as decimalv3(37, 37)) as decimalv3(75, 74));"""
    qt_sql_9_3_strict "${const_sql_9_3}"
    testFoldConst("${const_sql_9_3}")
    def const_sql_9_4 = """select "0.9000000000000000000000000000000000000", cast(cast("0.9000000000000000000000000000000000000" as decimalv3(37, 37)) as decimalv3(75, 74));"""
    qt_sql_9_4_strict "${const_sql_9_4}"
    testFoldConst("${const_sql_9_4}")
    def const_sql_9_5 = """select "0.9000000000000000000000000000000000001", cast(cast("0.9000000000000000000000000000000000001" as decimalv3(37, 37)) as decimalv3(75, 74));"""
    qt_sql_9_5_strict "${const_sql_9_5}"
    testFoldConst("${const_sql_9_5}")
    def const_sql_9_6 = """select "0.9999999999999999999999999999999999998", cast(cast("0.9999999999999999999999999999999999998" as decimalv3(37, 37)) as decimalv3(75, 74));"""
    qt_sql_9_6_strict "${const_sql_9_6}"
    testFoldConst("${const_sql_9_6}")
    def const_sql_9_7 = """select "0.9999999999999999999999999999999999999", cast(cast("0.9999999999999999999999999999999999999" as decimalv3(37, 37)) as decimalv3(75, 74));"""
    qt_sql_9_7_strict "${const_sql_9_7}"
    testFoldConst("${const_sql_9_7}")

    sql "set enable_strict_cast=false;"
    qt_sql_9_0_non_strict "${const_sql_9_0}"
    testFoldConst("${const_sql_9_0}")
    qt_sql_9_1_non_strict "${const_sql_9_1}"
    testFoldConst("${const_sql_9_1}")
    qt_sql_9_2_non_strict "${const_sql_9_2}"
    testFoldConst("${const_sql_9_2}")
    qt_sql_9_3_non_strict "${const_sql_9_3}"
    testFoldConst("${const_sql_9_3}")
    qt_sql_9_4_non_strict "${const_sql_9_4}"
    testFoldConst("${const_sql_9_4}")
    qt_sql_9_5_non_strict "${const_sql_9_5}"
    testFoldConst("${const_sql_9_5}")
    qt_sql_9_6_non_strict "${const_sql_9_6}"
    testFoldConst("${const_sql_9_6}")
    qt_sql_9_7_non_strict "${const_sql_9_7}"
    testFoldConst("${const_sql_9_7}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_10_0 = """select "0", cast(cast("0" as decimalv3(38, 0)) as decimalv3(75, 74));"""
    qt_sql_10_0_strict "${const_sql_10_0}"
    testFoldConst("${const_sql_10_0}")
    def const_sql_10_1 = """select "8", cast(cast("8" as decimalv3(38, 0)) as decimalv3(75, 74));"""
    qt_sql_10_1_strict "${const_sql_10_1}"
    testFoldConst("${const_sql_10_1}")
    def const_sql_10_2 = """select "9", cast(cast("9" as decimalv3(38, 0)) as decimalv3(75, 74));"""
    qt_sql_10_2_strict "${const_sql_10_2}"
    testFoldConst("${const_sql_10_2}")

    sql "set enable_strict_cast=false;"
    qt_sql_10_0_non_strict "${const_sql_10_0}"
    testFoldConst("${const_sql_10_0}")
    qt_sql_10_1_non_strict "${const_sql_10_1}"
    testFoldConst("${const_sql_10_1}")
    qt_sql_10_2_non_strict "${const_sql_10_2}"
    testFoldConst("${const_sql_10_2}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_11_0 = """select "0.0", cast(cast("0.0" as decimalv3(38, 1)) as decimalv3(75, 74));"""
    qt_sql_11_0_strict "${const_sql_11_0}"
    testFoldConst("${const_sql_11_0}")
    def const_sql_11_1 = """select "0.1", cast(cast("0.1" as decimalv3(38, 1)) as decimalv3(75, 74));"""
    qt_sql_11_1_strict "${const_sql_11_1}"
    testFoldConst("${const_sql_11_1}")
    def const_sql_11_2 = """select "0.8", cast(cast("0.8" as decimalv3(38, 1)) as decimalv3(75, 74));"""
    qt_sql_11_2_strict "${const_sql_11_2}"
    testFoldConst("${const_sql_11_2}")
    def const_sql_11_3 = """select "0.9", cast(cast("0.9" as decimalv3(38, 1)) as decimalv3(75, 74));"""
    qt_sql_11_3_strict "${const_sql_11_3}"
    testFoldConst("${const_sql_11_3}")
    def const_sql_11_4 = """select "8.0", cast(cast("8.0" as decimalv3(38, 1)) as decimalv3(75, 74));"""
    qt_sql_11_4_strict "${const_sql_11_4}"
    testFoldConst("${const_sql_11_4}")
    def const_sql_11_5 = """select "8.1", cast(cast("8.1" as decimalv3(38, 1)) as decimalv3(75, 74));"""
    qt_sql_11_5_strict "${const_sql_11_5}"
    testFoldConst("${const_sql_11_5}")
    def const_sql_11_6 = """select "8.8", cast(cast("8.8" as decimalv3(38, 1)) as decimalv3(75, 74));"""
    qt_sql_11_6_strict "${const_sql_11_6}"
    testFoldConst("${const_sql_11_6}")
    def const_sql_11_7 = """select "8.9", cast(cast("8.9" as decimalv3(38, 1)) as decimalv3(75, 74));"""
    qt_sql_11_7_strict "${const_sql_11_7}"
    testFoldConst("${const_sql_11_7}")
    def const_sql_11_8 = """select "9.0", cast(cast("9.0" as decimalv3(38, 1)) as decimalv3(75, 74));"""
    qt_sql_11_8_strict "${const_sql_11_8}"
    testFoldConst("${const_sql_11_8}")
    def const_sql_11_9 = """select "9.1", cast(cast("9.1" as decimalv3(38, 1)) as decimalv3(75, 74));"""
    qt_sql_11_9_strict "${const_sql_11_9}"
    testFoldConst("${const_sql_11_9}")
    def const_sql_11_10 = """select "9.8", cast(cast("9.8" as decimalv3(38, 1)) as decimalv3(75, 74));"""
    qt_sql_11_10_strict "${const_sql_11_10}"
    testFoldConst("${const_sql_11_10}")
    def const_sql_11_11 = """select "9.9", cast(cast("9.9" as decimalv3(38, 1)) as decimalv3(75, 74));"""
    qt_sql_11_11_strict "${const_sql_11_11}"
    testFoldConst("${const_sql_11_11}")

    sql "set enable_strict_cast=false;"
    qt_sql_11_0_non_strict "${const_sql_11_0}"
    testFoldConst("${const_sql_11_0}")
    qt_sql_11_1_non_strict "${const_sql_11_1}"
    testFoldConst("${const_sql_11_1}")
    qt_sql_11_2_non_strict "${const_sql_11_2}"
    testFoldConst("${const_sql_11_2}")
    qt_sql_11_3_non_strict "${const_sql_11_3}"
    testFoldConst("${const_sql_11_3}")
    qt_sql_11_4_non_strict "${const_sql_11_4}"
    testFoldConst("${const_sql_11_4}")
    qt_sql_11_5_non_strict "${const_sql_11_5}"
    testFoldConst("${const_sql_11_5}")
    qt_sql_11_6_non_strict "${const_sql_11_6}"
    testFoldConst("${const_sql_11_6}")
    qt_sql_11_7_non_strict "${const_sql_11_7}"
    testFoldConst("${const_sql_11_7}")
    qt_sql_11_8_non_strict "${const_sql_11_8}"
    testFoldConst("${const_sql_11_8}")
    qt_sql_11_9_non_strict "${const_sql_11_9}"
    testFoldConst("${const_sql_11_9}")
    qt_sql_11_10_non_strict "${const_sql_11_10}"
    testFoldConst("${const_sql_11_10}")
    qt_sql_11_11_non_strict "${const_sql_11_11}"
    testFoldConst("${const_sql_11_11}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_12_0 = """select "0.0000000000000000000", cast(cast("0.0000000000000000000" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_0_strict "${const_sql_12_0}"
    testFoldConst("${const_sql_12_0}")
    def const_sql_12_1 = """select "0.0000000000000000001", cast(cast("0.0000000000000000001" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_1_strict "${const_sql_12_1}"
    testFoldConst("${const_sql_12_1}")
    def const_sql_12_2 = """select "0.0000000000000000009", cast(cast("0.0000000000000000009" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_2_strict "${const_sql_12_2}"
    testFoldConst("${const_sql_12_2}")
    def const_sql_12_3 = """select "0.0999999999999999999", cast(cast("0.0999999999999999999" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_3_strict "${const_sql_12_3}"
    testFoldConst("${const_sql_12_3}")
    def const_sql_12_4 = """select "0.9000000000000000000", cast(cast("0.9000000000000000000" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_4_strict "${const_sql_12_4}"
    testFoldConst("${const_sql_12_4}")
    def const_sql_12_5 = """select "0.9000000000000000001", cast(cast("0.9000000000000000001" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_5_strict "${const_sql_12_5}"
    testFoldConst("${const_sql_12_5}")
    def const_sql_12_6 = """select "0.9999999999999999998", cast(cast("0.9999999999999999998" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_6_strict "${const_sql_12_6}"
    testFoldConst("${const_sql_12_6}")
    def const_sql_12_7 = """select "0.9999999999999999999", cast(cast("0.9999999999999999999" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_7_strict "${const_sql_12_7}"
    testFoldConst("${const_sql_12_7}")
    def const_sql_12_8 = """select "8.0000000000000000000", cast(cast("8.0000000000000000000" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_8_strict "${const_sql_12_8}"
    testFoldConst("${const_sql_12_8}")
    def const_sql_12_9 = """select "8.0000000000000000001", cast(cast("8.0000000000000000001" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_9_strict "${const_sql_12_9}"
    testFoldConst("${const_sql_12_9}")
    def const_sql_12_10 = """select "8.0000000000000000009", cast(cast("8.0000000000000000009" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_10_strict "${const_sql_12_10}"
    testFoldConst("${const_sql_12_10}")
    def const_sql_12_11 = """select "8.0999999999999999999", cast(cast("8.0999999999999999999" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_11_strict "${const_sql_12_11}"
    testFoldConst("${const_sql_12_11}")
    def const_sql_12_12 = """select "8.9000000000000000000", cast(cast("8.9000000000000000000" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_12_strict "${const_sql_12_12}"
    testFoldConst("${const_sql_12_12}")
    def const_sql_12_13 = """select "8.9000000000000000001", cast(cast("8.9000000000000000001" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_13_strict "${const_sql_12_13}"
    testFoldConst("${const_sql_12_13}")
    def const_sql_12_14 = """select "8.9999999999999999998", cast(cast("8.9999999999999999998" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_14_strict "${const_sql_12_14}"
    testFoldConst("${const_sql_12_14}")
    def const_sql_12_15 = """select "8.9999999999999999999", cast(cast("8.9999999999999999999" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_15_strict "${const_sql_12_15}"
    testFoldConst("${const_sql_12_15}")
    def const_sql_12_16 = """select "9.0000000000000000000", cast(cast("9.0000000000000000000" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_16_strict "${const_sql_12_16}"
    testFoldConst("${const_sql_12_16}")
    def const_sql_12_17 = """select "9.0000000000000000001", cast(cast("9.0000000000000000001" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_17_strict "${const_sql_12_17}"
    testFoldConst("${const_sql_12_17}")
    def const_sql_12_18 = """select "9.0000000000000000009", cast(cast("9.0000000000000000009" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_18_strict "${const_sql_12_18}"
    testFoldConst("${const_sql_12_18}")
    def const_sql_12_19 = """select "9.0999999999999999999", cast(cast("9.0999999999999999999" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_19_strict "${const_sql_12_19}"
    testFoldConst("${const_sql_12_19}")
    def const_sql_12_20 = """select "9.9000000000000000000", cast(cast("9.9000000000000000000" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_20_strict "${const_sql_12_20}"
    testFoldConst("${const_sql_12_20}")
    def const_sql_12_21 = """select "9.9000000000000000001", cast(cast("9.9000000000000000001" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_21_strict "${const_sql_12_21}"
    testFoldConst("${const_sql_12_21}")
    def const_sql_12_22 = """select "9.9999999999999999998", cast(cast("9.9999999999999999998" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_22_strict "${const_sql_12_22}"
    testFoldConst("${const_sql_12_22}")
    def const_sql_12_23 = """select "9.9999999999999999999", cast(cast("9.9999999999999999999" as decimalv3(38, 19)) as decimalv3(75, 74));"""
    qt_sql_12_23_strict "${const_sql_12_23}"
    testFoldConst("${const_sql_12_23}")

    sql "set enable_strict_cast=false;"
    qt_sql_12_0_non_strict "${const_sql_12_0}"
    testFoldConst("${const_sql_12_0}")
    qt_sql_12_1_non_strict "${const_sql_12_1}"
    testFoldConst("${const_sql_12_1}")
    qt_sql_12_2_non_strict "${const_sql_12_2}"
    testFoldConst("${const_sql_12_2}")
    qt_sql_12_3_non_strict "${const_sql_12_3}"
    testFoldConst("${const_sql_12_3}")
    qt_sql_12_4_non_strict "${const_sql_12_4}"
    testFoldConst("${const_sql_12_4}")
    qt_sql_12_5_non_strict "${const_sql_12_5}"
    testFoldConst("${const_sql_12_5}")
    qt_sql_12_6_non_strict "${const_sql_12_6}"
    testFoldConst("${const_sql_12_6}")
    qt_sql_12_7_non_strict "${const_sql_12_7}"
    testFoldConst("${const_sql_12_7}")
    qt_sql_12_8_non_strict "${const_sql_12_8}"
    testFoldConst("${const_sql_12_8}")
    qt_sql_12_9_non_strict "${const_sql_12_9}"
    testFoldConst("${const_sql_12_9}")
    qt_sql_12_10_non_strict "${const_sql_12_10}"
    testFoldConst("${const_sql_12_10}")
    qt_sql_12_11_non_strict "${const_sql_12_11}"
    testFoldConst("${const_sql_12_11}")
    qt_sql_12_12_non_strict "${const_sql_12_12}"
    testFoldConst("${const_sql_12_12}")
    qt_sql_12_13_non_strict "${const_sql_12_13}"
    testFoldConst("${const_sql_12_13}")
    qt_sql_12_14_non_strict "${const_sql_12_14}"
    testFoldConst("${const_sql_12_14}")
    qt_sql_12_15_non_strict "${const_sql_12_15}"
    testFoldConst("${const_sql_12_15}")
    qt_sql_12_16_non_strict "${const_sql_12_16}"
    testFoldConst("${const_sql_12_16}")
    qt_sql_12_17_non_strict "${const_sql_12_17}"
    testFoldConst("${const_sql_12_17}")
    qt_sql_12_18_non_strict "${const_sql_12_18}"
    testFoldConst("${const_sql_12_18}")
    qt_sql_12_19_non_strict "${const_sql_12_19}"
    testFoldConst("${const_sql_12_19}")
    qt_sql_12_20_non_strict "${const_sql_12_20}"
    testFoldConst("${const_sql_12_20}")
    qt_sql_12_21_non_strict "${const_sql_12_21}"
    testFoldConst("${const_sql_12_21}")
    qt_sql_12_22_non_strict "${const_sql_12_22}"
    testFoldConst("${const_sql_12_22}")
    qt_sql_12_23_non_strict "${const_sql_12_23}"
    testFoldConst("${const_sql_12_23}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_13_0 = """select "0.0000000000000000000000000000000000000", cast(cast("0.0000000000000000000000000000000000000" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_0_strict "${const_sql_13_0}"
    testFoldConst("${const_sql_13_0}")
    def const_sql_13_1 = """select "0.0000000000000000000000000000000000001", cast(cast("0.0000000000000000000000000000000000001" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_1_strict "${const_sql_13_1}"
    testFoldConst("${const_sql_13_1}")
    def const_sql_13_2 = """select "0.0000000000000000000000000000000000009", cast(cast("0.0000000000000000000000000000000000009" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_2_strict "${const_sql_13_2}"
    testFoldConst("${const_sql_13_2}")
    def const_sql_13_3 = """select "0.0999999999999999999999999999999999999", cast(cast("0.0999999999999999999999999999999999999" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_3_strict "${const_sql_13_3}"
    testFoldConst("${const_sql_13_3}")
    def const_sql_13_4 = """select "0.9000000000000000000000000000000000000", cast(cast("0.9000000000000000000000000000000000000" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_4_strict "${const_sql_13_4}"
    testFoldConst("${const_sql_13_4}")
    def const_sql_13_5 = """select "0.9000000000000000000000000000000000001", cast(cast("0.9000000000000000000000000000000000001" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_5_strict "${const_sql_13_5}"
    testFoldConst("${const_sql_13_5}")
    def const_sql_13_6 = """select "0.9999999999999999999999999999999999998", cast(cast("0.9999999999999999999999999999999999998" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_6_strict "${const_sql_13_6}"
    testFoldConst("${const_sql_13_6}")
    def const_sql_13_7 = """select "0.9999999999999999999999999999999999999", cast(cast("0.9999999999999999999999999999999999999" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_7_strict "${const_sql_13_7}"
    testFoldConst("${const_sql_13_7}")
    def const_sql_13_8 = """select "8.0000000000000000000000000000000000000", cast(cast("8.0000000000000000000000000000000000000" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_8_strict "${const_sql_13_8}"
    testFoldConst("${const_sql_13_8}")
    def const_sql_13_9 = """select "8.0000000000000000000000000000000000001", cast(cast("8.0000000000000000000000000000000000001" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_9_strict "${const_sql_13_9}"
    testFoldConst("${const_sql_13_9}")
    def const_sql_13_10 = """select "8.0000000000000000000000000000000000009", cast(cast("8.0000000000000000000000000000000000009" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_10_strict "${const_sql_13_10}"
    testFoldConst("${const_sql_13_10}")
    def const_sql_13_11 = """select "8.0999999999999999999999999999999999999", cast(cast("8.0999999999999999999999999999999999999" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_11_strict "${const_sql_13_11}"
    testFoldConst("${const_sql_13_11}")
    def const_sql_13_12 = """select "8.9000000000000000000000000000000000000", cast(cast("8.9000000000000000000000000000000000000" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_12_strict "${const_sql_13_12}"
    testFoldConst("${const_sql_13_12}")
    def const_sql_13_13 = """select "8.9000000000000000000000000000000000001", cast(cast("8.9000000000000000000000000000000000001" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_13_strict "${const_sql_13_13}"
    testFoldConst("${const_sql_13_13}")
    def const_sql_13_14 = """select "8.9999999999999999999999999999999999998", cast(cast("8.9999999999999999999999999999999999998" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_14_strict "${const_sql_13_14}"
    testFoldConst("${const_sql_13_14}")
    def const_sql_13_15 = """select "8.9999999999999999999999999999999999999", cast(cast("8.9999999999999999999999999999999999999" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_15_strict "${const_sql_13_15}"
    testFoldConst("${const_sql_13_15}")
    def const_sql_13_16 = """select "9.0000000000000000000000000000000000000", cast(cast("9.0000000000000000000000000000000000000" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_16_strict "${const_sql_13_16}"
    testFoldConst("${const_sql_13_16}")
    def const_sql_13_17 = """select "9.0000000000000000000000000000000000001", cast(cast("9.0000000000000000000000000000000000001" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_17_strict "${const_sql_13_17}"
    testFoldConst("${const_sql_13_17}")
    def const_sql_13_18 = """select "9.0000000000000000000000000000000000009", cast(cast("9.0000000000000000000000000000000000009" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_18_strict "${const_sql_13_18}"
    testFoldConst("${const_sql_13_18}")
    def const_sql_13_19 = """select "9.0999999999999999999999999999999999999", cast(cast("9.0999999999999999999999999999999999999" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_19_strict "${const_sql_13_19}"
    testFoldConst("${const_sql_13_19}")
    def const_sql_13_20 = """select "9.9000000000000000000000000000000000000", cast(cast("9.9000000000000000000000000000000000000" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_20_strict "${const_sql_13_20}"
    testFoldConst("${const_sql_13_20}")
    def const_sql_13_21 = """select "9.9000000000000000000000000000000000001", cast(cast("9.9000000000000000000000000000000000001" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_21_strict "${const_sql_13_21}"
    testFoldConst("${const_sql_13_21}")
    def const_sql_13_22 = """select "9.9999999999999999999999999999999999998", cast(cast("9.9999999999999999999999999999999999998" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_22_strict "${const_sql_13_22}"
    testFoldConst("${const_sql_13_22}")
    def const_sql_13_23 = """select "9.9999999999999999999999999999999999999", cast(cast("9.9999999999999999999999999999999999999" as decimalv3(38, 37)) as decimalv3(75, 74));"""
    qt_sql_13_23_strict "${const_sql_13_23}"
    testFoldConst("${const_sql_13_23}")

    sql "set enable_strict_cast=false;"
    qt_sql_13_0_non_strict "${const_sql_13_0}"
    testFoldConst("${const_sql_13_0}")
    qt_sql_13_1_non_strict "${const_sql_13_1}"
    testFoldConst("${const_sql_13_1}")
    qt_sql_13_2_non_strict "${const_sql_13_2}"
    testFoldConst("${const_sql_13_2}")
    qt_sql_13_3_non_strict "${const_sql_13_3}"
    testFoldConst("${const_sql_13_3}")
    qt_sql_13_4_non_strict "${const_sql_13_4}"
    testFoldConst("${const_sql_13_4}")
    qt_sql_13_5_non_strict "${const_sql_13_5}"
    testFoldConst("${const_sql_13_5}")
    qt_sql_13_6_non_strict "${const_sql_13_6}"
    testFoldConst("${const_sql_13_6}")
    qt_sql_13_7_non_strict "${const_sql_13_7}"
    testFoldConst("${const_sql_13_7}")
    qt_sql_13_8_non_strict "${const_sql_13_8}"
    testFoldConst("${const_sql_13_8}")
    qt_sql_13_9_non_strict "${const_sql_13_9}"
    testFoldConst("${const_sql_13_9}")
    qt_sql_13_10_non_strict "${const_sql_13_10}"
    testFoldConst("${const_sql_13_10}")
    qt_sql_13_11_non_strict "${const_sql_13_11}"
    testFoldConst("${const_sql_13_11}")
    qt_sql_13_12_non_strict "${const_sql_13_12}"
    testFoldConst("${const_sql_13_12}")
    qt_sql_13_13_non_strict "${const_sql_13_13}"
    testFoldConst("${const_sql_13_13}")
    qt_sql_13_14_non_strict "${const_sql_13_14}"
    testFoldConst("${const_sql_13_14}")
    qt_sql_13_15_non_strict "${const_sql_13_15}"
    testFoldConst("${const_sql_13_15}")
    qt_sql_13_16_non_strict "${const_sql_13_16}"
    testFoldConst("${const_sql_13_16}")
    qt_sql_13_17_non_strict "${const_sql_13_17}"
    testFoldConst("${const_sql_13_17}")
    qt_sql_13_18_non_strict "${const_sql_13_18}"
    testFoldConst("${const_sql_13_18}")
    qt_sql_13_19_non_strict "${const_sql_13_19}"
    testFoldConst("${const_sql_13_19}")
    qt_sql_13_20_non_strict "${const_sql_13_20}"
    testFoldConst("${const_sql_13_20}")
    qt_sql_13_21_non_strict "${const_sql_13_21}"
    testFoldConst("${const_sql_13_21}")
    qt_sql_13_22_non_strict "${const_sql_13_22}"
    testFoldConst("${const_sql_13_22}")
    qt_sql_13_23_non_strict "${const_sql_13_23}"
    testFoldConst("${const_sql_13_23}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_14_0 = """select "0.00000000000000000000000000000000000000", cast(cast("0.00000000000000000000000000000000000000" as decimalv3(38, 38)) as decimalv3(75, 74));"""
    qt_sql_14_0_strict "${const_sql_14_0}"
    testFoldConst("${const_sql_14_0}")
    def const_sql_14_1 = """select "0.00000000000000000000000000000000000001", cast(cast("0.00000000000000000000000000000000000001" as decimalv3(38, 38)) as decimalv3(75, 74));"""
    qt_sql_14_1_strict "${const_sql_14_1}"
    testFoldConst("${const_sql_14_1}")
    def const_sql_14_2 = """select "0.00000000000000000000000000000000000009", cast(cast("0.00000000000000000000000000000000000009" as decimalv3(38, 38)) as decimalv3(75, 74));"""
    qt_sql_14_2_strict "${const_sql_14_2}"
    testFoldConst("${const_sql_14_2}")
    def const_sql_14_3 = """select "0.09999999999999999999999999999999999999", cast(cast("0.09999999999999999999999999999999999999" as decimalv3(38, 38)) as decimalv3(75, 74));"""
    qt_sql_14_3_strict "${const_sql_14_3}"
    testFoldConst("${const_sql_14_3}")
    def const_sql_14_4 = """select "0.90000000000000000000000000000000000000", cast(cast("0.90000000000000000000000000000000000000" as decimalv3(38, 38)) as decimalv3(75, 74));"""
    qt_sql_14_4_strict "${const_sql_14_4}"
    testFoldConst("${const_sql_14_4}")
    def const_sql_14_5 = """select "0.90000000000000000000000000000000000001", cast(cast("0.90000000000000000000000000000000000001" as decimalv3(38, 38)) as decimalv3(75, 74));"""
    qt_sql_14_5_strict "${const_sql_14_5}"
    testFoldConst("${const_sql_14_5}")
    def const_sql_14_6 = """select "0.99999999999999999999999999999999999998", cast(cast("0.99999999999999999999999999999999999998" as decimalv3(38, 38)) as decimalv3(75, 74));"""
    qt_sql_14_6_strict "${const_sql_14_6}"
    testFoldConst("${const_sql_14_6}")
    def const_sql_14_7 = """select "0.99999999999999999999999999999999999999", cast(cast("0.99999999999999999999999999999999999999" as decimalv3(38, 38)) as decimalv3(75, 74));"""
    qt_sql_14_7_strict "${const_sql_14_7}"
    testFoldConst("${const_sql_14_7}")

    sql "set enable_strict_cast=false;"
    qt_sql_14_0_non_strict "${const_sql_14_0}"
    testFoldConst("${const_sql_14_0}")
    qt_sql_14_1_non_strict "${const_sql_14_1}"
    testFoldConst("${const_sql_14_1}")
    qt_sql_14_2_non_strict "${const_sql_14_2}"
    testFoldConst("${const_sql_14_2}")
    qt_sql_14_3_non_strict "${const_sql_14_3}"
    testFoldConst("${const_sql_14_3}")
    qt_sql_14_4_non_strict "${const_sql_14_4}"
    testFoldConst("${const_sql_14_4}")
    qt_sql_14_5_non_strict "${const_sql_14_5}"
    testFoldConst("${const_sql_14_5}")
    qt_sql_14_6_non_strict "${const_sql_14_6}"
    testFoldConst("${const_sql_14_6}")
    qt_sql_14_7_non_strict "${const_sql_14_7}"
    testFoldConst("${const_sql_14_7}")
}