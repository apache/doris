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


suite("test_cast_to_decimal128i_38_19_from_decimal256_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_decimal256 = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "0", cast(cast("0" as decimalv3(39, 0)) as decimalv3(38, 19));"""
    qt_sql_0_0_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    def const_sql_0_1 = """select "999999999999999999", cast(cast("999999999999999999" as decimalv3(39, 0)) as decimalv3(38, 19));"""
    qt_sql_0_1_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    def const_sql_0_2 = """select "9000000000000000000", cast(cast("9000000000000000000" as decimalv3(39, 0)) as decimalv3(38, 19));"""
    qt_sql_0_2_strict "${const_sql_0_2}"
    testFoldConst("${const_sql_0_2}")
    def const_sql_0_3 = """select "9000000000000000001", cast(cast("9000000000000000001" as decimalv3(39, 0)) as decimalv3(38, 19));"""
    qt_sql_0_3_strict "${const_sql_0_3}"
    testFoldConst("${const_sql_0_3}")
    def const_sql_0_4 = """select "9999999999999999998", cast(cast("9999999999999999998" as decimalv3(39, 0)) as decimalv3(38, 19));"""
    qt_sql_0_4_strict "${const_sql_0_4}"
    testFoldConst("${const_sql_0_4}")
    def const_sql_0_5 = """select "9999999999999999999", cast(cast("9999999999999999999" as decimalv3(39, 0)) as decimalv3(38, 19));"""
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
    def const_sql_1_0 = """select "0.0", cast(cast("0.0" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_0_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    def const_sql_1_1 = """select "0.1", cast(cast("0.1" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_1_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    def const_sql_1_2 = """select "0.8", cast(cast("0.8" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_2_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    def const_sql_1_3 = """select "0.9", cast(cast("0.9" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_3_strict "${const_sql_1_3}"
    testFoldConst("${const_sql_1_3}")
    def const_sql_1_4 = """select "999999999999999999.0", cast(cast("999999999999999999.0" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_4_strict "${const_sql_1_4}"
    testFoldConst("${const_sql_1_4}")
    def const_sql_1_5 = """select "999999999999999999.1", cast(cast("999999999999999999.1" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_5_strict "${const_sql_1_5}"
    testFoldConst("${const_sql_1_5}")
    def const_sql_1_6 = """select "999999999999999999.8", cast(cast("999999999999999999.8" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_6_strict "${const_sql_1_6}"
    testFoldConst("${const_sql_1_6}")
    def const_sql_1_7 = """select "999999999999999999.9", cast(cast("999999999999999999.9" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_7_strict "${const_sql_1_7}"
    testFoldConst("${const_sql_1_7}")
    def const_sql_1_8 = """select "9000000000000000000.0", cast(cast("9000000000000000000.0" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_8_strict "${const_sql_1_8}"
    testFoldConst("${const_sql_1_8}")
    def const_sql_1_9 = """select "9000000000000000000.1", cast(cast("9000000000000000000.1" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_9_strict "${const_sql_1_9}"
    testFoldConst("${const_sql_1_9}")
    def const_sql_1_10 = """select "9000000000000000000.8", cast(cast("9000000000000000000.8" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_10_strict "${const_sql_1_10}"
    testFoldConst("${const_sql_1_10}")
    def const_sql_1_11 = """select "9000000000000000000.9", cast(cast("9000000000000000000.9" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_11_strict "${const_sql_1_11}"
    testFoldConst("${const_sql_1_11}")
    def const_sql_1_12 = """select "9000000000000000001.0", cast(cast("9000000000000000001.0" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_12_strict "${const_sql_1_12}"
    testFoldConst("${const_sql_1_12}")
    def const_sql_1_13 = """select "9000000000000000001.1", cast(cast("9000000000000000001.1" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_13_strict "${const_sql_1_13}"
    testFoldConst("${const_sql_1_13}")
    def const_sql_1_14 = """select "9000000000000000001.8", cast(cast("9000000000000000001.8" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_14_strict "${const_sql_1_14}"
    testFoldConst("${const_sql_1_14}")
    def const_sql_1_15 = """select "9000000000000000001.9", cast(cast("9000000000000000001.9" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_15_strict "${const_sql_1_15}"
    testFoldConst("${const_sql_1_15}")
    def const_sql_1_16 = """select "9999999999999999998.0", cast(cast("9999999999999999998.0" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_16_strict "${const_sql_1_16}"
    testFoldConst("${const_sql_1_16}")
    def const_sql_1_17 = """select "9999999999999999998.1", cast(cast("9999999999999999998.1" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_17_strict "${const_sql_1_17}"
    testFoldConst("${const_sql_1_17}")
    def const_sql_1_18 = """select "9999999999999999998.8", cast(cast("9999999999999999998.8" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_18_strict "${const_sql_1_18}"
    testFoldConst("${const_sql_1_18}")
    def const_sql_1_19 = """select "9999999999999999998.9", cast(cast("9999999999999999998.9" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_19_strict "${const_sql_1_19}"
    testFoldConst("${const_sql_1_19}")
    def const_sql_1_20 = """select "9999999999999999999.0", cast(cast("9999999999999999999.0" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_20_strict "${const_sql_1_20}"
    testFoldConst("${const_sql_1_20}")
    def const_sql_1_21 = """select "9999999999999999999.1", cast(cast("9999999999999999999.1" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_21_strict "${const_sql_1_21}"
    testFoldConst("${const_sql_1_21}")
    def const_sql_1_22 = """select "9999999999999999999.8", cast(cast("9999999999999999999.8" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_22_strict "${const_sql_1_22}"
    testFoldConst("${const_sql_1_22}")
    def const_sql_1_23 = """select "9999999999999999999.9", cast(cast("9999999999999999999.9" as decimalv3(39, 1)) as decimalv3(38, 19));"""
    qt_sql_1_23_strict "${const_sql_1_23}"
    testFoldConst("${const_sql_1_23}")

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
    qt_sql_1_12_non_strict "${const_sql_1_12}"
    testFoldConst("${const_sql_1_12}")
    qt_sql_1_13_non_strict "${const_sql_1_13}"
    testFoldConst("${const_sql_1_13}")
    qt_sql_1_14_non_strict "${const_sql_1_14}"
    testFoldConst("${const_sql_1_14}")
    qt_sql_1_15_non_strict "${const_sql_1_15}"
    testFoldConst("${const_sql_1_15}")
    qt_sql_1_16_non_strict "${const_sql_1_16}"
    testFoldConst("${const_sql_1_16}")
    qt_sql_1_17_non_strict "${const_sql_1_17}"
    testFoldConst("${const_sql_1_17}")
    qt_sql_1_18_non_strict "${const_sql_1_18}"
    testFoldConst("${const_sql_1_18}")
    qt_sql_1_19_non_strict "${const_sql_1_19}"
    testFoldConst("${const_sql_1_19}")
    qt_sql_1_20_non_strict "${const_sql_1_20}"
    testFoldConst("${const_sql_1_20}")
    qt_sql_1_21_non_strict "${const_sql_1_21}"
    testFoldConst("${const_sql_1_21}")
    qt_sql_1_22_non_strict "${const_sql_1_22}"
    testFoldConst("${const_sql_1_22}")
    qt_sql_1_23_non_strict "${const_sql_1_23}"
    testFoldConst("${const_sql_1_23}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_2_0 = """select "0.00000000000000000000000000000000000000", cast(cast("0.00000000000000000000000000000000000000" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_0_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    def const_sql_2_1 = """select "0.00000000000000000000000000000000000001", cast(cast("0.00000000000000000000000000000000000001" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_1_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    def const_sql_2_2 = """select "0.00000000000000000000000000000000000009", cast(cast("0.00000000000000000000000000000000000009" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_2_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    def const_sql_2_3 = """select "0.09999999999999999999999999999999999999", cast(cast("0.09999999999999999999999999999999999999" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_3_strict "${const_sql_2_3}"
    testFoldConst("${const_sql_2_3}")
    def const_sql_2_4 = """select "0.90000000000000000000000000000000000000", cast(cast("0.90000000000000000000000000000000000000" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_4_strict "${const_sql_2_4}"
    testFoldConst("${const_sql_2_4}")
    def const_sql_2_5 = """select "0.90000000000000000000000000000000000001", cast(cast("0.90000000000000000000000000000000000001" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_5_strict "${const_sql_2_5}"
    testFoldConst("${const_sql_2_5}")
    def const_sql_2_6 = """select "0.99999999999999999999999999999999999998", cast(cast("0.99999999999999999999999999999999999998" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_6_strict "${const_sql_2_6}"
    testFoldConst("${const_sql_2_6}")
    def const_sql_2_7 = """select "0.99999999999999999999999999999999999999", cast(cast("0.99999999999999999999999999999999999999" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_7_strict "${const_sql_2_7}"
    testFoldConst("${const_sql_2_7}")
    def const_sql_2_8 = """select "8.00000000000000000000000000000000000000", cast(cast("8.00000000000000000000000000000000000000" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_8_strict "${const_sql_2_8}"
    testFoldConst("${const_sql_2_8}")
    def const_sql_2_9 = """select "8.00000000000000000000000000000000000001", cast(cast("8.00000000000000000000000000000000000001" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_9_strict "${const_sql_2_9}"
    testFoldConst("${const_sql_2_9}")
    def const_sql_2_10 = """select "8.00000000000000000000000000000000000009", cast(cast("8.00000000000000000000000000000000000009" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_10_strict "${const_sql_2_10}"
    testFoldConst("${const_sql_2_10}")
    def const_sql_2_11 = """select "8.09999999999999999999999999999999999999", cast(cast("8.09999999999999999999999999999999999999" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_11_strict "${const_sql_2_11}"
    testFoldConst("${const_sql_2_11}")
    def const_sql_2_12 = """select "8.90000000000000000000000000000000000000", cast(cast("8.90000000000000000000000000000000000000" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_12_strict "${const_sql_2_12}"
    testFoldConst("${const_sql_2_12}")
    def const_sql_2_13 = """select "8.90000000000000000000000000000000000001", cast(cast("8.90000000000000000000000000000000000001" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_13_strict "${const_sql_2_13}"
    testFoldConst("${const_sql_2_13}")
    def const_sql_2_14 = """select "8.99999999999999999999999999999999999998", cast(cast("8.99999999999999999999999999999999999998" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_14_strict "${const_sql_2_14}"
    testFoldConst("${const_sql_2_14}")
    def const_sql_2_15 = """select "8.99999999999999999999999999999999999999", cast(cast("8.99999999999999999999999999999999999999" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_15_strict "${const_sql_2_15}"
    testFoldConst("${const_sql_2_15}")
    def const_sql_2_16 = """select "9.00000000000000000000000000000000000000", cast(cast("9.00000000000000000000000000000000000000" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_16_strict "${const_sql_2_16}"
    testFoldConst("${const_sql_2_16}")
    def const_sql_2_17 = """select "9.00000000000000000000000000000000000001", cast(cast("9.00000000000000000000000000000000000001" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_17_strict "${const_sql_2_17}"
    testFoldConst("${const_sql_2_17}")
    def const_sql_2_18 = """select "9.00000000000000000000000000000000000009", cast(cast("9.00000000000000000000000000000000000009" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_18_strict "${const_sql_2_18}"
    testFoldConst("${const_sql_2_18}")
    def const_sql_2_19 = """select "9.09999999999999999999999999999999999999", cast(cast("9.09999999999999999999999999999999999999" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_19_strict "${const_sql_2_19}"
    testFoldConst("${const_sql_2_19}")
    def const_sql_2_20 = """select "9.90000000000000000000000000000000000000", cast(cast("9.90000000000000000000000000000000000000" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_20_strict "${const_sql_2_20}"
    testFoldConst("${const_sql_2_20}")
    def const_sql_2_21 = """select "9.90000000000000000000000000000000000001", cast(cast("9.90000000000000000000000000000000000001" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_21_strict "${const_sql_2_21}"
    testFoldConst("${const_sql_2_21}")
    def const_sql_2_22 = """select "9.99999999999999999999999999999999999998", cast(cast("9.99999999999999999999999999999999999998" as decimalv3(39, 38)) as decimalv3(38, 19));"""
    qt_sql_2_22_strict "${const_sql_2_22}"
    testFoldConst("${const_sql_2_22}")
    def const_sql_2_23 = """select "9.99999999999999999999999999999999999999", cast(cast("9.99999999999999999999999999999999999999" as decimalv3(39, 38)) as decimalv3(38, 19));"""
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
    def const_sql_3_0 = """select "0.000000000000000000000000000000000000000", cast(cast("0.000000000000000000000000000000000000000" as decimalv3(39, 39)) as decimalv3(38, 19));"""
    qt_sql_3_0_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    def const_sql_3_1 = """select "0.000000000000000000000000000000000000001", cast(cast("0.000000000000000000000000000000000000001" as decimalv3(39, 39)) as decimalv3(38, 19));"""
    qt_sql_3_1_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    def const_sql_3_2 = """select "0.000000000000000000000000000000000000009", cast(cast("0.000000000000000000000000000000000000009" as decimalv3(39, 39)) as decimalv3(38, 19));"""
    qt_sql_3_2_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    def const_sql_3_3 = """select "0.099999999999999999999999999999999999999", cast(cast("0.099999999999999999999999999999999999999" as decimalv3(39, 39)) as decimalv3(38, 19));"""
    qt_sql_3_3_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    def const_sql_3_4 = """select "0.900000000000000000000000000000000000000", cast(cast("0.900000000000000000000000000000000000000" as decimalv3(39, 39)) as decimalv3(38, 19));"""
    qt_sql_3_4_strict "${const_sql_3_4}"
    testFoldConst("${const_sql_3_4}")
    def const_sql_3_5 = """select "0.900000000000000000000000000000000000001", cast(cast("0.900000000000000000000000000000000000001" as decimalv3(39, 39)) as decimalv3(38, 19));"""
    qt_sql_3_5_strict "${const_sql_3_5}"
    testFoldConst("${const_sql_3_5}")
    def const_sql_3_6 = """select "0.999999999999999999999999999999999999998", cast(cast("0.999999999999999999999999999999999999998" as decimalv3(39, 39)) as decimalv3(38, 19));"""
    qt_sql_3_6_strict "${const_sql_3_6}"
    testFoldConst("${const_sql_3_6}")
    def const_sql_3_7 = """select "0.999999999999999999999999999999999999999", cast(cast("0.999999999999999999999999999999999999999" as decimalv3(39, 39)) as decimalv3(38, 19));"""
    qt_sql_3_7_strict "${const_sql_3_7}"
    testFoldConst("${const_sql_3_7}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_4_0 = """select "0", cast(cast("0" as decimalv3(76, 0)) as decimalv3(38, 19));"""
    qt_sql_4_0_strict "${const_sql_4_0}"
    testFoldConst("${const_sql_4_0}")
    def const_sql_4_1 = """select "999999999999999999", cast(cast("999999999999999999" as decimalv3(76, 0)) as decimalv3(38, 19));"""
    qt_sql_4_1_strict "${const_sql_4_1}"
    testFoldConst("${const_sql_4_1}")
    def const_sql_4_2 = """select "9000000000000000000", cast(cast("9000000000000000000" as decimalv3(76, 0)) as decimalv3(38, 19));"""
    qt_sql_4_2_strict "${const_sql_4_2}"
    testFoldConst("${const_sql_4_2}")
    def const_sql_4_3 = """select "9000000000000000001", cast(cast("9000000000000000001" as decimalv3(76, 0)) as decimalv3(38, 19));"""
    qt_sql_4_3_strict "${const_sql_4_3}"
    testFoldConst("${const_sql_4_3}")
    def const_sql_4_4 = """select "9999999999999999998", cast(cast("9999999999999999998" as decimalv3(76, 0)) as decimalv3(38, 19));"""
    qt_sql_4_4_strict "${const_sql_4_4}"
    testFoldConst("${const_sql_4_4}")
    def const_sql_4_5 = """select "9999999999999999999", cast(cast("9999999999999999999" as decimalv3(76, 0)) as decimalv3(38, 19));"""
    qt_sql_4_5_strict "${const_sql_4_5}"
    testFoldConst("${const_sql_4_5}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_5_0 = """select "0.0", cast(cast("0.0" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_0_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")
    def const_sql_5_1 = """select "0.1", cast(cast("0.1" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_1_strict "${const_sql_5_1}"
    testFoldConst("${const_sql_5_1}")
    def const_sql_5_2 = """select "0.8", cast(cast("0.8" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_2_strict "${const_sql_5_2}"
    testFoldConst("${const_sql_5_2}")
    def const_sql_5_3 = """select "0.9", cast(cast("0.9" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_3_strict "${const_sql_5_3}"
    testFoldConst("${const_sql_5_3}")
    def const_sql_5_4 = """select "999999999999999999.0", cast(cast("999999999999999999.0" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_4_strict "${const_sql_5_4}"
    testFoldConst("${const_sql_5_4}")
    def const_sql_5_5 = """select "999999999999999999.1", cast(cast("999999999999999999.1" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_5_strict "${const_sql_5_5}"
    testFoldConst("${const_sql_5_5}")
    def const_sql_5_6 = """select "999999999999999999.8", cast(cast("999999999999999999.8" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_6_strict "${const_sql_5_6}"
    testFoldConst("${const_sql_5_6}")
    def const_sql_5_7 = """select "999999999999999999.9", cast(cast("999999999999999999.9" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_7_strict "${const_sql_5_7}"
    testFoldConst("${const_sql_5_7}")
    def const_sql_5_8 = """select "9000000000000000000.0", cast(cast("9000000000000000000.0" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_8_strict "${const_sql_5_8}"
    testFoldConst("${const_sql_5_8}")
    def const_sql_5_9 = """select "9000000000000000000.1", cast(cast("9000000000000000000.1" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_9_strict "${const_sql_5_9}"
    testFoldConst("${const_sql_5_9}")
    def const_sql_5_10 = """select "9000000000000000000.8", cast(cast("9000000000000000000.8" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_10_strict "${const_sql_5_10}"
    testFoldConst("${const_sql_5_10}")
    def const_sql_5_11 = """select "9000000000000000000.9", cast(cast("9000000000000000000.9" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_11_strict "${const_sql_5_11}"
    testFoldConst("${const_sql_5_11}")
    def const_sql_5_12 = """select "9000000000000000001.0", cast(cast("9000000000000000001.0" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_12_strict "${const_sql_5_12}"
    testFoldConst("${const_sql_5_12}")
    def const_sql_5_13 = """select "9000000000000000001.1", cast(cast("9000000000000000001.1" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_13_strict "${const_sql_5_13}"
    testFoldConst("${const_sql_5_13}")
    def const_sql_5_14 = """select "9000000000000000001.8", cast(cast("9000000000000000001.8" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_14_strict "${const_sql_5_14}"
    testFoldConst("${const_sql_5_14}")
    def const_sql_5_15 = """select "9000000000000000001.9", cast(cast("9000000000000000001.9" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_15_strict "${const_sql_5_15}"
    testFoldConst("${const_sql_5_15}")
    def const_sql_5_16 = """select "9999999999999999998.0", cast(cast("9999999999999999998.0" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_16_strict "${const_sql_5_16}"
    testFoldConst("${const_sql_5_16}")
    def const_sql_5_17 = """select "9999999999999999998.1", cast(cast("9999999999999999998.1" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_17_strict "${const_sql_5_17}"
    testFoldConst("${const_sql_5_17}")
    def const_sql_5_18 = """select "9999999999999999998.8", cast(cast("9999999999999999998.8" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_18_strict "${const_sql_5_18}"
    testFoldConst("${const_sql_5_18}")
    def const_sql_5_19 = """select "9999999999999999998.9", cast(cast("9999999999999999998.9" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_19_strict "${const_sql_5_19}"
    testFoldConst("${const_sql_5_19}")
    def const_sql_5_20 = """select "9999999999999999999.0", cast(cast("9999999999999999999.0" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_20_strict "${const_sql_5_20}"
    testFoldConst("${const_sql_5_20}")
    def const_sql_5_21 = """select "9999999999999999999.1", cast(cast("9999999999999999999.1" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_21_strict "${const_sql_5_21}"
    testFoldConst("${const_sql_5_21}")
    def const_sql_5_22 = """select "9999999999999999999.8", cast(cast("9999999999999999999.8" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_22_strict "${const_sql_5_22}"
    testFoldConst("${const_sql_5_22}")
    def const_sql_5_23 = """select "9999999999999999999.9", cast(cast("9999999999999999999.9" as decimalv3(76, 1)) as decimalv3(38, 19));"""
    qt_sql_5_23_strict "${const_sql_5_23}"
    testFoldConst("${const_sql_5_23}")

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
    qt_sql_5_6_non_strict "${const_sql_5_6}"
    testFoldConst("${const_sql_5_6}")
    qt_sql_5_7_non_strict "${const_sql_5_7}"
    testFoldConst("${const_sql_5_7}")
    qt_sql_5_8_non_strict "${const_sql_5_8}"
    testFoldConst("${const_sql_5_8}")
    qt_sql_5_9_non_strict "${const_sql_5_9}"
    testFoldConst("${const_sql_5_9}")
    qt_sql_5_10_non_strict "${const_sql_5_10}"
    testFoldConst("${const_sql_5_10}")
    qt_sql_5_11_non_strict "${const_sql_5_11}"
    testFoldConst("${const_sql_5_11}")
    qt_sql_5_12_non_strict "${const_sql_5_12}"
    testFoldConst("${const_sql_5_12}")
    qt_sql_5_13_non_strict "${const_sql_5_13}"
    testFoldConst("${const_sql_5_13}")
    qt_sql_5_14_non_strict "${const_sql_5_14}"
    testFoldConst("${const_sql_5_14}")
    qt_sql_5_15_non_strict "${const_sql_5_15}"
    testFoldConst("${const_sql_5_15}")
    qt_sql_5_16_non_strict "${const_sql_5_16}"
    testFoldConst("${const_sql_5_16}")
    qt_sql_5_17_non_strict "${const_sql_5_17}"
    testFoldConst("${const_sql_5_17}")
    qt_sql_5_18_non_strict "${const_sql_5_18}"
    testFoldConst("${const_sql_5_18}")
    qt_sql_5_19_non_strict "${const_sql_5_19}"
    testFoldConst("${const_sql_5_19}")
    qt_sql_5_20_non_strict "${const_sql_5_20}"
    testFoldConst("${const_sql_5_20}")
    qt_sql_5_21_non_strict "${const_sql_5_21}"
    testFoldConst("${const_sql_5_21}")
    qt_sql_5_22_non_strict "${const_sql_5_22}"
    testFoldConst("${const_sql_5_22}")
    qt_sql_5_23_non_strict "${const_sql_5_23}"
    testFoldConst("${const_sql_5_23}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_6_0 = """select "0.000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("0.000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_0_strict "${const_sql_6_0}"
    testFoldConst("${const_sql_6_0}")
    def const_sql_6_1 = """select "0.000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("0.000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_1_strict "${const_sql_6_1}"
    testFoldConst("${const_sql_6_1}")
    def const_sql_6_2 = """select "0.000000000000000000000000000000000000000000000000000000000000000000000000009", cast(cast("0.000000000000000000000000000000000000000000000000000000000000000000000000009" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_2_strict "${const_sql_6_2}"
    testFoldConst("${const_sql_6_2}")
    def const_sql_6_3 = """select "0.099999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("0.099999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_3_strict "${const_sql_6_3}"
    testFoldConst("${const_sql_6_3}")
    def const_sql_6_4 = """select "0.900000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("0.900000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_4_strict "${const_sql_6_4}"
    testFoldConst("${const_sql_6_4}")
    def const_sql_6_5 = """select "0.900000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("0.900000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_5_strict "${const_sql_6_5}"
    testFoldConst("${const_sql_6_5}")
    def const_sql_6_6 = """select "0.999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("0.999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_6_strict "${const_sql_6_6}"
    testFoldConst("${const_sql_6_6}")
    def const_sql_6_7 = """select "0.999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("0.999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_7_strict "${const_sql_6_7}"
    testFoldConst("${const_sql_6_7}")
    def const_sql_6_8 = """select "8.000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("8.000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_8_strict "${const_sql_6_8}"
    testFoldConst("${const_sql_6_8}")
    def const_sql_6_9 = """select "8.000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("8.000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_9_strict "${const_sql_6_9}"
    testFoldConst("${const_sql_6_9}")
    def const_sql_6_10 = """select "8.000000000000000000000000000000000000000000000000000000000000000000000000009", cast(cast("8.000000000000000000000000000000000000000000000000000000000000000000000000009" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_10_strict "${const_sql_6_10}"
    testFoldConst("${const_sql_6_10}")
    def const_sql_6_11 = """select "8.099999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("8.099999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_11_strict "${const_sql_6_11}"
    testFoldConst("${const_sql_6_11}")
    def const_sql_6_12 = """select "8.900000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("8.900000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_12_strict "${const_sql_6_12}"
    testFoldConst("${const_sql_6_12}")
    def const_sql_6_13 = """select "8.900000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("8.900000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_13_strict "${const_sql_6_13}"
    testFoldConst("${const_sql_6_13}")
    def const_sql_6_14 = """select "8.999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("8.999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_14_strict "${const_sql_6_14}"
    testFoldConst("${const_sql_6_14}")
    def const_sql_6_15 = """select "8.999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("8.999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_15_strict "${const_sql_6_15}"
    testFoldConst("${const_sql_6_15}")
    def const_sql_6_16 = """select "9.000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("9.000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_16_strict "${const_sql_6_16}"
    testFoldConst("${const_sql_6_16}")
    def const_sql_6_17 = """select "9.000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("9.000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_17_strict "${const_sql_6_17}"
    testFoldConst("${const_sql_6_17}")
    def const_sql_6_18 = """select "9.000000000000000000000000000000000000000000000000000000000000000000000000009", cast(cast("9.000000000000000000000000000000000000000000000000000000000000000000000000009" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_18_strict "${const_sql_6_18}"
    testFoldConst("${const_sql_6_18}")
    def const_sql_6_19 = """select "9.099999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("9.099999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_19_strict "${const_sql_6_19}"
    testFoldConst("${const_sql_6_19}")
    def const_sql_6_20 = """select "9.900000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("9.900000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_20_strict "${const_sql_6_20}"
    testFoldConst("${const_sql_6_20}")
    def const_sql_6_21 = """select "9.900000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("9.900000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_21_strict "${const_sql_6_21}"
    testFoldConst("${const_sql_6_21}")
    def const_sql_6_22 = """select "9.999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("9.999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_22_strict "${const_sql_6_22}"
    testFoldConst("${const_sql_6_22}")
    def const_sql_6_23 = """select "9.999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("9.999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as decimalv3(38, 19));"""
    qt_sql_6_23_strict "${const_sql_6_23}"
    testFoldConst("${const_sql_6_23}")

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
    qt_sql_6_18_non_strict "${const_sql_6_18}"
    testFoldConst("${const_sql_6_18}")
    qt_sql_6_19_non_strict "${const_sql_6_19}"
    testFoldConst("${const_sql_6_19}")
    qt_sql_6_20_non_strict "${const_sql_6_20}"
    testFoldConst("${const_sql_6_20}")
    qt_sql_6_21_non_strict "${const_sql_6_21}"
    testFoldConst("${const_sql_6_21}")
    qt_sql_6_22_non_strict "${const_sql_6_22}"
    testFoldConst("${const_sql_6_22}")
    qt_sql_6_23_non_strict "${const_sql_6_23}"
    testFoldConst("${const_sql_6_23}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_7_0 = """select "0.0000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 76)) as decimalv3(38, 19));"""
    qt_sql_7_0_strict "${const_sql_7_0}"
    testFoldConst("${const_sql_7_0}")
    def const_sql_7_1 = """select "0.0000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 76)) as decimalv3(38, 19));"""
    qt_sql_7_1_strict "${const_sql_7_1}"
    testFoldConst("${const_sql_7_1}")
    def const_sql_7_2 = """select "0.0000000000000000000000000000000000000000000000000000000000000000000000000009", cast(cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000009" as decimalv3(76, 76)) as decimalv3(38, 19));"""
    qt_sql_7_2_strict "${const_sql_7_2}"
    testFoldConst("${const_sql_7_2}")
    def const_sql_7_3 = """select "0.0999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("0.0999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 76)) as decimalv3(38, 19));"""
    qt_sql_7_3_strict "${const_sql_7_3}"
    testFoldConst("${const_sql_7_3}")
    def const_sql_7_4 = """select "0.9000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("0.9000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 76)) as decimalv3(38, 19));"""
    qt_sql_7_4_strict "${const_sql_7_4}"
    testFoldConst("${const_sql_7_4}")
    def const_sql_7_5 = """select "0.9000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("0.9000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 76)) as decimalv3(38, 19));"""
    qt_sql_7_5_strict "${const_sql_7_5}"
    testFoldConst("${const_sql_7_5}")
    def const_sql_7_6 = """select "0.9999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("0.9999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 76)) as decimalv3(38, 19));"""
    qt_sql_7_6_strict "${const_sql_7_6}"
    testFoldConst("${const_sql_7_6}")
    def const_sql_7_7 = """select "0.9999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("0.9999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 76)) as decimalv3(38, 19));"""
    qt_sql_7_7_strict "${const_sql_7_7}"
    testFoldConst("${const_sql_7_7}")

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
}