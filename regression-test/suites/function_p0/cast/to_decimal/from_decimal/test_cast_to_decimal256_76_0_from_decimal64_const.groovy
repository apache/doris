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


suite("test_cast_to_decimal256_76_0_from_decimal64_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_decimal256 = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "0", cast(cast("0" as decimalv3(10, 0)) as decimalv3(76, 0));"""
    qt_sql_0_0_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    def const_sql_0_1 = """select "999999999", cast(cast("999999999" as decimalv3(10, 0)) as decimalv3(76, 0));"""
    qt_sql_0_1_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    def const_sql_0_2 = """select "9000000000", cast(cast("9000000000" as decimalv3(10, 0)) as decimalv3(76, 0));"""
    qt_sql_0_2_strict "${const_sql_0_2}"
    testFoldConst("${const_sql_0_2}")
    def const_sql_0_3 = """select "9000000001", cast(cast("9000000001" as decimalv3(10, 0)) as decimalv3(76, 0));"""
    qt_sql_0_3_strict "${const_sql_0_3}"
    testFoldConst("${const_sql_0_3}")
    def const_sql_0_4 = """select "9999999998", cast(cast("9999999998" as decimalv3(10, 0)) as decimalv3(76, 0));"""
    qt_sql_0_4_strict "${const_sql_0_4}"
    testFoldConst("${const_sql_0_4}")
    def const_sql_0_5 = """select "9999999999", cast(cast("9999999999" as decimalv3(10, 0)) as decimalv3(76, 0));"""
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
    def const_sql_1_0 = """select "0.0", cast(cast("0.0" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_0_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    def const_sql_1_1 = """select "0.1", cast(cast("0.1" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_1_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    def const_sql_1_2 = """select "0.8", cast(cast("0.8" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_2_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    def const_sql_1_3 = """select "0.9", cast(cast("0.9" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_3_strict "${const_sql_1_3}"
    testFoldConst("${const_sql_1_3}")
    def const_sql_1_4 = """select "99999999.0", cast(cast("99999999.0" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_4_strict "${const_sql_1_4}"
    testFoldConst("${const_sql_1_4}")
    def const_sql_1_5 = """select "99999999.1", cast(cast("99999999.1" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_5_strict "${const_sql_1_5}"
    testFoldConst("${const_sql_1_5}")
    def const_sql_1_6 = """select "99999999.8", cast(cast("99999999.8" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_6_strict "${const_sql_1_6}"
    testFoldConst("${const_sql_1_6}")
    def const_sql_1_7 = """select "99999999.9", cast(cast("99999999.9" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_7_strict "${const_sql_1_7}"
    testFoldConst("${const_sql_1_7}")
    def const_sql_1_8 = """select "900000000.0", cast(cast("900000000.0" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_8_strict "${const_sql_1_8}"
    testFoldConst("${const_sql_1_8}")
    def const_sql_1_9 = """select "900000000.1", cast(cast("900000000.1" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_9_strict "${const_sql_1_9}"
    testFoldConst("${const_sql_1_9}")
    def const_sql_1_10 = """select "900000000.8", cast(cast("900000000.8" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_10_strict "${const_sql_1_10}"
    testFoldConst("${const_sql_1_10}")
    def const_sql_1_11 = """select "900000000.9", cast(cast("900000000.9" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_11_strict "${const_sql_1_11}"
    testFoldConst("${const_sql_1_11}")
    def const_sql_1_12 = """select "900000001.0", cast(cast("900000001.0" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_12_strict "${const_sql_1_12}"
    testFoldConst("${const_sql_1_12}")
    def const_sql_1_13 = """select "900000001.1", cast(cast("900000001.1" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_13_strict "${const_sql_1_13}"
    testFoldConst("${const_sql_1_13}")
    def const_sql_1_14 = """select "900000001.8", cast(cast("900000001.8" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_14_strict "${const_sql_1_14}"
    testFoldConst("${const_sql_1_14}")
    def const_sql_1_15 = """select "900000001.9", cast(cast("900000001.9" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_15_strict "${const_sql_1_15}"
    testFoldConst("${const_sql_1_15}")
    def const_sql_1_16 = """select "999999998.0", cast(cast("999999998.0" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_16_strict "${const_sql_1_16}"
    testFoldConst("${const_sql_1_16}")
    def const_sql_1_17 = """select "999999998.1", cast(cast("999999998.1" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_17_strict "${const_sql_1_17}"
    testFoldConst("${const_sql_1_17}")
    def const_sql_1_18 = """select "999999998.8", cast(cast("999999998.8" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_18_strict "${const_sql_1_18}"
    testFoldConst("${const_sql_1_18}")
    def const_sql_1_19 = """select "999999998.9", cast(cast("999999998.9" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_19_strict "${const_sql_1_19}"
    testFoldConst("${const_sql_1_19}")
    def const_sql_1_20 = """select "999999999.0", cast(cast("999999999.0" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_20_strict "${const_sql_1_20}"
    testFoldConst("${const_sql_1_20}")
    def const_sql_1_21 = """select "999999999.1", cast(cast("999999999.1" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_21_strict "${const_sql_1_21}"
    testFoldConst("${const_sql_1_21}")
    def const_sql_1_22 = """select "999999999.8", cast(cast("999999999.8" as decimalv3(10, 1)) as decimalv3(76, 0));"""
    qt_sql_1_22_strict "${const_sql_1_22}"
    testFoldConst("${const_sql_1_22}")
    def const_sql_1_23 = """select "999999999.9", cast(cast("999999999.9" as decimalv3(10, 1)) as decimalv3(76, 0));"""
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
    def const_sql_2_0 = """select "0.00000", cast(cast("0.00000" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_0_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    def const_sql_2_1 = """select "0.00001", cast(cast("0.00001" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_1_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    def const_sql_2_2 = """select "0.00009", cast(cast("0.00009" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_2_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    def const_sql_2_3 = """select "0.09999", cast(cast("0.09999" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_3_strict "${const_sql_2_3}"
    testFoldConst("${const_sql_2_3}")
    def const_sql_2_4 = """select "0.90000", cast(cast("0.90000" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_4_strict "${const_sql_2_4}"
    testFoldConst("${const_sql_2_4}")
    def const_sql_2_5 = """select "0.90001", cast(cast("0.90001" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_5_strict "${const_sql_2_5}"
    testFoldConst("${const_sql_2_5}")
    def const_sql_2_6 = """select "0.99998", cast(cast("0.99998" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_6_strict "${const_sql_2_6}"
    testFoldConst("${const_sql_2_6}")
    def const_sql_2_7 = """select "0.99999", cast(cast("0.99999" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_7_strict "${const_sql_2_7}"
    testFoldConst("${const_sql_2_7}")
    def const_sql_2_8 = """select "9999.00000", cast(cast("9999.00000" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_8_strict "${const_sql_2_8}"
    testFoldConst("${const_sql_2_8}")
    def const_sql_2_9 = """select "9999.00001", cast(cast("9999.00001" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_9_strict "${const_sql_2_9}"
    testFoldConst("${const_sql_2_9}")
    def const_sql_2_10 = """select "9999.00009", cast(cast("9999.00009" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_10_strict "${const_sql_2_10}"
    testFoldConst("${const_sql_2_10}")
    def const_sql_2_11 = """select "9999.09999", cast(cast("9999.09999" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_11_strict "${const_sql_2_11}"
    testFoldConst("${const_sql_2_11}")
    def const_sql_2_12 = """select "9999.90000", cast(cast("9999.90000" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_12_strict "${const_sql_2_12}"
    testFoldConst("${const_sql_2_12}")
    def const_sql_2_13 = """select "9999.90001", cast(cast("9999.90001" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_13_strict "${const_sql_2_13}"
    testFoldConst("${const_sql_2_13}")
    def const_sql_2_14 = """select "9999.99998", cast(cast("9999.99998" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_14_strict "${const_sql_2_14}"
    testFoldConst("${const_sql_2_14}")
    def const_sql_2_15 = """select "9999.99999", cast(cast("9999.99999" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_15_strict "${const_sql_2_15}"
    testFoldConst("${const_sql_2_15}")
    def const_sql_2_16 = """select "90000.00000", cast(cast("90000.00000" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_16_strict "${const_sql_2_16}"
    testFoldConst("${const_sql_2_16}")
    def const_sql_2_17 = """select "90000.00001", cast(cast("90000.00001" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_17_strict "${const_sql_2_17}"
    testFoldConst("${const_sql_2_17}")
    def const_sql_2_18 = """select "90000.00009", cast(cast("90000.00009" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_18_strict "${const_sql_2_18}"
    testFoldConst("${const_sql_2_18}")
    def const_sql_2_19 = """select "90000.09999", cast(cast("90000.09999" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_19_strict "${const_sql_2_19}"
    testFoldConst("${const_sql_2_19}")
    def const_sql_2_20 = """select "90000.90000", cast(cast("90000.90000" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_20_strict "${const_sql_2_20}"
    testFoldConst("${const_sql_2_20}")
    def const_sql_2_21 = """select "90000.90001", cast(cast("90000.90001" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_21_strict "${const_sql_2_21}"
    testFoldConst("${const_sql_2_21}")
    def const_sql_2_22 = """select "90000.99998", cast(cast("90000.99998" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_22_strict "${const_sql_2_22}"
    testFoldConst("${const_sql_2_22}")
    def const_sql_2_23 = """select "90000.99999", cast(cast("90000.99999" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_23_strict "${const_sql_2_23}"
    testFoldConst("${const_sql_2_23}")
    def const_sql_2_24 = """select "90001.00000", cast(cast("90001.00000" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_24_strict "${const_sql_2_24}"
    testFoldConst("${const_sql_2_24}")
    def const_sql_2_25 = """select "90001.00001", cast(cast("90001.00001" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_25_strict "${const_sql_2_25}"
    testFoldConst("${const_sql_2_25}")
    def const_sql_2_26 = """select "90001.00009", cast(cast("90001.00009" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_26_strict "${const_sql_2_26}"
    testFoldConst("${const_sql_2_26}")
    def const_sql_2_27 = """select "90001.09999", cast(cast("90001.09999" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_27_strict "${const_sql_2_27}"
    testFoldConst("${const_sql_2_27}")
    def const_sql_2_28 = """select "90001.90000", cast(cast("90001.90000" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_28_strict "${const_sql_2_28}"
    testFoldConst("${const_sql_2_28}")
    def const_sql_2_29 = """select "90001.90001", cast(cast("90001.90001" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_29_strict "${const_sql_2_29}"
    testFoldConst("${const_sql_2_29}")
    def const_sql_2_30 = """select "90001.99998", cast(cast("90001.99998" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_30_strict "${const_sql_2_30}"
    testFoldConst("${const_sql_2_30}")
    def const_sql_2_31 = """select "90001.99999", cast(cast("90001.99999" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_31_strict "${const_sql_2_31}"
    testFoldConst("${const_sql_2_31}")
    def const_sql_2_32 = """select "99998.00000", cast(cast("99998.00000" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_32_strict "${const_sql_2_32}"
    testFoldConst("${const_sql_2_32}")
    def const_sql_2_33 = """select "99998.00001", cast(cast("99998.00001" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_33_strict "${const_sql_2_33}"
    testFoldConst("${const_sql_2_33}")
    def const_sql_2_34 = """select "99998.00009", cast(cast("99998.00009" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_34_strict "${const_sql_2_34}"
    testFoldConst("${const_sql_2_34}")
    def const_sql_2_35 = """select "99998.09999", cast(cast("99998.09999" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_35_strict "${const_sql_2_35}"
    testFoldConst("${const_sql_2_35}")
    def const_sql_2_36 = """select "99998.90000", cast(cast("99998.90000" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_36_strict "${const_sql_2_36}"
    testFoldConst("${const_sql_2_36}")
    def const_sql_2_37 = """select "99998.90001", cast(cast("99998.90001" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_37_strict "${const_sql_2_37}"
    testFoldConst("${const_sql_2_37}")
    def const_sql_2_38 = """select "99998.99998", cast(cast("99998.99998" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_38_strict "${const_sql_2_38}"
    testFoldConst("${const_sql_2_38}")
    def const_sql_2_39 = """select "99998.99999", cast(cast("99998.99999" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_39_strict "${const_sql_2_39}"
    testFoldConst("${const_sql_2_39}")
    def const_sql_2_40 = """select "99999.00000", cast(cast("99999.00000" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_40_strict "${const_sql_2_40}"
    testFoldConst("${const_sql_2_40}")
    def const_sql_2_41 = """select "99999.00001", cast(cast("99999.00001" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_41_strict "${const_sql_2_41}"
    testFoldConst("${const_sql_2_41}")
    def const_sql_2_42 = """select "99999.00009", cast(cast("99999.00009" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_42_strict "${const_sql_2_42}"
    testFoldConst("${const_sql_2_42}")
    def const_sql_2_43 = """select "99999.09999", cast(cast("99999.09999" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_43_strict "${const_sql_2_43}"
    testFoldConst("${const_sql_2_43}")
    def const_sql_2_44 = """select "99999.90000", cast(cast("99999.90000" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_44_strict "${const_sql_2_44}"
    testFoldConst("${const_sql_2_44}")
    def const_sql_2_45 = """select "99999.90001", cast(cast("99999.90001" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_45_strict "${const_sql_2_45}"
    testFoldConst("${const_sql_2_45}")
    def const_sql_2_46 = """select "99999.99998", cast(cast("99999.99998" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_46_strict "${const_sql_2_46}"
    testFoldConst("${const_sql_2_46}")
    def const_sql_2_47 = """select "99999.99999", cast(cast("99999.99999" as decimalv3(10, 5)) as decimalv3(76, 0));"""
    qt_sql_2_47_strict "${const_sql_2_47}"
    testFoldConst("${const_sql_2_47}")

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
    qt_sql_2_24_non_strict "${const_sql_2_24}"
    testFoldConst("${const_sql_2_24}")
    qt_sql_2_25_non_strict "${const_sql_2_25}"
    testFoldConst("${const_sql_2_25}")
    qt_sql_2_26_non_strict "${const_sql_2_26}"
    testFoldConst("${const_sql_2_26}")
    qt_sql_2_27_non_strict "${const_sql_2_27}"
    testFoldConst("${const_sql_2_27}")
    qt_sql_2_28_non_strict "${const_sql_2_28}"
    testFoldConst("${const_sql_2_28}")
    qt_sql_2_29_non_strict "${const_sql_2_29}"
    testFoldConst("${const_sql_2_29}")
    qt_sql_2_30_non_strict "${const_sql_2_30}"
    testFoldConst("${const_sql_2_30}")
    qt_sql_2_31_non_strict "${const_sql_2_31}"
    testFoldConst("${const_sql_2_31}")
    qt_sql_2_32_non_strict "${const_sql_2_32}"
    testFoldConst("${const_sql_2_32}")
    qt_sql_2_33_non_strict "${const_sql_2_33}"
    testFoldConst("${const_sql_2_33}")
    qt_sql_2_34_non_strict "${const_sql_2_34}"
    testFoldConst("${const_sql_2_34}")
    qt_sql_2_35_non_strict "${const_sql_2_35}"
    testFoldConst("${const_sql_2_35}")
    qt_sql_2_36_non_strict "${const_sql_2_36}"
    testFoldConst("${const_sql_2_36}")
    qt_sql_2_37_non_strict "${const_sql_2_37}"
    testFoldConst("${const_sql_2_37}")
    qt_sql_2_38_non_strict "${const_sql_2_38}"
    testFoldConst("${const_sql_2_38}")
    qt_sql_2_39_non_strict "${const_sql_2_39}"
    testFoldConst("${const_sql_2_39}")
    qt_sql_2_40_non_strict "${const_sql_2_40}"
    testFoldConst("${const_sql_2_40}")
    qt_sql_2_41_non_strict "${const_sql_2_41}"
    testFoldConst("${const_sql_2_41}")
    qt_sql_2_42_non_strict "${const_sql_2_42}"
    testFoldConst("${const_sql_2_42}")
    qt_sql_2_43_non_strict "${const_sql_2_43}"
    testFoldConst("${const_sql_2_43}")
    qt_sql_2_44_non_strict "${const_sql_2_44}"
    testFoldConst("${const_sql_2_44}")
    qt_sql_2_45_non_strict "${const_sql_2_45}"
    testFoldConst("${const_sql_2_45}")
    qt_sql_2_46_non_strict "${const_sql_2_46}"
    testFoldConst("${const_sql_2_46}")
    qt_sql_2_47_non_strict "${const_sql_2_47}"
    testFoldConst("${const_sql_2_47}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_3_0 = """select "0.000000000", cast(cast("0.000000000" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_0_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    def const_sql_3_1 = """select "0.000000001", cast(cast("0.000000001" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_1_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    def const_sql_3_2 = """select "0.000000009", cast(cast("0.000000009" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_2_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    def const_sql_3_3 = """select "0.099999999", cast(cast("0.099999999" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_3_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    def const_sql_3_4 = """select "0.900000000", cast(cast("0.900000000" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_4_strict "${const_sql_3_4}"
    testFoldConst("${const_sql_3_4}")
    def const_sql_3_5 = """select "0.900000001", cast(cast("0.900000001" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_5_strict "${const_sql_3_5}"
    testFoldConst("${const_sql_3_5}")
    def const_sql_3_6 = """select "0.999999998", cast(cast("0.999999998" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_6_strict "${const_sql_3_6}"
    testFoldConst("${const_sql_3_6}")
    def const_sql_3_7 = """select "0.999999999", cast(cast("0.999999999" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_7_strict "${const_sql_3_7}"
    testFoldConst("${const_sql_3_7}")
    def const_sql_3_8 = """select "8.000000000", cast(cast("8.000000000" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_8_strict "${const_sql_3_8}"
    testFoldConst("${const_sql_3_8}")
    def const_sql_3_9 = """select "8.000000001", cast(cast("8.000000001" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_9_strict "${const_sql_3_9}"
    testFoldConst("${const_sql_3_9}")
    def const_sql_3_10 = """select "8.000000009", cast(cast("8.000000009" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_10_strict "${const_sql_3_10}"
    testFoldConst("${const_sql_3_10}")
    def const_sql_3_11 = """select "8.099999999", cast(cast("8.099999999" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_11_strict "${const_sql_3_11}"
    testFoldConst("${const_sql_3_11}")
    def const_sql_3_12 = """select "8.900000000", cast(cast("8.900000000" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_12_strict "${const_sql_3_12}"
    testFoldConst("${const_sql_3_12}")
    def const_sql_3_13 = """select "8.900000001", cast(cast("8.900000001" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_13_strict "${const_sql_3_13}"
    testFoldConst("${const_sql_3_13}")
    def const_sql_3_14 = """select "8.999999998", cast(cast("8.999999998" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_14_strict "${const_sql_3_14}"
    testFoldConst("${const_sql_3_14}")
    def const_sql_3_15 = """select "8.999999999", cast(cast("8.999999999" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_15_strict "${const_sql_3_15}"
    testFoldConst("${const_sql_3_15}")
    def const_sql_3_16 = """select "9.000000000", cast(cast("9.000000000" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_16_strict "${const_sql_3_16}"
    testFoldConst("${const_sql_3_16}")
    def const_sql_3_17 = """select "9.000000001", cast(cast("9.000000001" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_17_strict "${const_sql_3_17}"
    testFoldConst("${const_sql_3_17}")
    def const_sql_3_18 = """select "9.000000009", cast(cast("9.000000009" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_18_strict "${const_sql_3_18}"
    testFoldConst("${const_sql_3_18}")
    def const_sql_3_19 = """select "9.099999999", cast(cast("9.099999999" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_19_strict "${const_sql_3_19}"
    testFoldConst("${const_sql_3_19}")
    def const_sql_3_20 = """select "9.900000000", cast(cast("9.900000000" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_20_strict "${const_sql_3_20}"
    testFoldConst("${const_sql_3_20}")
    def const_sql_3_21 = """select "9.900000001", cast(cast("9.900000001" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_21_strict "${const_sql_3_21}"
    testFoldConst("${const_sql_3_21}")
    def const_sql_3_22 = """select "9.999999998", cast(cast("9.999999998" as decimalv3(10, 9)) as decimalv3(76, 0));"""
    qt_sql_3_22_strict "${const_sql_3_22}"
    testFoldConst("${const_sql_3_22}")
    def const_sql_3_23 = """select "9.999999999", cast(cast("9.999999999" as decimalv3(10, 9)) as decimalv3(76, 0));"""
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
    def const_sql_4_0 = """select "0.0000000000", cast(cast("0.0000000000" as decimalv3(10, 10)) as decimalv3(76, 0));"""
    qt_sql_4_0_strict "${const_sql_4_0}"
    testFoldConst("${const_sql_4_0}")
    def const_sql_4_1 = """select "0.0000000001", cast(cast("0.0000000001" as decimalv3(10, 10)) as decimalv3(76, 0));"""
    qt_sql_4_1_strict "${const_sql_4_1}"
    testFoldConst("${const_sql_4_1}")
    def const_sql_4_2 = """select "0.0000000009", cast(cast("0.0000000009" as decimalv3(10, 10)) as decimalv3(76, 0));"""
    qt_sql_4_2_strict "${const_sql_4_2}"
    testFoldConst("${const_sql_4_2}")
    def const_sql_4_3 = """select "0.0999999999", cast(cast("0.0999999999" as decimalv3(10, 10)) as decimalv3(76, 0));"""
    qt_sql_4_3_strict "${const_sql_4_3}"
    testFoldConst("${const_sql_4_3}")
    def const_sql_4_4 = """select "0.9000000000", cast(cast("0.9000000000" as decimalv3(10, 10)) as decimalv3(76, 0));"""
    qt_sql_4_4_strict "${const_sql_4_4}"
    testFoldConst("${const_sql_4_4}")
    def const_sql_4_5 = """select "0.9000000001", cast(cast("0.9000000001" as decimalv3(10, 10)) as decimalv3(76, 0));"""
    qt_sql_4_5_strict "${const_sql_4_5}"
    testFoldConst("${const_sql_4_5}")
    def const_sql_4_6 = """select "0.9999999998", cast(cast("0.9999999998" as decimalv3(10, 10)) as decimalv3(76, 0));"""
    qt_sql_4_6_strict "${const_sql_4_6}"
    testFoldConst("${const_sql_4_6}")
    def const_sql_4_7 = """select "0.9999999999", cast(cast("0.9999999999" as decimalv3(10, 10)) as decimalv3(76, 0));"""
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
    def const_sql_5_0 = """select "0", cast(cast("0" as decimalv3(17, 0)) as decimalv3(76, 0));"""
    qt_sql_5_0_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")
    def const_sql_5_1 = """select "9999999999999999", cast(cast("9999999999999999" as decimalv3(17, 0)) as decimalv3(76, 0));"""
    qt_sql_5_1_strict "${const_sql_5_1}"
    testFoldConst("${const_sql_5_1}")
    def const_sql_5_2 = """select "90000000000000000", cast(cast("90000000000000000" as decimalv3(17, 0)) as decimalv3(76, 0));"""
    qt_sql_5_2_strict "${const_sql_5_2}"
    testFoldConst("${const_sql_5_2}")
    def const_sql_5_3 = """select "90000000000000001", cast(cast("90000000000000001" as decimalv3(17, 0)) as decimalv3(76, 0));"""
    qt_sql_5_3_strict "${const_sql_5_3}"
    testFoldConst("${const_sql_5_3}")
    def const_sql_5_4 = """select "99999999999999998", cast(cast("99999999999999998" as decimalv3(17, 0)) as decimalv3(76, 0));"""
    qt_sql_5_4_strict "${const_sql_5_4}"
    testFoldConst("${const_sql_5_4}")
    def const_sql_5_5 = """select "99999999999999999", cast(cast("99999999999999999" as decimalv3(17, 0)) as decimalv3(76, 0));"""
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
    def const_sql_6_0 = """select "0.0", cast(cast("0.0" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_0_strict "${const_sql_6_0}"
    testFoldConst("${const_sql_6_0}")
    def const_sql_6_1 = """select "0.1", cast(cast("0.1" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_1_strict "${const_sql_6_1}"
    testFoldConst("${const_sql_6_1}")
    def const_sql_6_2 = """select "0.8", cast(cast("0.8" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_2_strict "${const_sql_6_2}"
    testFoldConst("${const_sql_6_2}")
    def const_sql_6_3 = """select "0.9", cast(cast("0.9" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_3_strict "${const_sql_6_3}"
    testFoldConst("${const_sql_6_3}")
    def const_sql_6_4 = """select "999999999999999.0", cast(cast("999999999999999.0" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_4_strict "${const_sql_6_4}"
    testFoldConst("${const_sql_6_4}")
    def const_sql_6_5 = """select "999999999999999.1", cast(cast("999999999999999.1" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_5_strict "${const_sql_6_5}"
    testFoldConst("${const_sql_6_5}")
    def const_sql_6_6 = """select "999999999999999.8", cast(cast("999999999999999.8" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_6_strict "${const_sql_6_6}"
    testFoldConst("${const_sql_6_6}")
    def const_sql_6_7 = """select "999999999999999.9", cast(cast("999999999999999.9" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_7_strict "${const_sql_6_7}"
    testFoldConst("${const_sql_6_7}")
    def const_sql_6_8 = """select "9000000000000000.0", cast(cast("9000000000000000.0" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_8_strict "${const_sql_6_8}"
    testFoldConst("${const_sql_6_8}")
    def const_sql_6_9 = """select "9000000000000000.1", cast(cast("9000000000000000.1" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_9_strict "${const_sql_6_9}"
    testFoldConst("${const_sql_6_9}")
    def const_sql_6_10 = """select "9000000000000000.8", cast(cast("9000000000000000.8" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_10_strict "${const_sql_6_10}"
    testFoldConst("${const_sql_6_10}")
    def const_sql_6_11 = """select "9000000000000000.9", cast(cast("9000000000000000.9" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_11_strict "${const_sql_6_11}"
    testFoldConst("${const_sql_6_11}")
    def const_sql_6_12 = """select "9000000000000001.0", cast(cast("9000000000000001.0" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_12_strict "${const_sql_6_12}"
    testFoldConst("${const_sql_6_12}")
    def const_sql_6_13 = """select "9000000000000001.1", cast(cast("9000000000000001.1" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_13_strict "${const_sql_6_13}"
    testFoldConst("${const_sql_6_13}")
    def const_sql_6_14 = """select "9000000000000001.8", cast(cast("9000000000000001.8" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_14_strict "${const_sql_6_14}"
    testFoldConst("${const_sql_6_14}")
    def const_sql_6_15 = """select "9000000000000001.9", cast(cast("9000000000000001.9" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_15_strict "${const_sql_6_15}"
    testFoldConst("${const_sql_6_15}")
    def const_sql_6_16 = """select "9999999999999998.0", cast(cast("9999999999999998.0" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_16_strict "${const_sql_6_16}"
    testFoldConst("${const_sql_6_16}")
    def const_sql_6_17 = """select "9999999999999998.1", cast(cast("9999999999999998.1" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_17_strict "${const_sql_6_17}"
    testFoldConst("${const_sql_6_17}")
    def const_sql_6_18 = """select "9999999999999998.8", cast(cast("9999999999999998.8" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_18_strict "${const_sql_6_18}"
    testFoldConst("${const_sql_6_18}")
    def const_sql_6_19 = """select "9999999999999998.9", cast(cast("9999999999999998.9" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_19_strict "${const_sql_6_19}"
    testFoldConst("${const_sql_6_19}")
    def const_sql_6_20 = """select "9999999999999999.0", cast(cast("9999999999999999.0" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_20_strict "${const_sql_6_20}"
    testFoldConst("${const_sql_6_20}")
    def const_sql_6_21 = """select "9999999999999999.1", cast(cast("9999999999999999.1" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_21_strict "${const_sql_6_21}"
    testFoldConst("${const_sql_6_21}")
    def const_sql_6_22 = """select "9999999999999999.8", cast(cast("9999999999999999.8" as decimalv3(17, 1)) as decimalv3(76, 0));"""
    qt_sql_6_22_strict "${const_sql_6_22}"
    testFoldConst("${const_sql_6_22}")
    def const_sql_6_23 = """select "9999999999999999.9", cast(cast("9999999999999999.9" as decimalv3(17, 1)) as decimalv3(76, 0));"""
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
    def const_sql_7_0 = """select "0.00000000", cast(cast("0.00000000" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_0_strict "${const_sql_7_0}"
    testFoldConst("${const_sql_7_0}")
    def const_sql_7_1 = """select "0.00000001", cast(cast("0.00000001" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_1_strict "${const_sql_7_1}"
    testFoldConst("${const_sql_7_1}")
    def const_sql_7_2 = """select "0.00000009", cast(cast("0.00000009" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_2_strict "${const_sql_7_2}"
    testFoldConst("${const_sql_7_2}")
    def const_sql_7_3 = """select "0.09999999", cast(cast("0.09999999" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_3_strict "${const_sql_7_3}"
    testFoldConst("${const_sql_7_3}")
    def const_sql_7_4 = """select "0.90000000", cast(cast("0.90000000" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_4_strict "${const_sql_7_4}"
    testFoldConst("${const_sql_7_4}")
    def const_sql_7_5 = """select "0.90000001", cast(cast("0.90000001" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_5_strict "${const_sql_7_5}"
    testFoldConst("${const_sql_7_5}")
    def const_sql_7_6 = """select "0.99999998", cast(cast("0.99999998" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_6_strict "${const_sql_7_6}"
    testFoldConst("${const_sql_7_6}")
    def const_sql_7_7 = """select "0.99999999", cast(cast("0.99999999" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_7_strict "${const_sql_7_7}"
    testFoldConst("${const_sql_7_7}")
    def const_sql_7_8 = """select "99999999.00000000", cast(cast("99999999.00000000" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_8_strict "${const_sql_7_8}"
    testFoldConst("${const_sql_7_8}")
    def const_sql_7_9 = """select "99999999.00000001", cast(cast("99999999.00000001" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_9_strict "${const_sql_7_9}"
    testFoldConst("${const_sql_7_9}")
    def const_sql_7_10 = """select "99999999.00000009", cast(cast("99999999.00000009" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_10_strict "${const_sql_7_10}"
    testFoldConst("${const_sql_7_10}")
    def const_sql_7_11 = """select "99999999.09999999", cast(cast("99999999.09999999" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_11_strict "${const_sql_7_11}"
    testFoldConst("${const_sql_7_11}")
    def const_sql_7_12 = """select "99999999.90000000", cast(cast("99999999.90000000" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_12_strict "${const_sql_7_12}"
    testFoldConst("${const_sql_7_12}")
    def const_sql_7_13 = """select "99999999.90000001", cast(cast("99999999.90000001" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_13_strict "${const_sql_7_13}"
    testFoldConst("${const_sql_7_13}")
    def const_sql_7_14 = """select "99999999.99999998", cast(cast("99999999.99999998" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_14_strict "${const_sql_7_14}"
    testFoldConst("${const_sql_7_14}")
    def const_sql_7_15 = """select "99999999.99999999", cast(cast("99999999.99999999" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_15_strict "${const_sql_7_15}"
    testFoldConst("${const_sql_7_15}")
    def const_sql_7_16 = """select "900000000.00000000", cast(cast("900000000.00000000" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_16_strict "${const_sql_7_16}"
    testFoldConst("${const_sql_7_16}")
    def const_sql_7_17 = """select "900000000.00000001", cast(cast("900000000.00000001" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_17_strict "${const_sql_7_17}"
    testFoldConst("${const_sql_7_17}")
    def const_sql_7_18 = """select "900000000.00000009", cast(cast("900000000.00000009" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_18_strict "${const_sql_7_18}"
    testFoldConst("${const_sql_7_18}")
    def const_sql_7_19 = """select "900000000.09999999", cast(cast("900000000.09999999" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_19_strict "${const_sql_7_19}"
    testFoldConst("${const_sql_7_19}")
    def const_sql_7_20 = """select "900000000.90000000", cast(cast("900000000.90000000" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_20_strict "${const_sql_7_20}"
    testFoldConst("${const_sql_7_20}")
    def const_sql_7_21 = """select "900000000.90000001", cast(cast("900000000.90000001" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_21_strict "${const_sql_7_21}"
    testFoldConst("${const_sql_7_21}")
    def const_sql_7_22 = """select "900000000.99999998", cast(cast("900000000.99999998" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_22_strict "${const_sql_7_22}"
    testFoldConst("${const_sql_7_22}")
    def const_sql_7_23 = """select "900000000.99999999", cast(cast("900000000.99999999" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_23_strict "${const_sql_7_23}"
    testFoldConst("${const_sql_7_23}")
    def const_sql_7_24 = """select "900000001.00000000", cast(cast("900000001.00000000" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_24_strict "${const_sql_7_24}"
    testFoldConst("${const_sql_7_24}")
    def const_sql_7_25 = """select "900000001.00000001", cast(cast("900000001.00000001" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_25_strict "${const_sql_7_25}"
    testFoldConst("${const_sql_7_25}")
    def const_sql_7_26 = """select "900000001.00000009", cast(cast("900000001.00000009" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_26_strict "${const_sql_7_26}"
    testFoldConst("${const_sql_7_26}")
    def const_sql_7_27 = """select "900000001.09999999", cast(cast("900000001.09999999" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_27_strict "${const_sql_7_27}"
    testFoldConst("${const_sql_7_27}")
    def const_sql_7_28 = """select "900000001.90000000", cast(cast("900000001.90000000" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_28_strict "${const_sql_7_28}"
    testFoldConst("${const_sql_7_28}")
    def const_sql_7_29 = """select "900000001.90000001", cast(cast("900000001.90000001" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_29_strict "${const_sql_7_29}"
    testFoldConst("${const_sql_7_29}")
    def const_sql_7_30 = """select "900000001.99999998", cast(cast("900000001.99999998" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_30_strict "${const_sql_7_30}"
    testFoldConst("${const_sql_7_30}")
    def const_sql_7_31 = """select "900000001.99999999", cast(cast("900000001.99999999" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_31_strict "${const_sql_7_31}"
    testFoldConst("${const_sql_7_31}")
    def const_sql_7_32 = """select "999999998.00000000", cast(cast("999999998.00000000" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_32_strict "${const_sql_7_32}"
    testFoldConst("${const_sql_7_32}")
    def const_sql_7_33 = """select "999999998.00000001", cast(cast("999999998.00000001" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_33_strict "${const_sql_7_33}"
    testFoldConst("${const_sql_7_33}")
    def const_sql_7_34 = """select "999999998.00000009", cast(cast("999999998.00000009" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_34_strict "${const_sql_7_34}"
    testFoldConst("${const_sql_7_34}")
    def const_sql_7_35 = """select "999999998.09999999", cast(cast("999999998.09999999" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_35_strict "${const_sql_7_35}"
    testFoldConst("${const_sql_7_35}")
    def const_sql_7_36 = """select "999999998.90000000", cast(cast("999999998.90000000" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_36_strict "${const_sql_7_36}"
    testFoldConst("${const_sql_7_36}")
    def const_sql_7_37 = """select "999999998.90000001", cast(cast("999999998.90000001" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_37_strict "${const_sql_7_37}"
    testFoldConst("${const_sql_7_37}")
    def const_sql_7_38 = """select "999999998.99999998", cast(cast("999999998.99999998" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_38_strict "${const_sql_7_38}"
    testFoldConst("${const_sql_7_38}")
    def const_sql_7_39 = """select "999999998.99999999", cast(cast("999999998.99999999" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_39_strict "${const_sql_7_39}"
    testFoldConst("${const_sql_7_39}")
    def const_sql_7_40 = """select "999999999.00000000", cast(cast("999999999.00000000" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_40_strict "${const_sql_7_40}"
    testFoldConst("${const_sql_7_40}")
    def const_sql_7_41 = """select "999999999.00000001", cast(cast("999999999.00000001" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_41_strict "${const_sql_7_41}"
    testFoldConst("${const_sql_7_41}")
    def const_sql_7_42 = """select "999999999.00000009", cast(cast("999999999.00000009" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_42_strict "${const_sql_7_42}"
    testFoldConst("${const_sql_7_42}")
    def const_sql_7_43 = """select "999999999.09999999", cast(cast("999999999.09999999" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_43_strict "${const_sql_7_43}"
    testFoldConst("${const_sql_7_43}")
    def const_sql_7_44 = """select "999999999.90000000", cast(cast("999999999.90000000" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_44_strict "${const_sql_7_44}"
    testFoldConst("${const_sql_7_44}")
    def const_sql_7_45 = """select "999999999.90000001", cast(cast("999999999.90000001" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_45_strict "${const_sql_7_45}"
    testFoldConst("${const_sql_7_45}")
    def const_sql_7_46 = """select "999999999.99999998", cast(cast("999999999.99999998" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_46_strict "${const_sql_7_46}"
    testFoldConst("${const_sql_7_46}")
    def const_sql_7_47 = """select "999999999.99999999", cast(cast("999999999.99999999" as decimalv3(17, 8)) as decimalv3(76, 0));"""
    qt_sql_7_47_strict "${const_sql_7_47}"
    testFoldConst("${const_sql_7_47}")

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
    qt_sql_7_30_non_strict "${const_sql_7_30}"
    testFoldConst("${const_sql_7_30}")
    qt_sql_7_31_non_strict "${const_sql_7_31}"
    testFoldConst("${const_sql_7_31}")
    qt_sql_7_32_non_strict "${const_sql_7_32}"
    testFoldConst("${const_sql_7_32}")
    qt_sql_7_33_non_strict "${const_sql_7_33}"
    testFoldConst("${const_sql_7_33}")
    qt_sql_7_34_non_strict "${const_sql_7_34}"
    testFoldConst("${const_sql_7_34}")
    qt_sql_7_35_non_strict "${const_sql_7_35}"
    testFoldConst("${const_sql_7_35}")
    qt_sql_7_36_non_strict "${const_sql_7_36}"
    testFoldConst("${const_sql_7_36}")
    qt_sql_7_37_non_strict "${const_sql_7_37}"
    testFoldConst("${const_sql_7_37}")
    qt_sql_7_38_non_strict "${const_sql_7_38}"
    testFoldConst("${const_sql_7_38}")
    qt_sql_7_39_non_strict "${const_sql_7_39}"
    testFoldConst("${const_sql_7_39}")
    qt_sql_7_40_non_strict "${const_sql_7_40}"
    testFoldConst("${const_sql_7_40}")
    qt_sql_7_41_non_strict "${const_sql_7_41}"
    testFoldConst("${const_sql_7_41}")
    qt_sql_7_42_non_strict "${const_sql_7_42}"
    testFoldConst("${const_sql_7_42}")
    qt_sql_7_43_non_strict "${const_sql_7_43}"
    testFoldConst("${const_sql_7_43}")
    qt_sql_7_44_non_strict "${const_sql_7_44}"
    testFoldConst("${const_sql_7_44}")
    qt_sql_7_45_non_strict "${const_sql_7_45}"
    testFoldConst("${const_sql_7_45}")
    qt_sql_7_46_non_strict "${const_sql_7_46}"
    testFoldConst("${const_sql_7_46}")
    qt_sql_7_47_non_strict "${const_sql_7_47}"
    testFoldConst("${const_sql_7_47}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_8_0 = """select "0.0000000000000000", cast(cast("0.0000000000000000" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_0_strict "${const_sql_8_0}"
    testFoldConst("${const_sql_8_0}")
    def const_sql_8_1 = """select "0.0000000000000001", cast(cast("0.0000000000000001" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_1_strict "${const_sql_8_1}"
    testFoldConst("${const_sql_8_1}")
    def const_sql_8_2 = """select "0.0000000000000009", cast(cast("0.0000000000000009" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_2_strict "${const_sql_8_2}"
    testFoldConst("${const_sql_8_2}")
    def const_sql_8_3 = """select "0.0999999999999999", cast(cast("0.0999999999999999" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_3_strict "${const_sql_8_3}"
    testFoldConst("${const_sql_8_3}")
    def const_sql_8_4 = """select "0.9000000000000000", cast(cast("0.9000000000000000" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_4_strict "${const_sql_8_4}"
    testFoldConst("${const_sql_8_4}")
    def const_sql_8_5 = """select "0.9000000000000001", cast(cast("0.9000000000000001" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_5_strict "${const_sql_8_5}"
    testFoldConst("${const_sql_8_5}")
    def const_sql_8_6 = """select "0.9999999999999998", cast(cast("0.9999999999999998" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_6_strict "${const_sql_8_6}"
    testFoldConst("${const_sql_8_6}")
    def const_sql_8_7 = """select "0.9999999999999999", cast(cast("0.9999999999999999" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_7_strict "${const_sql_8_7}"
    testFoldConst("${const_sql_8_7}")
    def const_sql_8_8 = """select "8.0000000000000000", cast(cast("8.0000000000000000" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_8_strict "${const_sql_8_8}"
    testFoldConst("${const_sql_8_8}")
    def const_sql_8_9 = """select "8.0000000000000001", cast(cast("8.0000000000000001" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_9_strict "${const_sql_8_9}"
    testFoldConst("${const_sql_8_9}")
    def const_sql_8_10 = """select "8.0000000000000009", cast(cast("8.0000000000000009" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_10_strict "${const_sql_8_10}"
    testFoldConst("${const_sql_8_10}")
    def const_sql_8_11 = """select "8.0999999999999999", cast(cast("8.0999999999999999" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_11_strict "${const_sql_8_11}"
    testFoldConst("${const_sql_8_11}")
    def const_sql_8_12 = """select "8.9000000000000000", cast(cast("8.9000000000000000" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_12_strict "${const_sql_8_12}"
    testFoldConst("${const_sql_8_12}")
    def const_sql_8_13 = """select "8.9000000000000001", cast(cast("8.9000000000000001" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_13_strict "${const_sql_8_13}"
    testFoldConst("${const_sql_8_13}")
    def const_sql_8_14 = """select "8.9999999999999998", cast(cast("8.9999999999999998" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_14_strict "${const_sql_8_14}"
    testFoldConst("${const_sql_8_14}")
    def const_sql_8_15 = """select "8.9999999999999999", cast(cast("8.9999999999999999" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_15_strict "${const_sql_8_15}"
    testFoldConst("${const_sql_8_15}")
    def const_sql_8_16 = """select "9.0000000000000000", cast(cast("9.0000000000000000" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_16_strict "${const_sql_8_16}"
    testFoldConst("${const_sql_8_16}")
    def const_sql_8_17 = """select "9.0000000000000001", cast(cast("9.0000000000000001" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_17_strict "${const_sql_8_17}"
    testFoldConst("${const_sql_8_17}")
    def const_sql_8_18 = """select "9.0000000000000009", cast(cast("9.0000000000000009" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_18_strict "${const_sql_8_18}"
    testFoldConst("${const_sql_8_18}")
    def const_sql_8_19 = """select "9.0999999999999999", cast(cast("9.0999999999999999" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_19_strict "${const_sql_8_19}"
    testFoldConst("${const_sql_8_19}")
    def const_sql_8_20 = """select "9.9000000000000000", cast(cast("9.9000000000000000" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_20_strict "${const_sql_8_20}"
    testFoldConst("${const_sql_8_20}")
    def const_sql_8_21 = """select "9.9000000000000001", cast(cast("9.9000000000000001" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_21_strict "${const_sql_8_21}"
    testFoldConst("${const_sql_8_21}")
    def const_sql_8_22 = """select "9.9999999999999998", cast(cast("9.9999999999999998" as decimalv3(17, 16)) as decimalv3(76, 0));"""
    qt_sql_8_22_strict "${const_sql_8_22}"
    testFoldConst("${const_sql_8_22}")
    def const_sql_8_23 = """select "9.9999999999999999", cast(cast("9.9999999999999999" as decimalv3(17, 16)) as decimalv3(76, 0));"""
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
    def const_sql_9_0 = """select "0.00000000000000000", cast(cast("0.00000000000000000" as decimalv3(17, 17)) as decimalv3(76, 0));"""
    qt_sql_9_0_strict "${const_sql_9_0}"
    testFoldConst("${const_sql_9_0}")
    def const_sql_9_1 = """select "0.00000000000000001", cast(cast("0.00000000000000001" as decimalv3(17, 17)) as decimalv3(76, 0));"""
    qt_sql_9_1_strict "${const_sql_9_1}"
    testFoldConst("${const_sql_9_1}")
    def const_sql_9_2 = """select "0.00000000000000009", cast(cast("0.00000000000000009" as decimalv3(17, 17)) as decimalv3(76, 0));"""
    qt_sql_9_2_strict "${const_sql_9_2}"
    testFoldConst("${const_sql_9_2}")
    def const_sql_9_3 = """select "0.09999999999999999", cast(cast("0.09999999999999999" as decimalv3(17, 17)) as decimalv3(76, 0));"""
    qt_sql_9_3_strict "${const_sql_9_3}"
    testFoldConst("${const_sql_9_3}")
    def const_sql_9_4 = """select "0.90000000000000000", cast(cast("0.90000000000000000" as decimalv3(17, 17)) as decimalv3(76, 0));"""
    qt_sql_9_4_strict "${const_sql_9_4}"
    testFoldConst("${const_sql_9_4}")
    def const_sql_9_5 = """select "0.90000000000000001", cast(cast("0.90000000000000001" as decimalv3(17, 17)) as decimalv3(76, 0));"""
    qt_sql_9_5_strict "${const_sql_9_5}"
    testFoldConst("${const_sql_9_5}")
    def const_sql_9_6 = """select "0.99999999999999998", cast(cast("0.99999999999999998" as decimalv3(17, 17)) as decimalv3(76, 0));"""
    qt_sql_9_6_strict "${const_sql_9_6}"
    testFoldConst("${const_sql_9_6}")
    def const_sql_9_7 = """select "0.99999999999999999", cast(cast("0.99999999999999999" as decimalv3(17, 17)) as decimalv3(76, 0));"""
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
    def const_sql_10_0 = """select "0", cast(cast("0" as decimalv3(18, 0)) as decimalv3(76, 0));"""
    qt_sql_10_0_strict "${const_sql_10_0}"
    testFoldConst("${const_sql_10_0}")
    def const_sql_10_1 = """select "99999999999999999", cast(cast("99999999999999999" as decimalv3(18, 0)) as decimalv3(76, 0));"""
    qt_sql_10_1_strict "${const_sql_10_1}"
    testFoldConst("${const_sql_10_1}")
    def const_sql_10_2 = """select "900000000000000000", cast(cast("900000000000000000" as decimalv3(18, 0)) as decimalv3(76, 0));"""
    qt_sql_10_2_strict "${const_sql_10_2}"
    testFoldConst("${const_sql_10_2}")
    def const_sql_10_3 = """select "900000000000000001", cast(cast("900000000000000001" as decimalv3(18, 0)) as decimalv3(76, 0));"""
    qt_sql_10_3_strict "${const_sql_10_3}"
    testFoldConst("${const_sql_10_3}")
    def const_sql_10_4 = """select "999999999999999998", cast(cast("999999999999999998" as decimalv3(18, 0)) as decimalv3(76, 0));"""
    qt_sql_10_4_strict "${const_sql_10_4}"
    testFoldConst("${const_sql_10_4}")
    def const_sql_10_5 = """select "999999999999999999", cast(cast("999999999999999999" as decimalv3(18, 0)) as decimalv3(76, 0));"""
    qt_sql_10_5_strict "${const_sql_10_5}"
    testFoldConst("${const_sql_10_5}")

    sql "set enable_strict_cast=false;"
    qt_sql_10_0_non_strict "${const_sql_10_0}"
    testFoldConst("${const_sql_10_0}")
    qt_sql_10_1_non_strict "${const_sql_10_1}"
    testFoldConst("${const_sql_10_1}")
    qt_sql_10_2_non_strict "${const_sql_10_2}"
    testFoldConst("${const_sql_10_2}")
    qt_sql_10_3_non_strict "${const_sql_10_3}"
    testFoldConst("${const_sql_10_3}")
    qt_sql_10_4_non_strict "${const_sql_10_4}"
    testFoldConst("${const_sql_10_4}")
    qt_sql_10_5_non_strict "${const_sql_10_5}"
    testFoldConst("${const_sql_10_5}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_11_0 = """select "0.0", cast(cast("0.0" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_0_strict "${const_sql_11_0}"
    testFoldConst("${const_sql_11_0}")
    def const_sql_11_1 = """select "0.1", cast(cast("0.1" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_1_strict "${const_sql_11_1}"
    testFoldConst("${const_sql_11_1}")
    def const_sql_11_2 = """select "0.8", cast(cast("0.8" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_2_strict "${const_sql_11_2}"
    testFoldConst("${const_sql_11_2}")
    def const_sql_11_3 = """select "0.9", cast(cast("0.9" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_3_strict "${const_sql_11_3}"
    testFoldConst("${const_sql_11_3}")
    def const_sql_11_4 = """select "9999999999999999.0", cast(cast("9999999999999999.0" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_4_strict "${const_sql_11_4}"
    testFoldConst("${const_sql_11_4}")
    def const_sql_11_5 = """select "9999999999999999.1", cast(cast("9999999999999999.1" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_5_strict "${const_sql_11_5}"
    testFoldConst("${const_sql_11_5}")
    def const_sql_11_6 = """select "9999999999999999.8", cast(cast("9999999999999999.8" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_6_strict "${const_sql_11_6}"
    testFoldConst("${const_sql_11_6}")
    def const_sql_11_7 = """select "9999999999999999.9", cast(cast("9999999999999999.9" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_7_strict "${const_sql_11_7}"
    testFoldConst("${const_sql_11_7}")
    def const_sql_11_8 = """select "90000000000000000.0", cast(cast("90000000000000000.0" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_8_strict "${const_sql_11_8}"
    testFoldConst("${const_sql_11_8}")
    def const_sql_11_9 = """select "90000000000000000.1", cast(cast("90000000000000000.1" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_9_strict "${const_sql_11_9}"
    testFoldConst("${const_sql_11_9}")
    def const_sql_11_10 = """select "90000000000000000.8", cast(cast("90000000000000000.8" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_10_strict "${const_sql_11_10}"
    testFoldConst("${const_sql_11_10}")
    def const_sql_11_11 = """select "90000000000000000.9", cast(cast("90000000000000000.9" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_11_strict "${const_sql_11_11}"
    testFoldConst("${const_sql_11_11}")
    def const_sql_11_12 = """select "90000000000000001.0", cast(cast("90000000000000001.0" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_12_strict "${const_sql_11_12}"
    testFoldConst("${const_sql_11_12}")
    def const_sql_11_13 = """select "90000000000000001.1", cast(cast("90000000000000001.1" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_13_strict "${const_sql_11_13}"
    testFoldConst("${const_sql_11_13}")
    def const_sql_11_14 = """select "90000000000000001.8", cast(cast("90000000000000001.8" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_14_strict "${const_sql_11_14}"
    testFoldConst("${const_sql_11_14}")
    def const_sql_11_15 = """select "90000000000000001.9", cast(cast("90000000000000001.9" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_15_strict "${const_sql_11_15}"
    testFoldConst("${const_sql_11_15}")
    def const_sql_11_16 = """select "99999999999999998.0", cast(cast("99999999999999998.0" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_16_strict "${const_sql_11_16}"
    testFoldConst("${const_sql_11_16}")
    def const_sql_11_17 = """select "99999999999999998.1", cast(cast("99999999999999998.1" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_17_strict "${const_sql_11_17}"
    testFoldConst("${const_sql_11_17}")
    def const_sql_11_18 = """select "99999999999999998.8", cast(cast("99999999999999998.8" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_18_strict "${const_sql_11_18}"
    testFoldConst("${const_sql_11_18}")
    def const_sql_11_19 = """select "99999999999999998.9", cast(cast("99999999999999998.9" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_19_strict "${const_sql_11_19}"
    testFoldConst("${const_sql_11_19}")
    def const_sql_11_20 = """select "99999999999999999.0", cast(cast("99999999999999999.0" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_20_strict "${const_sql_11_20}"
    testFoldConst("${const_sql_11_20}")
    def const_sql_11_21 = """select "99999999999999999.1", cast(cast("99999999999999999.1" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_21_strict "${const_sql_11_21}"
    testFoldConst("${const_sql_11_21}")
    def const_sql_11_22 = """select "99999999999999999.8", cast(cast("99999999999999999.8" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_22_strict "${const_sql_11_22}"
    testFoldConst("${const_sql_11_22}")
    def const_sql_11_23 = """select "99999999999999999.9", cast(cast("99999999999999999.9" as decimalv3(18, 1)) as decimalv3(76, 0));"""
    qt_sql_11_23_strict "${const_sql_11_23}"
    testFoldConst("${const_sql_11_23}")

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
    qt_sql_11_12_non_strict "${const_sql_11_12}"
    testFoldConst("${const_sql_11_12}")
    qt_sql_11_13_non_strict "${const_sql_11_13}"
    testFoldConst("${const_sql_11_13}")
    qt_sql_11_14_non_strict "${const_sql_11_14}"
    testFoldConst("${const_sql_11_14}")
    qt_sql_11_15_non_strict "${const_sql_11_15}"
    testFoldConst("${const_sql_11_15}")
    qt_sql_11_16_non_strict "${const_sql_11_16}"
    testFoldConst("${const_sql_11_16}")
    qt_sql_11_17_non_strict "${const_sql_11_17}"
    testFoldConst("${const_sql_11_17}")
    qt_sql_11_18_non_strict "${const_sql_11_18}"
    testFoldConst("${const_sql_11_18}")
    qt_sql_11_19_non_strict "${const_sql_11_19}"
    testFoldConst("${const_sql_11_19}")
    qt_sql_11_20_non_strict "${const_sql_11_20}"
    testFoldConst("${const_sql_11_20}")
    qt_sql_11_21_non_strict "${const_sql_11_21}"
    testFoldConst("${const_sql_11_21}")
    qt_sql_11_22_non_strict "${const_sql_11_22}"
    testFoldConst("${const_sql_11_22}")
    qt_sql_11_23_non_strict "${const_sql_11_23}"
    testFoldConst("${const_sql_11_23}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_12_0 = """select "0.000000000", cast(cast("0.000000000" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_0_strict "${const_sql_12_0}"
    testFoldConst("${const_sql_12_0}")
    def const_sql_12_1 = """select "0.000000001", cast(cast("0.000000001" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_1_strict "${const_sql_12_1}"
    testFoldConst("${const_sql_12_1}")
    def const_sql_12_2 = """select "0.000000009", cast(cast("0.000000009" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_2_strict "${const_sql_12_2}"
    testFoldConst("${const_sql_12_2}")
    def const_sql_12_3 = """select "0.099999999", cast(cast("0.099999999" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_3_strict "${const_sql_12_3}"
    testFoldConst("${const_sql_12_3}")
    def const_sql_12_4 = """select "0.900000000", cast(cast("0.900000000" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_4_strict "${const_sql_12_4}"
    testFoldConst("${const_sql_12_4}")
    def const_sql_12_5 = """select "0.900000001", cast(cast("0.900000001" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_5_strict "${const_sql_12_5}"
    testFoldConst("${const_sql_12_5}")
    def const_sql_12_6 = """select "0.999999998", cast(cast("0.999999998" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_6_strict "${const_sql_12_6}"
    testFoldConst("${const_sql_12_6}")
    def const_sql_12_7 = """select "0.999999999", cast(cast("0.999999999" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_7_strict "${const_sql_12_7}"
    testFoldConst("${const_sql_12_7}")
    def const_sql_12_8 = """select "99999999.000000000", cast(cast("99999999.000000000" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_8_strict "${const_sql_12_8}"
    testFoldConst("${const_sql_12_8}")
    def const_sql_12_9 = """select "99999999.000000001", cast(cast("99999999.000000001" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_9_strict "${const_sql_12_9}"
    testFoldConst("${const_sql_12_9}")
    def const_sql_12_10 = """select "99999999.000000009", cast(cast("99999999.000000009" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_10_strict "${const_sql_12_10}"
    testFoldConst("${const_sql_12_10}")
    def const_sql_12_11 = """select "99999999.099999999", cast(cast("99999999.099999999" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_11_strict "${const_sql_12_11}"
    testFoldConst("${const_sql_12_11}")
    def const_sql_12_12 = """select "99999999.900000000", cast(cast("99999999.900000000" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_12_strict "${const_sql_12_12}"
    testFoldConst("${const_sql_12_12}")
    def const_sql_12_13 = """select "99999999.900000001", cast(cast("99999999.900000001" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_13_strict "${const_sql_12_13}"
    testFoldConst("${const_sql_12_13}")
    def const_sql_12_14 = """select "99999999.999999998", cast(cast("99999999.999999998" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_14_strict "${const_sql_12_14}"
    testFoldConst("${const_sql_12_14}")
    def const_sql_12_15 = """select "99999999.999999999", cast(cast("99999999.999999999" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_15_strict "${const_sql_12_15}"
    testFoldConst("${const_sql_12_15}")
    def const_sql_12_16 = """select "900000000.000000000", cast(cast("900000000.000000000" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_16_strict "${const_sql_12_16}"
    testFoldConst("${const_sql_12_16}")
    def const_sql_12_17 = """select "900000000.000000001", cast(cast("900000000.000000001" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_17_strict "${const_sql_12_17}"
    testFoldConst("${const_sql_12_17}")
    def const_sql_12_18 = """select "900000000.000000009", cast(cast("900000000.000000009" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_18_strict "${const_sql_12_18}"
    testFoldConst("${const_sql_12_18}")
    def const_sql_12_19 = """select "900000000.099999999", cast(cast("900000000.099999999" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_19_strict "${const_sql_12_19}"
    testFoldConst("${const_sql_12_19}")
    def const_sql_12_20 = """select "900000000.900000000", cast(cast("900000000.900000000" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_20_strict "${const_sql_12_20}"
    testFoldConst("${const_sql_12_20}")
    def const_sql_12_21 = """select "900000000.900000001", cast(cast("900000000.900000001" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_21_strict "${const_sql_12_21}"
    testFoldConst("${const_sql_12_21}")
    def const_sql_12_22 = """select "900000000.999999998", cast(cast("900000000.999999998" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_22_strict "${const_sql_12_22}"
    testFoldConst("${const_sql_12_22}")
    def const_sql_12_23 = """select "900000000.999999999", cast(cast("900000000.999999999" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_23_strict "${const_sql_12_23}"
    testFoldConst("${const_sql_12_23}")
    def const_sql_12_24 = """select "900000001.000000000", cast(cast("900000001.000000000" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_24_strict "${const_sql_12_24}"
    testFoldConst("${const_sql_12_24}")
    def const_sql_12_25 = """select "900000001.000000001", cast(cast("900000001.000000001" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_25_strict "${const_sql_12_25}"
    testFoldConst("${const_sql_12_25}")
    def const_sql_12_26 = """select "900000001.000000009", cast(cast("900000001.000000009" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_26_strict "${const_sql_12_26}"
    testFoldConst("${const_sql_12_26}")
    def const_sql_12_27 = """select "900000001.099999999", cast(cast("900000001.099999999" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_27_strict "${const_sql_12_27}"
    testFoldConst("${const_sql_12_27}")
    def const_sql_12_28 = """select "900000001.900000000", cast(cast("900000001.900000000" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_28_strict "${const_sql_12_28}"
    testFoldConst("${const_sql_12_28}")
    def const_sql_12_29 = """select "900000001.900000001", cast(cast("900000001.900000001" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_29_strict "${const_sql_12_29}"
    testFoldConst("${const_sql_12_29}")
    def const_sql_12_30 = """select "900000001.999999998", cast(cast("900000001.999999998" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_30_strict "${const_sql_12_30}"
    testFoldConst("${const_sql_12_30}")
    def const_sql_12_31 = """select "900000001.999999999", cast(cast("900000001.999999999" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_31_strict "${const_sql_12_31}"
    testFoldConst("${const_sql_12_31}")
    def const_sql_12_32 = """select "999999998.000000000", cast(cast("999999998.000000000" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_32_strict "${const_sql_12_32}"
    testFoldConst("${const_sql_12_32}")
    def const_sql_12_33 = """select "999999998.000000001", cast(cast("999999998.000000001" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_33_strict "${const_sql_12_33}"
    testFoldConst("${const_sql_12_33}")
    def const_sql_12_34 = """select "999999998.000000009", cast(cast("999999998.000000009" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_34_strict "${const_sql_12_34}"
    testFoldConst("${const_sql_12_34}")
    def const_sql_12_35 = """select "999999998.099999999", cast(cast("999999998.099999999" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_35_strict "${const_sql_12_35}"
    testFoldConst("${const_sql_12_35}")
    def const_sql_12_36 = """select "999999998.900000000", cast(cast("999999998.900000000" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_36_strict "${const_sql_12_36}"
    testFoldConst("${const_sql_12_36}")
    def const_sql_12_37 = """select "999999998.900000001", cast(cast("999999998.900000001" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_37_strict "${const_sql_12_37}"
    testFoldConst("${const_sql_12_37}")
    def const_sql_12_38 = """select "999999998.999999998", cast(cast("999999998.999999998" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_38_strict "${const_sql_12_38}"
    testFoldConst("${const_sql_12_38}")
    def const_sql_12_39 = """select "999999998.999999999", cast(cast("999999998.999999999" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_39_strict "${const_sql_12_39}"
    testFoldConst("${const_sql_12_39}")
    def const_sql_12_40 = """select "999999999.000000000", cast(cast("999999999.000000000" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_40_strict "${const_sql_12_40}"
    testFoldConst("${const_sql_12_40}")
    def const_sql_12_41 = """select "999999999.000000001", cast(cast("999999999.000000001" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_41_strict "${const_sql_12_41}"
    testFoldConst("${const_sql_12_41}")
    def const_sql_12_42 = """select "999999999.000000009", cast(cast("999999999.000000009" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_42_strict "${const_sql_12_42}"
    testFoldConst("${const_sql_12_42}")
    def const_sql_12_43 = """select "999999999.099999999", cast(cast("999999999.099999999" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_43_strict "${const_sql_12_43}"
    testFoldConst("${const_sql_12_43}")
    def const_sql_12_44 = """select "999999999.900000000", cast(cast("999999999.900000000" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_44_strict "${const_sql_12_44}"
    testFoldConst("${const_sql_12_44}")
    def const_sql_12_45 = """select "999999999.900000001", cast(cast("999999999.900000001" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_45_strict "${const_sql_12_45}"
    testFoldConst("${const_sql_12_45}")
    def const_sql_12_46 = """select "999999999.999999998", cast(cast("999999999.999999998" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_46_strict "${const_sql_12_46}"
    testFoldConst("${const_sql_12_46}")
    def const_sql_12_47 = """select "999999999.999999999", cast(cast("999999999.999999999" as decimalv3(18, 9)) as decimalv3(76, 0));"""
    qt_sql_12_47_strict "${const_sql_12_47}"
    testFoldConst("${const_sql_12_47}")

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
    qt_sql_12_24_non_strict "${const_sql_12_24}"
    testFoldConst("${const_sql_12_24}")
    qt_sql_12_25_non_strict "${const_sql_12_25}"
    testFoldConst("${const_sql_12_25}")
    qt_sql_12_26_non_strict "${const_sql_12_26}"
    testFoldConst("${const_sql_12_26}")
    qt_sql_12_27_non_strict "${const_sql_12_27}"
    testFoldConst("${const_sql_12_27}")
    qt_sql_12_28_non_strict "${const_sql_12_28}"
    testFoldConst("${const_sql_12_28}")
    qt_sql_12_29_non_strict "${const_sql_12_29}"
    testFoldConst("${const_sql_12_29}")
    qt_sql_12_30_non_strict "${const_sql_12_30}"
    testFoldConst("${const_sql_12_30}")
    qt_sql_12_31_non_strict "${const_sql_12_31}"
    testFoldConst("${const_sql_12_31}")
    qt_sql_12_32_non_strict "${const_sql_12_32}"
    testFoldConst("${const_sql_12_32}")
    qt_sql_12_33_non_strict "${const_sql_12_33}"
    testFoldConst("${const_sql_12_33}")
    qt_sql_12_34_non_strict "${const_sql_12_34}"
    testFoldConst("${const_sql_12_34}")
    qt_sql_12_35_non_strict "${const_sql_12_35}"
    testFoldConst("${const_sql_12_35}")
    qt_sql_12_36_non_strict "${const_sql_12_36}"
    testFoldConst("${const_sql_12_36}")
    qt_sql_12_37_non_strict "${const_sql_12_37}"
    testFoldConst("${const_sql_12_37}")
    qt_sql_12_38_non_strict "${const_sql_12_38}"
    testFoldConst("${const_sql_12_38}")
    qt_sql_12_39_non_strict "${const_sql_12_39}"
    testFoldConst("${const_sql_12_39}")
    qt_sql_12_40_non_strict "${const_sql_12_40}"
    testFoldConst("${const_sql_12_40}")
    qt_sql_12_41_non_strict "${const_sql_12_41}"
    testFoldConst("${const_sql_12_41}")
    qt_sql_12_42_non_strict "${const_sql_12_42}"
    testFoldConst("${const_sql_12_42}")
    qt_sql_12_43_non_strict "${const_sql_12_43}"
    testFoldConst("${const_sql_12_43}")
    qt_sql_12_44_non_strict "${const_sql_12_44}"
    testFoldConst("${const_sql_12_44}")
    qt_sql_12_45_non_strict "${const_sql_12_45}"
    testFoldConst("${const_sql_12_45}")
    qt_sql_12_46_non_strict "${const_sql_12_46}"
    testFoldConst("${const_sql_12_46}")
    qt_sql_12_47_non_strict "${const_sql_12_47}"
    testFoldConst("${const_sql_12_47}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_13_0 = """select "0.00000000000000000", cast(cast("0.00000000000000000" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_0_strict "${const_sql_13_0}"
    testFoldConst("${const_sql_13_0}")
    def const_sql_13_1 = """select "0.00000000000000001", cast(cast("0.00000000000000001" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_1_strict "${const_sql_13_1}"
    testFoldConst("${const_sql_13_1}")
    def const_sql_13_2 = """select "0.00000000000000009", cast(cast("0.00000000000000009" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_2_strict "${const_sql_13_2}"
    testFoldConst("${const_sql_13_2}")
    def const_sql_13_3 = """select "0.09999999999999999", cast(cast("0.09999999999999999" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_3_strict "${const_sql_13_3}"
    testFoldConst("${const_sql_13_3}")
    def const_sql_13_4 = """select "0.90000000000000000", cast(cast("0.90000000000000000" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_4_strict "${const_sql_13_4}"
    testFoldConst("${const_sql_13_4}")
    def const_sql_13_5 = """select "0.90000000000000001", cast(cast("0.90000000000000001" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_5_strict "${const_sql_13_5}"
    testFoldConst("${const_sql_13_5}")
    def const_sql_13_6 = """select "0.99999999999999998", cast(cast("0.99999999999999998" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_6_strict "${const_sql_13_6}"
    testFoldConst("${const_sql_13_6}")
    def const_sql_13_7 = """select "0.99999999999999999", cast(cast("0.99999999999999999" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_7_strict "${const_sql_13_7}"
    testFoldConst("${const_sql_13_7}")
    def const_sql_13_8 = """select "8.00000000000000000", cast(cast("8.00000000000000000" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_8_strict "${const_sql_13_8}"
    testFoldConst("${const_sql_13_8}")
    def const_sql_13_9 = """select "8.00000000000000001", cast(cast("8.00000000000000001" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_9_strict "${const_sql_13_9}"
    testFoldConst("${const_sql_13_9}")
    def const_sql_13_10 = """select "8.00000000000000009", cast(cast("8.00000000000000009" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_10_strict "${const_sql_13_10}"
    testFoldConst("${const_sql_13_10}")
    def const_sql_13_11 = """select "8.09999999999999999", cast(cast("8.09999999999999999" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_11_strict "${const_sql_13_11}"
    testFoldConst("${const_sql_13_11}")
    def const_sql_13_12 = """select "8.90000000000000000", cast(cast("8.90000000000000000" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_12_strict "${const_sql_13_12}"
    testFoldConst("${const_sql_13_12}")
    def const_sql_13_13 = """select "8.90000000000000001", cast(cast("8.90000000000000001" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_13_strict "${const_sql_13_13}"
    testFoldConst("${const_sql_13_13}")
    def const_sql_13_14 = """select "8.99999999999999998", cast(cast("8.99999999999999998" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_14_strict "${const_sql_13_14}"
    testFoldConst("${const_sql_13_14}")
    def const_sql_13_15 = """select "8.99999999999999999", cast(cast("8.99999999999999999" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_15_strict "${const_sql_13_15}"
    testFoldConst("${const_sql_13_15}")
    def const_sql_13_16 = """select "9.00000000000000000", cast(cast("9.00000000000000000" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_16_strict "${const_sql_13_16}"
    testFoldConst("${const_sql_13_16}")
    def const_sql_13_17 = """select "9.00000000000000001", cast(cast("9.00000000000000001" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_17_strict "${const_sql_13_17}"
    testFoldConst("${const_sql_13_17}")
    def const_sql_13_18 = """select "9.00000000000000009", cast(cast("9.00000000000000009" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_18_strict "${const_sql_13_18}"
    testFoldConst("${const_sql_13_18}")
    def const_sql_13_19 = """select "9.09999999999999999", cast(cast("9.09999999999999999" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_19_strict "${const_sql_13_19}"
    testFoldConst("${const_sql_13_19}")
    def const_sql_13_20 = """select "9.90000000000000000", cast(cast("9.90000000000000000" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_20_strict "${const_sql_13_20}"
    testFoldConst("${const_sql_13_20}")
    def const_sql_13_21 = """select "9.90000000000000001", cast(cast("9.90000000000000001" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_21_strict "${const_sql_13_21}"
    testFoldConst("${const_sql_13_21}")
    def const_sql_13_22 = """select "9.99999999999999998", cast(cast("9.99999999999999998" as decimalv3(18, 17)) as decimalv3(76, 0));"""
    qt_sql_13_22_strict "${const_sql_13_22}"
    testFoldConst("${const_sql_13_22}")
    def const_sql_13_23 = """select "9.99999999999999999", cast(cast("9.99999999999999999" as decimalv3(18, 17)) as decimalv3(76, 0));"""
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
    def const_sql_14_0 = """select "0.000000000000000000", cast(cast("0.000000000000000000" as decimalv3(18, 18)) as decimalv3(76, 0));"""
    qt_sql_14_0_strict "${const_sql_14_0}"
    testFoldConst("${const_sql_14_0}")
    def const_sql_14_1 = """select "0.000000000000000001", cast(cast("0.000000000000000001" as decimalv3(18, 18)) as decimalv3(76, 0));"""
    qt_sql_14_1_strict "${const_sql_14_1}"
    testFoldConst("${const_sql_14_1}")
    def const_sql_14_2 = """select "0.000000000000000009", cast(cast("0.000000000000000009" as decimalv3(18, 18)) as decimalv3(76, 0));"""
    qt_sql_14_2_strict "${const_sql_14_2}"
    testFoldConst("${const_sql_14_2}")
    def const_sql_14_3 = """select "0.099999999999999999", cast(cast("0.099999999999999999" as decimalv3(18, 18)) as decimalv3(76, 0));"""
    qt_sql_14_3_strict "${const_sql_14_3}"
    testFoldConst("${const_sql_14_3}")
    def const_sql_14_4 = """select "0.900000000000000000", cast(cast("0.900000000000000000" as decimalv3(18, 18)) as decimalv3(76, 0));"""
    qt_sql_14_4_strict "${const_sql_14_4}"
    testFoldConst("${const_sql_14_4}")
    def const_sql_14_5 = """select "0.900000000000000001", cast(cast("0.900000000000000001" as decimalv3(18, 18)) as decimalv3(76, 0));"""
    qt_sql_14_5_strict "${const_sql_14_5}"
    testFoldConst("${const_sql_14_5}")
    def const_sql_14_6 = """select "0.999999999999999998", cast(cast("0.999999999999999998" as decimalv3(18, 18)) as decimalv3(76, 0));"""
    qt_sql_14_6_strict "${const_sql_14_6}"
    testFoldConst("${const_sql_14_6}")
    def const_sql_14_7 = """select "0.999999999999999999", cast(cast("0.999999999999999999" as decimalv3(18, 18)) as decimalv3(76, 0));"""
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