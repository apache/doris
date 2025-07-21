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


suite("test_cast_to_decimal256_76_1_from_decimal32_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_decimal256 = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "0", cast(cast("0" as decimalv3(1, 0)) as decimalv3(76, 1));"""
    qt_sql_0_0_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    def const_sql_0_1 = """select "8", cast(cast("8" as decimalv3(1, 0)) as decimalv3(76, 1));"""
    qt_sql_0_1_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    def const_sql_0_2 = """select "9", cast(cast("9" as decimalv3(1, 0)) as decimalv3(76, 1));"""
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
    def const_sql_1_0 = """select "0.0", cast(cast("0.0" as decimalv3(1, 1)) as decimalv3(76, 1));"""
    qt_sql_1_0_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    def const_sql_1_1 = """select "0.1", cast(cast("0.1" as decimalv3(1, 1)) as decimalv3(76, 1));"""
    qt_sql_1_1_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    def const_sql_1_2 = """select "0.8", cast(cast("0.8" as decimalv3(1, 1)) as decimalv3(76, 1));"""
    qt_sql_1_2_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    def const_sql_1_3 = """select "0.9", cast(cast("0.9" as decimalv3(1, 1)) as decimalv3(76, 1));"""
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
    def const_sql_2_0 = """select "0", cast(cast("0" as decimalv3(4, 0)) as decimalv3(76, 1));"""
    qt_sql_2_0_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    def const_sql_2_1 = """select "999", cast(cast("999" as decimalv3(4, 0)) as decimalv3(76, 1));"""
    qt_sql_2_1_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    def const_sql_2_2 = """select "9000", cast(cast("9000" as decimalv3(4, 0)) as decimalv3(76, 1));"""
    qt_sql_2_2_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    def const_sql_2_3 = """select "9001", cast(cast("9001" as decimalv3(4, 0)) as decimalv3(76, 1));"""
    qt_sql_2_3_strict "${const_sql_2_3}"
    testFoldConst("${const_sql_2_3}")
    def const_sql_2_4 = """select "9998", cast(cast("9998" as decimalv3(4, 0)) as decimalv3(76, 1));"""
    qt_sql_2_4_strict "${const_sql_2_4}"
    testFoldConst("${const_sql_2_4}")
    def const_sql_2_5 = """select "9999", cast(cast("9999" as decimalv3(4, 0)) as decimalv3(76, 1));"""
    qt_sql_2_5_strict "${const_sql_2_5}"
    testFoldConst("${const_sql_2_5}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_3_0 = """select "0.0", cast(cast("0.0" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_0_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    def const_sql_3_1 = """select "0.1", cast(cast("0.1" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_1_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    def const_sql_3_2 = """select "0.8", cast(cast("0.8" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_2_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    def const_sql_3_3 = """select "0.9", cast(cast("0.9" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_3_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    def const_sql_3_4 = """select "99.0", cast(cast("99.0" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_4_strict "${const_sql_3_4}"
    testFoldConst("${const_sql_3_4}")
    def const_sql_3_5 = """select "99.1", cast(cast("99.1" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_5_strict "${const_sql_3_5}"
    testFoldConst("${const_sql_3_5}")
    def const_sql_3_6 = """select "99.8", cast(cast("99.8" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_6_strict "${const_sql_3_6}"
    testFoldConst("${const_sql_3_6}")
    def const_sql_3_7 = """select "99.9", cast(cast("99.9" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_7_strict "${const_sql_3_7}"
    testFoldConst("${const_sql_3_7}")
    def const_sql_3_8 = """select "900.0", cast(cast("900.0" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_8_strict "${const_sql_3_8}"
    testFoldConst("${const_sql_3_8}")
    def const_sql_3_9 = """select "900.1", cast(cast("900.1" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_9_strict "${const_sql_3_9}"
    testFoldConst("${const_sql_3_9}")
    def const_sql_3_10 = """select "900.8", cast(cast("900.8" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_10_strict "${const_sql_3_10}"
    testFoldConst("${const_sql_3_10}")
    def const_sql_3_11 = """select "900.9", cast(cast("900.9" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_11_strict "${const_sql_3_11}"
    testFoldConst("${const_sql_3_11}")
    def const_sql_3_12 = """select "901.0", cast(cast("901.0" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_12_strict "${const_sql_3_12}"
    testFoldConst("${const_sql_3_12}")
    def const_sql_3_13 = """select "901.1", cast(cast("901.1" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_13_strict "${const_sql_3_13}"
    testFoldConst("${const_sql_3_13}")
    def const_sql_3_14 = """select "901.8", cast(cast("901.8" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_14_strict "${const_sql_3_14}"
    testFoldConst("${const_sql_3_14}")
    def const_sql_3_15 = """select "901.9", cast(cast("901.9" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_15_strict "${const_sql_3_15}"
    testFoldConst("${const_sql_3_15}")
    def const_sql_3_16 = """select "998.0", cast(cast("998.0" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_16_strict "${const_sql_3_16}"
    testFoldConst("${const_sql_3_16}")
    def const_sql_3_17 = """select "998.1", cast(cast("998.1" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_17_strict "${const_sql_3_17}"
    testFoldConst("${const_sql_3_17}")
    def const_sql_3_18 = """select "998.8", cast(cast("998.8" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_18_strict "${const_sql_3_18}"
    testFoldConst("${const_sql_3_18}")
    def const_sql_3_19 = """select "998.9", cast(cast("998.9" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_19_strict "${const_sql_3_19}"
    testFoldConst("${const_sql_3_19}")
    def const_sql_3_20 = """select "999.0", cast(cast("999.0" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_20_strict "${const_sql_3_20}"
    testFoldConst("${const_sql_3_20}")
    def const_sql_3_21 = """select "999.1", cast(cast("999.1" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_21_strict "${const_sql_3_21}"
    testFoldConst("${const_sql_3_21}")
    def const_sql_3_22 = """select "999.8", cast(cast("999.8" as decimalv3(4, 1)) as decimalv3(76, 1));"""
    qt_sql_3_22_strict "${const_sql_3_22}"
    testFoldConst("${const_sql_3_22}")
    def const_sql_3_23 = """select "999.9", cast(cast("999.9" as decimalv3(4, 1)) as decimalv3(76, 1));"""
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
    def const_sql_4_0 = """select "0.00", cast(cast("0.00" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_0_strict "${const_sql_4_0}"
    testFoldConst("${const_sql_4_0}")
    def const_sql_4_1 = """select "0.01", cast(cast("0.01" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_1_strict "${const_sql_4_1}"
    testFoldConst("${const_sql_4_1}")
    def const_sql_4_2 = """select "0.09", cast(cast("0.09" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_2_strict "${const_sql_4_2}"
    testFoldConst("${const_sql_4_2}")
    def const_sql_4_3 = """select "0.90", cast(cast("0.90" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_3_strict "${const_sql_4_3}"
    testFoldConst("${const_sql_4_3}")
    def const_sql_4_4 = """select "0.91", cast(cast("0.91" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_4_strict "${const_sql_4_4}"
    testFoldConst("${const_sql_4_4}")
    def const_sql_4_5 = """select "0.98", cast(cast("0.98" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_5_strict "${const_sql_4_5}"
    testFoldConst("${const_sql_4_5}")
    def const_sql_4_6 = """select "0.99", cast(cast("0.99" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_6_strict "${const_sql_4_6}"
    testFoldConst("${const_sql_4_6}")
    def const_sql_4_7 = """select "9.00", cast(cast("9.00" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_7_strict "${const_sql_4_7}"
    testFoldConst("${const_sql_4_7}")
    def const_sql_4_8 = """select "9.01", cast(cast("9.01" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_8_strict "${const_sql_4_8}"
    testFoldConst("${const_sql_4_8}")
    def const_sql_4_9 = """select "9.09", cast(cast("9.09" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_9_strict "${const_sql_4_9}"
    testFoldConst("${const_sql_4_9}")
    def const_sql_4_10 = """select "9.90", cast(cast("9.90" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_10_strict "${const_sql_4_10}"
    testFoldConst("${const_sql_4_10}")
    def const_sql_4_11 = """select "9.91", cast(cast("9.91" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_11_strict "${const_sql_4_11}"
    testFoldConst("${const_sql_4_11}")
    def const_sql_4_12 = """select "9.98", cast(cast("9.98" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_12_strict "${const_sql_4_12}"
    testFoldConst("${const_sql_4_12}")
    def const_sql_4_13 = """select "9.99", cast(cast("9.99" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_13_strict "${const_sql_4_13}"
    testFoldConst("${const_sql_4_13}")
    def const_sql_4_14 = """select "90.00", cast(cast("90.00" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_14_strict "${const_sql_4_14}"
    testFoldConst("${const_sql_4_14}")
    def const_sql_4_15 = """select "90.01", cast(cast("90.01" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_15_strict "${const_sql_4_15}"
    testFoldConst("${const_sql_4_15}")
    def const_sql_4_16 = """select "90.09", cast(cast("90.09" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_16_strict "${const_sql_4_16}"
    testFoldConst("${const_sql_4_16}")
    def const_sql_4_17 = """select "90.90", cast(cast("90.90" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_17_strict "${const_sql_4_17}"
    testFoldConst("${const_sql_4_17}")
    def const_sql_4_18 = """select "90.91", cast(cast("90.91" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_18_strict "${const_sql_4_18}"
    testFoldConst("${const_sql_4_18}")
    def const_sql_4_19 = """select "90.98", cast(cast("90.98" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_19_strict "${const_sql_4_19}"
    testFoldConst("${const_sql_4_19}")
    def const_sql_4_20 = """select "90.99", cast(cast("90.99" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_20_strict "${const_sql_4_20}"
    testFoldConst("${const_sql_4_20}")
    def const_sql_4_21 = """select "91.00", cast(cast("91.00" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_21_strict "${const_sql_4_21}"
    testFoldConst("${const_sql_4_21}")
    def const_sql_4_22 = """select "91.01", cast(cast("91.01" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_22_strict "${const_sql_4_22}"
    testFoldConst("${const_sql_4_22}")
    def const_sql_4_23 = """select "91.09", cast(cast("91.09" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_23_strict "${const_sql_4_23}"
    testFoldConst("${const_sql_4_23}")
    def const_sql_4_24 = """select "91.90", cast(cast("91.90" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_24_strict "${const_sql_4_24}"
    testFoldConst("${const_sql_4_24}")
    def const_sql_4_25 = """select "91.91", cast(cast("91.91" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_25_strict "${const_sql_4_25}"
    testFoldConst("${const_sql_4_25}")
    def const_sql_4_26 = """select "91.98", cast(cast("91.98" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_26_strict "${const_sql_4_26}"
    testFoldConst("${const_sql_4_26}")
    def const_sql_4_27 = """select "91.99", cast(cast("91.99" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_27_strict "${const_sql_4_27}"
    testFoldConst("${const_sql_4_27}")
    def const_sql_4_28 = """select "98.00", cast(cast("98.00" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_28_strict "${const_sql_4_28}"
    testFoldConst("${const_sql_4_28}")
    def const_sql_4_29 = """select "98.01", cast(cast("98.01" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_29_strict "${const_sql_4_29}"
    testFoldConst("${const_sql_4_29}")
    def const_sql_4_30 = """select "98.09", cast(cast("98.09" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_30_strict "${const_sql_4_30}"
    testFoldConst("${const_sql_4_30}")
    def const_sql_4_31 = """select "98.90", cast(cast("98.90" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_31_strict "${const_sql_4_31}"
    testFoldConst("${const_sql_4_31}")
    def const_sql_4_32 = """select "98.91", cast(cast("98.91" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_32_strict "${const_sql_4_32}"
    testFoldConst("${const_sql_4_32}")
    def const_sql_4_33 = """select "98.98", cast(cast("98.98" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_33_strict "${const_sql_4_33}"
    testFoldConst("${const_sql_4_33}")
    def const_sql_4_34 = """select "98.99", cast(cast("98.99" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_34_strict "${const_sql_4_34}"
    testFoldConst("${const_sql_4_34}")
    def const_sql_4_35 = """select "99.00", cast(cast("99.00" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_35_strict "${const_sql_4_35}"
    testFoldConst("${const_sql_4_35}")
    def const_sql_4_36 = """select "99.01", cast(cast("99.01" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_36_strict "${const_sql_4_36}"
    testFoldConst("${const_sql_4_36}")
    def const_sql_4_37 = """select "99.09", cast(cast("99.09" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_37_strict "${const_sql_4_37}"
    testFoldConst("${const_sql_4_37}")
    def const_sql_4_38 = """select "99.90", cast(cast("99.90" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_38_strict "${const_sql_4_38}"
    testFoldConst("${const_sql_4_38}")
    def const_sql_4_39 = """select "99.91", cast(cast("99.91" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_39_strict "${const_sql_4_39}"
    testFoldConst("${const_sql_4_39}")
    def const_sql_4_40 = """select "99.98", cast(cast("99.98" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_40_strict "${const_sql_4_40}"
    testFoldConst("${const_sql_4_40}")
    def const_sql_4_41 = """select "99.99", cast(cast("99.99" as decimalv3(4, 2)) as decimalv3(76, 1));"""
    qt_sql_4_41_strict "${const_sql_4_41}"
    testFoldConst("${const_sql_4_41}")

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
    qt_sql_4_30_non_strict "${const_sql_4_30}"
    testFoldConst("${const_sql_4_30}")
    qt_sql_4_31_non_strict "${const_sql_4_31}"
    testFoldConst("${const_sql_4_31}")
    qt_sql_4_32_non_strict "${const_sql_4_32}"
    testFoldConst("${const_sql_4_32}")
    qt_sql_4_33_non_strict "${const_sql_4_33}"
    testFoldConst("${const_sql_4_33}")
    qt_sql_4_34_non_strict "${const_sql_4_34}"
    testFoldConst("${const_sql_4_34}")
    qt_sql_4_35_non_strict "${const_sql_4_35}"
    testFoldConst("${const_sql_4_35}")
    qt_sql_4_36_non_strict "${const_sql_4_36}"
    testFoldConst("${const_sql_4_36}")
    qt_sql_4_37_non_strict "${const_sql_4_37}"
    testFoldConst("${const_sql_4_37}")
    qt_sql_4_38_non_strict "${const_sql_4_38}"
    testFoldConst("${const_sql_4_38}")
    qt_sql_4_39_non_strict "${const_sql_4_39}"
    testFoldConst("${const_sql_4_39}")
    qt_sql_4_40_non_strict "${const_sql_4_40}"
    testFoldConst("${const_sql_4_40}")
    qt_sql_4_41_non_strict "${const_sql_4_41}"
    testFoldConst("${const_sql_4_41}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_5_0 = """select "0.000", cast(cast("0.000" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_0_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")
    def const_sql_5_1 = """select "0.001", cast(cast("0.001" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_1_strict "${const_sql_5_1}"
    testFoldConst("${const_sql_5_1}")
    def const_sql_5_2 = """select "0.009", cast(cast("0.009" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_2_strict "${const_sql_5_2}"
    testFoldConst("${const_sql_5_2}")
    def const_sql_5_3 = """select "0.099", cast(cast("0.099" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_3_strict "${const_sql_5_3}"
    testFoldConst("${const_sql_5_3}")
    def const_sql_5_4 = """select "0.900", cast(cast("0.900" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_4_strict "${const_sql_5_4}"
    testFoldConst("${const_sql_5_4}")
    def const_sql_5_5 = """select "0.901", cast(cast("0.901" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_5_strict "${const_sql_5_5}"
    testFoldConst("${const_sql_5_5}")
    def const_sql_5_6 = """select "0.998", cast(cast("0.998" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_6_strict "${const_sql_5_6}"
    testFoldConst("${const_sql_5_6}")
    def const_sql_5_7 = """select "0.999", cast(cast("0.999" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_7_strict "${const_sql_5_7}"
    testFoldConst("${const_sql_5_7}")
    def const_sql_5_8 = """select "8.000", cast(cast("8.000" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_8_strict "${const_sql_5_8}"
    testFoldConst("${const_sql_5_8}")
    def const_sql_5_9 = """select "8.001", cast(cast("8.001" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_9_strict "${const_sql_5_9}"
    testFoldConst("${const_sql_5_9}")
    def const_sql_5_10 = """select "8.009", cast(cast("8.009" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_10_strict "${const_sql_5_10}"
    testFoldConst("${const_sql_5_10}")
    def const_sql_5_11 = """select "8.099", cast(cast("8.099" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_11_strict "${const_sql_5_11}"
    testFoldConst("${const_sql_5_11}")
    def const_sql_5_12 = """select "8.900", cast(cast("8.900" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_12_strict "${const_sql_5_12}"
    testFoldConst("${const_sql_5_12}")
    def const_sql_5_13 = """select "8.901", cast(cast("8.901" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_13_strict "${const_sql_5_13}"
    testFoldConst("${const_sql_5_13}")
    def const_sql_5_14 = """select "8.998", cast(cast("8.998" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_14_strict "${const_sql_5_14}"
    testFoldConst("${const_sql_5_14}")
    def const_sql_5_15 = """select "8.999", cast(cast("8.999" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_15_strict "${const_sql_5_15}"
    testFoldConst("${const_sql_5_15}")
    def const_sql_5_16 = """select "9.000", cast(cast("9.000" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_16_strict "${const_sql_5_16}"
    testFoldConst("${const_sql_5_16}")
    def const_sql_5_17 = """select "9.001", cast(cast("9.001" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_17_strict "${const_sql_5_17}"
    testFoldConst("${const_sql_5_17}")
    def const_sql_5_18 = """select "9.009", cast(cast("9.009" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_18_strict "${const_sql_5_18}"
    testFoldConst("${const_sql_5_18}")
    def const_sql_5_19 = """select "9.099", cast(cast("9.099" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_19_strict "${const_sql_5_19}"
    testFoldConst("${const_sql_5_19}")
    def const_sql_5_20 = """select "9.900", cast(cast("9.900" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_20_strict "${const_sql_5_20}"
    testFoldConst("${const_sql_5_20}")
    def const_sql_5_21 = """select "9.901", cast(cast("9.901" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_21_strict "${const_sql_5_21}"
    testFoldConst("${const_sql_5_21}")
    def const_sql_5_22 = """select "9.998", cast(cast("9.998" as decimalv3(4, 3)) as decimalv3(76, 1));"""
    qt_sql_5_22_strict "${const_sql_5_22}"
    testFoldConst("${const_sql_5_22}")
    def const_sql_5_23 = """select "9.999", cast(cast("9.999" as decimalv3(4, 3)) as decimalv3(76, 1));"""
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
    def const_sql_6_0 = """select "0.0000", cast(cast("0.0000" as decimalv3(4, 4)) as decimalv3(76, 1));"""
    qt_sql_6_0_strict "${const_sql_6_0}"
    testFoldConst("${const_sql_6_0}")
    def const_sql_6_1 = """select "0.0001", cast(cast("0.0001" as decimalv3(4, 4)) as decimalv3(76, 1));"""
    qt_sql_6_1_strict "${const_sql_6_1}"
    testFoldConst("${const_sql_6_1}")
    def const_sql_6_2 = """select "0.0009", cast(cast("0.0009" as decimalv3(4, 4)) as decimalv3(76, 1));"""
    qt_sql_6_2_strict "${const_sql_6_2}"
    testFoldConst("${const_sql_6_2}")
    def const_sql_6_3 = """select "0.0999", cast(cast("0.0999" as decimalv3(4, 4)) as decimalv3(76, 1));"""
    qt_sql_6_3_strict "${const_sql_6_3}"
    testFoldConst("${const_sql_6_3}")
    def const_sql_6_4 = """select "0.9000", cast(cast("0.9000" as decimalv3(4, 4)) as decimalv3(76, 1));"""
    qt_sql_6_4_strict "${const_sql_6_4}"
    testFoldConst("${const_sql_6_4}")
    def const_sql_6_5 = """select "0.9001", cast(cast("0.9001" as decimalv3(4, 4)) as decimalv3(76, 1));"""
    qt_sql_6_5_strict "${const_sql_6_5}"
    testFoldConst("${const_sql_6_5}")
    def const_sql_6_6 = """select "0.9998", cast(cast("0.9998" as decimalv3(4, 4)) as decimalv3(76, 1));"""
    qt_sql_6_6_strict "${const_sql_6_6}"
    testFoldConst("${const_sql_6_6}")
    def const_sql_6_7 = """select "0.9999", cast(cast("0.9999" as decimalv3(4, 4)) as decimalv3(76, 1));"""
    qt_sql_6_7_strict "${const_sql_6_7}"
    testFoldConst("${const_sql_6_7}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_7_0 = """select "0", cast(cast("0" as decimalv3(8, 0)) as decimalv3(76, 1));"""
    qt_sql_7_0_strict "${const_sql_7_0}"
    testFoldConst("${const_sql_7_0}")
    def const_sql_7_1 = """select "9999999", cast(cast("9999999" as decimalv3(8, 0)) as decimalv3(76, 1));"""
    qt_sql_7_1_strict "${const_sql_7_1}"
    testFoldConst("${const_sql_7_1}")
    def const_sql_7_2 = """select "90000000", cast(cast("90000000" as decimalv3(8, 0)) as decimalv3(76, 1));"""
    qt_sql_7_2_strict "${const_sql_7_2}"
    testFoldConst("${const_sql_7_2}")
    def const_sql_7_3 = """select "90000001", cast(cast("90000001" as decimalv3(8, 0)) as decimalv3(76, 1));"""
    qt_sql_7_3_strict "${const_sql_7_3}"
    testFoldConst("${const_sql_7_3}")
    def const_sql_7_4 = """select "99999998", cast(cast("99999998" as decimalv3(8, 0)) as decimalv3(76, 1));"""
    qt_sql_7_4_strict "${const_sql_7_4}"
    testFoldConst("${const_sql_7_4}")
    def const_sql_7_5 = """select "99999999", cast(cast("99999999" as decimalv3(8, 0)) as decimalv3(76, 1));"""
    qt_sql_7_5_strict "${const_sql_7_5}"
    testFoldConst("${const_sql_7_5}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_8_0 = """select "0.0", cast(cast("0.0" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_0_strict "${const_sql_8_0}"
    testFoldConst("${const_sql_8_0}")
    def const_sql_8_1 = """select "0.1", cast(cast("0.1" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_1_strict "${const_sql_8_1}"
    testFoldConst("${const_sql_8_1}")
    def const_sql_8_2 = """select "0.8", cast(cast("0.8" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_2_strict "${const_sql_8_2}"
    testFoldConst("${const_sql_8_2}")
    def const_sql_8_3 = """select "0.9", cast(cast("0.9" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_3_strict "${const_sql_8_3}"
    testFoldConst("${const_sql_8_3}")
    def const_sql_8_4 = """select "999999.0", cast(cast("999999.0" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_4_strict "${const_sql_8_4}"
    testFoldConst("${const_sql_8_4}")
    def const_sql_8_5 = """select "999999.1", cast(cast("999999.1" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_5_strict "${const_sql_8_5}"
    testFoldConst("${const_sql_8_5}")
    def const_sql_8_6 = """select "999999.8", cast(cast("999999.8" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_6_strict "${const_sql_8_6}"
    testFoldConst("${const_sql_8_6}")
    def const_sql_8_7 = """select "999999.9", cast(cast("999999.9" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_7_strict "${const_sql_8_7}"
    testFoldConst("${const_sql_8_7}")
    def const_sql_8_8 = """select "9000000.0", cast(cast("9000000.0" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_8_strict "${const_sql_8_8}"
    testFoldConst("${const_sql_8_8}")
    def const_sql_8_9 = """select "9000000.1", cast(cast("9000000.1" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_9_strict "${const_sql_8_9}"
    testFoldConst("${const_sql_8_9}")
    def const_sql_8_10 = """select "9000000.8", cast(cast("9000000.8" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_10_strict "${const_sql_8_10}"
    testFoldConst("${const_sql_8_10}")
    def const_sql_8_11 = """select "9000000.9", cast(cast("9000000.9" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_11_strict "${const_sql_8_11}"
    testFoldConst("${const_sql_8_11}")
    def const_sql_8_12 = """select "9000001.0", cast(cast("9000001.0" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_12_strict "${const_sql_8_12}"
    testFoldConst("${const_sql_8_12}")
    def const_sql_8_13 = """select "9000001.1", cast(cast("9000001.1" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_13_strict "${const_sql_8_13}"
    testFoldConst("${const_sql_8_13}")
    def const_sql_8_14 = """select "9000001.8", cast(cast("9000001.8" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_14_strict "${const_sql_8_14}"
    testFoldConst("${const_sql_8_14}")
    def const_sql_8_15 = """select "9000001.9", cast(cast("9000001.9" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_15_strict "${const_sql_8_15}"
    testFoldConst("${const_sql_8_15}")
    def const_sql_8_16 = """select "9999998.0", cast(cast("9999998.0" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_16_strict "${const_sql_8_16}"
    testFoldConst("${const_sql_8_16}")
    def const_sql_8_17 = """select "9999998.1", cast(cast("9999998.1" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_17_strict "${const_sql_8_17}"
    testFoldConst("${const_sql_8_17}")
    def const_sql_8_18 = """select "9999998.8", cast(cast("9999998.8" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_18_strict "${const_sql_8_18}"
    testFoldConst("${const_sql_8_18}")
    def const_sql_8_19 = """select "9999998.9", cast(cast("9999998.9" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_19_strict "${const_sql_8_19}"
    testFoldConst("${const_sql_8_19}")
    def const_sql_8_20 = """select "9999999.0", cast(cast("9999999.0" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_20_strict "${const_sql_8_20}"
    testFoldConst("${const_sql_8_20}")
    def const_sql_8_21 = """select "9999999.1", cast(cast("9999999.1" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_21_strict "${const_sql_8_21}"
    testFoldConst("${const_sql_8_21}")
    def const_sql_8_22 = """select "9999999.8", cast(cast("9999999.8" as decimalv3(8, 1)) as decimalv3(76, 1));"""
    qt_sql_8_22_strict "${const_sql_8_22}"
    testFoldConst("${const_sql_8_22}")
    def const_sql_8_23 = """select "9999999.9", cast(cast("9999999.9" as decimalv3(8, 1)) as decimalv3(76, 1));"""
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
    def const_sql_9_0 = """select "0.0000", cast(cast("0.0000" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_0_strict "${const_sql_9_0}"
    testFoldConst("${const_sql_9_0}")
    def const_sql_9_1 = """select "0.0001", cast(cast("0.0001" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_1_strict "${const_sql_9_1}"
    testFoldConst("${const_sql_9_1}")
    def const_sql_9_2 = """select "0.0009", cast(cast("0.0009" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_2_strict "${const_sql_9_2}"
    testFoldConst("${const_sql_9_2}")
    def const_sql_9_3 = """select "0.0999", cast(cast("0.0999" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_3_strict "${const_sql_9_3}"
    testFoldConst("${const_sql_9_3}")
    def const_sql_9_4 = """select "0.9000", cast(cast("0.9000" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_4_strict "${const_sql_9_4}"
    testFoldConst("${const_sql_9_4}")
    def const_sql_9_5 = """select "0.9001", cast(cast("0.9001" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_5_strict "${const_sql_9_5}"
    testFoldConst("${const_sql_9_5}")
    def const_sql_9_6 = """select "0.9998", cast(cast("0.9998" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_6_strict "${const_sql_9_6}"
    testFoldConst("${const_sql_9_6}")
    def const_sql_9_7 = """select "0.9999", cast(cast("0.9999" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_7_strict "${const_sql_9_7}"
    testFoldConst("${const_sql_9_7}")
    def const_sql_9_8 = """select "999.0000", cast(cast("999.0000" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_8_strict "${const_sql_9_8}"
    testFoldConst("${const_sql_9_8}")
    def const_sql_9_9 = """select "999.0001", cast(cast("999.0001" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_9_strict "${const_sql_9_9}"
    testFoldConst("${const_sql_9_9}")
    def const_sql_9_10 = """select "999.0009", cast(cast("999.0009" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_10_strict "${const_sql_9_10}"
    testFoldConst("${const_sql_9_10}")
    def const_sql_9_11 = """select "999.0999", cast(cast("999.0999" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_11_strict "${const_sql_9_11}"
    testFoldConst("${const_sql_9_11}")
    def const_sql_9_12 = """select "999.9000", cast(cast("999.9000" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_12_strict "${const_sql_9_12}"
    testFoldConst("${const_sql_9_12}")
    def const_sql_9_13 = """select "999.9001", cast(cast("999.9001" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_13_strict "${const_sql_9_13}"
    testFoldConst("${const_sql_9_13}")
    def const_sql_9_14 = """select "999.9998", cast(cast("999.9998" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_14_strict "${const_sql_9_14}"
    testFoldConst("${const_sql_9_14}")
    def const_sql_9_15 = """select "999.9999", cast(cast("999.9999" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_15_strict "${const_sql_9_15}"
    testFoldConst("${const_sql_9_15}")
    def const_sql_9_16 = """select "9000.0000", cast(cast("9000.0000" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_16_strict "${const_sql_9_16}"
    testFoldConst("${const_sql_9_16}")
    def const_sql_9_17 = """select "9000.0001", cast(cast("9000.0001" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_17_strict "${const_sql_9_17}"
    testFoldConst("${const_sql_9_17}")
    def const_sql_9_18 = """select "9000.0009", cast(cast("9000.0009" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_18_strict "${const_sql_9_18}"
    testFoldConst("${const_sql_9_18}")
    def const_sql_9_19 = """select "9000.0999", cast(cast("9000.0999" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_19_strict "${const_sql_9_19}"
    testFoldConst("${const_sql_9_19}")
    def const_sql_9_20 = """select "9000.9000", cast(cast("9000.9000" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_20_strict "${const_sql_9_20}"
    testFoldConst("${const_sql_9_20}")
    def const_sql_9_21 = """select "9000.9001", cast(cast("9000.9001" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_21_strict "${const_sql_9_21}"
    testFoldConst("${const_sql_9_21}")
    def const_sql_9_22 = """select "9000.9998", cast(cast("9000.9998" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_22_strict "${const_sql_9_22}"
    testFoldConst("${const_sql_9_22}")
    def const_sql_9_23 = """select "9000.9999", cast(cast("9000.9999" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_23_strict "${const_sql_9_23}"
    testFoldConst("${const_sql_9_23}")
    def const_sql_9_24 = """select "9001.0000", cast(cast("9001.0000" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_24_strict "${const_sql_9_24}"
    testFoldConst("${const_sql_9_24}")
    def const_sql_9_25 = """select "9001.0001", cast(cast("9001.0001" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_25_strict "${const_sql_9_25}"
    testFoldConst("${const_sql_9_25}")
    def const_sql_9_26 = """select "9001.0009", cast(cast("9001.0009" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_26_strict "${const_sql_9_26}"
    testFoldConst("${const_sql_9_26}")
    def const_sql_9_27 = """select "9001.0999", cast(cast("9001.0999" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_27_strict "${const_sql_9_27}"
    testFoldConst("${const_sql_9_27}")
    def const_sql_9_28 = """select "9001.9000", cast(cast("9001.9000" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_28_strict "${const_sql_9_28}"
    testFoldConst("${const_sql_9_28}")
    def const_sql_9_29 = """select "9001.9001", cast(cast("9001.9001" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_29_strict "${const_sql_9_29}"
    testFoldConst("${const_sql_9_29}")
    def const_sql_9_30 = """select "9001.9998", cast(cast("9001.9998" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_30_strict "${const_sql_9_30}"
    testFoldConst("${const_sql_9_30}")
    def const_sql_9_31 = """select "9001.9999", cast(cast("9001.9999" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_31_strict "${const_sql_9_31}"
    testFoldConst("${const_sql_9_31}")
    def const_sql_9_32 = """select "9998.0000", cast(cast("9998.0000" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_32_strict "${const_sql_9_32}"
    testFoldConst("${const_sql_9_32}")
    def const_sql_9_33 = """select "9998.0001", cast(cast("9998.0001" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_33_strict "${const_sql_9_33}"
    testFoldConst("${const_sql_9_33}")
    def const_sql_9_34 = """select "9998.0009", cast(cast("9998.0009" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_34_strict "${const_sql_9_34}"
    testFoldConst("${const_sql_9_34}")
    def const_sql_9_35 = """select "9998.0999", cast(cast("9998.0999" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_35_strict "${const_sql_9_35}"
    testFoldConst("${const_sql_9_35}")
    def const_sql_9_36 = """select "9998.9000", cast(cast("9998.9000" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_36_strict "${const_sql_9_36}"
    testFoldConst("${const_sql_9_36}")
    def const_sql_9_37 = """select "9998.9001", cast(cast("9998.9001" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_37_strict "${const_sql_9_37}"
    testFoldConst("${const_sql_9_37}")
    def const_sql_9_38 = """select "9998.9998", cast(cast("9998.9998" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_38_strict "${const_sql_9_38}"
    testFoldConst("${const_sql_9_38}")
    def const_sql_9_39 = """select "9998.9999", cast(cast("9998.9999" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_39_strict "${const_sql_9_39}"
    testFoldConst("${const_sql_9_39}")
    def const_sql_9_40 = """select "9999.0000", cast(cast("9999.0000" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_40_strict "${const_sql_9_40}"
    testFoldConst("${const_sql_9_40}")
    def const_sql_9_41 = """select "9999.0001", cast(cast("9999.0001" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_41_strict "${const_sql_9_41}"
    testFoldConst("${const_sql_9_41}")
    def const_sql_9_42 = """select "9999.0009", cast(cast("9999.0009" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_42_strict "${const_sql_9_42}"
    testFoldConst("${const_sql_9_42}")
    def const_sql_9_43 = """select "9999.0999", cast(cast("9999.0999" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_43_strict "${const_sql_9_43}"
    testFoldConst("${const_sql_9_43}")
    def const_sql_9_44 = """select "9999.9000", cast(cast("9999.9000" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_44_strict "${const_sql_9_44}"
    testFoldConst("${const_sql_9_44}")
    def const_sql_9_45 = """select "9999.9001", cast(cast("9999.9001" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_45_strict "${const_sql_9_45}"
    testFoldConst("${const_sql_9_45}")
    def const_sql_9_46 = """select "9999.9998", cast(cast("9999.9998" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_46_strict "${const_sql_9_46}"
    testFoldConst("${const_sql_9_46}")
    def const_sql_9_47 = """select "9999.9999", cast(cast("9999.9999" as decimalv3(8, 4)) as decimalv3(76, 1));"""
    qt_sql_9_47_strict "${const_sql_9_47}"
    testFoldConst("${const_sql_9_47}")

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
    qt_sql_9_8_non_strict "${const_sql_9_8}"
    testFoldConst("${const_sql_9_8}")
    qt_sql_9_9_non_strict "${const_sql_9_9}"
    testFoldConst("${const_sql_9_9}")
    qt_sql_9_10_non_strict "${const_sql_9_10}"
    testFoldConst("${const_sql_9_10}")
    qt_sql_9_11_non_strict "${const_sql_9_11}"
    testFoldConst("${const_sql_9_11}")
    qt_sql_9_12_non_strict "${const_sql_9_12}"
    testFoldConst("${const_sql_9_12}")
    qt_sql_9_13_non_strict "${const_sql_9_13}"
    testFoldConst("${const_sql_9_13}")
    qt_sql_9_14_non_strict "${const_sql_9_14}"
    testFoldConst("${const_sql_9_14}")
    qt_sql_9_15_non_strict "${const_sql_9_15}"
    testFoldConst("${const_sql_9_15}")
    qt_sql_9_16_non_strict "${const_sql_9_16}"
    testFoldConst("${const_sql_9_16}")
    qt_sql_9_17_non_strict "${const_sql_9_17}"
    testFoldConst("${const_sql_9_17}")
    qt_sql_9_18_non_strict "${const_sql_9_18}"
    testFoldConst("${const_sql_9_18}")
    qt_sql_9_19_non_strict "${const_sql_9_19}"
    testFoldConst("${const_sql_9_19}")
    qt_sql_9_20_non_strict "${const_sql_9_20}"
    testFoldConst("${const_sql_9_20}")
    qt_sql_9_21_non_strict "${const_sql_9_21}"
    testFoldConst("${const_sql_9_21}")
    qt_sql_9_22_non_strict "${const_sql_9_22}"
    testFoldConst("${const_sql_9_22}")
    qt_sql_9_23_non_strict "${const_sql_9_23}"
    testFoldConst("${const_sql_9_23}")
    qt_sql_9_24_non_strict "${const_sql_9_24}"
    testFoldConst("${const_sql_9_24}")
    qt_sql_9_25_non_strict "${const_sql_9_25}"
    testFoldConst("${const_sql_9_25}")
    qt_sql_9_26_non_strict "${const_sql_9_26}"
    testFoldConst("${const_sql_9_26}")
    qt_sql_9_27_non_strict "${const_sql_9_27}"
    testFoldConst("${const_sql_9_27}")
    qt_sql_9_28_non_strict "${const_sql_9_28}"
    testFoldConst("${const_sql_9_28}")
    qt_sql_9_29_non_strict "${const_sql_9_29}"
    testFoldConst("${const_sql_9_29}")
    qt_sql_9_30_non_strict "${const_sql_9_30}"
    testFoldConst("${const_sql_9_30}")
    qt_sql_9_31_non_strict "${const_sql_9_31}"
    testFoldConst("${const_sql_9_31}")
    qt_sql_9_32_non_strict "${const_sql_9_32}"
    testFoldConst("${const_sql_9_32}")
    qt_sql_9_33_non_strict "${const_sql_9_33}"
    testFoldConst("${const_sql_9_33}")
    qt_sql_9_34_non_strict "${const_sql_9_34}"
    testFoldConst("${const_sql_9_34}")
    qt_sql_9_35_non_strict "${const_sql_9_35}"
    testFoldConst("${const_sql_9_35}")
    qt_sql_9_36_non_strict "${const_sql_9_36}"
    testFoldConst("${const_sql_9_36}")
    qt_sql_9_37_non_strict "${const_sql_9_37}"
    testFoldConst("${const_sql_9_37}")
    qt_sql_9_38_non_strict "${const_sql_9_38}"
    testFoldConst("${const_sql_9_38}")
    qt_sql_9_39_non_strict "${const_sql_9_39}"
    testFoldConst("${const_sql_9_39}")
    qt_sql_9_40_non_strict "${const_sql_9_40}"
    testFoldConst("${const_sql_9_40}")
    qt_sql_9_41_non_strict "${const_sql_9_41}"
    testFoldConst("${const_sql_9_41}")
    qt_sql_9_42_non_strict "${const_sql_9_42}"
    testFoldConst("${const_sql_9_42}")
    qt_sql_9_43_non_strict "${const_sql_9_43}"
    testFoldConst("${const_sql_9_43}")
    qt_sql_9_44_non_strict "${const_sql_9_44}"
    testFoldConst("${const_sql_9_44}")
    qt_sql_9_45_non_strict "${const_sql_9_45}"
    testFoldConst("${const_sql_9_45}")
    qt_sql_9_46_non_strict "${const_sql_9_46}"
    testFoldConst("${const_sql_9_46}")
    qt_sql_9_47_non_strict "${const_sql_9_47}"
    testFoldConst("${const_sql_9_47}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_10_0 = """select "0.0000000", cast(cast("0.0000000" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_0_strict "${const_sql_10_0}"
    testFoldConst("${const_sql_10_0}")
    def const_sql_10_1 = """select "0.0000001", cast(cast("0.0000001" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_1_strict "${const_sql_10_1}"
    testFoldConst("${const_sql_10_1}")
    def const_sql_10_2 = """select "0.0000009", cast(cast("0.0000009" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_2_strict "${const_sql_10_2}"
    testFoldConst("${const_sql_10_2}")
    def const_sql_10_3 = """select "0.0999999", cast(cast("0.0999999" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_3_strict "${const_sql_10_3}"
    testFoldConst("${const_sql_10_3}")
    def const_sql_10_4 = """select "0.9000000", cast(cast("0.9000000" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_4_strict "${const_sql_10_4}"
    testFoldConst("${const_sql_10_4}")
    def const_sql_10_5 = """select "0.9000001", cast(cast("0.9000001" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_5_strict "${const_sql_10_5}"
    testFoldConst("${const_sql_10_5}")
    def const_sql_10_6 = """select "0.9999998", cast(cast("0.9999998" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_6_strict "${const_sql_10_6}"
    testFoldConst("${const_sql_10_6}")
    def const_sql_10_7 = """select "0.9999999", cast(cast("0.9999999" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_7_strict "${const_sql_10_7}"
    testFoldConst("${const_sql_10_7}")
    def const_sql_10_8 = """select "8.0000000", cast(cast("8.0000000" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_8_strict "${const_sql_10_8}"
    testFoldConst("${const_sql_10_8}")
    def const_sql_10_9 = """select "8.0000001", cast(cast("8.0000001" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_9_strict "${const_sql_10_9}"
    testFoldConst("${const_sql_10_9}")
    def const_sql_10_10 = """select "8.0000009", cast(cast("8.0000009" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_10_strict "${const_sql_10_10}"
    testFoldConst("${const_sql_10_10}")
    def const_sql_10_11 = """select "8.0999999", cast(cast("8.0999999" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_11_strict "${const_sql_10_11}"
    testFoldConst("${const_sql_10_11}")
    def const_sql_10_12 = """select "8.9000000", cast(cast("8.9000000" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_12_strict "${const_sql_10_12}"
    testFoldConst("${const_sql_10_12}")
    def const_sql_10_13 = """select "8.9000001", cast(cast("8.9000001" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_13_strict "${const_sql_10_13}"
    testFoldConst("${const_sql_10_13}")
    def const_sql_10_14 = """select "8.9999998", cast(cast("8.9999998" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_14_strict "${const_sql_10_14}"
    testFoldConst("${const_sql_10_14}")
    def const_sql_10_15 = """select "8.9999999", cast(cast("8.9999999" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_15_strict "${const_sql_10_15}"
    testFoldConst("${const_sql_10_15}")
    def const_sql_10_16 = """select "9.0000000", cast(cast("9.0000000" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_16_strict "${const_sql_10_16}"
    testFoldConst("${const_sql_10_16}")
    def const_sql_10_17 = """select "9.0000001", cast(cast("9.0000001" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_17_strict "${const_sql_10_17}"
    testFoldConst("${const_sql_10_17}")
    def const_sql_10_18 = """select "9.0000009", cast(cast("9.0000009" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_18_strict "${const_sql_10_18}"
    testFoldConst("${const_sql_10_18}")
    def const_sql_10_19 = """select "9.0999999", cast(cast("9.0999999" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_19_strict "${const_sql_10_19}"
    testFoldConst("${const_sql_10_19}")
    def const_sql_10_20 = """select "9.9000000", cast(cast("9.9000000" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_20_strict "${const_sql_10_20}"
    testFoldConst("${const_sql_10_20}")
    def const_sql_10_21 = """select "9.9000001", cast(cast("9.9000001" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_21_strict "${const_sql_10_21}"
    testFoldConst("${const_sql_10_21}")
    def const_sql_10_22 = """select "9.9999998", cast(cast("9.9999998" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_22_strict "${const_sql_10_22}"
    testFoldConst("${const_sql_10_22}")
    def const_sql_10_23 = """select "9.9999999", cast(cast("9.9999999" as decimalv3(8, 7)) as decimalv3(76, 1));"""
    qt_sql_10_23_strict "${const_sql_10_23}"
    testFoldConst("${const_sql_10_23}")

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
    qt_sql_10_6_non_strict "${const_sql_10_6}"
    testFoldConst("${const_sql_10_6}")
    qt_sql_10_7_non_strict "${const_sql_10_7}"
    testFoldConst("${const_sql_10_7}")
    qt_sql_10_8_non_strict "${const_sql_10_8}"
    testFoldConst("${const_sql_10_8}")
    qt_sql_10_9_non_strict "${const_sql_10_9}"
    testFoldConst("${const_sql_10_9}")
    qt_sql_10_10_non_strict "${const_sql_10_10}"
    testFoldConst("${const_sql_10_10}")
    qt_sql_10_11_non_strict "${const_sql_10_11}"
    testFoldConst("${const_sql_10_11}")
    qt_sql_10_12_non_strict "${const_sql_10_12}"
    testFoldConst("${const_sql_10_12}")
    qt_sql_10_13_non_strict "${const_sql_10_13}"
    testFoldConst("${const_sql_10_13}")
    qt_sql_10_14_non_strict "${const_sql_10_14}"
    testFoldConst("${const_sql_10_14}")
    qt_sql_10_15_non_strict "${const_sql_10_15}"
    testFoldConst("${const_sql_10_15}")
    qt_sql_10_16_non_strict "${const_sql_10_16}"
    testFoldConst("${const_sql_10_16}")
    qt_sql_10_17_non_strict "${const_sql_10_17}"
    testFoldConst("${const_sql_10_17}")
    qt_sql_10_18_non_strict "${const_sql_10_18}"
    testFoldConst("${const_sql_10_18}")
    qt_sql_10_19_non_strict "${const_sql_10_19}"
    testFoldConst("${const_sql_10_19}")
    qt_sql_10_20_non_strict "${const_sql_10_20}"
    testFoldConst("${const_sql_10_20}")
    qt_sql_10_21_non_strict "${const_sql_10_21}"
    testFoldConst("${const_sql_10_21}")
    qt_sql_10_22_non_strict "${const_sql_10_22}"
    testFoldConst("${const_sql_10_22}")
    qt_sql_10_23_non_strict "${const_sql_10_23}"
    testFoldConst("${const_sql_10_23}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_11_0 = """select "0.00000000", cast(cast("0.00000000" as decimalv3(8, 8)) as decimalv3(76, 1));"""
    qt_sql_11_0_strict "${const_sql_11_0}"
    testFoldConst("${const_sql_11_0}")
    def const_sql_11_1 = """select "0.00000001", cast(cast("0.00000001" as decimalv3(8, 8)) as decimalv3(76, 1));"""
    qt_sql_11_1_strict "${const_sql_11_1}"
    testFoldConst("${const_sql_11_1}")
    def const_sql_11_2 = """select "0.00000009", cast(cast("0.00000009" as decimalv3(8, 8)) as decimalv3(76, 1));"""
    qt_sql_11_2_strict "${const_sql_11_2}"
    testFoldConst("${const_sql_11_2}")
    def const_sql_11_3 = """select "0.09999999", cast(cast("0.09999999" as decimalv3(8, 8)) as decimalv3(76, 1));"""
    qt_sql_11_3_strict "${const_sql_11_3}"
    testFoldConst("${const_sql_11_3}")
    def const_sql_11_4 = """select "0.90000000", cast(cast("0.90000000" as decimalv3(8, 8)) as decimalv3(76, 1));"""
    qt_sql_11_4_strict "${const_sql_11_4}"
    testFoldConst("${const_sql_11_4}")
    def const_sql_11_5 = """select "0.90000001", cast(cast("0.90000001" as decimalv3(8, 8)) as decimalv3(76, 1));"""
    qt_sql_11_5_strict "${const_sql_11_5}"
    testFoldConst("${const_sql_11_5}")
    def const_sql_11_6 = """select "0.99999998", cast(cast("0.99999998" as decimalv3(8, 8)) as decimalv3(76, 1));"""
    qt_sql_11_6_strict "${const_sql_11_6}"
    testFoldConst("${const_sql_11_6}")
    def const_sql_11_7 = """select "0.99999999", cast(cast("0.99999999" as decimalv3(8, 8)) as decimalv3(76, 1));"""
    qt_sql_11_7_strict "${const_sql_11_7}"
    testFoldConst("${const_sql_11_7}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_12_0 = """select "0", cast(cast("0" as decimalv3(9, 0)) as decimalv3(76, 1));"""
    qt_sql_12_0_strict "${const_sql_12_0}"
    testFoldConst("${const_sql_12_0}")
    def const_sql_12_1 = """select "99999999", cast(cast("99999999" as decimalv3(9, 0)) as decimalv3(76, 1));"""
    qt_sql_12_1_strict "${const_sql_12_1}"
    testFoldConst("${const_sql_12_1}")
    def const_sql_12_2 = """select "900000000", cast(cast("900000000" as decimalv3(9, 0)) as decimalv3(76, 1));"""
    qt_sql_12_2_strict "${const_sql_12_2}"
    testFoldConst("${const_sql_12_2}")
    def const_sql_12_3 = """select "900000001", cast(cast("900000001" as decimalv3(9, 0)) as decimalv3(76, 1));"""
    qt_sql_12_3_strict "${const_sql_12_3}"
    testFoldConst("${const_sql_12_3}")
    def const_sql_12_4 = """select "999999998", cast(cast("999999998" as decimalv3(9, 0)) as decimalv3(76, 1));"""
    qt_sql_12_4_strict "${const_sql_12_4}"
    testFoldConst("${const_sql_12_4}")
    def const_sql_12_5 = """select "999999999", cast(cast("999999999" as decimalv3(9, 0)) as decimalv3(76, 1));"""
    qt_sql_12_5_strict "${const_sql_12_5}"
    testFoldConst("${const_sql_12_5}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_13_0 = """select "0.0", cast(cast("0.0" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_0_strict "${const_sql_13_0}"
    testFoldConst("${const_sql_13_0}")
    def const_sql_13_1 = """select "0.1", cast(cast("0.1" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_1_strict "${const_sql_13_1}"
    testFoldConst("${const_sql_13_1}")
    def const_sql_13_2 = """select "0.8", cast(cast("0.8" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_2_strict "${const_sql_13_2}"
    testFoldConst("${const_sql_13_2}")
    def const_sql_13_3 = """select "0.9", cast(cast("0.9" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_3_strict "${const_sql_13_3}"
    testFoldConst("${const_sql_13_3}")
    def const_sql_13_4 = """select "9999999.0", cast(cast("9999999.0" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_4_strict "${const_sql_13_4}"
    testFoldConst("${const_sql_13_4}")
    def const_sql_13_5 = """select "9999999.1", cast(cast("9999999.1" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_5_strict "${const_sql_13_5}"
    testFoldConst("${const_sql_13_5}")
    def const_sql_13_6 = """select "9999999.8", cast(cast("9999999.8" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_6_strict "${const_sql_13_6}"
    testFoldConst("${const_sql_13_6}")
    def const_sql_13_7 = """select "9999999.9", cast(cast("9999999.9" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_7_strict "${const_sql_13_7}"
    testFoldConst("${const_sql_13_7}")
    def const_sql_13_8 = """select "90000000.0", cast(cast("90000000.0" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_8_strict "${const_sql_13_8}"
    testFoldConst("${const_sql_13_8}")
    def const_sql_13_9 = """select "90000000.1", cast(cast("90000000.1" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_9_strict "${const_sql_13_9}"
    testFoldConst("${const_sql_13_9}")
    def const_sql_13_10 = """select "90000000.8", cast(cast("90000000.8" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_10_strict "${const_sql_13_10}"
    testFoldConst("${const_sql_13_10}")
    def const_sql_13_11 = """select "90000000.9", cast(cast("90000000.9" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_11_strict "${const_sql_13_11}"
    testFoldConst("${const_sql_13_11}")
    def const_sql_13_12 = """select "90000001.0", cast(cast("90000001.0" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_12_strict "${const_sql_13_12}"
    testFoldConst("${const_sql_13_12}")
    def const_sql_13_13 = """select "90000001.1", cast(cast("90000001.1" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_13_strict "${const_sql_13_13}"
    testFoldConst("${const_sql_13_13}")
    def const_sql_13_14 = """select "90000001.8", cast(cast("90000001.8" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_14_strict "${const_sql_13_14}"
    testFoldConst("${const_sql_13_14}")
    def const_sql_13_15 = """select "90000001.9", cast(cast("90000001.9" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_15_strict "${const_sql_13_15}"
    testFoldConst("${const_sql_13_15}")
    def const_sql_13_16 = """select "99999998.0", cast(cast("99999998.0" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_16_strict "${const_sql_13_16}"
    testFoldConst("${const_sql_13_16}")
    def const_sql_13_17 = """select "99999998.1", cast(cast("99999998.1" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_17_strict "${const_sql_13_17}"
    testFoldConst("${const_sql_13_17}")
    def const_sql_13_18 = """select "99999998.8", cast(cast("99999998.8" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_18_strict "${const_sql_13_18}"
    testFoldConst("${const_sql_13_18}")
    def const_sql_13_19 = """select "99999998.9", cast(cast("99999998.9" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_19_strict "${const_sql_13_19}"
    testFoldConst("${const_sql_13_19}")
    def const_sql_13_20 = """select "99999999.0", cast(cast("99999999.0" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_20_strict "${const_sql_13_20}"
    testFoldConst("${const_sql_13_20}")
    def const_sql_13_21 = """select "99999999.1", cast(cast("99999999.1" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_21_strict "${const_sql_13_21}"
    testFoldConst("${const_sql_13_21}")
    def const_sql_13_22 = """select "99999999.8", cast(cast("99999999.8" as decimalv3(9, 1)) as decimalv3(76, 1));"""
    qt_sql_13_22_strict "${const_sql_13_22}"
    testFoldConst("${const_sql_13_22}")
    def const_sql_13_23 = """select "99999999.9", cast(cast("99999999.9" as decimalv3(9, 1)) as decimalv3(76, 1));"""
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
    def const_sql_14_0 = """select "0.0000", cast(cast("0.0000" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_0_strict "${const_sql_14_0}"
    testFoldConst("${const_sql_14_0}")
    def const_sql_14_1 = """select "0.0001", cast(cast("0.0001" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_1_strict "${const_sql_14_1}"
    testFoldConst("${const_sql_14_1}")
    def const_sql_14_2 = """select "0.0009", cast(cast("0.0009" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_2_strict "${const_sql_14_2}"
    testFoldConst("${const_sql_14_2}")
    def const_sql_14_3 = """select "0.0999", cast(cast("0.0999" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_3_strict "${const_sql_14_3}"
    testFoldConst("${const_sql_14_3}")
    def const_sql_14_4 = """select "0.9000", cast(cast("0.9000" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_4_strict "${const_sql_14_4}"
    testFoldConst("${const_sql_14_4}")
    def const_sql_14_5 = """select "0.9001", cast(cast("0.9001" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_5_strict "${const_sql_14_5}"
    testFoldConst("${const_sql_14_5}")
    def const_sql_14_6 = """select "0.9998", cast(cast("0.9998" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_6_strict "${const_sql_14_6}"
    testFoldConst("${const_sql_14_6}")
    def const_sql_14_7 = """select "0.9999", cast(cast("0.9999" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_7_strict "${const_sql_14_7}"
    testFoldConst("${const_sql_14_7}")
    def const_sql_14_8 = """select "9999.0000", cast(cast("9999.0000" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_8_strict "${const_sql_14_8}"
    testFoldConst("${const_sql_14_8}")
    def const_sql_14_9 = """select "9999.0001", cast(cast("9999.0001" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_9_strict "${const_sql_14_9}"
    testFoldConst("${const_sql_14_9}")
    def const_sql_14_10 = """select "9999.0009", cast(cast("9999.0009" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_10_strict "${const_sql_14_10}"
    testFoldConst("${const_sql_14_10}")
    def const_sql_14_11 = """select "9999.0999", cast(cast("9999.0999" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_11_strict "${const_sql_14_11}"
    testFoldConst("${const_sql_14_11}")
    def const_sql_14_12 = """select "9999.9000", cast(cast("9999.9000" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_12_strict "${const_sql_14_12}"
    testFoldConst("${const_sql_14_12}")
    def const_sql_14_13 = """select "9999.9001", cast(cast("9999.9001" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_13_strict "${const_sql_14_13}"
    testFoldConst("${const_sql_14_13}")
    def const_sql_14_14 = """select "9999.9998", cast(cast("9999.9998" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_14_strict "${const_sql_14_14}"
    testFoldConst("${const_sql_14_14}")
    def const_sql_14_15 = """select "9999.9999", cast(cast("9999.9999" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_15_strict "${const_sql_14_15}"
    testFoldConst("${const_sql_14_15}")
    def const_sql_14_16 = """select "90000.0000", cast(cast("90000.0000" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_16_strict "${const_sql_14_16}"
    testFoldConst("${const_sql_14_16}")
    def const_sql_14_17 = """select "90000.0001", cast(cast("90000.0001" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_17_strict "${const_sql_14_17}"
    testFoldConst("${const_sql_14_17}")
    def const_sql_14_18 = """select "90000.0009", cast(cast("90000.0009" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_18_strict "${const_sql_14_18}"
    testFoldConst("${const_sql_14_18}")
    def const_sql_14_19 = """select "90000.0999", cast(cast("90000.0999" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_19_strict "${const_sql_14_19}"
    testFoldConst("${const_sql_14_19}")
    def const_sql_14_20 = """select "90000.9000", cast(cast("90000.9000" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_20_strict "${const_sql_14_20}"
    testFoldConst("${const_sql_14_20}")
    def const_sql_14_21 = """select "90000.9001", cast(cast("90000.9001" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_21_strict "${const_sql_14_21}"
    testFoldConst("${const_sql_14_21}")
    def const_sql_14_22 = """select "90000.9998", cast(cast("90000.9998" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_22_strict "${const_sql_14_22}"
    testFoldConst("${const_sql_14_22}")
    def const_sql_14_23 = """select "90000.9999", cast(cast("90000.9999" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_23_strict "${const_sql_14_23}"
    testFoldConst("${const_sql_14_23}")
    def const_sql_14_24 = """select "90001.0000", cast(cast("90001.0000" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_24_strict "${const_sql_14_24}"
    testFoldConst("${const_sql_14_24}")
    def const_sql_14_25 = """select "90001.0001", cast(cast("90001.0001" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_25_strict "${const_sql_14_25}"
    testFoldConst("${const_sql_14_25}")
    def const_sql_14_26 = """select "90001.0009", cast(cast("90001.0009" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_26_strict "${const_sql_14_26}"
    testFoldConst("${const_sql_14_26}")
    def const_sql_14_27 = """select "90001.0999", cast(cast("90001.0999" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_27_strict "${const_sql_14_27}"
    testFoldConst("${const_sql_14_27}")
    def const_sql_14_28 = """select "90001.9000", cast(cast("90001.9000" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_28_strict "${const_sql_14_28}"
    testFoldConst("${const_sql_14_28}")
    def const_sql_14_29 = """select "90001.9001", cast(cast("90001.9001" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_29_strict "${const_sql_14_29}"
    testFoldConst("${const_sql_14_29}")
    def const_sql_14_30 = """select "90001.9998", cast(cast("90001.9998" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_30_strict "${const_sql_14_30}"
    testFoldConst("${const_sql_14_30}")
    def const_sql_14_31 = """select "90001.9999", cast(cast("90001.9999" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_31_strict "${const_sql_14_31}"
    testFoldConst("${const_sql_14_31}")
    def const_sql_14_32 = """select "99998.0000", cast(cast("99998.0000" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_32_strict "${const_sql_14_32}"
    testFoldConst("${const_sql_14_32}")
    def const_sql_14_33 = """select "99998.0001", cast(cast("99998.0001" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_33_strict "${const_sql_14_33}"
    testFoldConst("${const_sql_14_33}")
    def const_sql_14_34 = """select "99998.0009", cast(cast("99998.0009" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_34_strict "${const_sql_14_34}"
    testFoldConst("${const_sql_14_34}")
    def const_sql_14_35 = """select "99998.0999", cast(cast("99998.0999" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_35_strict "${const_sql_14_35}"
    testFoldConst("${const_sql_14_35}")
    def const_sql_14_36 = """select "99998.9000", cast(cast("99998.9000" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_36_strict "${const_sql_14_36}"
    testFoldConst("${const_sql_14_36}")
    def const_sql_14_37 = """select "99998.9001", cast(cast("99998.9001" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_37_strict "${const_sql_14_37}"
    testFoldConst("${const_sql_14_37}")
    def const_sql_14_38 = """select "99998.9998", cast(cast("99998.9998" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_38_strict "${const_sql_14_38}"
    testFoldConst("${const_sql_14_38}")
    def const_sql_14_39 = """select "99998.9999", cast(cast("99998.9999" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_39_strict "${const_sql_14_39}"
    testFoldConst("${const_sql_14_39}")
    def const_sql_14_40 = """select "99999.0000", cast(cast("99999.0000" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_40_strict "${const_sql_14_40}"
    testFoldConst("${const_sql_14_40}")
    def const_sql_14_41 = """select "99999.0001", cast(cast("99999.0001" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_41_strict "${const_sql_14_41}"
    testFoldConst("${const_sql_14_41}")
    def const_sql_14_42 = """select "99999.0009", cast(cast("99999.0009" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_42_strict "${const_sql_14_42}"
    testFoldConst("${const_sql_14_42}")
    def const_sql_14_43 = """select "99999.0999", cast(cast("99999.0999" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_43_strict "${const_sql_14_43}"
    testFoldConst("${const_sql_14_43}")
    def const_sql_14_44 = """select "99999.9000", cast(cast("99999.9000" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_44_strict "${const_sql_14_44}"
    testFoldConst("${const_sql_14_44}")
    def const_sql_14_45 = """select "99999.9001", cast(cast("99999.9001" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_45_strict "${const_sql_14_45}"
    testFoldConst("${const_sql_14_45}")
    def const_sql_14_46 = """select "99999.9998", cast(cast("99999.9998" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_46_strict "${const_sql_14_46}"
    testFoldConst("${const_sql_14_46}")
    def const_sql_14_47 = """select "99999.9999", cast(cast("99999.9999" as decimalv3(9, 4)) as decimalv3(76, 1));"""
    qt_sql_14_47_strict "${const_sql_14_47}"
    testFoldConst("${const_sql_14_47}")

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
    qt_sql_14_8_non_strict "${const_sql_14_8}"
    testFoldConst("${const_sql_14_8}")
    qt_sql_14_9_non_strict "${const_sql_14_9}"
    testFoldConst("${const_sql_14_9}")
    qt_sql_14_10_non_strict "${const_sql_14_10}"
    testFoldConst("${const_sql_14_10}")
    qt_sql_14_11_non_strict "${const_sql_14_11}"
    testFoldConst("${const_sql_14_11}")
    qt_sql_14_12_non_strict "${const_sql_14_12}"
    testFoldConst("${const_sql_14_12}")
    qt_sql_14_13_non_strict "${const_sql_14_13}"
    testFoldConst("${const_sql_14_13}")
    qt_sql_14_14_non_strict "${const_sql_14_14}"
    testFoldConst("${const_sql_14_14}")
    qt_sql_14_15_non_strict "${const_sql_14_15}"
    testFoldConst("${const_sql_14_15}")
    qt_sql_14_16_non_strict "${const_sql_14_16}"
    testFoldConst("${const_sql_14_16}")
    qt_sql_14_17_non_strict "${const_sql_14_17}"
    testFoldConst("${const_sql_14_17}")
    qt_sql_14_18_non_strict "${const_sql_14_18}"
    testFoldConst("${const_sql_14_18}")
    qt_sql_14_19_non_strict "${const_sql_14_19}"
    testFoldConst("${const_sql_14_19}")
    qt_sql_14_20_non_strict "${const_sql_14_20}"
    testFoldConst("${const_sql_14_20}")
    qt_sql_14_21_non_strict "${const_sql_14_21}"
    testFoldConst("${const_sql_14_21}")
    qt_sql_14_22_non_strict "${const_sql_14_22}"
    testFoldConst("${const_sql_14_22}")
    qt_sql_14_23_non_strict "${const_sql_14_23}"
    testFoldConst("${const_sql_14_23}")
    qt_sql_14_24_non_strict "${const_sql_14_24}"
    testFoldConst("${const_sql_14_24}")
    qt_sql_14_25_non_strict "${const_sql_14_25}"
    testFoldConst("${const_sql_14_25}")
    qt_sql_14_26_non_strict "${const_sql_14_26}"
    testFoldConst("${const_sql_14_26}")
    qt_sql_14_27_non_strict "${const_sql_14_27}"
    testFoldConst("${const_sql_14_27}")
    qt_sql_14_28_non_strict "${const_sql_14_28}"
    testFoldConst("${const_sql_14_28}")
    qt_sql_14_29_non_strict "${const_sql_14_29}"
    testFoldConst("${const_sql_14_29}")
    qt_sql_14_30_non_strict "${const_sql_14_30}"
    testFoldConst("${const_sql_14_30}")
    qt_sql_14_31_non_strict "${const_sql_14_31}"
    testFoldConst("${const_sql_14_31}")
    qt_sql_14_32_non_strict "${const_sql_14_32}"
    testFoldConst("${const_sql_14_32}")
    qt_sql_14_33_non_strict "${const_sql_14_33}"
    testFoldConst("${const_sql_14_33}")
    qt_sql_14_34_non_strict "${const_sql_14_34}"
    testFoldConst("${const_sql_14_34}")
    qt_sql_14_35_non_strict "${const_sql_14_35}"
    testFoldConst("${const_sql_14_35}")
    qt_sql_14_36_non_strict "${const_sql_14_36}"
    testFoldConst("${const_sql_14_36}")
    qt_sql_14_37_non_strict "${const_sql_14_37}"
    testFoldConst("${const_sql_14_37}")
    qt_sql_14_38_non_strict "${const_sql_14_38}"
    testFoldConst("${const_sql_14_38}")
    qt_sql_14_39_non_strict "${const_sql_14_39}"
    testFoldConst("${const_sql_14_39}")
    qt_sql_14_40_non_strict "${const_sql_14_40}"
    testFoldConst("${const_sql_14_40}")
    qt_sql_14_41_non_strict "${const_sql_14_41}"
    testFoldConst("${const_sql_14_41}")
    qt_sql_14_42_non_strict "${const_sql_14_42}"
    testFoldConst("${const_sql_14_42}")
    qt_sql_14_43_non_strict "${const_sql_14_43}"
    testFoldConst("${const_sql_14_43}")
    qt_sql_14_44_non_strict "${const_sql_14_44}"
    testFoldConst("${const_sql_14_44}")
    qt_sql_14_45_non_strict "${const_sql_14_45}"
    testFoldConst("${const_sql_14_45}")
    qt_sql_14_46_non_strict "${const_sql_14_46}"
    testFoldConst("${const_sql_14_46}")
    qt_sql_14_47_non_strict "${const_sql_14_47}"
    testFoldConst("${const_sql_14_47}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_15_0 = """select "0.00000000", cast(cast("0.00000000" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_0_strict "${const_sql_15_0}"
    testFoldConst("${const_sql_15_0}")
    def const_sql_15_1 = """select "0.00000001", cast(cast("0.00000001" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_1_strict "${const_sql_15_1}"
    testFoldConst("${const_sql_15_1}")
    def const_sql_15_2 = """select "0.00000009", cast(cast("0.00000009" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_2_strict "${const_sql_15_2}"
    testFoldConst("${const_sql_15_2}")
    def const_sql_15_3 = """select "0.09999999", cast(cast("0.09999999" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_3_strict "${const_sql_15_3}"
    testFoldConst("${const_sql_15_3}")
    def const_sql_15_4 = """select "0.90000000", cast(cast("0.90000000" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_4_strict "${const_sql_15_4}"
    testFoldConst("${const_sql_15_4}")
    def const_sql_15_5 = """select "0.90000001", cast(cast("0.90000001" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_5_strict "${const_sql_15_5}"
    testFoldConst("${const_sql_15_5}")
    def const_sql_15_6 = """select "0.99999998", cast(cast("0.99999998" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_6_strict "${const_sql_15_6}"
    testFoldConst("${const_sql_15_6}")
    def const_sql_15_7 = """select "0.99999999", cast(cast("0.99999999" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_7_strict "${const_sql_15_7}"
    testFoldConst("${const_sql_15_7}")
    def const_sql_15_8 = """select "8.00000000", cast(cast("8.00000000" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_8_strict "${const_sql_15_8}"
    testFoldConst("${const_sql_15_8}")
    def const_sql_15_9 = """select "8.00000001", cast(cast("8.00000001" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_9_strict "${const_sql_15_9}"
    testFoldConst("${const_sql_15_9}")
    def const_sql_15_10 = """select "8.00000009", cast(cast("8.00000009" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_10_strict "${const_sql_15_10}"
    testFoldConst("${const_sql_15_10}")
    def const_sql_15_11 = """select "8.09999999", cast(cast("8.09999999" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_11_strict "${const_sql_15_11}"
    testFoldConst("${const_sql_15_11}")
    def const_sql_15_12 = """select "8.90000000", cast(cast("8.90000000" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_12_strict "${const_sql_15_12}"
    testFoldConst("${const_sql_15_12}")
    def const_sql_15_13 = """select "8.90000001", cast(cast("8.90000001" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_13_strict "${const_sql_15_13}"
    testFoldConst("${const_sql_15_13}")
    def const_sql_15_14 = """select "8.99999998", cast(cast("8.99999998" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_14_strict "${const_sql_15_14}"
    testFoldConst("${const_sql_15_14}")
    def const_sql_15_15 = """select "8.99999999", cast(cast("8.99999999" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_15_strict "${const_sql_15_15}"
    testFoldConst("${const_sql_15_15}")
    def const_sql_15_16 = """select "9.00000000", cast(cast("9.00000000" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_16_strict "${const_sql_15_16}"
    testFoldConst("${const_sql_15_16}")
    def const_sql_15_17 = """select "9.00000001", cast(cast("9.00000001" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_17_strict "${const_sql_15_17}"
    testFoldConst("${const_sql_15_17}")
    def const_sql_15_18 = """select "9.00000009", cast(cast("9.00000009" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_18_strict "${const_sql_15_18}"
    testFoldConst("${const_sql_15_18}")
    def const_sql_15_19 = """select "9.09999999", cast(cast("9.09999999" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_19_strict "${const_sql_15_19}"
    testFoldConst("${const_sql_15_19}")
    def const_sql_15_20 = """select "9.90000000", cast(cast("9.90000000" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_20_strict "${const_sql_15_20}"
    testFoldConst("${const_sql_15_20}")
    def const_sql_15_21 = """select "9.90000001", cast(cast("9.90000001" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_21_strict "${const_sql_15_21}"
    testFoldConst("${const_sql_15_21}")
    def const_sql_15_22 = """select "9.99999998", cast(cast("9.99999998" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_22_strict "${const_sql_15_22}"
    testFoldConst("${const_sql_15_22}")
    def const_sql_15_23 = """select "9.99999999", cast(cast("9.99999999" as decimalv3(9, 8)) as decimalv3(76, 1));"""
    qt_sql_15_23_strict "${const_sql_15_23}"
    testFoldConst("${const_sql_15_23}")

    sql "set enable_strict_cast=false;"
    qt_sql_15_0_non_strict "${const_sql_15_0}"
    testFoldConst("${const_sql_15_0}")
    qt_sql_15_1_non_strict "${const_sql_15_1}"
    testFoldConst("${const_sql_15_1}")
    qt_sql_15_2_non_strict "${const_sql_15_2}"
    testFoldConst("${const_sql_15_2}")
    qt_sql_15_3_non_strict "${const_sql_15_3}"
    testFoldConst("${const_sql_15_3}")
    qt_sql_15_4_non_strict "${const_sql_15_4}"
    testFoldConst("${const_sql_15_4}")
    qt_sql_15_5_non_strict "${const_sql_15_5}"
    testFoldConst("${const_sql_15_5}")
    qt_sql_15_6_non_strict "${const_sql_15_6}"
    testFoldConst("${const_sql_15_6}")
    qt_sql_15_7_non_strict "${const_sql_15_7}"
    testFoldConst("${const_sql_15_7}")
    qt_sql_15_8_non_strict "${const_sql_15_8}"
    testFoldConst("${const_sql_15_8}")
    qt_sql_15_9_non_strict "${const_sql_15_9}"
    testFoldConst("${const_sql_15_9}")
    qt_sql_15_10_non_strict "${const_sql_15_10}"
    testFoldConst("${const_sql_15_10}")
    qt_sql_15_11_non_strict "${const_sql_15_11}"
    testFoldConst("${const_sql_15_11}")
    qt_sql_15_12_non_strict "${const_sql_15_12}"
    testFoldConst("${const_sql_15_12}")
    qt_sql_15_13_non_strict "${const_sql_15_13}"
    testFoldConst("${const_sql_15_13}")
    qt_sql_15_14_non_strict "${const_sql_15_14}"
    testFoldConst("${const_sql_15_14}")
    qt_sql_15_15_non_strict "${const_sql_15_15}"
    testFoldConst("${const_sql_15_15}")
    qt_sql_15_16_non_strict "${const_sql_15_16}"
    testFoldConst("${const_sql_15_16}")
    qt_sql_15_17_non_strict "${const_sql_15_17}"
    testFoldConst("${const_sql_15_17}")
    qt_sql_15_18_non_strict "${const_sql_15_18}"
    testFoldConst("${const_sql_15_18}")
    qt_sql_15_19_non_strict "${const_sql_15_19}"
    testFoldConst("${const_sql_15_19}")
    qt_sql_15_20_non_strict "${const_sql_15_20}"
    testFoldConst("${const_sql_15_20}")
    qt_sql_15_21_non_strict "${const_sql_15_21}"
    testFoldConst("${const_sql_15_21}")
    qt_sql_15_22_non_strict "${const_sql_15_22}"
    testFoldConst("${const_sql_15_22}")
    qt_sql_15_23_non_strict "${const_sql_15_23}"
    testFoldConst("${const_sql_15_23}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_16_0 = """select "0.000000000", cast(cast("0.000000000" as decimalv3(9, 9)) as decimalv3(76, 1));"""
    qt_sql_16_0_strict "${const_sql_16_0}"
    testFoldConst("${const_sql_16_0}")
    def const_sql_16_1 = """select "0.000000001", cast(cast("0.000000001" as decimalv3(9, 9)) as decimalv3(76, 1));"""
    qt_sql_16_1_strict "${const_sql_16_1}"
    testFoldConst("${const_sql_16_1}")
    def const_sql_16_2 = """select "0.000000009", cast(cast("0.000000009" as decimalv3(9, 9)) as decimalv3(76, 1));"""
    qt_sql_16_2_strict "${const_sql_16_2}"
    testFoldConst("${const_sql_16_2}")
    def const_sql_16_3 = """select "0.099999999", cast(cast("0.099999999" as decimalv3(9, 9)) as decimalv3(76, 1));"""
    qt_sql_16_3_strict "${const_sql_16_3}"
    testFoldConst("${const_sql_16_3}")
    def const_sql_16_4 = """select "0.900000000", cast(cast("0.900000000" as decimalv3(9, 9)) as decimalv3(76, 1));"""
    qt_sql_16_4_strict "${const_sql_16_4}"
    testFoldConst("${const_sql_16_4}")
    def const_sql_16_5 = """select "0.900000001", cast(cast("0.900000001" as decimalv3(9, 9)) as decimalv3(76, 1));"""
    qt_sql_16_5_strict "${const_sql_16_5}"
    testFoldConst("${const_sql_16_5}")
    def const_sql_16_6 = """select "0.999999998", cast(cast("0.999999998" as decimalv3(9, 9)) as decimalv3(76, 1));"""
    qt_sql_16_6_strict "${const_sql_16_6}"
    testFoldConst("${const_sql_16_6}")
    def const_sql_16_7 = """select "0.999999999", cast(cast("0.999999999" as decimalv3(9, 9)) as decimalv3(76, 1));"""
    qt_sql_16_7_strict "${const_sql_16_7}"
    testFoldConst("${const_sql_16_7}")

    sql "set enable_strict_cast=false;"
    qt_sql_16_0_non_strict "${const_sql_16_0}"
    testFoldConst("${const_sql_16_0}")
    qt_sql_16_1_non_strict "${const_sql_16_1}"
    testFoldConst("${const_sql_16_1}")
    qt_sql_16_2_non_strict "${const_sql_16_2}"
    testFoldConst("${const_sql_16_2}")
    qt_sql_16_3_non_strict "${const_sql_16_3}"
    testFoldConst("${const_sql_16_3}")
    qt_sql_16_4_non_strict "${const_sql_16_4}"
    testFoldConst("${const_sql_16_4}")
    qt_sql_16_5_non_strict "${const_sql_16_5}"
    testFoldConst("${const_sql_16_5}")
    qt_sql_16_6_non_strict "${const_sql_16_6}"
    testFoldConst("${const_sql_16_6}")
    qt_sql_16_7_non_strict "${const_sql_16_7}"
    testFoldConst("${const_sql_16_7}")
}