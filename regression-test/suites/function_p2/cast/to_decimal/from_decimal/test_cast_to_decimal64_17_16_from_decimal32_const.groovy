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


suite("test_cast_to_decimal64_17_16_from_decimal32_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "0", cast(cast("0" as decimalv3(1, 0)) as decimalv3(17, 16));"""
    qt_sql_0_0_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    def const_sql_0_1 = """select "8", cast(cast("8" as decimalv3(1, 0)) as decimalv3(17, 16));"""
    qt_sql_0_1_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    def const_sql_0_2 = """select "9", cast(cast("9" as decimalv3(1, 0)) as decimalv3(17, 16));"""
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
    def const_sql_1_0 = """select "0.0", cast(cast("0.0" as decimalv3(1, 1)) as decimalv3(17, 16));"""
    qt_sql_1_0_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    def const_sql_1_1 = """select "0.1", cast(cast("0.1" as decimalv3(1, 1)) as decimalv3(17, 16));"""
    qt_sql_1_1_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    def const_sql_1_2 = """select "0.8", cast(cast("0.8" as decimalv3(1, 1)) as decimalv3(17, 16));"""
    qt_sql_1_2_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    def const_sql_1_3 = """select "0.9", cast(cast("0.9" as decimalv3(1, 1)) as decimalv3(17, 16));"""
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
    def const_sql_2_0 = """select "0", cast(cast("0" as decimalv3(4, 0)) as decimalv3(17, 16));"""
    qt_sql_2_0_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    def const_sql_2_1 = """select "8", cast(cast("8" as decimalv3(4, 0)) as decimalv3(17, 16));"""
    qt_sql_2_1_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    def const_sql_2_2 = """select "9", cast(cast("9" as decimalv3(4, 0)) as decimalv3(17, 16));"""
    qt_sql_2_2_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")

    sql "set enable_strict_cast=false;"
    qt_sql_2_0_non_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    qt_sql_2_1_non_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    qt_sql_2_2_non_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_3_0 = """select "0.0", cast(cast("0.0" as decimalv3(4, 1)) as decimalv3(17, 16));"""
    qt_sql_3_0_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    def const_sql_3_1 = """select "0.1", cast(cast("0.1" as decimalv3(4, 1)) as decimalv3(17, 16));"""
    qt_sql_3_1_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    def const_sql_3_2 = """select "0.8", cast(cast("0.8" as decimalv3(4, 1)) as decimalv3(17, 16));"""
    qt_sql_3_2_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    def const_sql_3_3 = """select "0.9", cast(cast("0.9" as decimalv3(4, 1)) as decimalv3(17, 16));"""
    qt_sql_3_3_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    def const_sql_3_4 = """select "8.0", cast(cast("8.0" as decimalv3(4, 1)) as decimalv3(17, 16));"""
    qt_sql_3_4_strict "${const_sql_3_4}"
    testFoldConst("${const_sql_3_4}")
    def const_sql_3_5 = """select "8.1", cast(cast("8.1" as decimalv3(4, 1)) as decimalv3(17, 16));"""
    qt_sql_3_5_strict "${const_sql_3_5}"
    testFoldConst("${const_sql_3_5}")
    def const_sql_3_6 = """select "8.8", cast(cast("8.8" as decimalv3(4, 1)) as decimalv3(17, 16));"""
    qt_sql_3_6_strict "${const_sql_3_6}"
    testFoldConst("${const_sql_3_6}")
    def const_sql_3_7 = """select "8.9", cast(cast("8.9" as decimalv3(4, 1)) as decimalv3(17, 16));"""
    qt_sql_3_7_strict "${const_sql_3_7}"
    testFoldConst("${const_sql_3_7}")
    def const_sql_3_8 = """select "9.0", cast(cast("9.0" as decimalv3(4, 1)) as decimalv3(17, 16));"""
    qt_sql_3_8_strict "${const_sql_3_8}"
    testFoldConst("${const_sql_3_8}")
    def const_sql_3_9 = """select "9.1", cast(cast("9.1" as decimalv3(4, 1)) as decimalv3(17, 16));"""
    qt_sql_3_9_strict "${const_sql_3_9}"
    testFoldConst("${const_sql_3_9}")
    def const_sql_3_10 = """select "9.8", cast(cast("9.8" as decimalv3(4, 1)) as decimalv3(17, 16));"""
    qt_sql_3_10_strict "${const_sql_3_10}"
    testFoldConst("${const_sql_3_10}")
    def const_sql_3_11 = """select "9.9", cast(cast("9.9" as decimalv3(4, 1)) as decimalv3(17, 16));"""
    qt_sql_3_11_strict "${const_sql_3_11}"
    testFoldConst("${const_sql_3_11}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_4_0 = """select "0.00", cast(cast("0.00" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_0_strict "${const_sql_4_0}"
    testFoldConst("${const_sql_4_0}")
    def const_sql_4_1 = """select "0.01", cast(cast("0.01" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_1_strict "${const_sql_4_1}"
    testFoldConst("${const_sql_4_1}")
    def const_sql_4_2 = """select "0.09", cast(cast("0.09" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_2_strict "${const_sql_4_2}"
    testFoldConst("${const_sql_4_2}")
    def const_sql_4_3 = """select "0.90", cast(cast("0.90" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_3_strict "${const_sql_4_3}"
    testFoldConst("${const_sql_4_3}")
    def const_sql_4_4 = """select "0.91", cast(cast("0.91" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_4_strict "${const_sql_4_4}"
    testFoldConst("${const_sql_4_4}")
    def const_sql_4_5 = """select "0.98", cast(cast("0.98" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_5_strict "${const_sql_4_5}"
    testFoldConst("${const_sql_4_5}")
    def const_sql_4_6 = """select "0.99", cast(cast("0.99" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_6_strict "${const_sql_4_6}"
    testFoldConst("${const_sql_4_6}")
    def const_sql_4_7 = """select "8.00", cast(cast("8.00" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_7_strict "${const_sql_4_7}"
    testFoldConst("${const_sql_4_7}")
    def const_sql_4_8 = """select "8.01", cast(cast("8.01" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_8_strict "${const_sql_4_8}"
    testFoldConst("${const_sql_4_8}")
    def const_sql_4_9 = """select "8.09", cast(cast("8.09" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_9_strict "${const_sql_4_9}"
    testFoldConst("${const_sql_4_9}")
    def const_sql_4_10 = """select "8.90", cast(cast("8.90" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_10_strict "${const_sql_4_10}"
    testFoldConst("${const_sql_4_10}")
    def const_sql_4_11 = """select "8.91", cast(cast("8.91" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_11_strict "${const_sql_4_11}"
    testFoldConst("${const_sql_4_11}")
    def const_sql_4_12 = """select "8.98", cast(cast("8.98" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_12_strict "${const_sql_4_12}"
    testFoldConst("${const_sql_4_12}")
    def const_sql_4_13 = """select "8.99", cast(cast("8.99" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_13_strict "${const_sql_4_13}"
    testFoldConst("${const_sql_4_13}")
    def const_sql_4_14 = """select "9.00", cast(cast("9.00" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_14_strict "${const_sql_4_14}"
    testFoldConst("${const_sql_4_14}")
    def const_sql_4_15 = """select "9.01", cast(cast("9.01" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_15_strict "${const_sql_4_15}"
    testFoldConst("${const_sql_4_15}")
    def const_sql_4_16 = """select "9.09", cast(cast("9.09" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_16_strict "${const_sql_4_16}"
    testFoldConst("${const_sql_4_16}")
    def const_sql_4_17 = """select "9.90", cast(cast("9.90" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_17_strict "${const_sql_4_17}"
    testFoldConst("${const_sql_4_17}")
    def const_sql_4_18 = """select "9.91", cast(cast("9.91" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_18_strict "${const_sql_4_18}"
    testFoldConst("${const_sql_4_18}")
    def const_sql_4_19 = """select "9.98", cast(cast("9.98" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_19_strict "${const_sql_4_19}"
    testFoldConst("${const_sql_4_19}")
    def const_sql_4_20 = """select "9.99", cast(cast("9.99" as decimalv3(4, 2)) as decimalv3(17, 16));"""
    qt_sql_4_20_strict "${const_sql_4_20}"
    testFoldConst("${const_sql_4_20}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_5_0 = """select "0.000", cast(cast("0.000" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_0_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")
    def const_sql_5_1 = """select "0.001", cast(cast("0.001" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_1_strict "${const_sql_5_1}"
    testFoldConst("${const_sql_5_1}")
    def const_sql_5_2 = """select "0.009", cast(cast("0.009" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_2_strict "${const_sql_5_2}"
    testFoldConst("${const_sql_5_2}")
    def const_sql_5_3 = """select "0.099", cast(cast("0.099" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_3_strict "${const_sql_5_3}"
    testFoldConst("${const_sql_5_3}")
    def const_sql_5_4 = """select "0.900", cast(cast("0.900" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_4_strict "${const_sql_5_4}"
    testFoldConst("${const_sql_5_4}")
    def const_sql_5_5 = """select "0.901", cast(cast("0.901" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_5_strict "${const_sql_5_5}"
    testFoldConst("${const_sql_5_5}")
    def const_sql_5_6 = """select "0.998", cast(cast("0.998" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_6_strict "${const_sql_5_6}"
    testFoldConst("${const_sql_5_6}")
    def const_sql_5_7 = """select "0.999", cast(cast("0.999" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_7_strict "${const_sql_5_7}"
    testFoldConst("${const_sql_5_7}")
    def const_sql_5_8 = """select "8.000", cast(cast("8.000" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_8_strict "${const_sql_5_8}"
    testFoldConst("${const_sql_5_8}")
    def const_sql_5_9 = """select "8.001", cast(cast("8.001" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_9_strict "${const_sql_5_9}"
    testFoldConst("${const_sql_5_9}")
    def const_sql_5_10 = """select "8.009", cast(cast("8.009" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_10_strict "${const_sql_5_10}"
    testFoldConst("${const_sql_5_10}")
    def const_sql_5_11 = """select "8.099", cast(cast("8.099" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_11_strict "${const_sql_5_11}"
    testFoldConst("${const_sql_5_11}")
    def const_sql_5_12 = """select "8.900", cast(cast("8.900" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_12_strict "${const_sql_5_12}"
    testFoldConst("${const_sql_5_12}")
    def const_sql_5_13 = """select "8.901", cast(cast("8.901" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_13_strict "${const_sql_5_13}"
    testFoldConst("${const_sql_5_13}")
    def const_sql_5_14 = """select "8.998", cast(cast("8.998" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_14_strict "${const_sql_5_14}"
    testFoldConst("${const_sql_5_14}")
    def const_sql_5_15 = """select "8.999", cast(cast("8.999" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_15_strict "${const_sql_5_15}"
    testFoldConst("${const_sql_5_15}")
    def const_sql_5_16 = """select "9.000", cast(cast("9.000" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_16_strict "${const_sql_5_16}"
    testFoldConst("${const_sql_5_16}")
    def const_sql_5_17 = """select "9.001", cast(cast("9.001" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_17_strict "${const_sql_5_17}"
    testFoldConst("${const_sql_5_17}")
    def const_sql_5_18 = """select "9.009", cast(cast("9.009" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_18_strict "${const_sql_5_18}"
    testFoldConst("${const_sql_5_18}")
    def const_sql_5_19 = """select "9.099", cast(cast("9.099" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_19_strict "${const_sql_5_19}"
    testFoldConst("${const_sql_5_19}")
    def const_sql_5_20 = """select "9.900", cast(cast("9.900" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_20_strict "${const_sql_5_20}"
    testFoldConst("${const_sql_5_20}")
    def const_sql_5_21 = """select "9.901", cast(cast("9.901" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_21_strict "${const_sql_5_21}"
    testFoldConst("${const_sql_5_21}")
    def const_sql_5_22 = """select "9.998", cast(cast("9.998" as decimalv3(4, 3)) as decimalv3(17, 16));"""
    qt_sql_5_22_strict "${const_sql_5_22}"
    testFoldConst("${const_sql_5_22}")
    def const_sql_5_23 = """select "9.999", cast(cast("9.999" as decimalv3(4, 3)) as decimalv3(17, 16));"""
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
    def const_sql_6_0 = """select "0.0000", cast(cast("0.0000" as decimalv3(4, 4)) as decimalv3(17, 16));"""
    qt_sql_6_0_strict "${const_sql_6_0}"
    testFoldConst("${const_sql_6_0}")
    def const_sql_6_1 = """select "0.0001", cast(cast("0.0001" as decimalv3(4, 4)) as decimalv3(17, 16));"""
    qt_sql_6_1_strict "${const_sql_6_1}"
    testFoldConst("${const_sql_6_1}")
    def const_sql_6_2 = """select "0.0009", cast(cast("0.0009" as decimalv3(4, 4)) as decimalv3(17, 16));"""
    qt_sql_6_2_strict "${const_sql_6_2}"
    testFoldConst("${const_sql_6_2}")
    def const_sql_6_3 = """select "0.0999", cast(cast("0.0999" as decimalv3(4, 4)) as decimalv3(17, 16));"""
    qt_sql_6_3_strict "${const_sql_6_3}"
    testFoldConst("${const_sql_6_3}")
    def const_sql_6_4 = """select "0.9000", cast(cast("0.9000" as decimalv3(4, 4)) as decimalv3(17, 16));"""
    qt_sql_6_4_strict "${const_sql_6_4}"
    testFoldConst("${const_sql_6_4}")
    def const_sql_6_5 = """select "0.9001", cast(cast("0.9001" as decimalv3(4, 4)) as decimalv3(17, 16));"""
    qt_sql_6_5_strict "${const_sql_6_5}"
    testFoldConst("${const_sql_6_5}")
    def const_sql_6_6 = """select "0.9998", cast(cast("0.9998" as decimalv3(4, 4)) as decimalv3(17, 16));"""
    qt_sql_6_6_strict "${const_sql_6_6}"
    testFoldConst("${const_sql_6_6}")
    def const_sql_6_7 = """select "0.9999", cast(cast("0.9999" as decimalv3(4, 4)) as decimalv3(17, 16));"""
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
    def const_sql_7_0 = """select "0", cast(cast("0" as decimalv3(8, 0)) as decimalv3(17, 16));"""
    qt_sql_7_0_strict "${const_sql_7_0}"
    testFoldConst("${const_sql_7_0}")
    def const_sql_7_1 = """select "8", cast(cast("8" as decimalv3(8, 0)) as decimalv3(17, 16));"""
    qt_sql_7_1_strict "${const_sql_7_1}"
    testFoldConst("${const_sql_7_1}")
    def const_sql_7_2 = """select "9", cast(cast("9" as decimalv3(8, 0)) as decimalv3(17, 16));"""
    qt_sql_7_2_strict "${const_sql_7_2}"
    testFoldConst("${const_sql_7_2}")

    sql "set enable_strict_cast=false;"
    qt_sql_7_0_non_strict "${const_sql_7_0}"
    testFoldConst("${const_sql_7_0}")
    qt_sql_7_1_non_strict "${const_sql_7_1}"
    testFoldConst("${const_sql_7_1}")
    qt_sql_7_2_non_strict "${const_sql_7_2}"
    testFoldConst("${const_sql_7_2}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_8_0 = """select "0.0", cast(cast("0.0" as decimalv3(8, 1)) as decimalv3(17, 16));"""
    qt_sql_8_0_strict "${const_sql_8_0}"
    testFoldConst("${const_sql_8_0}")
    def const_sql_8_1 = """select "0.1", cast(cast("0.1" as decimalv3(8, 1)) as decimalv3(17, 16));"""
    qt_sql_8_1_strict "${const_sql_8_1}"
    testFoldConst("${const_sql_8_1}")
    def const_sql_8_2 = """select "0.8", cast(cast("0.8" as decimalv3(8, 1)) as decimalv3(17, 16));"""
    qt_sql_8_2_strict "${const_sql_8_2}"
    testFoldConst("${const_sql_8_2}")
    def const_sql_8_3 = """select "0.9", cast(cast("0.9" as decimalv3(8, 1)) as decimalv3(17, 16));"""
    qt_sql_8_3_strict "${const_sql_8_3}"
    testFoldConst("${const_sql_8_3}")
    def const_sql_8_4 = """select "8.0", cast(cast("8.0" as decimalv3(8, 1)) as decimalv3(17, 16));"""
    qt_sql_8_4_strict "${const_sql_8_4}"
    testFoldConst("${const_sql_8_4}")
    def const_sql_8_5 = """select "8.1", cast(cast("8.1" as decimalv3(8, 1)) as decimalv3(17, 16));"""
    qt_sql_8_5_strict "${const_sql_8_5}"
    testFoldConst("${const_sql_8_5}")
    def const_sql_8_6 = """select "8.8", cast(cast("8.8" as decimalv3(8, 1)) as decimalv3(17, 16));"""
    qt_sql_8_6_strict "${const_sql_8_6}"
    testFoldConst("${const_sql_8_6}")
    def const_sql_8_7 = """select "8.9", cast(cast("8.9" as decimalv3(8, 1)) as decimalv3(17, 16));"""
    qt_sql_8_7_strict "${const_sql_8_7}"
    testFoldConst("${const_sql_8_7}")
    def const_sql_8_8 = """select "9.0", cast(cast("9.0" as decimalv3(8, 1)) as decimalv3(17, 16));"""
    qt_sql_8_8_strict "${const_sql_8_8}"
    testFoldConst("${const_sql_8_8}")
    def const_sql_8_9 = """select "9.1", cast(cast("9.1" as decimalv3(8, 1)) as decimalv3(17, 16));"""
    qt_sql_8_9_strict "${const_sql_8_9}"
    testFoldConst("${const_sql_8_9}")
    def const_sql_8_10 = """select "9.8", cast(cast("9.8" as decimalv3(8, 1)) as decimalv3(17, 16));"""
    qt_sql_8_10_strict "${const_sql_8_10}"
    testFoldConst("${const_sql_8_10}")
    def const_sql_8_11 = """select "9.9", cast(cast("9.9" as decimalv3(8, 1)) as decimalv3(17, 16));"""
    qt_sql_8_11_strict "${const_sql_8_11}"
    testFoldConst("${const_sql_8_11}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_9_0 = """select "0.0000", cast(cast("0.0000" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_0_strict "${const_sql_9_0}"
    testFoldConst("${const_sql_9_0}")
    def const_sql_9_1 = """select "0.0001", cast(cast("0.0001" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_1_strict "${const_sql_9_1}"
    testFoldConst("${const_sql_9_1}")
    def const_sql_9_2 = """select "0.0009", cast(cast("0.0009" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_2_strict "${const_sql_9_2}"
    testFoldConst("${const_sql_9_2}")
    def const_sql_9_3 = """select "0.0999", cast(cast("0.0999" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_3_strict "${const_sql_9_3}"
    testFoldConst("${const_sql_9_3}")
    def const_sql_9_4 = """select "0.9000", cast(cast("0.9000" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_4_strict "${const_sql_9_4}"
    testFoldConst("${const_sql_9_4}")
    def const_sql_9_5 = """select "0.9001", cast(cast("0.9001" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_5_strict "${const_sql_9_5}"
    testFoldConst("${const_sql_9_5}")
    def const_sql_9_6 = """select "0.9998", cast(cast("0.9998" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_6_strict "${const_sql_9_6}"
    testFoldConst("${const_sql_9_6}")
    def const_sql_9_7 = """select "0.9999", cast(cast("0.9999" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_7_strict "${const_sql_9_7}"
    testFoldConst("${const_sql_9_7}")
    def const_sql_9_8 = """select "8.0000", cast(cast("8.0000" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_8_strict "${const_sql_9_8}"
    testFoldConst("${const_sql_9_8}")
    def const_sql_9_9 = """select "8.0001", cast(cast("8.0001" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_9_strict "${const_sql_9_9}"
    testFoldConst("${const_sql_9_9}")
    def const_sql_9_10 = """select "8.0009", cast(cast("8.0009" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_10_strict "${const_sql_9_10}"
    testFoldConst("${const_sql_9_10}")
    def const_sql_9_11 = """select "8.0999", cast(cast("8.0999" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_11_strict "${const_sql_9_11}"
    testFoldConst("${const_sql_9_11}")
    def const_sql_9_12 = """select "8.9000", cast(cast("8.9000" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_12_strict "${const_sql_9_12}"
    testFoldConst("${const_sql_9_12}")
    def const_sql_9_13 = """select "8.9001", cast(cast("8.9001" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_13_strict "${const_sql_9_13}"
    testFoldConst("${const_sql_9_13}")
    def const_sql_9_14 = """select "8.9998", cast(cast("8.9998" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_14_strict "${const_sql_9_14}"
    testFoldConst("${const_sql_9_14}")
    def const_sql_9_15 = """select "8.9999", cast(cast("8.9999" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_15_strict "${const_sql_9_15}"
    testFoldConst("${const_sql_9_15}")
    def const_sql_9_16 = """select "9.0000", cast(cast("9.0000" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_16_strict "${const_sql_9_16}"
    testFoldConst("${const_sql_9_16}")
    def const_sql_9_17 = """select "9.0001", cast(cast("9.0001" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_17_strict "${const_sql_9_17}"
    testFoldConst("${const_sql_9_17}")
    def const_sql_9_18 = """select "9.0009", cast(cast("9.0009" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_18_strict "${const_sql_9_18}"
    testFoldConst("${const_sql_9_18}")
    def const_sql_9_19 = """select "9.0999", cast(cast("9.0999" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_19_strict "${const_sql_9_19}"
    testFoldConst("${const_sql_9_19}")
    def const_sql_9_20 = """select "9.9000", cast(cast("9.9000" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_20_strict "${const_sql_9_20}"
    testFoldConst("${const_sql_9_20}")
    def const_sql_9_21 = """select "9.9001", cast(cast("9.9001" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_21_strict "${const_sql_9_21}"
    testFoldConst("${const_sql_9_21}")
    def const_sql_9_22 = """select "9.9998", cast(cast("9.9998" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_22_strict "${const_sql_9_22}"
    testFoldConst("${const_sql_9_22}")
    def const_sql_9_23 = """select "9.9999", cast(cast("9.9999" as decimalv3(8, 4)) as decimalv3(17, 16));"""
    qt_sql_9_23_strict "${const_sql_9_23}"
    testFoldConst("${const_sql_9_23}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_10_0 = """select "0.0000000", cast(cast("0.0000000" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_0_strict "${const_sql_10_0}"
    testFoldConst("${const_sql_10_0}")
    def const_sql_10_1 = """select "0.0000001", cast(cast("0.0000001" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_1_strict "${const_sql_10_1}"
    testFoldConst("${const_sql_10_1}")
    def const_sql_10_2 = """select "0.0000009", cast(cast("0.0000009" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_2_strict "${const_sql_10_2}"
    testFoldConst("${const_sql_10_2}")
    def const_sql_10_3 = """select "0.0999999", cast(cast("0.0999999" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_3_strict "${const_sql_10_3}"
    testFoldConst("${const_sql_10_3}")
    def const_sql_10_4 = """select "0.9000000", cast(cast("0.9000000" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_4_strict "${const_sql_10_4}"
    testFoldConst("${const_sql_10_4}")
    def const_sql_10_5 = """select "0.9000001", cast(cast("0.9000001" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_5_strict "${const_sql_10_5}"
    testFoldConst("${const_sql_10_5}")
    def const_sql_10_6 = """select "0.9999998", cast(cast("0.9999998" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_6_strict "${const_sql_10_6}"
    testFoldConst("${const_sql_10_6}")
    def const_sql_10_7 = """select "0.9999999", cast(cast("0.9999999" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_7_strict "${const_sql_10_7}"
    testFoldConst("${const_sql_10_7}")
    def const_sql_10_8 = """select "8.0000000", cast(cast("8.0000000" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_8_strict "${const_sql_10_8}"
    testFoldConst("${const_sql_10_8}")
    def const_sql_10_9 = """select "8.0000001", cast(cast("8.0000001" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_9_strict "${const_sql_10_9}"
    testFoldConst("${const_sql_10_9}")
    def const_sql_10_10 = """select "8.0000009", cast(cast("8.0000009" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_10_strict "${const_sql_10_10}"
    testFoldConst("${const_sql_10_10}")
    def const_sql_10_11 = """select "8.0999999", cast(cast("8.0999999" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_11_strict "${const_sql_10_11}"
    testFoldConst("${const_sql_10_11}")
    def const_sql_10_12 = """select "8.9000000", cast(cast("8.9000000" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_12_strict "${const_sql_10_12}"
    testFoldConst("${const_sql_10_12}")
    def const_sql_10_13 = """select "8.9000001", cast(cast("8.9000001" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_13_strict "${const_sql_10_13}"
    testFoldConst("${const_sql_10_13}")
    def const_sql_10_14 = """select "8.9999998", cast(cast("8.9999998" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_14_strict "${const_sql_10_14}"
    testFoldConst("${const_sql_10_14}")
    def const_sql_10_15 = """select "8.9999999", cast(cast("8.9999999" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_15_strict "${const_sql_10_15}"
    testFoldConst("${const_sql_10_15}")
    def const_sql_10_16 = """select "9.0000000", cast(cast("9.0000000" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_16_strict "${const_sql_10_16}"
    testFoldConst("${const_sql_10_16}")
    def const_sql_10_17 = """select "9.0000001", cast(cast("9.0000001" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_17_strict "${const_sql_10_17}"
    testFoldConst("${const_sql_10_17}")
    def const_sql_10_18 = """select "9.0000009", cast(cast("9.0000009" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_18_strict "${const_sql_10_18}"
    testFoldConst("${const_sql_10_18}")
    def const_sql_10_19 = """select "9.0999999", cast(cast("9.0999999" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_19_strict "${const_sql_10_19}"
    testFoldConst("${const_sql_10_19}")
    def const_sql_10_20 = """select "9.9000000", cast(cast("9.9000000" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_20_strict "${const_sql_10_20}"
    testFoldConst("${const_sql_10_20}")
    def const_sql_10_21 = """select "9.9000001", cast(cast("9.9000001" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_21_strict "${const_sql_10_21}"
    testFoldConst("${const_sql_10_21}")
    def const_sql_10_22 = """select "9.9999998", cast(cast("9.9999998" as decimalv3(8, 7)) as decimalv3(17, 16));"""
    qt_sql_10_22_strict "${const_sql_10_22}"
    testFoldConst("${const_sql_10_22}")
    def const_sql_10_23 = """select "9.9999999", cast(cast("9.9999999" as decimalv3(8, 7)) as decimalv3(17, 16));"""
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
    def const_sql_11_0 = """select "0.00000000", cast(cast("0.00000000" as decimalv3(8, 8)) as decimalv3(17, 16));"""
    qt_sql_11_0_strict "${const_sql_11_0}"
    testFoldConst("${const_sql_11_0}")
    def const_sql_11_1 = """select "0.00000001", cast(cast("0.00000001" as decimalv3(8, 8)) as decimalv3(17, 16));"""
    qt_sql_11_1_strict "${const_sql_11_1}"
    testFoldConst("${const_sql_11_1}")
    def const_sql_11_2 = """select "0.00000009", cast(cast("0.00000009" as decimalv3(8, 8)) as decimalv3(17, 16));"""
    qt_sql_11_2_strict "${const_sql_11_2}"
    testFoldConst("${const_sql_11_2}")
    def const_sql_11_3 = """select "0.09999999", cast(cast("0.09999999" as decimalv3(8, 8)) as decimalv3(17, 16));"""
    qt_sql_11_3_strict "${const_sql_11_3}"
    testFoldConst("${const_sql_11_3}")
    def const_sql_11_4 = """select "0.90000000", cast(cast("0.90000000" as decimalv3(8, 8)) as decimalv3(17, 16));"""
    qt_sql_11_4_strict "${const_sql_11_4}"
    testFoldConst("${const_sql_11_4}")
    def const_sql_11_5 = """select "0.90000001", cast(cast("0.90000001" as decimalv3(8, 8)) as decimalv3(17, 16));"""
    qt_sql_11_5_strict "${const_sql_11_5}"
    testFoldConst("${const_sql_11_5}")
    def const_sql_11_6 = """select "0.99999998", cast(cast("0.99999998" as decimalv3(8, 8)) as decimalv3(17, 16));"""
    qt_sql_11_6_strict "${const_sql_11_6}"
    testFoldConst("${const_sql_11_6}")
    def const_sql_11_7 = """select "0.99999999", cast(cast("0.99999999" as decimalv3(8, 8)) as decimalv3(17, 16));"""
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
    def const_sql_12_0 = """select "0", cast(cast("0" as decimalv3(9, 0)) as decimalv3(17, 16));"""
    qt_sql_12_0_strict "${const_sql_12_0}"
    testFoldConst("${const_sql_12_0}")
    def const_sql_12_1 = """select "8", cast(cast("8" as decimalv3(9, 0)) as decimalv3(17, 16));"""
    qt_sql_12_1_strict "${const_sql_12_1}"
    testFoldConst("${const_sql_12_1}")
    def const_sql_12_2 = """select "9", cast(cast("9" as decimalv3(9, 0)) as decimalv3(17, 16));"""
    qt_sql_12_2_strict "${const_sql_12_2}"
    testFoldConst("${const_sql_12_2}")

    sql "set enable_strict_cast=false;"
    qt_sql_12_0_non_strict "${const_sql_12_0}"
    testFoldConst("${const_sql_12_0}")
    qt_sql_12_1_non_strict "${const_sql_12_1}"
    testFoldConst("${const_sql_12_1}")
    qt_sql_12_2_non_strict "${const_sql_12_2}"
    testFoldConst("${const_sql_12_2}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_13_0 = """select "0.0", cast(cast("0.0" as decimalv3(9, 1)) as decimalv3(17, 16));"""
    qt_sql_13_0_strict "${const_sql_13_0}"
    testFoldConst("${const_sql_13_0}")
    def const_sql_13_1 = """select "0.1", cast(cast("0.1" as decimalv3(9, 1)) as decimalv3(17, 16));"""
    qt_sql_13_1_strict "${const_sql_13_1}"
    testFoldConst("${const_sql_13_1}")
    def const_sql_13_2 = """select "0.8", cast(cast("0.8" as decimalv3(9, 1)) as decimalv3(17, 16));"""
    qt_sql_13_2_strict "${const_sql_13_2}"
    testFoldConst("${const_sql_13_2}")
    def const_sql_13_3 = """select "0.9", cast(cast("0.9" as decimalv3(9, 1)) as decimalv3(17, 16));"""
    qt_sql_13_3_strict "${const_sql_13_3}"
    testFoldConst("${const_sql_13_3}")
    def const_sql_13_4 = """select "8.0", cast(cast("8.0" as decimalv3(9, 1)) as decimalv3(17, 16));"""
    qt_sql_13_4_strict "${const_sql_13_4}"
    testFoldConst("${const_sql_13_4}")
    def const_sql_13_5 = """select "8.1", cast(cast("8.1" as decimalv3(9, 1)) as decimalv3(17, 16));"""
    qt_sql_13_5_strict "${const_sql_13_5}"
    testFoldConst("${const_sql_13_5}")
    def const_sql_13_6 = """select "8.8", cast(cast("8.8" as decimalv3(9, 1)) as decimalv3(17, 16));"""
    qt_sql_13_6_strict "${const_sql_13_6}"
    testFoldConst("${const_sql_13_6}")
    def const_sql_13_7 = """select "8.9", cast(cast("8.9" as decimalv3(9, 1)) as decimalv3(17, 16));"""
    qt_sql_13_7_strict "${const_sql_13_7}"
    testFoldConst("${const_sql_13_7}")
    def const_sql_13_8 = """select "9.0", cast(cast("9.0" as decimalv3(9, 1)) as decimalv3(17, 16));"""
    qt_sql_13_8_strict "${const_sql_13_8}"
    testFoldConst("${const_sql_13_8}")
    def const_sql_13_9 = """select "9.1", cast(cast("9.1" as decimalv3(9, 1)) as decimalv3(17, 16));"""
    qt_sql_13_9_strict "${const_sql_13_9}"
    testFoldConst("${const_sql_13_9}")
    def const_sql_13_10 = """select "9.8", cast(cast("9.8" as decimalv3(9, 1)) as decimalv3(17, 16));"""
    qt_sql_13_10_strict "${const_sql_13_10}"
    testFoldConst("${const_sql_13_10}")
    def const_sql_13_11 = """select "9.9", cast(cast("9.9" as decimalv3(9, 1)) as decimalv3(17, 16));"""
    qt_sql_13_11_strict "${const_sql_13_11}"
    testFoldConst("${const_sql_13_11}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_14_0 = """select "0.0000", cast(cast("0.0000" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_0_strict "${const_sql_14_0}"
    testFoldConst("${const_sql_14_0}")
    def const_sql_14_1 = """select "0.0001", cast(cast("0.0001" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_1_strict "${const_sql_14_1}"
    testFoldConst("${const_sql_14_1}")
    def const_sql_14_2 = """select "0.0009", cast(cast("0.0009" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_2_strict "${const_sql_14_2}"
    testFoldConst("${const_sql_14_2}")
    def const_sql_14_3 = """select "0.0999", cast(cast("0.0999" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_3_strict "${const_sql_14_3}"
    testFoldConst("${const_sql_14_3}")
    def const_sql_14_4 = """select "0.9000", cast(cast("0.9000" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_4_strict "${const_sql_14_4}"
    testFoldConst("${const_sql_14_4}")
    def const_sql_14_5 = """select "0.9001", cast(cast("0.9001" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_5_strict "${const_sql_14_5}"
    testFoldConst("${const_sql_14_5}")
    def const_sql_14_6 = """select "0.9998", cast(cast("0.9998" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_6_strict "${const_sql_14_6}"
    testFoldConst("${const_sql_14_6}")
    def const_sql_14_7 = """select "0.9999", cast(cast("0.9999" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_7_strict "${const_sql_14_7}"
    testFoldConst("${const_sql_14_7}")
    def const_sql_14_8 = """select "8.0000", cast(cast("8.0000" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_8_strict "${const_sql_14_8}"
    testFoldConst("${const_sql_14_8}")
    def const_sql_14_9 = """select "8.0001", cast(cast("8.0001" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_9_strict "${const_sql_14_9}"
    testFoldConst("${const_sql_14_9}")
    def const_sql_14_10 = """select "8.0009", cast(cast("8.0009" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_10_strict "${const_sql_14_10}"
    testFoldConst("${const_sql_14_10}")
    def const_sql_14_11 = """select "8.0999", cast(cast("8.0999" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_11_strict "${const_sql_14_11}"
    testFoldConst("${const_sql_14_11}")
    def const_sql_14_12 = """select "8.9000", cast(cast("8.9000" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_12_strict "${const_sql_14_12}"
    testFoldConst("${const_sql_14_12}")
    def const_sql_14_13 = """select "8.9001", cast(cast("8.9001" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_13_strict "${const_sql_14_13}"
    testFoldConst("${const_sql_14_13}")
    def const_sql_14_14 = """select "8.9998", cast(cast("8.9998" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_14_strict "${const_sql_14_14}"
    testFoldConst("${const_sql_14_14}")
    def const_sql_14_15 = """select "8.9999", cast(cast("8.9999" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_15_strict "${const_sql_14_15}"
    testFoldConst("${const_sql_14_15}")
    def const_sql_14_16 = """select "9.0000", cast(cast("9.0000" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_16_strict "${const_sql_14_16}"
    testFoldConst("${const_sql_14_16}")
    def const_sql_14_17 = """select "9.0001", cast(cast("9.0001" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_17_strict "${const_sql_14_17}"
    testFoldConst("${const_sql_14_17}")
    def const_sql_14_18 = """select "9.0009", cast(cast("9.0009" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_18_strict "${const_sql_14_18}"
    testFoldConst("${const_sql_14_18}")
    def const_sql_14_19 = """select "9.0999", cast(cast("9.0999" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_19_strict "${const_sql_14_19}"
    testFoldConst("${const_sql_14_19}")
    def const_sql_14_20 = """select "9.9000", cast(cast("9.9000" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_20_strict "${const_sql_14_20}"
    testFoldConst("${const_sql_14_20}")
    def const_sql_14_21 = """select "9.9001", cast(cast("9.9001" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_21_strict "${const_sql_14_21}"
    testFoldConst("${const_sql_14_21}")
    def const_sql_14_22 = """select "9.9998", cast(cast("9.9998" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_22_strict "${const_sql_14_22}"
    testFoldConst("${const_sql_14_22}")
    def const_sql_14_23 = """select "9.9999", cast(cast("9.9999" as decimalv3(9, 4)) as decimalv3(17, 16));"""
    qt_sql_14_23_strict "${const_sql_14_23}"
    testFoldConst("${const_sql_14_23}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_15_0 = """select "0.00000000", cast(cast("0.00000000" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_0_strict "${const_sql_15_0}"
    testFoldConst("${const_sql_15_0}")
    def const_sql_15_1 = """select "0.00000001", cast(cast("0.00000001" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_1_strict "${const_sql_15_1}"
    testFoldConst("${const_sql_15_1}")
    def const_sql_15_2 = """select "0.00000009", cast(cast("0.00000009" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_2_strict "${const_sql_15_2}"
    testFoldConst("${const_sql_15_2}")
    def const_sql_15_3 = """select "0.09999999", cast(cast("0.09999999" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_3_strict "${const_sql_15_3}"
    testFoldConst("${const_sql_15_3}")
    def const_sql_15_4 = """select "0.90000000", cast(cast("0.90000000" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_4_strict "${const_sql_15_4}"
    testFoldConst("${const_sql_15_4}")
    def const_sql_15_5 = """select "0.90000001", cast(cast("0.90000001" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_5_strict "${const_sql_15_5}"
    testFoldConst("${const_sql_15_5}")
    def const_sql_15_6 = """select "0.99999998", cast(cast("0.99999998" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_6_strict "${const_sql_15_6}"
    testFoldConst("${const_sql_15_6}")
    def const_sql_15_7 = """select "0.99999999", cast(cast("0.99999999" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_7_strict "${const_sql_15_7}"
    testFoldConst("${const_sql_15_7}")
    def const_sql_15_8 = """select "8.00000000", cast(cast("8.00000000" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_8_strict "${const_sql_15_8}"
    testFoldConst("${const_sql_15_8}")
    def const_sql_15_9 = """select "8.00000001", cast(cast("8.00000001" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_9_strict "${const_sql_15_9}"
    testFoldConst("${const_sql_15_9}")
    def const_sql_15_10 = """select "8.00000009", cast(cast("8.00000009" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_10_strict "${const_sql_15_10}"
    testFoldConst("${const_sql_15_10}")
    def const_sql_15_11 = """select "8.09999999", cast(cast("8.09999999" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_11_strict "${const_sql_15_11}"
    testFoldConst("${const_sql_15_11}")
    def const_sql_15_12 = """select "8.90000000", cast(cast("8.90000000" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_12_strict "${const_sql_15_12}"
    testFoldConst("${const_sql_15_12}")
    def const_sql_15_13 = """select "8.90000001", cast(cast("8.90000001" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_13_strict "${const_sql_15_13}"
    testFoldConst("${const_sql_15_13}")
    def const_sql_15_14 = """select "8.99999998", cast(cast("8.99999998" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_14_strict "${const_sql_15_14}"
    testFoldConst("${const_sql_15_14}")
    def const_sql_15_15 = """select "8.99999999", cast(cast("8.99999999" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_15_strict "${const_sql_15_15}"
    testFoldConst("${const_sql_15_15}")
    def const_sql_15_16 = """select "9.00000000", cast(cast("9.00000000" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_16_strict "${const_sql_15_16}"
    testFoldConst("${const_sql_15_16}")
    def const_sql_15_17 = """select "9.00000001", cast(cast("9.00000001" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_17_strict "${const_sql_15_17}"
    testFoldConst("${const_sql_15_17}")
    def const_sql_15_18 = """select "9.00000009", cast(cast("9.00000009" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_18_strict "${const_sql_15_18}"
    testFoldConst("${const_sql_15_18}")
    def const_sql_15_19 = """select "9.09999999", cast(cast("9.09999999" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_19_strict "${const_sql_15_19}"
    testFoldConst("${const_sql_15_19}")
    def const_sql_15_20 = """select "9.90000000", cast(cast("9.90000000" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_20_strict "${const_sql_15_20}"
    testFoldConst("${const_sql_15_20}")
    def const_sql_15_21 = """select "9.90000001", cast(cast("9.90000001" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_21_strict "${const_sql_15_21}"
    testFoldConst("${const_sql_15_21}")
    def const_sql_15_22 = """select "9.99999998", cast(cast("9.99999998" as decimalv3(9, 8)) as decimalv3(17, 16));"""
    qt_sql_15_22_strict "${const_sql_15_22}"
    testFoldConst("${const_sql_15_22}")
    def const_sql_15_23 = """select "9.99999999", cast(cast("9.99999999" as decimalv3(9, 8)) as decimalv3(17, 16));"""
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
    def const_sql_16_0 = """select "0.000000000", cast(cast("0.000000000" as decimalv3(9, 9)) as decimalv3(17, 16));"""
    qt_sql_16_0_strict "${const_sql_16_0}"
    testFoldConst("${const_sql_16_0}")
    def const_sql_16_1 = """select "0.000000001", cast(cast("0.000000001" as decimalv3(9, 9)) as decimalv3(17, 16));"""
    qt_sql_16_1_strict "${const_sql_16_1}"
    testFoldConst("${const_sql_16_1}")
    def const_sql_16_2 = """select "0.000000009", cast(cast("0.000000009" as decimalv3(9, 9)) as decimalv3(17, 16));"""
    qt_sql_16_2_strict "${const_sql_16_2}"
    testFoldConst("${const_sql_16_2}")
    def const_sql_16_3 = """select "0.099999999", cast(cast("0.099999999" as decimalv3(9, 9)) as decimalv3(17, 16));"""
    qt_sql_16_3_strict "${const_sql_16_3}"
    testFoldConst("${const_sql_16_3}")
    def const_sql_16_4 = """select "0.900000000", cast(cast("0.900000000" as decimalv3(9, 9)) as decimalv3(17, 16));"""
    qt_sql_16_4_strict "${const_sql_16_4}"
    testFoldConst("${const_sql_16_4}")
    def const_sql_16_5 = """select "0.900000001", cast(cast("0.900000001" as decimalv3(9, 9)) as decimalv3(17, 16));"""
    qt_sql_16_5_strict "${const_sql_16_5}"
    testFoldConst("${const_sql_16_5}")
    def const_sql_16_6 = """select "0.999999998", cast(cast("0.999999998" as decimalv3(9, 9)) as decimalv3(17, 16));"""
    qt_sql_16_6_strict "${const_sql_16_6}"
    testFoldConst("${const_sql_16_6}")
    def const_sql_16_7 = """select "0.999999999", cast(cast("0.999999999" as decimalv3(9, 9)) as decimalv3(17, 16));"""
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