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


suite("test_cast_to_decimal32_8_8_from_decimal64_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "0", cast(cast("0" as decimalv3(10, 0)) as decimalv3(8, 8));"""
    qt_sql_0_0_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")

    sql "set enable_strict_cast=false;"
    qt_sql_0_0_non_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_1_0 = """select "0.0", cast(cast("0.0" as decimalv3(10, 1)) as decimalv3(8, 8));"""
    qt_sql_1_0_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    def const_sql_1_1 = """select "0.1", cast(cast("0.1" as decimalv3(10, 1)) as decimalv3(8, 8));"""
    qt_sql_1_1_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    def const_sql_1_2 = """select "0.8", cast(cast("0.8" as decimalv3(10, 1)) as decimalv3(8, 8));"""
    qt_sql_1_2_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    def const_sql_1_3 = """select "0.9", cast(cast("0.9" as decimalv3(10, 1)) as decimalv3(8, 8));"""
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
    def const_sql_2_0 = """select "0.00000", cast(cast("0.00000" as decimalv3(10, 5)) as decimalv3(8, 8));"""
    qt_sql_2_0_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    def const_sql_2_1 = """select "0.00001", cast(cast("0.00001" as decimalv3(10, 5)) as decimalv3(8, 8));"""
    qt_sql_2_1_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    def const_sql_2_2 = """select "0.00009", cast(cast("0.00009" as decimalv3(10, 5)) as decimalv3(8, 8));"""
    qt_sql_2_2_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    def const_sql_2_3 = """select "0.09999", cast(cast("0.09999" as decimalv3(10, 5)) as decimalv3(8, 8));"""
    qt_sql_2_3_strict "${const_sql_2_3}"
    testFoldConst("${const_sql_2_3}")
    def const_sql_2_4 = """select "0.90000", cast(cast("0.90000" as decimalv3(10, 5)) as decimalv3(8, 8));"""
    qt_sql_2_4_strict "${const_sql_2_4}"
    testFoldConst("${const_sql_2_4}")
    def const_sql_2_5 = """select "0.90001", cast(cast("0.90001" as decimalv3(10, 5)) as decimalv3(8, 8));"""
    qt_sql_2_5_strict "${const_sql_2_5}"
    testFoldConst("${const_sql_2_5}")
    def const_sql_2_6 = """select "0.99998", cast(cast("0.99998" as decimalv3(10, 5)) as decimalv3(8, 8));"""
    qt_sql_2_6_strict "${const_sql_2_6}"
    testFoldConst("${const_sql_2_6}")
    def const_sql_2_7 = """select "0.99999", cast(cast("0.99999" as decimalv3(10, 5)) as decimalv3(8, 8));"""
    qt_sql_2_7_strict "${const_sql_2_7}"
    testFoldConst("${const_sql_2_7}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_3_0 = """select "0.000000000", cast(cast("0.000000000" as decimalv3(10, 9)) as decimalv3(8, 8));"""
    qt_sql_3_0_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    def const_sql_3_1 = """select "0.000000001", cast(cast("0.000000001" as decimalv3(10, 9)) as decimalv3(8, 8));"""
    qt_sql_3_1_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    def const_sql_3_2 = """select "0.000000009", cast(cast("0.000000009" as decimalv3(10, 9)) as decimalv3(8, 8));"""
    qt_sql_3_2_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    def const_sql_3_3 = """select "0.099999999", cast(cast("0.099999999" as decimalv3(10, 9)) as decimalv3(8, 8));"""
    qt_sql_3_3_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    def const_sql_3_4 = """select "0.900000000", cast(cast("0.900000000" as decimalv3(10, 9)) as decimalv3(8, 8));"""
    qt_sql_3_4_strict "${const_sql_3_4}"
    testFoldConst("${const_sql_3_4}")
    def const_sql_3_5 = """select "0.900000001", cast(cast("0.900000001" as decimalv3(10, 9)) as decimalv3(8, 8));"""
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
    def const_sql_4_0 = """select "0.0000000000", cast(cast("0.0000000000" as decimalv3(10, 10)) as decimalv3(8, 8));"""
    qt_sql_4_0_strict "${const_sql_4_0}"
    testFoldConst("${const_sql_4_0}")
    def const_sql_4_1 = """select "0.0000000001", cast(cast("0.0000000001" as decimalv3(10, 10)) as decimalv3(8, 8));"""
    qt_sql_4_1_strict "${const_sql_4_1}"
    testFoldConst("${const_sql_4_1}")
    def const_sql_4_2 = """select "0.0000000009", cast(cast("0.0000000009" as decimalv3(10, 10)) as decimalv3(8, 8));"""
    qt_sql_4_2_strict "${const_sql_4_2}"
    testFoldConst("${const_sql_4_2}")
    def const_sql_4_3 = """select "0.0999999999", cast(cast("0.0999999999" as decimalv3(10, 10)) as decimalv3(8, 8));"""
    qt_sql_4_3_strict "${const_sql_4_3}"
    testFoldConst("${const_sql_4_3}")
    def const_sql_4_4 = """select "0.9000000000", cast(cast("0.9000000000" as decimalv3(10, 10)) as decimalv3(8, 8));"""
    qt_sql_4_4_strict "${const_sql_4_4}"
    testFoldConst("${const_sql_4_4}")
    def const_sql_4_5 = """select "0.9000000001", cast(cast("0.9000000001" as decimalv3(10, 10)) as decimalv3(8, 8));"""
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
    def const_sql_5_0 = """select "0", cast(cast("0" as decimalv3(17, 0)) as decimalv3(8, 8));"""
    qt_sql_5_0_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")

    sql "set enable_strict_cast=false;"
    qt_sql_5_0_non_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_6_0 = """select "0.0", cast(cast("0.0" as decimalv3(17, 1)) as decimalv3(8, 8));"""
    qt_sql_6_0_strict "${const_sql_6_0}"
    testFoldConst("${const_sql_6_0}")
    def const_sql_6_1 = """select "0.1", cast(cast("0.1" as decimalv3(17, 1)) as decimalv3(8, 8));"""
    qt_sql_6_1_strict "${const_sql_6_1}"
    testFoldConst("${const_sql_6_1}")
    def const_sql_6_2 = """select "0.8", cast(cast("0.8" as decimalv3(17, 1)) as decimalv3(8, 8));"""
    qt_sql_6_2_strict "${const_sql_6_2}"
    testFoldConst("${const_sql_6_2}")
    def const_sql_6_3 = """select "0.9", cast(cast("0.9" as decimalv3(17, 1)) as decimalv3(8, 8));"""
    qt_sql_6_3_strict "${const_sql_6_3}"
    testFoldConst("${const_sql_6_3}")

    sql "set enable_strict_cast=false;"
    qt_sql_6_0_non_strict "${const_sql_6_0}"
    testFoldConst("${const_sql_6_0}")
    qt_sql_6_1_non_strict "${const_sql_6_1}"
    testFoldConst("${const_sql_6_1}")
    qt_sql_6_2_non_strict "${const_sql_6_2}"
    testFoldConst("${const_sql_6_2}")
    qt_sql_6_3_non_strict "${const_sql_6_3}"
    testFoldConst("${const_sql_6_3}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_7_0 = """select "0.00000000", cast(cast("0.00000000" as decimalv3(17, 8)) as decimalv3(8, 8));"""
    qt_sql_7_0_strict "${const_sql_7_0}"
    testFoldConst("${const_sql_7_0}")
    def const_sql_7_1 = """select "0.00000001", cast(cast("0.00000001" as decimalv3(17, 8)) as decimalv3(8, 8));"""
    qt_sql_7_1_strict "${const_sql_7_1}"
    testFoldConst("${const_sql_7_1}")
    def const_sql_7_2 = """select "0.00000009", cast(cast("0.00000009" as decimalv3(17, 8)) as decimalv3(8, 8));"""
    qt_sql_7_2_strict "${const_sql_7_2}"
    testFoldConst("${const_sql_7_2}")
    def const_sql_7_3 = """select "0.09999999", cast(cast("0.09999999" as decimalv3(17, 8)) as decimalv3(8, 8));"""
    qt_sql_7_3_strict "${const_sql_7_3}"
    testFoldConst("${const_sql_7_3}")
    def const_sql_7_4 = """select "0.90000000", cast(cast("0.90000000" as decimalv3(17, 8)) as decimalv3(8, 8));"""
    qt_sql_7_4_strict "${const_sql_7_4}"
    testFoldConst("${const_sql_7_4}")
    def const_sql_7_5 = """select "0.90000001", cast(cast("0.90000001" as decimalv3(17, 8)) as decimalv3(8, 8));"""
    qt_sql_7_5_strict "${const_sql_7_5}"
    testFoldConst("${const_sql_7_5}")
    def const_sql_7_6 = """select "0.99999998", cast(cast("0.99999998" as decimalv3(17, 8)) as decimalv3(8, 8));"""
    qt_sql_7_6_strict "${const_sql_7_6}"
    testFoldConst("${const_sql_7_6}")
    def const_sql_7_7 = """select "0.99999999", cast(cast("0.99999999" as decimalv3(17, 8)) as decimalv3(8, 8));"""
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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_8_0 = """select "0.0000000000000000", cast(cast("0.0000000000000000" as decimalv3(17, 16)) as decimalv3(8, 8));"""
    qt_sql_8_0_strict "${const_sql_8_0}"
    testFoldConst("${const_sql_8_0}")
    def const_sql_8_1 = """select "0.0000000000000001", cast(cast("0.0000000000000001" as decimalv3(17, 16)) as decimalv3(8, 8));"""
    qt_sql_8_1_strict "${const_sql_8_1}"
    testFoldConst("${const_sql_8_1}")
    def const_sql_8_2 = """select "0.0000000000000009", cast(cast("0.0000000000000009" as decimalv3(17, 16)) as decimalv3(8, 8));"""
    qt_sql_8_2_strict "${const_sql_8_2}"
    testFoldConst("${const_sql_8_2}")
    def const_sql_8_3 = """select "0.0999999999999999", cast(cast("0.0999999999999999" as decimalv3(17, 16)) as decimalv3(8, 8));"""
    qt_sql_8_3_strict "${const_sql_8_3}"
    testFoldConst("${const_sql_8_3}")
    def const_sql_8_4 = """select "0.9000000000000000", cast(cast("0.9000000000000000" as decimalv3(17, 16)) as decimalv3(8, 8));"""
    qt_sql_8_4_strict "${const_sql_8_4}"
    testFoldConst("${const_sql_8_4}")
    def const_sql_8_5 = """select "0.9000000000000001", cast(cast("0.9000000000000001" as decimalv3(17, 16)) as decimalv3(8, 8));"""
    qt_sql_8_5_strict "${const_sql_8_5}"
    testFoldConst("${const_sql_8_5}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_9_0 = """select "0.00000000000000000", cast(cast("0.00000000000000000" as decimalv3(17, 17)) as decimalv3(8, 8));"""
    qt_sql_9_0_strict "${const_sql_9_0}"
    testFoldConst("${const_sql_9_0}")
    def const_sql_9_1 = """select "0.00000000000000001", cast(cast("0.00000000000000001" as decimalv3(17, 17)) as decimalv3(8, 8));"""
    qt_sql_9_1_strict "${const_sql_9_1}"
    testFoldConst("${const_sql_9_1}")
    def const_sql_9_2 = """select "0.00000000000000009", cast(cast("0.00000000000000009" as decimalv3(17, 17)) as decimalv3(8, 8));"""
    qt_sql_9_2_strict "${const_sql_9_2}"
    testFoldConst("${const_sql_9_2}")
    def const_sql_9_3 = """select "0.09999999999999999", cast(cast("0.09999999999999999" as decimalv3(17, 17)) as decimalv3(8, 8));"""
    qt_sql_9_3_strict "${const_sql_9_3}"
    testFoldConst("${const_sql_9_3}")
    def const_sql_9_4 = """select "0.90000000000000000", cast(cast("0.90000000000000000" as decimalv3(17, 17)) as decimalv3(8, 8));"""
    qt_sql_9_4_strict "${const_sql_9_4}"
    testFoldConst("${const_sql_9_4}")
    def const_sql_9_5 = """select "0.90000000000000001", cast(cast("0.90000000000000001" as decimalv3(17, 17)) as decimalv3(8, 8));"""
    qt_sql_9_5_strict "${const_sql_9_5}"
    testFoldConst("${const_sql_9_5}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_10_0 = """select "0", cast(cast("0" as decimalv3(18, 0)) as decimalv3(8, 8));"""
    qt_sql_10_0_strict "${const_sql_10_0}"
    testFoldConst("${const_sql_10_0}")

    sql "set enable_strict_cast=false;"
    qt_sql_10_0_non_strict "${const_sql_10_0}"
    testFoldConst("${const_sql_10_0}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_11_0 = """select "0.0", cast(cast("0.0" as decimalv3(18, 1)) as decimalv3(8, 8));"""
    qt_sql_11_0_strict "${const_sql_11_0}"
    testFoldConst("${const_sql_11_0}")
    def const_sql_11_1 = """select "0.1", cast(cast("0.1" as decimalv3(18, 1)) as decimalv3(8, 8));"""
    qt_sql_11_1_strict "${const_sql_11_1}"
    testFoldConst("${const_sql_11_1}")
    def const_sql_11_2 = """select "0.8", cast(cast("0.8" as decimalv3(18, 1)) as decimalv3(8, 8));"""
    qt_sql_11_2_strict "${const_sql_11_2}"
    testFoldConst("${const_sql_11_2}")
    def const_sql_11_3 = """select "0.9", cast(cast("0.9" as decimalv3(18, 1)) as decimalv3(8, 8));"""
    qt_sql_11_3_strict "${const_sql_11_3}"
    testFoldConst("${const_sql_11_3}")

    sql "set enable_strict_cast=false;"
    qt_sql_11_0_non_strict "${const_sql_11_0}"
    testFoldConst("${const_sql_11_0}")
    qt_sql_11_1_non_strict "${const_sql_11_1}"
    testFoldConst("${const_sql_11_1}")
    qt_sql_11_2_non_strict "${const_sql_11_2}"
    testFoldConst("${const_sql_11_2}")
    qt_sql_11_3_non_strict "${const_sql_11_3}"
    testFoldConst("${const_sql_11_3}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_12_0 = """select "0.000000000", cast(cast("0.000000000" as decimalv3(18, 9)) as decimalv3(8, 8));"""
    qt_sql_12_0_strict "${const_sql_12_0}"
    testFoldConst("${const_sql_12_0}")
    def const_sql_12_1 = """select "0.000000001", cast(cast("0.000000001" as decimalv3(18, 9)) as decimalv3(8, 8));"""
    qt_sql_12_1_strict "${const_sql_12_1}"
    testFoldConst("${const_sql_12_1}")
    def const_sql_12_2 = """select "0.000000009", cast(cast("0.000000009" as decimalv3(18, 9)) as decimalv3(8, 8));"""
    qt_sql_12_2_strict "${const_sql_12_2}"
    testFoldConst("${const_sql_12_2}")
    def const_sql_12_3 = """select "0.099999999", cast(cast("0.099999999" as decimalv3(18, 9)) as decimalv3(8, 8));"""
    qt_sql_12_3_strict "${const_sql_12_3}"
    testFoldConst("${const_sql_12_3}")
    def const_sql_12_4 = """select "0.900000000", cast(cast("0.900000000" as decimalv3(18, 9)) as decimalv3(8, 8));"""
    qt_sql_12_4_strict "${const_sql_12_4}"
    testFoldConst("${const_sql_12_4}")
    def const_sql_12_5 = """select "0.900000001", cast(cast("0.900000001" as decimalv3(18, 9)) as decimalv3(8, 8));"""
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
    def const_sql_13_0 = """select "0.00000000000000000", cast(cast("0.00000000000000000" as decimalv3(18, 17)) as decimalv3(8, 8));"""
    qt_sql_13_0_strict "${const_sql_13_0}"
    testFoldConst("${const_sql_13_0}")
    def const_sql_13_1 = """select "0.00000000000000001", cast(cast("0.00000000000000001" as decimalv3(18, 17)) as decimalv3(8, 8));"""
    qt_sql_13_1_strict "${const_sql_13_1}"
    testFoldConst("${const_sql_13_1}")
    def const_sql_13_2 = """select "0.00000000000000009", cast(cast("0.00000000000000009" as decimalv3(18, 17)) as decimalv3(8, 8));"""
    qt_sql_13_2_strict "${const_sql_13_2}"
    testFoldConst("${const_sql_13_2}")
    def const_sql_13_3 = """select "0.09999999999999999", cast(cast("0.09999999999999999" as decimalv3(18, 17)) as decimalv3(8, 8));"""
    qt_sql_13_3_strict "${const_sql_13_3}"
    testFoldConst("${const_sql_13_3}")
    def const_sql_13_4 = """select "0.90000000000000000", cast(cast("0.90000000000000000" as decimalv3(18, 17)) as decimalv3(8, 8));"""
    qt_sql_13_4_strict "${const_sql_13_4}"
    testFoldConst("${const_sql_13_4}")
    def const_sql_13_5 = """select "0.90000000000000001", cast(cast("0.90000000000000001" as decimalv3(18, 17)) as decimalv3(8, 8));"""
    qt_sql_13_5_strict "${const_sql_13_5}"
    testFoldConst("${const_sql_13_5}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_14_0 = """select "0.000000000000000000", cast(cast("0.000000000000000000" as decimalv3(18, 18)) as decimalv3(8, 8));"""
    qt_sql_14_0_strict "${const_sql_14_0}"
    testFoldConst("${const_sql_14_0}")
    def const_sql_14_1 = """select "0.000000000000000001", cast(cast("0.000000000000000001" as decimalv3(18, 18)) as decimalv3(8, 8));"""
    qt_sql_14_1_strict "${const_sql_14_1}"
    testFoldConst("${const_sql_14_1}")
    def const_sql_14_2 = """select "0.000000000000000009", cast(cast("0.000000000000000009" as decimalv3(18, 18)) as decimalv3(8, 8));"""
    qt_sql_14_2_strict "${const_sql_14_2}"
    testFoldConst("${const_sql_14_2}")
    def const_sql_14_3 = """select "0.099999999999999999", cast(cast("0.099999999999999999" as decimalv3(18, 18)) as decimalv3(8, 8));"""
    qt_sql_14_3_strict "${const_sql_14_3}"
    testFoldConst("${const_sql_14_3}")
    def const_sql_14_4 = """select "0.900000000000000000", cast(cast("0.900000000000000000" as decimalv3(18, 18)) as decimalv3(8, 8));"""
    qt_sql_14_4_strict "${const_sql_14_4}"
    testFoldConst("${const_sql_14_4}")
    def const_sql_14_5 = """select "0.900000000000000001", cast(cast("0.900000000000000001" as decimalv3(18, 18)) as decimalv3(8, 8));"""
    qt_sql_14_5_strict "${const_sql_14_5}"
    testFoldConst("${const_sql_14_5}")

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
}