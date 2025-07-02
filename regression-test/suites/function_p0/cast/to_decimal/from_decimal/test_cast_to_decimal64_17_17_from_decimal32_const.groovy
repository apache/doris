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


suite("test_cast_to_decimal64_17_17_from_decimal32_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "0", cast(cast("0" as decimalv3(1, 0)) as decimalv3(17, 17));"""
    qt_sql_0_0_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")

    sql "set enable_strict_cast=false;"
    qt_sql_0_0_non_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_1_0 = """select "0.0", cast(cast("0.0" as decimalv3(1, 1)) as decimalv3(17, 17));"""
    qt_sql_1_0_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    def const_sql_1_1 = """select "0.1", cast(cast("0.1" as decimalv3(1, 1)) as decimalv3(17, 17));"""
    qt_sql_1_1_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    def const_sql_1_2 = """select "0.8", cast(cast("0.8" as decimalv3(1, 1)) as decimalv3(17, 17));"""
    qt_sql_1_2_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    def const_sql_1_3 = """select "0.9", cast(cast("0.9" as decimalv3(1, 1)) as decimalv3(17, 17));"""
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
    def const_sql_2_0 = """select "0", cast(cast("0" as decimalv3(4, 0)) as decimalv3(17, 17));"""
    qt_sql_2_0_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")

    sql "set enable_strict_cast=false;"
    qt_sql_2_0_non_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_3_0 = """select "0.0", cast(cast("0.0" as decimalv3(4, 1)) as decimalv3(17, 17));"""
    qt_sql_3_0_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    def const_sql_3_1 = """select "0.1", cast(cast("0.1" as decimalv3(4, 1)) as decimalv3(17, 17));"""
    qt_sql_3_1_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    def const_sql_3_2 = """select "0.8", cast(cast("0.8" as decimalv3(4, 1)) as decimalv3(17, 17));"""
    qt_sql_3_2_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    def const_sql_3_3 = """select "0.9", cast(cast("0.9" as decimalv3(4, 1)) as decimalv3(17, 17));"""
    qt_sql_3_3_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")

    sql "set enable_strict_cast=false;"
    qt_sql_3_0_non_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    qt_sql_3_1_non_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    qt_sql_3_2_non_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    qt_sql_3_3_non_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_4_0 = """select "0.00", cast(cast("0.00" as decimalv3(4, 2)) as decimalv3(17, 17));"""
    qt_sql_4_0_strict "${const_sql_4_0}"
    testFoldConst("${const_sql_4_0}")
    def const_sql_4_1 = """select "0.01", cast(cast("0.01" as decimalv3(4, 2)) as decimalv3(17, 17));"""
    qt_sql_4_1_strict "${const_sql_4_1}"
    testFoldConst("${const_sql_4_1}")
    def const_sql_4_2 = """select "0.09", cast(cast("0.09" as decimalv3(4, 2)) as decimalv3(17, 17));"""
    qt_sql_4_2_strict "${const_sql_4_2}"
    testFoldConst("${const_sql_4_2}")
    def const_sql_4_3 = """select "0.90", cast(cast("0.90" as decimalv3(4, 2)) as decimalv3(17, 17));"""
    qt_sql_4_3_strict "${const_sql_4_3}"
    testFoldConst("${const_sql_4_3}")
    def const_sql_4_4 = """select "0.91", cast(cast("0.91" as decimalv3(4, 2)) as decimalv3(17, 17));"""
    qt_sql_4_4_strict "${const_sql_4_4}"
    testFoldConst("${const_sql_4_4}")
    def const_sql_4_5 = """select "0.98", cast(cast("0.98" as decimalv3(4, 2)) as decimalv3(17, 17));"""
    qt_sql_4_5_strict "${const_sql_4_5}"
    testFoldConst("${const_sql_4_5}")
    def const_sql_4_6 = """select "0.99", cast(cast("0.99" as decimalv3(4, 2)) as decimalv3(17, 17));"""
    qt_sql_4_6_strict "${const_sql_4_6}"
    testFoldConst("${const_sql_4_6}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_5_0 = """select "0.000", cast(cast("0.000" as decimalv3(4, 3)) as decimalv3(17, 17));"""
    qt_sql_5_0_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")
    def const_sql_5_1 = """select "0.001", cast(cast("0.001" as decimalv3(4, 3)) as decimalv3(17, 17));"""
    qt_sql_5_1_strict "${const_sql_5_1}"
    testFoldConst("${const_sql_5_1}")
    def const_sql_5_2 = """select "0.009", cast(cast("0.009" as decimalv3(4, 3)) as decimalv3(17, 17));"""
    qt_sql_5_2_strict "${const_sql_5_2}"
    testFoldConst("${const_sql_5_2}")
    def const_sql_5_3 = """select "0.099", cast(cast("0.099" as decimalv3(4, 3)) as decimalv3(17, 17));"""
    qt_sql_5_3_strict "${const_sql_5_3}"
    testFoldConst("${const_sql_5_3}")
    def const_sql_5_4 = """select "0.900", cast(cast("0.900" as decimalv3(4, 3)) as decimalv3(17, 17));"""
    qt_sql_5_4_strict "${const_sql_5_4}"
    testFoldConst("${const_sql_5_4}")
    def const_sql_5_5 = """select "0.901", cast(cast("0.901" as decimalv3(4, 3)) as decimalv3(17, 17));"""
    qt_sql_5_5_strict "${const_sql_5_5}"
    testFoldConst("${const_sql_5_5}")
    def const_sql_5_6 = """select "0.998", cast(cast("0.998" as decimalv3(4, 3)) as decimalv3(17, 17));"""
    qt_sql_5_6_strict "${const_sql_5_6}"
    testFoldConst("${const_sql_5_6}")
    def const_sql_5_7 = """select "0.999", cast(cast("0.999" as decimalv3(4, 3)) as decimalv3(17, 17));"""
    qt_sql_5_7_strict "${const_sql_5_7}"
    testFoldConst("${const_sql_5_7}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_6_0 = """select "0.0000", cast(cast("0.0000" as decimalv3(4, 4)) as decimalv3(17, 17));"""
    qt_sql_6_0_strict "${const_sql_6_0}"
    testFoldConst("${const_sql_6_0}")
    def const_sql_6_1 = """select "0.0001", cast(cast("0.0001" as decimalv3(4, 4)) as decimalv3(17, 17));"""
    qt_sql_6_1_strict "${const_sql_6_1}"
    testFoldConst("${const_sql_6_1}")
    def const_sql_6_2 = """select "0.0009", cast(cast("0.0009" as decimalv3(4, 4)) as decimalv3(17, 17));"""
    qt_sql_6_2_strict "${const_sql_6_2}"
    testFoldConst("${const_sql_6_2}")
    def const_sql_6_3 = """select "0.0999", cast(cast("0.0999" as decimalv3(4, 4)) as decimalv3(17, 17));"""
    qt_sql_6_3_strict "${const_sql_6_3}"
    testFoldConst("${const_sql_6_3}")
    def const_sql_6_4 = """select "0.9000", cast(cast("0.9000" as decimalv3(4, 4)) as decimalv3(17, 17));"""
    qt_sql_6_4_strict "${const_sql_6_4}"
    testFoldConst("${const_sql_6_4}")
    def const_sql_6_5 = """select "0.9001", cast(cast("0.9001" as decimalv3(4, 4)) as decimalv3(17, 17));"""
    qt_sql_6_5_strict "${const_sql_6_5}"
    testFoldConst("${const_sql_6_5}")
    def const_sql_6_6 = """select "0.9998", cast(cast("0.9998" as decimalv3(4, 4)) as decimalv3(17, 17));"""
    qt_sql_6_6_strict "${const_sql_6_6}"
    testFoldConst("${const_sql_6_6}")
    def const_sql_6_7 = """select "0.9999", cast(cast("0.9999" as decimalv3(4, 4)) as decimalv3(17, 17));"""
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
    def const_sql_7_0 = """select "0", cast(cast("0" as decimalv3(8, 0)) as decimalv3(17, 17));"""
    qt_sql_7_0_strict "${const_sql_7_0}"
    testFoldConst("${const_sql_7_0}")

    sql "set enable_strict_cast=false;"
    qt_sql_7_0_non_strict "${const_sql_7_0}"
    testFoldConst("${const_sql_7_0}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_8_0 = """select "0.0", cast(cast("0.0" as decimalv3(8, 1)) as decimalv3(17, 17));"""
    qt_sql_8_0_strict "${const_sql_8_0}"
    testFoldConst("${const_sql_8_0}")
    def const_sql_8_1 = """select "0.1", cast(cast("0.1" as decimalv3(8, 1)) as decimalv3(17, 17));"""
    qt_sql_8_1_strict "${const_sql_8_1}"
    testFoldConst("${const_sql_8_1}")
    def const_sql_8_2 = """select "0.8", cast(cast("0.8" as decimalv3(8, 1)) as decimalv3(17, 17));"""
    qt_sql_8_2_strict "${const_sql_8_2}"
    testFoldConst("${const_sql_8_2}")
    def const_sql_8_3 = """select "0.9", cast(cast("0.9" as decimalv3(8, 1)) as decimalv3(17, 17));"""
    qt_sql_8_3_strict "${const_sql_8_3}"
    testFoldConst("${const_sql_8_3}")

    sql "set enable_strict_cast=false;"
    qt_sql_8_0_non_strict "${const_sql_8_0}"
    testFoldConst("${const_sql_8_0}")
    qt_sql_8_1_non_strict "${const_sql_8_1}"
    testFoldConst("${const_sql_8_1}")
    qt_sql_8_2_non_strict "${const_sql_8_2}"
    testFoldConst("${const_sql_8_2}")
    qt_sql_8_3_non_strict "${const_sql_8_3}"
    testFoldConst("${const_sql_8_3}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_9_0 = """select "0.0000", cast(cast("0.0000" as decimalv3(8, 4)) as decimalv3(17, 17));"""
    qt_sql_9_0_strict "${const_sql_9_0}"
    testFoldConst("${const_sql_9_0}")
    def const_sql_9_1 = """select "0.0001", cast(cast("0.0001" as decimalv3(8, 4)) as decimalv3(17, 17));"""
    qt_sql_9_1_strict "${const_sql_9_1}"
    testFoldConst("${const_sql_9_1}")
    def const_sql_9_2 = """select "0.0009", cast(cast("0.0009" as decimalv3(8, 4)) as decimalv3(17, 17));"""
    qt_sql_9_2_strict "${const_sql_9_2}"
    testFoldConst("${const_sql_9_2}")
    def const_sql_9_3 = """select "0.0999", cast(cast("0.0999" as decimalv3(8, 4)) as decimalv3(17, 17));"""
    qt_sql_9_3_strict "${const_sql_9_3}"
    testFoldConst("${const_sql_9_3}")
    def const_sql_9_4 = """select "0.9000", cast(cast("0.9000" as decimalv3(8, 4)) as decimalv3(17, 17));"""
    qt_sql_9_4_strict "${const_sql_9_4}"
    testFoldConst("${const_sql_9_4}")
    def const_sql_9_5 = """select "0.9001", cast(cast("0.9001" as decimalv3(8, 4)) as decimalv3(17, 17));"""
    qt_sql_9_5_strict "${const_sql_9_5}"
    testFoldConst("${const_sql_9_5}")
    def const_sql_9_6 = """select "0.9998", cast(cast("0.9998" as decimalv3(8, 4)) as decimalv3(17, 17));"""
    qt_sql_9_6_strict "${const_sql_9_6}"
    testFoldConst("${const_sql_9_6}")
    def const_sql_9_7 = """select "0.9999", cast(cast("0.9999" as decimalv3(8, 4)) as decimalv3(17, 17));"""
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
    def const_sql_10_0 = """select "0.0000000", cast(cast("0.0000000" as decimalv3(8, 7)) as decimalv3(17, 17));"""
    qt_sql_10_0_strict "${const_sql_10_0}"
    testFoldConst("${const_sql_10_0}")
    def const_sql_10_1 = """select "0.0000001", cast(cast("0.0000001" as decimalv3(8, 7)) as decimalv3(17, 17));"""
    qt_sql_10_1_strict "${const_sql_10_1}"
    testFoldConst("${const_sql_10_1}")
    def const_sql_10_2 = """select "0.0000009", cast(cast("0.0000009" as decimalv3(8, 7)) as decimalv3(17, 17));"""
    qt_sql_10_2_strict "${const_sql_10_2}"
    testFoldConst("${const_sql_10_2}")
    def const_sql_10_3 = """select "0.0999999", cast(cast("0.0999999" as decimalv3(8, 7)) as decimalv3(17, 17));"""
    qt_sql_10_3_strict "${const_sql_10_3}"
    testFoldConst("${const_sql_10_3}")
    def const_sql_10_4 = """select "0.9000000", cast(cast("0.9000000" as decimalv3(8, 7)) as decimalv3(17, 17));"""
    qt_sql_10_4_strict "${const_sql_10_4}"
    testFoldConst("${const_sql_10_4}")
    def const_sql_10_5 = """select "0.9000001", cast(cast("0.9000001" as decimalv3(8, 7)) as decimalv3(17, 17));"""
    qt_sql_10_5_strict "${const_sql_10_5}"
    testFoldConst("${const_sql_10_5}")
    def const_sql_10_6 = """select "0.9999998", cast(cast("0.9999998" as decimalv3(8, 7)) as decimalv3(17, 17));"""
    qt_sql_10_6_strict "${const_sql_10_6}"
    testFoldConst("${const_sql_10_6}")
    def const_sql_10_7 = """select "0.9999999", cast(cast("0.9999999" as decimalv3(8, 7)) as decimalv3(17, 17));"""
    qt_sql_10_7_strict "${const_sql_10_7}"
    testFoldConst("${const_sql_10_7}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_11_0 = """select "0.00000000", cast(cast("0.00000000" as decimalv3(8, 8)) as decimalv3(17, 17));"""
    qt_sql_11_0_strict "${const_sql_11_0}"
    testFoldConst("${const_sql_11_0}")
    def const_sql_11_1 = """select "0.00000001", cast(cast("0.00000001" as decimalv3(8, 8)) as decimalv3(17, 17));"""
    qt_sql_11_1_strict "${const_sql_11_1}"
    testFoldConst("${const_sql_11_1}")
    def const_sql_11_2 = """select "0.00000009", cast(cast("0.00000009" as decimalv3(8, 8)) as decimalv3(17, 17));"""
    qt_sql_11_2_strict "${const_sql_11_2}"
    testFoldConst("${const_sql_11_2}")
    def const_sql_11_3 = """select "0.09999999", cast(cast("0.09999999" as decimalv3(8, 8)) as decimalv3(17, 17));"""
    qt_sql_11_3_strict "${const_sql_11_3}"
    testFoldConst("${const_sql_11_3}")
    def const_sql_11_4 = """select "0.90000000", cast(cast("0.90000000" as decimalv3(8, 8)) as decimalv3(17, 17));"""
    qt_sql_11_4_strict "${const_sql_11_4}"
    testFoldConst("${const_sql_11_4}")
    def const_sql_11_5 = """select "0.90000001", cast(cast("0.90000001" as decimalv3(8, 8)) as decimalv3(17, 17));"""
    qt_sql_11_5_strict "${const_sql_11_5}"
    testFoldConst("${const_sql_11_5}")
    def const_sql_11_6 = """select "0.99999998", cast(cast("0.99999998" as decimalv3(8, 8)) as decimalv3(17, 17));"""
    qt_sql_11_6_strict "${const_sql_11_6}"
    testFoldConst("${const_sql_11_6}")
    def const_sql_11_7 = """select "0.99999999", cast(cast("0.99999999" as decimalv3(8, 8)) as decimalv3(17, 17));"""
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
    def const_sql_12_0 = """select "0", cast(cast("0" as decimalv3(9, 0)) as decimalv3(17, 17));"""
    qt_sql_12_0_strict "${const_sql_12_0}"
    testFoldConst("${const_sql_12_0}")

    sql "set enable_strict_cast=false;"
    qt_sql_12_0_non_strict "${const_sql_12_0}"
    testFoldConst("${const_sql_12_0}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_13_0 = """select "0.0", cast(cast("0.0" as decimalv3(9, 1)) as decimalv3(17, 17));"""
    qt_sql_13_0_strict "${const_sql_13_0}"
    testFoldConst("${const_sql_13_0}")
    def const_sql_13_1 = """select "0.1", cast(cast("0.1" as decimalv3(9, 1)) as decimalv3(17, 17));"""
    qt_sql_13_1_strict "${const_sql_13_1}"
    testFoldConst("${const_sql_13_1}")
    def const_sql_13_2 = """select "0.8", cast(cast("0.8" as decimalv3(9, 1)) as decimalv3(17, 17));"""
    qt_sql_13_2_strict "${const_sql_13_2}"
    testFoldConst("${const_sql_13_2}")
    def const_sql_13_3 = """select "0.9", cast(cast("0.9" as decimalv3(9, 1)) as decimalv3(17, 17));"""
    qt_sql_13_3_strict "${const_sql_13_3}"
    testFoldConst("${const_sql_13_3}")

    sql "set enable_strict_cast=false;"
    qt_sql_13_0_non_strict "${const_sql_13_0}"
    testFoldConst("${const_sql_13_0}")
    qt_sql_13_1_non_strict "${const_sql_13_1}"
    testFoldConst("${const_sql_13_1}")
    qt_sql_13_2_non_strict "${const_sql_13_2}"
    testFoldConst("${const_sql_13_2}")
    qt_sql_13_3_non_strict "${const_sql_13_3}"
    testFoldConst("${const_sql_13_3}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_14_0 = """select "0.0000", cast(cast("0.0000" as decimalv3(9, 4)) as decimalv3(17, 17));"""
    qt_sql_14_0_strict "${const_sql_14_0}"
    testFoldConst("${const_sql_14_0}")
    def const_sql_14_1 = """select "0.0001", cast(cast("0.0001" as decimalv3(9, 4)) as decimalv3(17, 17));"""
    qt_sql_14_1_strict "${const_sql_14_1}"
    testFoldConst("${const_sql_14_1}")
    def const_sql_14_2 = """select "0.0009", cast(cast("0.0009" as decimalv3(9, 4)) as decimalv3(17, 17));"""
    qt_sql_14_2_strict "${const_sql_14_2}"
    testFoldConst("${const_sql_14_2}")
    def const_sql_14_3 = """select "0.0999", cast(cast("0.0999" as decimalv3(9, 4)) as decimalv3(17, 17));"""
    qt_sql_14_3_strict "${const_sql_14_3}"
    testFoldConst("${const_sql_14_3}")
    def const_sql_14_4 = """select "0.9000", cast(cast("0.9000" as decimalv3(9, 4)) as decimalv3(17, 17));"""
    qt_sql_14_4_strict "${const_sql_14_4}"
    testFoldConst("${const_sql_14_4}")
    def const_sql_14_5 = """select "0.9001", cast(cast("0.9001" as decimalv3(9, 4)) as decimalv3(17, 17));"""
    qt_sql_14_5_strict "${const_sql_14_5}"
    testFoldConst("${const_sql_14_5}")
    def const_sql_14_6 = """select "0.9998", cast(cast("0.9998" as decimalv3(9, 4)) as decimalv3(17, 17));"""
    qt_sql_14_6_strict "${const_sql_14_6}"
    testFoldConst("${const_sql_14_6}")
    def const_sql_14_7 = """select "0.9999", cast(cast("0.9999" as decimalv3(9, 4)) as decimalv3(17, 17));"""
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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_15_0 = """select "0.00000000", cast(cast("0.00000000" as decimalv3(9, 8)) as decimalv3(17, 17));"""
    qt_sql_15_0_strict "${const_sql_15_0}"
    testFoldConst("${const_sql_15_0}")
    def const_sql_15_1 = """select "0.00000001", cast(cast("0.00000001" as decimalv3(9, 8)) as decimalv3(17, 17));"""
    qt_sql_15_1_strict "${const_sql_15_1}"
    testFoldConst("${const_sql_15_1}")
    def const_sql_15_2 = """select "0.00000009", cast(cast("0.00000009" as decimalv3(9, 8)) as decimalv3(17, 17));"""
    qt_sql_15_2_strict "${const_sql_15_2}"
    testFoldConst("${const_sql_15_2}")
    def const_sql_15_3 = """select "0.09999999", cast(cast("0.09999999" as decimalv3(9, 8)) as decimalv3(17, 17));"""
    qt_sql_15_3_strict "${const_sql_15_3}"
    testFoldConst("${const_sql_15_3}")
    def const_sql_15_4 = """select "0.90000000", cast(cast("0.90000000" as decimalv3(9, 8)) as decimalv3(17, 17));"""
    qt_sql_15_4_strict "${const_sql_15_4}"
    testFoldConst("${const_sql_15_4}")
    def const_sql_15_5 = """select "0.90000001", cast(cast("0.90000001" as decimalv3(9, 8)) as decimalv3(17, 17));"""
    qt_sql_15_5_strict "${const_sql_15_5}"
    testFoldConst("${const_sql_15_5}")
    def const_sql_15_6 = """select "0.99999998", cast(cast("0.99999998" as decimalv3(9, 8)) as decimalv3(17, 17));"""
    qt_sql_15_6_strict "${const_sql_15_6}"
    testFoldConst("${const_sql_15_6}")
    def const_sql_15_7 = """select "0.99999999", cast(cast("0.99999999" as decimalv3(9, 8)) as decimalv3(17, 17));"""
    qt_sql_15_7_strict "${const_sql_15_7}"
    testFoldConst("${const_sql_15_7}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_16_0 = """select "0.000000000", cast(cast("0.000000000" as decimalv3(9, 9)) as decimalv3(17, 17));"""
    qt_sql_16_0_strict "${const_sql_16_0}"
    testFoldConst("${const_sql_16_0}")
    def const_sql_16_1 = """select "0.000000001", cast(cast("0.000000001" as decimalv3(9, 9)) as decimalv3(17, 17));"""
    qt_sql_16_1_strict "${const_sql_16_1}"
    testFoldConst("${const_sql_16_1}")
    def const_sql_16_2 = """select "0.000000009", cast(cast("0.000000009" as decimalv3(9, 9)) as decimalv3(17, 17));"""
    qt_sql_16_2_strict "${const_sql_16_2}"
    testFoldConst("${const_sql_16_2}")
    def const_sql_16_3 = """select "0.099999999", cast(cast("0.099999999" as decimalv3(9, 9)) as decimalv3(17, 17));"""
    qt_sql_16_3_strict "${const_sql_16_3}"
    testFoldConst("${const_sql_16_3}")
    def const_sql_16_4 = """select "0.900000000", cast(cast("0.900000000" as decimalv3(9, 9)) as decimalv3(17, 17));"""
    qt_sql_16_4_strict "${const_sql_16_4}"
    testFoldConst("${const_sql_16_4}")
    def const_sql_16_5 = """select "0.900000001", cast(cast("0.900000001" as decimalv3(9, 9)) as decimalv3(17, 17));"""
    qt_sql_16_5_strict "${const_sql_16_5}"
    testFoldConst("${const_sql_16_5}")
    def const_sql_16_6 = """select "0.999999998", cast(cast("0.999999998" as decimalv3(9, 9)) as decimalv3(17, 17));"""
    qt_sql_16_6_strict "${const_sql_16_6}"
    testFoldConst("${const_sql_16_6}")
    def const_sql_16_7 = """select "0.999999999", cast(cast("0.999999999" as decimalv3(9, 9)) as decimalv3(17, 17));"""
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