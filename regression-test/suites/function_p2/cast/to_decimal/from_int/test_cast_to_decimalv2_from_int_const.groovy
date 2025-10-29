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


suite("test_cast_to_decimalv2_from_int_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "-9", cast(cast("-9" as tinyint) as decimalv3(1, 0));"""
    qt_sql_0_0_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    def const_sql_0_1 = """select "-8", cast(cast("-8" as tinyint) as decimalv3(1, 0));"""
    qt_sql_0_1_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    def const_sql_0_2 = """select "-1", cast(cast("-1" as tinyint) as decimalv3(1, 0));"""
    qt_sql_0_2_strict "${const_sql_0_2}"
    testFoldConst("${const_sql_0_2}")
    def const_sql_0_3 = """select "0", cast(cast("0" as tinyint) as decimalv3(1, 0));"""
    qt_sql_0_3_strict "${const_sql_0_3}"
    testFoldConst("${const_sql_0_3}")
    def const_sql_0_4 = """select "1", cast(cast("1" as tinyint) as decimalv3(1, 0));"""
    qt_sql_0_4_strict "${const_sql_0_4}"
    testFoldConst("${const_sql_0_4}")
    def const_sql_0_5 = """select "8", cast(cast("8" as tinyint) as decimalv3(1, 0));"""
    qt_sql_0_5_strict "${const_sql_0_5}"
    testFoldConst("${const_sql_0_5}")
    def const_sql_0_6 = """select "9", cast(cast("9" as tinyint) as decimalv3(1, 0));"""
    qt_sql_0_6_strict "${const_sql_0_6}"
    testFoldConst("${const_sql_0_6}")

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
    qt_sql_0_6_non_strict "${const_sql_0_6}"
    testFoldConst("${const_sql_0_6}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_1_0 = """select "-128", cast(cast("-128" as tinyint) as decimalv3(13, 0));"""
    qt_sql_1_0_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    def const_sql_1_1 = """select "-127", cast(cast("-127" as tinyint) as decimalv3(13, 0));"""
    qt_sql_1_1_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    def const_sql_1_2 = """select "-9", cast(cast("-9" as tinyint) as decimalv3(13, 0));"""
    qt_sql_1_2_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    def const_sql_1_3 = """select "-1", cast(cast("-1" as tinyint) as decimalv3(13, 0));"""
    qt_sql_1_3_strict "${const_sql_1_3}"
    testFoldConst("${const_sql_1_3}")
    def const_sql_1_4 = """select "0", cast(cast("0" as tinyint) as decimalv3(13, 0));"""
    qt_sql_1_4_strict "${const_sql_1_4}"
    testFoldConst("${const_sql_1_4}")
    def const_sql_1_5 = """select "1", cast(cast("1" as tinyint) as decimalv3(13, 0));"""
    qt_sql_1_5_strict "${const_sql_1_5}"
    testFoldConst("${const_sql_1_5}")
    def const_sql_1_6 = """select "9", cast(cast("9" as tinyint) as decimalv3(13, 0));"""
    qt_sql_1_6_strict "${const_sql_1_6}"
    testFoldConst("${const_sql_1_6}")
    def const_sql_1_7 = """select "126", cast(cast("126" as tinyint) as decimalv3(13, 0));"""
    qt_sql_1_7_strict "${const_sql_1_7}"
    testFoldConst("${const_sql_1_7}")
    def const_sql_1_8 = """select "127", cast(cast("127" as tinyint) as decimalv3(13, 0));"""
    qt_sql_1_8_strict "${const_sql_1_8}"
    testFoldConst("${const_sql_1_8}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_2_0 = """select "-128", cast(cast("-128" as tinyint) as decimalv3(13, 6));"""
    qt_sql_2_0_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    def const_sql_2_1 = """select "-127", cast(cast("-127" as tinyint) as decimalv3(13, 6));"""
    qt_sql_2_1_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    def const_sql_2_2 = """select "-9", cast(cast("-9" as tinyint) as decimalv3(13, 6));"""
    qt_sql_2_2_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    def const_sql_2_3 = """select "-1", cast(cast("-1" as tinyint) as decimalv3(13, 6));"""
    qt_sql_2_3_strict "${const_sql_2_3}"
    testFoldConst("${const_sql_2_3}")
    def const_sql_2_4 = """select "0", cast(cast("0" as tinyint) as decimalv3(13, 6));"""
    qt_sql_2_4_strict "${const_sql_2_4}"
    testFoldConst("${const_sql_2_4}")
    def const_sql_2_5 = """select "1", cast(cast("1" as tinyint) as decimalv3(13, 6));"""
    qt_sql_2_5_strict "${const_sql_2_5}"
    testFoldConst("${const_sql_2_5}")
    def const_sql_2_6 = """select "9", cast(cast("9" as tinyint) as decimalv3(13, 6));"""
    qt_sql_2_6_strict "${const_sql_2_6}"
    testFoldConst("${const_sql_2_6}")
    def const_sql_2_7 = """select "126", cast(cast("126" as tinyint) as decimalv3(13, 6));"""
    qt_sql_2_7_strict "${const_sql_2_7}"
    testFoldConst("${const_sql_2_7}")
    def const_sql_2_8 = """select "127", cast(cast("127" as tinyint) as decimalv3(13, 6));"""
    qt_sql_2_8_strict "${const_sql_2_8}"
    testFoldConst("${const_sql_2_8}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_3_0 = """select "-9", cast(cast("-9" as tinyint) as decimalv3(13, 12));"""
    qt_sql_3_0_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    def const_sql_3_1 = """select "-8", cast(cast("-8" as tinyint) as decimalv3(13, 12));"""
    qt_sql_3_1_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    def const_sql_3_2 = """select "-1", cast(cast("-1" as tinyint) as decimalv3(13, 12));"""
    qt_sql_3_2_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    def const_sql_3_3 = """select "0", cast(cast("0" as tinyint) as decimalv3(13, 12));"""
    qt_sql_3_3_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    def const_sql_3_4 = """select "1", cast(cast("1" as tinyint) as decimalv3(13, 12));"""
    qt_sql_3_4_strict "${const_sql_3_4}"
    testFoldConst("${const_sql_3_4}")
    def const_sql_3_5 = """select "8", cast(cast("8" as tinyint) as decimalv3(13, 12));"""
    qt_sql_3_5_strict "${const_sql_3_5}"
    testFoldConst("${const_sql_3_5}")
    def const_sql_3_6 = """select "9", cast(cast("9" as tinyint) as decimalv3(13, 12));"""
    qt_sql_3_6_strict "${const_sql_3_6}"
    testFoldConst("${const_sql_3_6}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_4_0 = """select "-128", cast(cast("-128" as tinyint) as decimalv3(27, 0));"""
    qt_sql_4_0_strict "${const_sql_4_0}"
    testFoldConst("${const_sql_4_0}")
    def const_sql_4_1 = """select "-127", cast(cast("-127" as tinyint) as decimalv3(27, 0));"""
    qt_sql_4_1_strict "${const_sql_4_1}"
    testFoldConst("${const_sql_4_1}")
    def const_sql_4_2 = """select "-9", cast(cast("-9" as tinyint) as decimalv3(27, 0));"""
    qt_sql_4_2_strict "${const_sql_4_2}"
    testFoldConst("${const_sql_4_2}")
    def const_sql_4_3 = """select "-1", cast(cast("-1" as tinyint) as decimalv3(27, 0));"""
    qt_sql_4_3_strict "${const_sql_4_3}"
    testFoldConst("${const_sql_4_3}")
    def const_sql_4_4 = """select "0", cast(cast("0" as tinyint) as decimalv3(27, 0));"""
    qt_sql_4_4_strict "${const_sql_4_4}"
    testFoldConst("${const_sql_4_4}")
    def const_sql_4_5 = """select "1", cast(cast("1" as tinyint) as decimalv3(27, 0));"""
    qt_sql_4_5_strict "${const_sql_4_5}"
    testFoldConst("${const_sql_4_5}")
    def const_sql_4_6 = """select "9", cast(cast("9" as tinyint) as decimalv3(27, 0));"""
    qt_sql_4_6_strict "${const_sql_4_6}"
    testFoldConst("${const_sql_4_6}")
    def const_sql_4_7 = """select "126", cast(cast("126" as tinyint) as decimalv3(27, 0));"""
    qt_sql_4_7_strict "${const_sql_4_7}"
    testFoldConst("${const_sql_4_7}")
    def const_sql_4_8 = """select "127", cast(cast("127" as tinyint) as decimalv3(27, 0));"""
    qt_sql_4_8_strict "${const_sql_4_8}"
    testFoldConst("${const_sql_4_8}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_5_0 = """select "-128", cast(cast("-128" as tinyint) as decimalv3(27, 13));"""
    qt_sql_5_0_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")
    def const_sql_5_1 = """select "-127", cast(cast("-127" as tinyint) as decimalv3(27, 13));"""
    qt_sql_5_1_strict "${const_sql_5_1}"
    testFoldConst("${const_sql_5_1}")
    def const_sql_5_2 = """select "-9", cast(cast("-9" as tinyint) as decimalv3(27, 13));"""
    qt_sql_5_2_strict "${const_sql_5_2}"
    testFoldConst("${const_sql_5_2}")
    def const_sql_5_3 = """select "-1", cast(cast("-1" as tinyint) as decimalv3(27, 13));"""
    qt_sql_5_3_strict "${const_sql_5_3}"
    testFoldConst("${const_sql_5_3}")
    def const_sql_5_4 = """select "0", cast(cast("0" as tinyint) as decimalv3(27, 13));"""
    qt_sql_5_4_strict "${const_sql_5_4}"
    testFoldConst("${const_sql_5_4}")
    def const_sql_5_5 = """select "1", cast(cast("1" as tinyint) as decimalv3(27, 13));"""
    qt_sql_5_5_strict "${const_sql_5_5}"
    testFoldConst("${const_sql_5_5}")
    def const_sql_5_6 = """select "9", cast(cast("9" as tinyint) as decimalv3(27, 13));"""
    qt_sql_5_6_strict "${const_sql_5_6}"
    testFoldConst("${const_sql_5_6}")
    def const_sql_5_7 = """select "126", cast(cast("126" as tinyint) as decimalv3(27, 13));"""
    qt_sql_5_7_strict "${const_sql_5_7}"
    testFoldConst("${const_sql_5_7}")
    def const_sql_5_8 = """select "127", cast(cast("127" as tinyint) as decimalv3(27, 13));"""
    qt_sql_5_8_strict "${const_sql_5_8}"
    testFoldConst("${const_sql_5_8}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_6_0 = """select "-9", cast(cast("-9" as tinyint) as decimalv3(27, 26));"""
    qt_sql_6_0_strict "${const_sql_6_0}"
    testFoldConst("${const_sql_6_0}")
    def const_sql_6_1 = """select "-8", cast(cast("-8" as tinyint) as decimalv3(27, 26));"""
    qt_sql_6_1_strict "${const_sql_6_1}"
    testFoldConst("${const_sql_6_1}")
    def const_sql_6_2 = """select "-1", cast(cast("-1" as tinyint) as decimalv3(27, 26));"""
    qt_sql_6_2_strict "${const_sql_6_2}"
    testFoldConst("${const_sql_6_2}")
    def const_sql_6_3 = """select "0", cast(cast("0" as tinyint) as decimalv3(27, 26));"""
    qt_sql_6_3_strict "${const_sql_6_3}"
    testFoldConst("${const_sql_6_3}")
    def const_sql_6_4 = """select "1", cast(cast("1" as tinyint) as decimalv3(27, 26));"""
    qt_sql_6_4_strict "${const_sql_6_4}"
    testFoldConst("${const_sql_6_4}")
    def const_sql_6_5 = """select "8", cast(cast("8" as tinyint) as decimalv3(27, 26));"""
    qt_sql_6_5_strict "${const_sql_6_5}"
    testFoldConst("${const_sql_6_5}")
    def const_sql_6_6 = """select "9", cast(cast("9" as tinyint) as decimalv3(27, 26));"""
    qt_sql_6_6_strict "${const_sql_6_6}"
    testFoldConst("${const_sql_6_6}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_7_0 = """select "-9", cast(cast("-9" as smallint) as decimalv3(1, 0));"""
    qt_sql_7_0_strict "${const_sql_7_0}"
    testFoldConst("${const_sql_7_0}")
    def const_sql_7_1 = """select "-8", cast(cast("-8" as smallint) as decimalv3(1, 0));"""
    qt_sql_7_1_strict "${const_sql_7_1}"
    testFoldConst("${const_sql_7_1}")
    def const_sql_7_2 = """select "-1", cast(cast("-1" as smallint) as decimalv3(1, 0));"""
    qt_sql_7_2_strict "${const_sql_7_2}"
    testFoldConst("${const_sql_7_2}")
    def const_sql_7_3 = """select "0", cast(cast("0" as smallint) as decimalv3(1, 0));"""
    qt_sql_7_3_strict "${const_sql_7_3}"
    testFoldConst("${const_sql_7_3}")
    def const_sql_7_4 = """select "1", cast(cast("1" as smallint) as decimalv3(1, 0));"""
    qt_sql_7_4_strict "${const_sql_7_4}"
    testFoldConst("${const_sql_7_4}")
    def const_sql_7_5 = """select "8", cast(cast("8" as smallint) as decimalv3(1, 0));"""
    qt_sql_7_5_strict "${const_sql_7_5}"
    testFoldConst("${const_sql_7_5}")
    def const_sql_7_6 = """select "9", cast(cast("9" as smallint) as decimalv3(1, 0));"""
    qt_sql_7_6_strict "${const_sql_7_6}"
    testFoldConst("${const_sql_7_6}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_8_0 = """select "-32768", cast(cast("-32768" as smallint) as decimalv3(13, 0));"""
    qt_sql_8_0_strict "${const_sql_8_0}"
    testFoldConst("${const_sql_8_0}")
    def const_sql_8_1 = """select "-32767", cast(cast("-32767" as smallint) as decimalv3(13, 0));"""
    qt_sql_8_1_strict "${const_sql_8_1}"
    testFoldConst("${const_sql_8_1}")
    def const_sql_8_2 = """select "-9", cast(cast("-9" as smallint) as decimalv3(13, 0));"""
    qt_sql_8_2_strict "${const_sql_8_2}"
    testFoldConst("${const_sql_8_2}")
    def const_sql_8_3 = """select "-1", cast(cast("-1" as smallint) as decimalv3(13, 0));"""
    qt_sql_8_3_strict "${const_sql_8_3}"
    testFoldConst("${const_sql_8_3}")
    def const_sql_8_4 = """select "0", cast(cast("0" as smallint) as decimalv3(13, 0));"""
    qt_sql_8_4_strict "${const_sql_8_4}"
    testFoldConst("${const_sql_8_4}")
    def const_sql_8_5 = """select "1", cast(cast("1" as smallint) as decimalv3(13, 0));"""
    qt_sql_8_5_strict "${const_sql_8_5}"
    testFoldConst("${const_sql_8_5}")
    def const_sql_8_6 = """select "9", cast(cast("9" as smallint) as decimalv3(13, 0));"""
    qt_sql_8_6_strict "${const_sql_8_6}"
    testFoldConst("${const_sql_8_6}")
    def const_sql_8_7 = """select "32766", cast(cast("32766" as smallint) as decimalv3(13, 0));"""
    qt_sql_8_7_strict "${const_sql_8_7}"
    testFoldConst("${const_sql_8_7}")
    def const_sql_8_8 = """select "32767", cast(cast("32767" as smallint) as decimalv3(13, 0));"""
    qt_sql_8_8_strict "${const_sql_8_8}"
    testFoldConst("${const_sql_8_8}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_9_0 = """select "-32768", cast(cast("-32768" as smallint) as decimalv3(13, 6));"""
    qt_sql_9_0_strict "${const_sql_9_0}"
    testFoldConst("${const_sql_9_0}")
    def const_sql_9_1 = """select "-32767", cast(cast("-32767" as smallint) as decimalv3(13, 6));"""
    qt_sql_9_1_strict "${const_sql_9_1}"
    testFoldConst("${const_sql_9_1}")
    def const_sql_9_2 = """select "-9", cast(cast("-9" as smallint) as decimalv3(13, 6));"""
    qt_sql_9_2_strict "${const_sql_9_2}"
    testFoldConst("${const_sql_9_2}")
    def const_sql_9_3 = """select "-1", cast(cast("-1" as smallint) as decimalv3(13, 6));"""
    qt_sql_9_3_strict "${const_sql_9_3}"
    testFoldConst("${const_sql_9_3}")
    def const_sql_9_4 = """select "0", cast(cast("0" as smallint) as decimalv3(13, 6));"""
    qt_sql_9_4_strict "${const_sql_9_4}"
    testFoldConst("${const_sql_9_4}")
    def const_sql_9_5 = """select "1", cast(cast("1" as smallint) as decimalv3(13, 6));"""
    qt_sql_9_5_strict "${const_sql_9_5}"
    testFoldConst("${const_sql_9_5}")
    def const_sql_9_6 = """select "9", cast(cast("9" as smallint) as decimalv3(13, 6));"""
    qt_sql_9_6_strict "${const_sql_9_6}"
    testFoldConst("${const_sql_9_6}")
    def const_sql_9_7 = """select "32766", cast(cast("32766" as smallint) as decimalv3(13, 6));"""
    qt_sql_9_7_strict "${const_sql_9_7}"
    testFoldConst("${const_sql_9_7}")
    def const_sql_9_8 = """select "32767", cast(cast("32767" as smallint) as decimalv3(13, 6));"""
    qt_sql_9_8_strict "${const_sql_9_8}"
    testFoldConst("${const_sql_9_8}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_10_0 = """select "-9", cast(cast("-9" as smallint) as decimalv3(13, 12));"""
    qt_sql_10_0_strict "${const_sql_10_0}"
    testFoldConst("${const_sql_10_0}")
    def const_sql_10_1 = """select "-8", cast(cast("-8" as smallint) as decimalv3(13, 12));"""
    qt_sql_10_1_strict "${const_sql_10_1}"
    testFoldConst("${const_sql_10_1}")
    def const_sql_10_2 = """select "-1", cast(cast("-1" as smallint) as decimalv3(13, 12));"""
    qt_sql_10_2_strict "${const_sql_10_2}"
    testFoldConst("${const_sql_10_2}")
    def const_sql_10_3 = """select "0", cast(cast("0" as smallint) as decimalv3(13, 12));"""
    qt_sql_10_3_strict "${const_sql_10_3}"
    testFoldConst("${const_sql_10_3}")
    def const_sql_10_4 = """select "1", cast(cast("1" as smallint) as decimalv3(13, 12));"""
    qt_sql_10_4_strict "${const_sql_10_4}"
    testFoldConst("${const_sql_10_4}")
    def const_sql_10_5 = """select "8", cast(cast("8" as smallint) as decimalv3(13, 12));"""
    qt_sql_10_5_strict "${const_sql_10_5}"
    testFoldConst("${const_sql_10_5}")
    def const_sql_10_6 = """select "9", cast(cast("9" as smallint) as decimalv3(13, 12));"""
    qt_sql_10_6_strict "${const_sql_10_6}"
    testFoldConst("${const_sql_10_6}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_11_0 = """select "-32768", cast(cast("-32768" as smallint) as decimalv3(27, 0));"""
    qt_sql_11_0_strict "${const_sql_11_0}"
    testFoldConst("${const_sql_11_0}")
    def const_sql_11_1 = """select "-32767", cast(cast("-32767" as smallint) as decimalv3(27, 0));"""
    qt_sql_11_1_strict "${const_sql_11_1}"
    testFoldConst("${const_sql_11_1}")
    def const_sql_11_2 = """select "-9", cast(cast("-9" as smallint) as decimalv3(27, 0));"""
    qt_sql_11_2_strict "${const_sql_11_2}"
    testFoldConst("${const_sql_11_2}")
    def const_sql_11_3 = """select "-1", cast(cast("-1" as smallint) as decimalv3(27, 0));"""
    qt_sql_11_3_strict "${const_sql_11_3}"
    testFoldConst("${const_sql_11_3}")
    def const_sql_11_4 = """select "0", cast(cast("0" as smallint) as decimalv3(27, 0));"""
    qt_sql_11_4_strict "${const_sql_11_4}"
    testFoldConst("${const_sql_11_4}")
    def const_sql_11_5 = """select "1", cast(cast("1" as smallint) as decimalv3(27, 0));"""
    qt_sql_11_5_strict "${const_sql_11_5}"
    testFoldConst("${const_sql_11_5}")
    def const_sql_11_6 = """select "9", cast(cast("9" as smallint) as decimalv3(27, 0));"""
    qt_sql_11_6_strict "${const_sql_11_6}"
    testFoldConst("${const_sql_11_6}")
    def const_sql_11_7 = """select "32766", cast(cast("32766" as smallint) as decimalv3(27, 0));"""
    qt_sql_11_7_strict "${const_sql_11_7}"
    testFoldConst("${const_sql_11_7}")
    def const_sql_11_8 = """select "32767", cast(cast("32767" as smallint) as decimalv3(27, 0));"""
    qt_sql_11_8_strict "${const_sql_11_8}"
    testFoldConst("${const_sql_11_8}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_12_0 = """select "-32768", cast(cast("-32768" as smallint) as decimalv3(27, 13));"""
    qt_sql_12_0_strict "${const_sql_12_0}"
    testFoldConst("${const_sql_12_0}")
    def const_sql_12_1 = """select "-32767", cast(cast("-32767" as smallint) as decimalv3(27, 13));"""
    qt_sql_12_1_strict "${const_sql_12_1}"
    testFoldConst("${const_sql_12_1}")
    def const_sql_12_2 = """select "-9", cast(cast("-9" as smallint) as decimalv3(27, 13));"""
    qt_sql_12_2_strict "${const_sql_12_2}"
    testFoldConst("${const_sql_12_2}")
    def const_sql_12_3 = """select "-1", cast(cast("-1" as smallint) as decimalv3(27, 13));"""
    qt_sql_12_3_strict "${const_sql_12_3}"
    testFoldConst("${const_sql_12_3}")
    def const_sql_12_4 = """select "0", cast(cast("0" as smallint) as decimalv3(27, 13));"""
    qt_sql_12_4_strict "${const_sql_12_4}"
    testFoldConst("${const_sql_12_4}")
    def const_sql_12_5 = """select "1", cast(cast("1" as smallint) as decimalv3(27, 13));"""
    qt_sql_12_5_strict "${const_sql_12_5}"
    testFoldConst("${const_sql_12_5}")
    def const_sql_12_6 = """select "9", cast(cast("9" as smallint) as decimalv3(27, 13));"""
    qt_sql_12_6_strict "${const_sql_12_6}"
    testFoldConst("${const_sql_12_6}")
    def const_sql_12_7 = """select "32766", cast(cast("32766" as smallint) as decimalv3(27, 13));"""
    qt_sql_12_7_strict "${const_sql_12_7}"
    testFoldConst("${const_sql_12_7}")
    def const_sql_12_8 = """select "32767", cast(cast("32767" as smallint) as decimalv3(27, 13));"""
    qt_sql_12_8_strict "${const_sql_12_8}"
    testFoldConst("${const_sql_12_8}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_13_0 = """select "-9", cast(cast("-9" as smallint) as decimalv3(27, 26));"""
    qt_sql_13_0_strict "${const_sql_13_0}"
    testFoldConst("${const_sql_13_0}")
    def const_sql_13_1 = """select "-8", cast(cast("-8" as smallint) as decimalv3(27, 26));"""
    qt_sql_13_1_strict "${const_sql_13_1}"
    testFoldConst("${const_sql_13_1}")
    def const_sql_13_2 = """select "-1", cast(cast("-1" as smallint) as decimalv3(27, 26));"""
    qt_sql_13_2_strict "${const_sql_13_2}"
    testFoldConst("${const_sql_13_2}")
    def const_sql_13_3 = """select "0", cast(cast("0" as smallint) as decimalv3(27, 26));"""
    qt_sql_13_3_strict "${const_sql_13_3}"
    testFoldConst("${const_sql_13_3}")
    def const_sql_13_4 = """select "1", cast(cast("1" as smallint) as decimalv3(27, 26));"""
    qt_sql_13_4_strict "${const_sql_13_4}"
    testFoldConst("${const_sql_13_4}")
    def const_sql_13_5 = """select "8", cast(cast("8" as smallint) as decimalv3(27, 26));"""
    qt_sql_13_5_strict "${const_sql_13_5}"
    testFoldConst("${const_sql_13_5}")
    def const_sql_13_6 = """select "9", cast(cast("9" as smallint) as decimalv3(27, 26));"""
    qt_sql_13_6_strict "${const_sql_13_6}"
    testFoldConst("${const_sql_13_6}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_14_0 = """select "-9", cast(cast("-9" as int) as decimalv3(1, 0));"""
    qt_sql_14_0_strict "${const_sql_14_0}"
    testFoldConst("${const_sql_14_0}")
    def const_sql_14_1 = """select "-8", cast(cast("-8" as int) as decimalv3(1, 0));"""
    qt_sql_14_1_strict "${const_sql_14_1}"
    testFoldConst("${const_sql_14_1}")
    def const_sql_14_2 = """select "-1", cast(cast("-1" as int) as decimalv3(1, 0));"""
    qt_sql_14_2_strict "${const_sql_14_2}"
    testFoldConst("${const_sql_14_2}")
    def const_sql_14_3 = """select "0", cast(cast("0" as int) as decimalv3(1, 0));"""
    qt_sql_14_3_strict "${const_sql_14_3}"
    testFoldConst("${const_sql_14_3}")
    def const_sql_14_4 = """select "1", cast(cast("1" as int) as decimalv3(1, 0));"""
    qt_sql_14_4_strict "${const_sql_14_4}"
    testFoldConst("${const_sql_14_4}")
    def const_sql_14_5 = """select "8", cast(cast("8" as int) as decimalv3(1, 0));"""
    qt_sql_14_5_strict "${const_sql_14_5}"
    testFoldConst("${const_sql_14_5}")
    def const_sql_14_6 = """select "9", cast(cast("9" as int) as decimalv3(1, 0));"""
    qt_sql_14_6_strict "${const_sql_14_6}"
    testFoldConst("${const_sql_14_6}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_15_0 = """select "-2147483648", cast(cast("-2147483648" as int) as decimalv3(13, 0));"""
    qt_sql_15_0_strict "${const_sql_15_0}"
    testFoldConst("${const_sql_15_0}")
    def const_sql_15_1 = """select "-2147483647", cast(cast("-2147483647" as int) as decimalv3(13, 0));"""
    qt_sql_15_1_strict "${const_sql_15_1}"
    testFoldConst("${const_sql_15_1}")
    def const_sql_15_2 = """select "-9", cast(cast("-9" as int) as decimalv3(13, 0));"""
    qt_sql_15_2_strict "${const_sql_15_2}"
    testFoldConst("${const_sql_15_2}")
    def const_sql_15_3 = """select "-1", cast(cast("-1" as int) as decimalv3(13, 0));"""
    qt_sql_15_3_strict "${const_sql_15_3}"
    testFoldConst("${const_sql_15_3}")
    def const_sql_15_4 = """select "0", cast(cast("0" as int) as decimalv3(13, 0));"""
    qt_sql_15_4_strict "${const_sql_15_4}"
    testFoldConst("${const_sql_15_4}")
    def const_sql_15_5 = """select "1", cast(cast("1" as int) as decimalv3(13, 0));"""
    qt_sql_15_5_strict "${const_sql_15_5}"
    testFoldConst("${const_sql_15_5}")
    def const_sql_15_6 = """select "9", cast(cast("9" as int) as decimalv3(13, 0));"""
    qt_sql_15_6_strict "${const_sql_15_6}"
    testFoldConst("${const_sql_15_6}")
    def const_sql_15_7 = """select "2147483646", cast(cast("2147483646" as int) as decimalv3(13, 0));"""
    qt_sql_15_7_strict "${const_sql_15_7}"
    testFoldConst("${const_sql_15_7}")
    def const_sql_15_8 = """select "2147483647", cast(cast("2147483647" as int) as decimalv3(13, 0));"""
    qt_sql_15_8_strict "${const_sql_15_8}"
    testFoldConst("${const_sql_15_8}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_16_0 = """select "-9999999", cast(cast("-9999999" as int) as decimalv3(13, 6));"""
    qt_sql_16_0_strict "${const_sql_16_0}"
    testFoldConst("${const_sql_16_0}")
    def const_sql_16_1 = """select "-9000001", cast(cast("-9000001" as int) as decimalv3(13, 6));"""
    qt_sql_16_1_strict "${const_sql_16_1}"
    testFoldConst("${const_sql_16_1}")
    def const_sql_16_2 = """select "-9000000", cast(cast("-9000000" as int) as decimalv3(13, 6));"""
    qt_sql_16_2_strict "${const_sql_16_2}"
    testFoldConst("${const_sql_16_2}")
    def const_sql_16_3 = """select "-999999", cast(cast("-999999" as int) as decimalv3(13, 6));"""
    qt_sql_16_3_strict "${const_sql_16_3}"
    testFoldConst("${const_sql_16_3}")
    def const_sql_16_4 = """select "-9", cast(cast("-9" as int) as decimalv3(13, 6));"""
    qt_sql_16_4_strict "${const_sql_16_4}"
    testFoldConst("${const_sql_16_4}")
    def const_sql_16_5 = """select "-1", cast(cast("-1" as int) as decimalv3(13, 6));"""
    qt_sql_16_5_strict "${const_sql_16_5}"
    testFoldConst("${const_sql_16_5}")
    def const_sql_16_6 = """select "0", cast(cast("0" as int) as decimalv3(13, 6));"""
    qt_sql_16_6_strict "${const_sql_16_6}"
    testFoldConst("${const_sql_16_6}")
    def const_sql_16_7 = """select "1", cast(cast("1" as int) as decimalv3(13, 6));"""
    qt_sql_16_7_strict "${const_sql_16_7}"
    testFoldConst("${const_sql_16_7}")
    def const_sql_16_8 = """select "9", cast(cast("9" as int) as decimalv3(13, 6));"""
    qt_sql_16_8_strict "${const_sql_16_8}"
    testFoldConst("${const_sql_16_8}")
    def const_sql_16_9 = """select "999999", cast(cast("999999" as int) as decimalv3(13, 6));"""
    qt_sql_16_9_strict "${const_sql_16_9}"
    testFoldConst("${const_sql_16_9}")
    def const_sql_16_10 = """select "9000000", cast(cast("9000000" as int) as decimalv3(13, 6));"""
    qt_sql_16_10_strict "${const_sql_16_10}"
    testFoldConst("${const_sql_16_10}")
    def const_sql_16_11 = """select "9000001", cast(cast("9000001" as int) as decimalv3(13, 6));"""
    qt_sql_16_11_strict "${const_sql_16_11}"
    testFoldConst("${const_sql_16_11}")
    def const_sql_16_12 = """select "9999999", cast(cast("9999999" as int) as decimalv3(13, 6));"""
    qt_sql_16_12_strict "${const_sql_16_12}"
    testFoldConst("${const_sql_16_12}")

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
    qt_sql_16_8_non_strict "${const_sql_16_8}"
    testFoldConst("${const_sql_16_8}")
    qt_sql_16_9_non_strict "${const_sql_16_9}"
    testFoldConst("${const_sql_16_9}")
    qt_sql_16_10_non_strict "${const_sql_16_10}"
    testFoldConst("${const_sql_16_10}")
    qt_sql_16_11_non_strict "${const_sql_16_11}"
    testFoldConst("${const_sql_16_11}")
    qt_sql_16_12_non_strict "${const_sql_16_12}"
    testFoldConst("${const_sql_16_12}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_17_0 = """select "-9", cast(cast("-9" as int) as decimalv3(13, 12));"""
    qt_sql_17_0_strict "${const_sql_17_0}"
    testFoldConst("${const_sql_17_0}")
    def const_sql_17_1 = """select "-8", cast(cast("-8" as int) as decimalv3(13, 12));"""
    qt_sql_17_1_strict "${const_sql_17_1}"
    testFoldConst("${const_sql_17_1}")
    def const_sql_17_2 = """select "-1", cast(cast("-1" as int) as decimalv3(13, 12));"""
    qt_sql_17_2_strict "${const_sql_17_2}"
    testFoldConst("${const_sql_17_2}")
    def const_sql_17_3 = """select "0", cast(cast("0" as int) as decimalv3(13, 12));"""
    qt_sql_17_3_strict "${const_sql_17_3}"
    testFoldConst("${const_sql_17_3}")
    def const_sql_17_4 = """select "1", cast(cast("1" as int) as decimalv3(13, 12));"""
    qt_sql_17_4_strict "${const_sql_17_4}"
    testFoldConst("${const_sql_17_4}")
    def const_sql_17_5 = """select "8", cast(cast("8" as int) as decimalv3(13, 12));"""
    qt_sql_17_5_strict "${const_sql_17_5}"
    testFoldConst("${const_sql_17_5}")
    def const_sql_17_6 = """select "9", cast(cast("9" as int) as decimalv3(13, 12));"""
    qt_sql_17_6_strict "${const_sql_17_6}"
    testFoldConst("${const_sql_17_6}")

    sql "set enable_strict_cast=false;"
    qt_sql_17_0_non_strict "${const_sql_17_0}"
    testFoldConst("${const_sql_17_0}")
    qt_sql_17_1_non_strict "${const_sql_17_1}"
    testFoldConst("${const_sql_17_1}")
    qt_sql_17_2_non_strict "${const_sql_17_2}"
    testFoldConst("${const_sql_17_2}")
    qt_sql_17_3_non_strict "${const_sql_17_3}"
    testFoldConst("${const_sql_17_3}")
    qt_sql_17_4_non_strict "${const_sql_17_4}"
    testFoldConst("${const_sql_17_4}")
    qt_sql_17_5_non_strict "${const_sql_17_5}"
    testFoldConst("${const_sql_17_5}")
    qt_sql_17_6_non_strict "${const_sql_17_6}"
    testFoldConst("${const_sql_17_6}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_18_0 = """select "-2147483648", cast(cast("-2147483648" as int) as decimalv3(27, 0));"""
    qt_sql_18_0_strict "${const_sql_18_0}"
    testFoldConst("${const_sql_18_0}")
    def const_sql_18_1 = """select "-2147483647", cast(cast("-2147483647" as int) as decimalv3(27, 0));"""
    qt_sql_18_1_strict "${const_sql_18_1}"
    testFoldConst("${const_sql_18_1}")
    def const_sql_18_2 = """select "-9", cast(cast("-9" as int) as decimalv3(27, 0));"""
    qt_sql_18_2_strict "${const_sql_18_2}"
    testFoldConst("${const_sql_18_2}")
    def const_sql_18_3 = """select "-1", cast(cast("-1" as int) as decimalv3(27, 0));"""
    qt_sql_18_3_strict "${const_sql_18_3}"
    testFoldConst("${const_sql_18_3}")
    def const_sql_18_4 = """select "0", cast(cast("0" as int) as decimalv3(27, 0));"""
    qt_sql_18_4_strict "${const_sql_18_4}"
    testFoldConst("${const_sql_18_4}")
    def const_sql_18_5 = """select "1", cast(cast("1" as int) as decimalv3(27, 0));"""
    qt_sql_18_5_strict "${const_sql_18_5}"
    testFoldConst("${const_sql_18_5}")
    def const_sql_18_6 = """select "9", cast(cast("9" as int) as decimalv3(27, 0));"""
    qt_sql_18_6_strict "${const_sql_18_6}"
    testFoldConst("${const_sql_18_6}")
    def const_sql_18_7 = """select "2147483646", cast(cast("2147483646" as int) as decimalv3(27, 0));"""
    qt_sql_18_7_strict "${const_sql_18_7}"
    testFoldConst("${const_sql_18_7}")
    def const_sql_18_8 = """select "2147483647", cast(cast("2147483647" as int) as decimalv3(27, 0));"""
    qt_sql_18_8_strict "${const_sql_18_8}"
    testFoldConst("${const_sql_18_8}")

    sql "set enable_strict_cast=false;"
    qt_sql_18_0_non_strict "${const_sql_18_0}"
    testFoldConst("${const_sql_18_0}")
    qt_sql_18_1_non_strict "${const_sql_18_1}"
    testFoldConst("${const_sql_18_1}")
    qt_sql_18_2_non_strict "${const_sql_18_2}"
    testFoldConst("${const_sql_18_2}")
    qt_sql_18_3_non_strict "${const_sql_18_3}"
    testFoldConst("${const_sql_18_3}")
    qt_sql_18_4_non_strict "${const_sql_18_4}"
    testFoldConst("${const_sql_18_4}")
    qt_sql_18_5_non_strict "${const_sql_18_5}"
    testFoldConst("${const_sql_18_5}")
    qt_sql_18_6_non_strict "${const_sql_18_6}"
    testFoldConst("${const_sql_18_6}")
    qt_sql_18_7_non_strict "${const_sql_18_7}"
    testFoldConst("${const_sql_18_7}")
    qt_sql_18_8_non_strict "${const_sql_18_8}"
    testFoldConst("${const_sql_18_8}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_19_0 = """select "-2147483648", cast(cast("-2147483648" as int) as decimalv3(27, 13));"""
    qt_sql_19_0_strict "${const_sql_19_0}"
    testFoldConst("${const_sql_19_0}")
    def const_sql_19_1 = """select "-2147483647", cast(cast("-2147483647" as int) as decimalv3(27, 13));"""
    qt_sql_19_1_strict "${const_sql_19_1}"
    testFoldConst("${const_sql_19_1}")
    def const_sql_19_2 = """select "-9", cast(cast("-9" as int) as decimalv3(27, 13));"""
    qt_sql_19_2_strict "${const_sql_19_2}"
    testFoldConst("${const_sql_19_2}")
    def const_sql_19_3 = """select "-1", cast(cast("-1" as int) as decimalv3(27, 13));"""
    qt_sql_19_3_strict "${const_sql_19_3}"
    testFoldConst("${const_sql_19_3}")
    def const_sql_19_4 = """select "0", cast(cast("0" as int) as decimalv3(27, 13));"""
    qt_sql_19_4_strict "${const_sql_19_4}"
    testFoldConst("${const_sql_19_4}")
    def const_sql_19_5 = """select "1", cast(cast("1" as int) as decimalv3(27, 13));"""
    qt_sql_19_5_strict "${const_sql_19_5}"
    testFoldConst("${const_sql_19_5}")
    def const_sql_19_6 = """select "9", cast(cast("9" as int) as decimalv3(27, 13));"""
    qt_sql_19_6_strict "${const_sql_19_6}"
    testFoldConst("${const_sql_19_6}")
    def const_sql_19_7 = """select "2147483646", cast(cast("2147483646" as int) as decimalv3(27, 13));"""
    qt_sql_19_7_strict "${const_sql_19_7}"
    testFoldConst("${const_sql_19_7}")
    def const_sql_19_8 = """select "2147483647", cast(cast("2147483647" as int) as decimalv3(27, 13));"""
    qt_sql_19_8_strict "${const_sql_19_8}"
    testFoldConst("${const_sql_19_8}")

    sql "set enable_strict_cast=false;"
    qt_sql_19_0_non_strict "${const_sql_19_0}"
    testFoldConst("${const_sql_19_0}")
    qt_sql_19_1_non_strict "${const_sql_19_1}"
    testFoldConst("${const_sql_19_1}")
    qt_sql_19_2_non_strict "${const_sql_19_2}"
    testFoldConst("${const_sql_19_2}")
    qt_sql_19_3_non_strict "${const_sql_19_3}"
    testFoldConst("${const_sql_19_3}")
    qt_sql_19_4_non_strict "${const_sql_19_4}"
    testFoldConst("${const_sql_19_4}")
    qt_sql_19_5_non_strict "${const_sql_19_5}"
    testFoldConst("${const_sql_19_5}")
    qt_sql_19_6_non_strict "${const_sql_19_6}"
    testFoldConst("${const_sql_19_6}")
    qt_sql_19_7_non_strict "${const_sql_19_7}"
    testFoldConst("${const_sql_19_7}")
    qt_sql_19_8_non_strict "${const_sql_19_8}"
    testFoldConst("${const_sql_19_8}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_20_0 = """select "-9", cast(cast("-9" as int) as decimalv3(27, 26));"""
    qt_sql_20_0_strict "${const_sql_20_0}"
    testFoldConst("${const_sql_20_0}")
    def const_sql_20_1 = """select "-8", cast(cast("-8" as int) as decimalv3(27, 26));"""
    qt_sql_20_1_strict "${const_sql_20_1}"
    testFoldConst("${const_sql_20_1}")
    def const_sql_20_2 = """select "-1", cast(cast("-1" as int) as decimalv3(27, 26));"""
    qt_sql_20_2_strict "${const_sql_20_2}"
    testFoldConst("${const_sql_20_2}")
    def const_sql_20_3 = """select "0", cast(cast("0" as int) as decimalv3(27, 26));"""
    qt_sql_20_3_strict "${const_sql_20_3}"
    testFoldConst("${const_sql_20_3}")
    def const_sql_20_4 = """select "1", cast(cast("1" as int) as decimalv3(27, 26));"""
    qt_sql_20_4_strict "${const_sql_20_4}"
    testFoldConst("${const_sql_20_4}")
    def const_sql_20_5 = """select "8", cast(cast("8" as int) as decimalv3(27, 26));"""
    qt_sql_20_5_strict "${const_sql_20_5}"
    testFoldConst("${const_sql_20_5}")
    def const_sql_20_6 = """select "9", cast(cast("9" as int) as decimalv3(27, 26));"""
    qt_sql_20_6_strict "${const_sql_20_6}"
    testFoldConst("${const_sql_20_6}")

    sql "set enable_strict_cast=false;"
    qt_sql_20_0_non_strict "${const_sql_20_0}"
    testFoldConst("${const_sql_20_0}")
    qt_sql_20_1_non_strict "${const_sql_20_1}"
    testFoldConst("${const_sql_20_1}")
    qt_sql_20_2_non_strict "${const_sql_20_2}"
    testFoldConst("${const_sql_20_2}")
    qt_sql_20_3_non_strict "${const_sql_20_3}"
    testFoldConst("${const_sql_20_3}")
    qt_sql_20_4_non_strict "${const_sql_20_4}"
    testFoldConst("${const_sql_20_4}")
    qt_sql_20_5_non_strict "${const_sql_20_5}"
    testFoldConst("${const_sql_20_5}")
    qt_sql_20_6_non_strict "${const_sql_20_6}"
    testFoldConst("${const_sql_20_6}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_21_0 = """select "-9", cast(cast("-9" as bigint) as decimalv3(1, 0));"""
    qt_sql_21_0_strict "${const_sql_21_0}"
    testFoldConst("${const_sql_21_0}")
    def const_sql_21_1 = """select "-8", cast(cast("-8" as bigint) as decimalv3(1, 0));"""
    qt_sql_21_1_strict "${const_sql_21_1}"
    testFoldConst("${const_sql_21_1}")
    def const_sql_21_2 = """select "-1", cast(cast("-1" as bigint) as decimalv3(1, 0));"""
    qt_sql_21_2_strict "${const_sql_21_2}"
    testFoldConst("${const_sql_21_2}")
    def const_sql_21_3 = """select "0", cast(cast("0" as bigint) as decimalv3(1, 0));"""
    qt_sql_21_3_strict "${const_sql_21_3}"
    testFoldConst("${const_sql_21_3}")
    def const_sql_21_4 = """select "1", cast(cast("1" as bigint) as decimalv3(1, 0));"""
    qt_sql_21_4_strict "${const_sql_21_4}"
    testFoldConst("${const_sql_21_4}")
    def const_sql_21_5 = """select "8", cast(cast("8" as bigint) as decimalv3(1, 0));"""
    qt_sql_21_5_strict "${const_sql_21_5}"
    testFoldConst("${const_sql_21_5}")
    def const_sql_21_6 = """select "9", cast(cast("9" as bigint) as decimalv3(1, 0));"""
    qt_sql_21_6_strict "${const_sql_21_6}"
    testFoldConst("${const_sql_21_6}")

    sql "set enable_strict_cast=false;"
    qt_sql_21_0_non_strict "${const_sql_21_0}"
    testFoldConst("${const_sql_21_0}")
    qt_sql_21_1_non_strict "${const_sql_21_1}"
    testFoldConst("${const_sql_21_1}")
    qt_sql_21_2_non_strict "${const_sql_21_2}"
    testFoldConst("${const_sql_21_2}")
    qt_sql_21_3_non_strict "${const_sql_21_3}"
    testFoldConst("${const_sql_21_3}")
    qt_sql_21_4_non_strict "${const_sql_21_4}"
    testFoldConst("${const_sql_21_4}")
    qt_sql_21_5_non_strict "${const_sql_21_5}"
    testFoldConst("${const_sql_21_5}")
    qt_sql_21_6_non_strict "${const_sql_21_6}"
    testFoldConst("${const_sql_21_6}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_22_0 = """select "-9999999999999", cast(cast("-9999999999999" as bigint) as decimalv3(13, 0));"""
    qt_sql_22_0_strict "${const_sql_22_0}"
    testFoldConst("${const_sql_22_0}")
    def const_sql_22_1 = """select "-9000000000001", cast(cast("-9000000000001" as bigint) as decimalv3(13, 0));"""
    qt_sql_22_1_strict "${const_sql_22_1}"
    testFoldConst("${const_sql_22_1}")
    def const_sql_22_2 = """select "-9000000000000", cast(cast("-9000000000000" as bigint) as decimalv3(13, 0));"""
    qt_sql_22_2_strict "${const_sql_22_2}"
    testFoldConst("${const_sql_22_2}")
    def const_sql_22_3 = """select "-999999999999", cast(cast("-999999999999" as bigint) as decimalv3(13, 0));"""
    qt_sql_22_3_strict "${const_sql_22_3}"
    testFoldConst("${const_sql_22_3}")
    def const_sql_22_4 = """select "-9", cast(cast("-9" as bigint) as decimalv3(13, 0));"""
    qt_sql_22_4_strict "${const_sql_22_4}"
    testFoldConst("${const_sql_22_4}")
    def const_sql_22_5 = """select "-1", cast(cast("-1" as bigint) as decimalv3(13, 0));"""
    qt_sql_22_5_strict "${const_sql_22_5}"
    testFoldConst("${const_sql_22_5}")
    def const_sql_22_6 = """select "0", cast(cast("0" as bigint) as decimalv3(13, 0));"""
    qt_sql_22_6_strict "${const_sql_22_6}"
    testFoldConst("${const_sql_22_6}")
    def const_sql_22_7 = """select "1", cast(cast("1" as bigint) as decimalv3(13, 0));"""
    qt_sql_22_7_strict "${const_sql_22_7}"
    testFoldConst("${const_sql_22_7}")
    def const_sql_22_8 = """select "9", cast(cast("9" as bigint) as decimalv3(13, 0));"""
    qt_sql_22_8_strict "${const_sql_22_8}"
    testFoldConst("${const_sql_22_8}")
    def const_sql_22_9 = """select "999999999999", cast(cast("999999999999" as bigint) as decimalv3(13, 0));"""
    qt_sql_22_9_strict "${const_sql_22_9}"
    testFoldConst("${const_sql_22_9}")
    def const_sql_22_10 = """select "9000000000000", cast(cast("9000000000000" as bigint) as decimalv3(13, 0));"""
    qt_sql_22_10_strict "${const_sql_22_10}"
    testFoldConst("${const_sql_22_10}")
    def const_sql_22_11 = """select "9000000000001", cast(cast("9000000000001" as bigint) as decimalv3(13, 0));"""
    qt_sql_22_11_strict "${const_sql_22_11}"
    testFoldConst("${const_sql_22_11}")
    def const_sql_22_12 = """select "9999999999999", cast(cast("9999999999999" as bigint) as decimalv3(13, 0));"""
    qt_sql_22_12_strict "${const_sql_22_12}"
    testFoldConst("${const_sql_22_12}")

    sql "set enable_strict_cast=false;"
    qt_sql_22_0_non_strict "${const_sql_22_0}"
    testFoldConst("${const_sql_22_0}")
    qt_sql_22_1_non_strict "${const_sql_22_1}"
    testFoldConst("${const_sql_22_1}")
    qt_sql_22_2_non_strict "${const_sql_22_2}"
    testFoldConst("${const_sql_22_2}")
    qt_sql_22_3_non_strict "${const_sql_22_3}"
    testFoldConst("${const_sql_22_3}")
    qt_sql_22_4_non_strict "${const_sql_22_4}"
    testFoldConst("${const_sql_22_4}")
    qt_sql_22_5_non_strict "${const_sql_22_5}"
    testFoldConst("${const_sql_22_5}")
    qt_sql_22_6_non_strict "${const_sql_22_6}"
    testFoldConst("${const_sql_22_6}")
    qt_sql_22_7_non_strict "${const_sql_22_7}"
    testFoldConst("${const_sql_22_7}")
    qt_sql_22_8_non_strict "${const_sql_22_8}"
    testFoldConst("${const_sql_22_8}")
    qt_sql_22_9_non_strict "${const_sql_22_9}"
    testFoldConst("${const_sql_22_9}")
    qt_sql_22_10_non_strict "${const_sql_22_10}"
    testFoldConst("${const_sql_22_10}")
    qt_sql_22_11_non_strict "${const_sql_22_11}"
    testFoldConst("${const_sql_22_11}")
    qt_sql_22_12_non_strict "${const_sql_22_12}"
    testFoldConst("${const_sql_22_12}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_23_0 = """select "-9999999", cast(cast("-9999999" as bigint) as decimalv3(13, 6));"""
    qt_sql_23_0_strict "${const_sql_23_0}"
    testFoldConst("${const_sql_23_0}")
    def const_sql_23_1 = """select "-9000001", cast(cast("-9000001" as bigint) as decimalv3(13, 6));"""
    qt_sql_23_1_strict "${const_sql_23_1}"
    testFoldConst("${const_sql_23_1}")
    def const_sql_23_2 = """select "-9000000", cast(cast("-9000000" as bigint) as decimalv3(13, 6));"""
    qt_sql_23_2_strict "${const_sql_23_2}"
    testFoldConst("${const_sql_23_2}")
    def const_sql_23_3 = """select "-999999", cast(cast("-999999" as bigint) as decimalv3(13, 6));"""
    qt_sql_23_3_strict "${const_sql_23_3}"
    testFoldConst("${const_sql_23_3}")
    def const_sql_23_4 = """select "-9", cast(cast("-9" as bigint) as decimalv3(13, 6));"""
    qt_sql_23_4_strict "${const_sql_23_4}"
    testFoldConst("${const_sql_23_4}")
    def const_sql_23_5 = """select "-1", cast(cast("-1" as bigint) as decimalv3(13, 6));"""
    qt_sql_23_5_strict "${const_sql_23_5}"
    testFoldConst("${const_sql_23_5}")
    def const_sql_23_6 = """select "0", cast(cast("0" as bigint) as decimalv3(13, 6));"""
    qt_sql_23_6_strict "${const_sql_23_6}"
    testFoldConst("${const_sql_23_6}")
    def const_sql_23_7 = """select "1", cast(cast("1" as bigint) as decimalv3(13, 6));"""
    qt_sql_23_7_strict "${const_sql_23_7}"
    testFoldConst("${const_sql_23_7}")
    def const_sql_23_8 = """select "9", cast(cast("9" as bigint) as decimalv3(13, 6));"""
    qt_sql_23_8_strict "${const_sql_23_8}"
    testFoldConst("${const_sql_23_8}")
    def const_sql_23_9 = """select "999999", cast(cast("999999" as bigint) as decimalv3(13, 6));"""
    qt_sql_23_9_strict "${const_sql_23_9}"
    testFoldConst("${const_sql_23_9}")
    def const_sql_23_10 = """select "9000000", cast(cast("9000000" as bigint) as decimalv3(13, 6));"""
    qt_sql_23_10_strict "${const_sql_23_10}"
    testFoldConst("${const_sql_23_10}")
    def const_sql_23_11 = """select "9000001", cast(cast("9000001" as bigint) as decimalv3(13, 6));"""
    qt_sql_23_11_strict "${const_sql_23_11}"
    testFoldConst("${const_sql_23_11}")
    def const_sql_23_12 = """select "9999999", cast(cast("9999999" as bigint) as decimalv3(13, 6));"""
    qt_sql_23_12_strict "${const_sql_23_12}"
    testFoldConst("${const_sql_23_12}")

    sql "set enable_strict_cast=false;"
    qt_sql_23_0_non_strict "${const_sql_23_0}"
    testFoldConst("${const_sql_23_0}")
    qt_sql_23_1_non_strict "${const_sql_23_1}"
    testFoldConst("${const_sql_23_1}")
    qt_sql_23_2_non_strict "${const_sql_23_2}"
    testFoldConst("${const_sql_23_2}")
    qt_sql_23_3_non_strict "${const_sql_23_3}"
    testFoldConst("${const_sql_23_3}")
    qt_sql_23_4_non_strict "${const_sql_23_4}"
    testFoldConst("${const_sql_23_4}")
    qt_sql_23_5_non_strict "${const_sql_23_5}"
    testFoldConst("${const_sql_23_5}")
    qt_sql_23_6_non_strict "${const_sql_23_6}"
    testFoldConst("${const_sql_23_6}")
    qt_sql_23_7_non_strict "${const_sql_23_7}"
    testFoldConst("${const_sql_23_7}")
    qt_sql_23_8_non_strict "${const_sql_23_8}"
    testFoldConst("${const_sql_23_8}")
    qt_sql_23_9_non_strict "${const_sql_23_9}"
    testFoldConst("${const_sql_23_9}")
    qt_sql_23_10_non_strict "${const_sql_23_10}"
    testFoldConst("${const_sql_23_10}")
    qt_sql_23_11_non_strict "${const_sql_23_11}"
    testFoldConst("${const_sql_23_11}")
    qt_sql_23_12_non_strict "${const_sql_23_12}"
    testFoldConst("${const_sql_23_12}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_24_0 = """select "-9", cast(cast("-9" as bigint) as decimalv3(13, 12));"""
    qt_sql_24_0_strict "${const_sql_24_0}"
    testFoldConst("${const_sql_24_0}")
    def const_sql_24_1 = """select "-8", cast(cast("-8" as bigint) as decimalv3(13, 12));"""
    qt_sql_24_1_strict "${const_sql_24_1}"
    testFoldConst("${const_sql_24_1}")
    def const_sql_24_2 = """select "-1", cast(cast("-1" as bigint) as decimalv3(13, 12));"""
    qt_sql_24_2_strict "${const_sql_24_2}"
    testFoldConst("${const_sql_24_2}")
    def const_sql_24_3 = """select "0", cast(cast("0" as bigint) as decimalv3(13, 12));"""
    qt_sql_24_3_strict "${const_sql_24_3}"
    testFoldConst("${const_sql_24_3}")
    def const_sql_24_4 = """select "1", cast(cast("1" as bigint) as decimalv3(13, 12));"""
    qt_sql_24_4_strict "${const_sql_24_4}"
    testFoldConst("${const_sql_24_4}")
    def const_sql_24_5 = """select "8", cast(cast("8" as bigint) as decimalv3(13, 12));"""
    qt_sql_24_5_strict "${const_sql_24_5}"
    testFoldConst("${const_sql_24_5}")
    def const_sql_24_6 = """select "9", cast(cast("9" as bigint) as decimalv3(13, 12));"""
    qt_sql_24_6_strict "${const_sql_24_6}"
    testFoldConst("${const_sql_24_6}")

    sql "set enable_strict_cast=false;"
    qt_sql_24_0_non_strict "${const_sql_24_0}"
    testFoldConst("${const_sql_24_0}")
    qt_sql_24_1_non_strict "${const_sql_24_1}"
    testFoldConst("${const_sql_24_1}")
    qt_sql_24_2_non_strict "${const_sql_24_2}"
    testFoldConst("${const_sql_24_2}")
    qt_sql_24_3_non_strict "${const_sql_24_3}"
    testFoldConst("${const_sql_24_3}")
    qt_sql_24_4_non_strict "${const_sql_24_4}"
    testFoldConst("${const_sql_24_4}")
    qt_sql_24_5_non_strict "${const_sql_24_5}"
    testFoldConst("${const_sql_24_5}")
    qt_sql_24_6_non_strict "${const_sql_24_6}"
    testFoldConst("${const_sql_24_6}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_25_0 = """select "-9223372036854775808", cast(cast("-9223372036854775808" as bigint) as decimalv3(27, 0));"""
    qt_sql_25_0_strict "${const_sql_25_0}"
    testFoldConst("${const_sql_25_0}")
    def const_sql_25_1 = """select "-9223372036854775807", cast(cast("-9223372036854775807" as bigint) as decimalv3(27, 0));"""
    qt_sql_25_1_strict "${const_sql_25_1}"
    testFoldConst("${const_sql_25_1}")
    def const_sql_25_2 = """select "-9", cast(cast("-9" as bigint) as decimalv3(27, 0));"""
    qt_sql_25_2_strict "${const_sql_25_2}"
    testFoldConst("${const_sql_25_2}")
    def const_sql_25_3 = """select "-1", cast(cast("-1" as bigint) as decimalv3(27, 0));"""
    qt_sql_25_3_strict "${const_sql_25_3}"
    testFoldConst("${const_sql_25_3}")
    def const_sql_25_4 = """select "0", cast(cast("0" as bigint) as decimalv3(27, 0));"""
    qt_sql_25_4_strict "${const_sql_25_4}"
    testFoldConst("${const_sql_25_4}")
    def const_sql_25_5 = """select "1", cast(cast("1" as bigint) as decimalv3(27, 0));"""
    qt_sql_25_5_strict "${const_sql_25_5}"
    testFoldConst("${const_sql_25_5}")
    def const_sql_25_6 = """select "9", cast(cast("9" as bigint) as decimalv3(27, 0));"""
    qt_sql_25_6_strict "${const_sql_25_6}"
    testFoldConst("${const_sql_25_6}")
    def const_sql_25_7 = """select "9223372036854775806", cast(cast("9223372036854775806" as bigint) as decimalv3(27, 0));"""
    qt_sql_25_7_strict "${const_sql_25_7}"
    testFoldConst("${const_sql_25_7}")
    def const_sql_25_8 = """select "9223372036854775807", cast(cast("9223372036854775807" as bigint) as decimalv3(27, 0));"""
    qt_sql_25_8_strict "${const_sql_25_8}"
    testFoldConst("${const_sql_25_8}")

    sql "set enable_strict_cast=false;"
    qt_sql_25_0_non_strict "${const_sql_25_0}"
    testFoldConst("${const_sql_25_0}")
    qt_sql_25_1_non_strict "${const_sql_25_1}"
    testFoldConst("${const_sql_25_1}")
    qt_sql_25_2_non_strict "${const_sql_25_2}"
    testFoldConst("${const_sql_25_2}")
    qt_sql_25_3_non_strict "${const_sql_25_3}"
    testFoldConst("${const_sql_25_3}")
    qt_sql_25_4_non_strict "${const_sql_25_4}"
    testFoldConst("${const_sql_25_4}")
    qt_sql_25_5_non_strict "${const_sql_25_5}"
    testFoldConst("${const_sql_25_5}")
    qt_sql_25_6_non_strict "${const_sql_25_6}"
    testFoldConst("${const_sql_25_6}")
    qt_sql_25_7_non_strict "${const_sql_25_7}"
    testFoldConst("${const_sql_25_7}")
    qt_sql_25_8_non_strict "${const_sql_25_8}"
    testFoldConst("${const_sql_25_8}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_26_0 = """select "-99999999999999", cast(cast("-99999999999999" as bigint) as decimalv3(27, 13));"""
    qt_sql_26_0_strict "${const_sql_26_0}"
    testFoldConst("${const_sql_26_0}")
    def const_sql_26_1 = """select "-90000000000001", cast(cast("-90000000000001" as bigint) as decimalv3(27, 13));"""
    qt_sql_26_1_strict "${const_sql_26_1}"
    testFoldConst("${const_sql_26_1}")
    def const_sql_26_2 = """select "-90000000000000", cast(cast("-90000000000000" as bigint) as decimalv3(27, 13));"""
    qt_sql_26_2_strict "${const_sql_26_2}"
    testFoldConst("${const_sql_26_2}")
    def const_sql_26_3 = """select "-9999999999999", cast(cast("-9999999999999" as bigint) as decimalv3(27, 13));"""
    qt_sql_26_3_strict "${const_sql_26_3}"
    testFoldConst("${const_sql_26_3}")
    def const_sql_26_4 = """select "-9", cast(cast("-9" as bigint) as decimalv3(27, 13));"""
    qt_sql_26_4_strict "${const_sql_26_4}"
    testFoldConst("${const_sql_26_4}")
    def const_sql_26_5 = """select "-1", cast(cast("-1" as bigint) as decimalv3(27, 13));"""
    qt_sql_26_5_strict "${const_sql_26_5}"
    testFoldConst("${const_sql_26_5}")
    def const_sql_26_6 = """select "0", cast(cast("0" as bigint) as decimalv3(27, 13));"""
    qt_sql_26_6_strict "${const_sql_26_6}"
    testFoldConst("${const_sql_26_6}")
    def const_sql_26_7 = """select "1", cast(cast("1" as bigint) as decimalv3(27, 13));"""
    qt_sql_26_7_strict "${const_sql_26_7}"
    testFoldConst("${const_sql_26_7}")
    def const_sql_26_8 = """select "9", cast(cast("9" as bigint) as decimalv3(27, 13));"""
    qt_sql_26_8_strict "${const_sql_26_8}"
    testFoldConst("${const_sql_26_8}")
    def const_sql_26_9 = """select "9999999999999", cast(cast("9999999999999" as bigint) as decimalv3(27, 13));"""
    qt_sql_26_9_strict "${const_sql_26_9}"
    testFoldConst("${const_sql_26_9}")
    def const_sql_26_10 = """select "90000000000000", cast(cast("90000000000000" as bigint) as decimalv3(27, 13));"""
    qt_sql_26_10_strict "${const_sql_26_10}"
    testFoldConst("${const_sql_26_10}")
    def const_sql_26_11 = """select "90000000000001", cast(cast("90000000000001" as bigint) as decimalv3(27, 13));"""
    qt_sql_26_11_strict "${const_sql_26_11}"
    testFoldConst("${const_sql_26_11}")
    def const_sql_26_12 = """select "99999999999999", cast(cast("99999999999999" as bigint) as decimalv3(27, 13));"""
    qt_sql_26_12_strict "${const_sql_26_12}"
    testFoldConst("${const_sql_26_12}")

    sql "set enable_strict_cast=false;"
    qt_sql_26_0_non_strict "${const_sql_26_0}"
    testFoldConst("${const_sql_26_0}")
    qt_sql_26_1_non_strict "${const_sql_26_1}"
    testFoldConst("${const_sql_26_1}")
    qt_sql_26_2_non_strict "${const_sql_26_2}"
    testFoldConst("${const_sql_26_2}")
    qt_sql_26_3_non_strict "${const_sql_26_3}"
    testFoldConst("${const_sql_26_3}")
    qt_sql_26_4_non_strict "${const_sql_26_4}"
    testFoldConst("${const_sql_26_4}")
    qt_sql_26_5_non_strict "${const_sql_26_5}"
    testFoldConst("${const_sql_26_5}")
    qt_sql_26_6_non_strict "${const_sql_26_6}"
    testFoldConst("${const_sql_26_6}")
    qt_sql_26_7_non_strict "${const_sql_26_7}"
    testFoldConst("${const_sql_26_7}")
    qt_sql_26_8_non_strict "${const_sql_26_8}"
    testFoldConst("${const_sql_26_8}")
    qt_sql_26_9_non_strict "${const_sql_26_9}"
    testFoldConst("${const_sql_26_9}")
    qt_sql_26_10_non_strict "${const_sql_26_10}"
    testFoldConst("${const_sql_26_10}")
    qt_sql_26_11_non_strict "${const_sql_26_11}"
    testFoldConst("${const_sql_26_11}")
    qt_sql_26_12_non_strict "${const_sql_26_12}"
    testFoldConst("${const_sql_26_12}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_27_0 = """select "-9", cast(cast("-9" as bigint) as decimalv3(27, 26));"""
    qt_sql_27_0_strict "${const_sql_27_0}"
    testFoldConst("${const_sql_27_0}")
    def const_sql_27_1 = """select "-8", cast(cast("-8" as bigint) as decimalv3(27, 26));"""
    qt_sql_27_1_strict "${const_sql_27_1}"
    testFoldConst("${const_sql_27_1}")
    def const_sql_27_2 = """select "-1", cast(cast("-1" as bigint) as decimalv3(27, 26));"""
    qt_sql_27_2_strict "${const_sql_27_2}"
    testFoldConst("${const_sql_27_2}")
    def const_sql_27_3 = """select "0", cast(cast("0" as bigint) as decimalv3(27, 26));"""
    qt_sql_27_3_strict "${const_sql_27_3}"
    testFoldConst("${const_sql_27_3}")
    def const_sql_27_4 = """select "1", cast(cast("1" as bigint) as decimalv3(27, 26));"""
    qt_sql_27_4_strict "${const_sql_27_4}"
    testFoldConst("${const_sql_27_4}")
    def const_sql_27_5 = """select "8", cast(cast("8" as bigint) as decimalv3(27, 26));"""
    qt_sql_27_5_strict "${const_sql_27_5}"
    testFoldConst("${const_sql_27_5}")
    def const_sql_27_6 = """select "9", cast(cast("9" as bigint) as decimalv3(27, 26));"""
    qt_sql_27_6_strict "${const_sql_27_6}"
    testFoldConst("${const_sql_27_6}")

    sql "set enable_strict_cast=false;"
    qt_sql_27_0_non_strict "${const_sql_27_0}"
    testFoldConst("${const_sql_27_0}")
    qt_sql_27_1_non_strict "${const_sql_27_1}"
    testFoldConst("${const_sql_27_1}")
    qt_sql_27_2_non_strict "${const_sql_27_2}"
    testFoldConst("${const_sql_27_2}")
    qt_sql_27_3_non_strict "${const_sql_27_3}"
    testFoldConst("${const_sql_27_3}")
    qt_sql_27_4_non_strict "${const_sql_27_4}"
    testFoldConst("${const_sql_27_4}")
    qt_sql_27_5_non_strict "${const_sql_27_5}"
    testFoldConst("${const_sql_27_5}")
    qt_sql_27_6_non_strict "${const_sql_27_6}"
    testFoldConst("${const_sql_27_6}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_28_0 = """select "-9", cast(cast("-9" as largeint) as decimalv3(1, 0));"""
    qt_sql_28_0_strict "${const_sql_28_0}"
    testFoldConst("${const_sql_28_0}")
    def const_sql_28_1 = """select "-8", cast(cast("-8" as largeint) as decimalv3(1, 0));"""
    qt_sql_28_1_strict "${const_sql_28_1}"
    testFoldConst("${const_sql_28_1}")
    def const_sql_28_2 = """select "-1", cast(cast("-1" as largeint) as decimalv3(1, 0));"""
    qt_sql_28_2_strict "${const_sql_28_2}"
    testFoldConst("${const_sql_28_2}")
    def const_sql_28_3 = """select "0", cast(cast("0" as largeint) as decimalv3(1, 0));"""
    qt_sql_28_3_strict "${const_sql_28_3}"
    testFoldConst("${const_sql_28_3}")
    def const_sql_28_4 = """select "1", cast(cast("1" as largeint) as decimalv3(1, 0));"""
    qt_sql_28_4_strict "${const_sql_28_4}"
    testFoldConst("${const_sql_28_4}")
    def const_sql_28_5 = """select "8", cast(cast("8" as largeint) as decimalv3(1, 0));"""
    qt_sql_28_5_strict "${const_sql_28_5}"
    testFoldConst("${const_sql_28_5}")
    def const_sql_28_6 = """select "9", cast(cast("9" as largeint) as decimalv3(1, 0));"""
    qt_sql_28_6_strict "${const_sql_28_6}"
    testFoldConst("${const_sql_28_6}")

    sql "set enable_strict_cast=false;"
    qt_sql_28_0_non_strict "${const_sql_28_0}"
    testFoldConst("${const_sql_28_0}")
    qt_sql_28_1_non_strict "${const_sql_28_1}"
    testFoldConst("${const_sql_28_1}")
    qt_sql_28_2_non_strict "${const_sql_28_2}"
    testFoldConst("${const_sql_28_2}")
    qt_sql_28_3_non_strict "${const_sql_28_3}"
    testFoldConst("${const_sql_28_3}")
    qt_sql_28_4_non_strict "${const_sql_28_4}"
    testFoldConst("${const_sql_28_4}")
    qt_sql_28_5_non_strict "${const_sql_28_5}"
    testFoldConst("${const_sql_28_5}")
    qt_sql_28_6_non_strict "${const_sql_28_6}"
    testFoldConst("${const_sql_28_6}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_29_0 = """select "-9999999999999", cast(cast("-9999999999999" as largeint) as decimalv3(13, 0));"""
    qt_sql_29_0_strict "${const_sql_29_0}"
    testFoldConst("${const_sql_29_0}")
    def const_sql_29_1 = """select "-9000000000001", cast(cast("-9000000000001" as largeint) as decimalv3(13, 0));"""
    qt_sql_29_1_strict "${const_sql_29_1}"
    testFoldConst("${const_sql_29_1}")
    def const_sql_29_2 = """select "-9000000000000", cast(cast("-9000000000000" as largeint) as decimalv3(13, 0));"""
    qt_sql_29_2_strict "${const_sql_29_2}"
    testFoldConst("${const_sql_29_2}")
    def const_sql_29_3 = """select "-999999999999", cast(cast("-999999999999" as largeint) as decimalv3(13, 0));"""
    qt_sql_29_3_strict "${const_sql_29_3}"
    testFoldConst("${const_sql_29_3}")
    def const_sql_29_4 = """select "-9", cast(cast("-9" as largeint) as decimalv3(13, 0));"""
    qt_sql_29_4_strict "${const_sql_29_4}"
    testFoldConst("${const_sql_29_4}")
    def const_sql_29_5 = """select "-1", cast(cast("-1" as largeint) as decimalv3(13, 0));"""
    qt_sql_29_5_strict "${const_sql_29_5}"
    testFoldConst("${const_sql_29_5}")
    def const_sql_29_6 = """select "0", cast(cast("0" as largeint) as decimalv3(13, 0));"""
    qt_sql_29_6_strict "${const_sql_29_6}"
    testFoldConst("${const_sql_29_6}")
    def const_sql_29_7 = """select "1", cast(cast("1" as largeint) as decimalv3(13, 0));"""
    qt_sql_29_7_strict "${const_sql_29_7}"
    testFoldConst("${const_sql_29_7}")
    def const_sql_29_8 = """select "9", cast(cast("9" as largeint) as decimalv3(13, 0));"""
    qt_sql_29_8_strict "${const_sql_29_8}"
    testFoldConst("${const_sql_29_8}")
    def const_sql_29_9 = """select "999999999999", cast(cast("999999999999" as largeint) as decimalv3(13, 0));"""
    qt_sql_29_9_strict "${const_sql_29_9}"
    testFoldConst("${const_sql_29_9}")
    def const_sql_29_10 = """select "9000000000000", cast(cast("9000000000000" as largeint) as decimalv3(13, 0));"""
    qt_sql_29_10_strict "${const_sql_29_10}"
    testFoldConst("${const_sql_29_10}")
    def const_sql_29_11 = """select "9000000000001", cast(cast("9000000000001" as largeint) as decimalv3(13, 0));"""
    qt_sql_29_11_strict "${const_sql_29_11}"
    testFoldConst("${const_sql_29_11}")
    def const_sql_29_12 = """select "9999999999999", cast(cast("9999999999999" as largeint) as decimalv3(13, 0));"""
    qt_sql_29_12_strict "${const_sql_29_12}"
    testFoldConst("${const_sql_29_12}")

    sql "set enable_strict_cast=false;"
    qt_sql_29_0_non_strict "${const_sql_29_0}"
    testFoldConst("${const_sql_29_0}")
    qt_sql_29_1_non_strict "${const_sql_29_1}"
    testFoldConst("${const_sql_29_1}")
    qt_sql_29_2_non_strict "${const_sql_29_2}"
    testFoldConst("${const_sql_29_2}")
    qt_sql_29_3_non_strict "${const_sql_29_3}"
    testFoldConst("${const_sql_29_3}")
    qt_sql_29_4_non_strict "${const_sql_29_4}"
    testFoldConst("${const_sql_29_4}")
    qt_sql_29_5_non_strict "${const_sql_29_5}"
    testFoldConst("${const_sql_29_5}")
    qt_sql_29_6_non_strict "${const_sql_29_6}"
    testFoldConst("${const_sql_29_6}")
    qt_sql_29_7_non_strict "${const_sql_29_7}"
    testFoldConst("${const_sql_29_7}")
    qt_sql_29_8_non_strict "${const_sql_29_8}"
    testFoldConst("${const_sql_29_8}")
    qt_sql_29_9_non_strict "${const_sql_29_9}"
    testFoldConst("${const_sql_29_9}")
    qt_sql_29_10_non_strict "${const_sql_29_10}"
    testFoldConst("${const_sql_29_10}")
    qt_sql_29_11_non_strict "${const_sql_29_11}"
    testFoldConst("${const_sql_29_11}")
    qt_sql_29_12_non_strict "${const_sql_29_12}"
    testFoldConst("${const_sql_29_12}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_30_0 = """select "-9999999", cast(cast("-9999999" as largeint) as decimalv3(13, 6));"""
    qt_sql_30_0_strict "${const_sql_30_0}"
    testFoldConst("${const_sql_30_0}")
    def const_sql_30_1 = """select "-9000001", cast(cast("-9000001" as largeint) as decimalv3(13, 6));"""
    qt_sql_30_1_strict "${const_sql_30_1}"
    testFoldConst("${const_sql_30_1}")
    def const_sql_30_2 = """select "-9000000", cast(cast("-9000000" as largeint) as decimalv3(13, 6));"""
    qt_sql_30_2_strict "${const_sql_30_2}"
    testFoldConst("${const_sql_30_2}")
    def const_sql_30_3 = """select "-999999", cast(cast("-999999" as largeint) as decimalv3(13, 6));"""
    qt_sql_30_3_strict "${const_sql_30_3}"
    testFoldConst("${const_sql_30_3}")
    def const_sql_30_4 = """select "-9", cast(cast("-9" as largeint) as decimalv3(13, 6));"""
    qt_sql_30_4_strict "${const_sql_30_4}"
    testFoldConst("${const_sql_30_4}")
    def const_sql_30_5 = """select "-1", cast(cast("-1" as largeint) as decimalv3(13, 6));"""
    qt_sql_30_5_strict "${const_sql_30_5}"
    testFoldConst("${const_sql_30_5}")
    def const_sql_30_6 = """select "0", cast(cast("0" as largeint) as decimalv3(13, 6));"""
    qt_sql_30_6_strict "${const_sql_30_6}"
    testFoldConst("${const_sql_30_6}")
    def const_sql_30_7 = """select "1", cast(cast("1" as largeint) as decimalv3(13, 6));"""
    qt_sql_30_7_strict "${const_sql_30_7}"
    testFoldConst("${const_sql_30_7}")
    def const_sql_30_8 = """select "9", cast(cast("9" as largeint) as decimalv3(13, 6));"""
    qt_sql_30_8_strict "${const_sql_30_8}"
    testFoldConst("${const_sql_30_8}")
    def const_sql_30_9 = """select "999999", cast(cast("999999" as largeint) as decimalv3(13, 6));"""
    qt_sql_30_9_strict "${const_sql_30_9}"
    testFoldConst("${const_sql_30_9}")
    def const_sql_30_10 = """select "9000000", cast(cast("9000000" as largeint) as decimalv3(13, 6));"""
    qt_sql_30_10_strict "${const_sql_30_10}"
    testFoldConst("${const_sql_30_10}")
    def const_sql_30_11 = """select "9000001", cast(cast("9000001" as largeint) as decimalv3(13, 6));"""
    qt_sql_30_11_strict "${const_sql_30_11}"
    testFoldConst("${const_sql_30_11}")
    def const_sql_30_12 = """select "9999999", cast(cast("9999999" as largeint) as decimalv3(13, 6));"""
    qt_sql_30_12_strict "${const_sql_30_12}"
    testFoldConst("${const_sql_30_12}")

    sql "set enable_strict_cast=false;"
    qt_sql_30_0_non_strict "${const_sql_30_0}"
    testFoldConst("${const_sql_30_0}")
    qt_sql_30_1_non_strict "${const_sql_30_1}"
    testFoldConst("${const_sql_30_1}")
    qt_sql_30_2_non_strict "${const_sql_30_2}"
    testFoldConst("${const_sql_30_2}")
    qt_sql_30_3_non_strict "${const_sql_30_3}"
    testFoldConst("${const_sql_30_3}")
    qt_sql_30_4_non_strict "${const_sql_30_4}"
    testFoldConst("${const_sql_30_4}")
    qt_sql_30_5_non_strict "${const_sql_30_5}"
    testFoldConst("${const_sql_30_5}")
    qt_sql_30_6_non_strict "${const_sql_30_6}"
    testFoldConst("${const_sql_30_6}")
    qt_sql_30_7_non_strict "${const_sql_30_7}"
    testFoldConst("${const_sql_30_7}")
    qt_sql_30_8_non_strict "${const_sql_30_8}"
    testFoldConst("${const_sql_30_8}")
    qt_sql_30_9_non_strict "${const_sql_30_9}"
    testFoldConst("${const_sql_30_9}")
    qt_sql_30_10_non_strict "${const_sql_30_10}"
    testFoldConst("${const_sql_30_10}")
    qt_sql_30_11_non_strict "${const_sql_30_11}"
    testFoldConst("${const_sql_30_11}")
    qt_sql_30_12_non_strict "${const_sql_30_12}"
    testFoldConst("${const_sql_30_12}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_31_0 = """select "-9", cast(cast("-9" as largeint) as decimalv3(13, 12));"""
    qt_sql_31_0_strict "${const_sql_31_0}"
    testFoldConst("${const_sql_31_0}")
    def const_sql_31_1 = """select "-8", cast(cast("-8" as largeint) as decimalv3(13, 12));"""
    qt_sql_31_1_strict "${const_sql_31_1}"
    testFoldConst("${const_sql_31_1}")
    def const_sql_31_2 = """select "-1", cast(cast("-1" as largeint) as decimalv3(13, 12));"""
    qt_sql_31_2_strict "${const_sql_31_2}"
    testFoldConst("${const_sql_31_2}")
    def const_sql_31_3 = """select "0", cast(cast("0" as largeint) as decimalv3(13, 12));"""
    qt_sql_31_3_strict "${const_sql_31_3}"
    testFoldConst("${const_sql_31_3}")
    def const_sql_31_4 = """select "1", cast(cast("1" as largeint) as decimalv3(13, 12));"""
    qt_sql_31_4_strict "${const_sql_31_4}"
    testFoldConst("${const_sql_31_4}")
    def const_sql_31_5 = """select "8", cast(cast("8" as largeint) as decimalv3(13, 12));"""
    qt_sql_31_5_strict "${const_sql_31_5}"
    testFoldConst("${const_sql_31_5}")
    def const_sql_31_6 = """select "9", cast(cast("9" as largeint) as decimalv3(13, 12));"""
    qt_sql_31_6_strict "${const_sql_31_6}"
    testFoldConst("${const_sql_31_6}")

    sql "set enable_strict_cast=false;"
    qt_sql_31_0_non_strict "${const_sql_31_0}"
    testFoldConst("${const_sql_31_0}")
    qt_sql_31_1_non_strict "${const_sql_31_1}"
    testFoldConst("${const_sql_31_1}")
    qt_sql_31_2_non_strict "${const_sql_31_2}"
    testFoldConst("${const_sql_31_2}")
    qt_sql_31_3_non_strict "${const_sql_31_3}"
    testFoldConst("${const_sql_31_3}")
    qt_sql_31_4_non_strict "${const_sql_31_4}"
    testFoldConst("${const_sql_31_4}")
    qt_sql_31_5_non_strict "${const_sql_31_5}"
    testFoldConst("${const_sql_31_5}")
    qt_sql_31_6_non_strict "${const_sql_31_6}"
    testFoldConst("${const_sql_31_6}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_32_0 = """select "-999999999999999999999999999", cast(cast("-999999999999999999999999999" as largeint) as decimalv3(27, 0));"""
    qt_sql_32_0_strict "${const_sql_32_0}"
    testFoldConst("${const_sql_32_0}")
    def const_sql_32_1 = """select "-900000000000000000000000001", cast(cast("-900000000000000000000000001" as largeint) as decimalv3(27, 0));"""
    qt_sql_32_1_strict "${const_sql_32_1}"
    testFoldConst("${const_sql_32_1}")
    def const_sql_32_2 = """select "-900000000000000000000000000", cast(cast("-900000000000000000000000000" as largeint) as decimalv3(27, 0));"""
    qt_sql_32_2_strict "${const_sql_32_2}"
    testFoldConst("${const_sql_32_2}")
    def const_sql_32_3 = """select "-99999999999999999999999999", cast(cast("-99999999999999999999999999" as largeint) as decimalv3(27, 0));"""
    qt_sql_32_3_strict "${const_sql_32_3}"
    testFoldConst("${const_sql_32_3}")
    def const_sql_32_4 = """select "-9", cast(cast("-9" as largeint) as decimalv3(27, 0));"""
    qt_sql_32_4_strict "${const_sql_32_4}"
    testFoldConst("${const_sql_32_4}")
    def const_sql_32_5 = """select "-1", cast(cast("-1" as largeint) as decimalv3(27, 0));"""
    qt_sql_32_5_strict "${const_sql_32_5}"
    testFoldConst("${const_sql_32_5}")
    def const_sql_32_6 = """select "0", cast(cast("0" as largeint) as decimalv3(27, 0));"""
    qt_sql_32_6_strict "${const_sql_32_6}"
    testFoldConst("${const_sql_32_6}")
    def const_sql_32_7 = """select "1", cast(cast("1" as largeint) as decimalv3(27, 0));"""
    qt_sql_32_7_strict "${const_sql_32_7}"
    testFoldConst("${const_sql_32_7}")
    def const_sql_32_8 = """select "9", cast(cast("9" as largeint) as decimalv3(27, 0));"""
    qt_sql_32_8_strict "${const_sql_32_8}"
    testFoldConst("${const_sql_32_8}")
    def const_sql_32_9 = """select "99999999999999999999999999", cast(cast("99999999999999999999999999" as largeint) as decimalv3(27, 0));"""
    qt_sql_32_9_strict "${const_sql_32_9}"
    testFoldConst("${const_sql_32_9}")
    def const_sql_32_10 = """select "900000000000000000000000000", cast(cast("900000000000000000000000000" as largeint) as decimalv3(27, 0));"""
    qt_sql_32_10_strict "${const_sql_32_10}"
    testFoldConst("${const_sql_32_10}")
    def const_sql_32_11 = """select "900000000000000000000000001", cast(cast("900000000000000000000000001" as largeint) as decimalv3(27, 0));"""
    qt_sql_32_11_strict "${const_sql_32_11}"
    testFoldConst("${const_sql_32_11}")
    def const_sql_32_12 = """select "999999999999999999999999999", cast(cast("999999999999999999999999999" as largeint) as decimalv3(27, 0));"""
    qt_sql_32_12_strict "${const_sql_32_12}"
    testFoldConst("${const_sql_32_12}")

    sql "set enable_strict_cast=false;"
    qt_sql_32_0_non_strict "${const_sql_32_0}"
    testFoldConst("${const_sql_32_0}")
    qt_sql_32_1_non_strict "${const_sql_32_1}"
    testFoldConst("${const_sql_32_1}")
    qt_sql_32_2_non_strict "${const_sql_32_2}"
    testFoldConst("${const_sql_32_2}")
    qt_sql_32_3_non_strict "${const_sql_32_3}"
    testFoldConst("${const_sql_32_3}")
    qt_sql_32_4_non_strict "${const_sql_32_4}"
    testFoldConst("${const_sql_32_4}")
    qt_sql_32_5_non_strict "${const_sql_32_5}"
    testFoldConst("${const_sql_32_5}")
    qt_sql_32_6_non_strict "${const_sql_32_6}"
    testFoldConst("${const_sql_32_6}")
    qt_sql_32_7_non_strict "${const_sql_32_7}"
    testFoldConst("${const_sql_32_7}")
    qt_sql_32_8_non_strict "${const_sql_32_8}"
    testFoldConst("${const_sql_32_8}")
    qt_sql_32_9_non_strict "${const_sql_32_9}"
    testFoldConst("${const_sql_32_9}")
    qt_sql_32_10_non_strict "${const_sql_32_10}"
    testFoldConst("${const_sql_32_10}")
    qt_sql_32_11_non_strict "${const_sql_32_11}"
    testFoldConst("${const_sql_32_11}")
    qt_sql_32_12_non_strict "${const_sql_32_12}"
    testFoldConst("${const_sql_32_12}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_33_0 = """select "-99999999999999", cast(cast("-99999999999999" as largeint) as decimalv3(27, 13));"""
    qt_sql_33_0_strict "${const_sql_33_0}"
    testFoldConst("${const_sql_33_0}")
    def const_sql_33_1 = """select "-90000000000001", cast(cast("-90000000000001" as largeint) as decimalv3(27, 13));"""
    qt_sql_33_1_strict "${const_sql_33_1}"
    testFoldConst("${const_sql_33_1}")
    def const_sql_33_2 = """select "-90000000000000", cast(cast("-90000000000000" as largeint) as decimalv3(27, 13));"""
    qt_sql_33_2_strict "${const_sql_33_2}"
    testFoldConst("${const_sql_33_2}")
    def const_sql_33_3 = """select "-9999999999999", cast(cast("-9999999999999" as largeint) as decimalv3(27, 13));"""
    qt_sql_33_3_strict "${const_sql_33_3}"
    testFoldConst("${const_sql_33_3}")
    def const_sql_33_4 = """select "-9", cast(cast("-9" as largeint) as decimalv3(27, 13));"""
    qt_sql_33_4_strict "${const_sql_33_4}"
    testFoldConst("${const_sql_33_4}")
    def const_sql_33_5 = """select "-1", cast(cast("-1" as largeint) as decimalv3(27, 13));"""
    qt_sql_33_5_strict "${const_sql_33_5}"
    testFoldConst("${const_sql_33_5}")
    def const_sql_33_6 = """select "0", cast(cast("0" as largeint) as decimalv3(27, 13));"""
    qt_sql_33_6_strict "${const_sql_33_6}"
    testFoldConst("${const_sql_33_6}")
    def const_sql_33_7 = """select "1", cast(cast("1" as largeint) as decimalv3(27, 13));"""
    qt_sql_33_7_strict "${const_sql_33_7}"
    testFoldConst("${const_sql_33_7}")
    def const_sql_33_8 = """select "9", cast(cast("9" as largeint) as decimalv3(27, 13));"""
    qt_sql_33_8_strict "${const_sql_33_8}"
    testFoldConst("${const_sql_33_8}")
    def const_sql_33_9 = """select "9999999999999", cast(cast("9999999999999" as largeint) as decimalv3(27, 13));"""
    qt_sql_33_9_strict "${const_sql_33_9}"
    testFoldConst("${const_sql_33_9}")
    def const_sql_33_10 = """select "90000000000000", cast(cast("90000000000000" as largeint) as decimalv3(27, 13));"""
    qt_sql_33_10_strict "${const_sql_33_10}"
    testFoldConst("${const_sql_33_10}")
    def const_sql_33_11 = """select "90000000000001", cast(cast("90000000000001" as largeint) as decimalv3(27, 13));"""
    qt_sql_33_11_strict "${const_sql_33_11}"
    testFoldConst("${const_sql_33_11}")
    def const_sql_33_12 = """select "99999999999999", cast(cast("99999999999999" as largeint) as decimalv3(27, 13));"""
    qt_sql_33_12_strict "${const_sql_33_12}"
    testFoldConst("${const_sql_33_12}")

    sql "set enable_strict_cast=false;"
    qt_sql_33_0_non_strict "${const_sql_33_0}"
    testFoldConst("${const_sql_33_0}")
    qt_sql_33_1_non_strict "${const_sql_33_1}"
    testFoldConst("${const_sql_33_1}")
    qt_sql_33_2_non_strict "${const_sql_33_2}"
    testFoldConst("${const_sql_33_2}")
    qt_sql_33_3_non_strict "${const_sql_33_3}"
    testFoldConst("${const_sql_33_3}")
    qt_sql_33_4_non_strict "${const_sql_33_4}"
    testFoldConst("${const_sql_33_4}")
    qt_sql_33_5_non_strict "${const_sql_33_5}"
    testFoldConst("${const_sql_33_5}")
    qt_sql_33_6_non_strict "${const_sql_33_6}"
    testFoldConst("${const_sql_33_6}")
    qt_sql_33_7_non_strict "${const_sql_33_7}"
    testFoldConst("${const_sql_33_7}")
    qt_sql_33_8_non_strict "${const_sql_33_8}"
    testFoldConst("${const_sql_33_8}")
    qt_sql_33_9_non_strict "${const_sql_33_9}"
    testFoldConst("${const_sql_33_9}")
    qt_sql_33_10_non_strict "${const_sql_33_10}"
    testFoldConst("${const_sql_33_10}")
    qt_sql_33_11_non_strict "${const_sql_33_11}"
    testFoldConst("${const_sql_33_11}")
    qt_sql_33_12_non_strict "${const_sql_33_12}"
    testFoldConst("${const_sql_33_12}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_34_0 = """select "-9", cast(cast("-9" as largeint) as decimalv3(27, 26));"""
    qt_sql_34_0_strict "${const_sql_34_0}"
    testFoldConst("${const_sql_34_0}")
    def const_sql_34_1 = """select "-8", cast(cast("-8" as largeint) as decimalv3(27, 26));"""
    qt_sql_34_1_strict "${const_sql_34_1}"
    testFoldConst("${const_sql_34_1}")
    def const_sql_34_2 = """select "-1", cast(cast("-1" as largeint) as decimalv3(27, 26));"""
    qt_sql_34_2_strict "${const_sql_34_2}"
    testFoldConst("${const_sql_34_2}")
    def const_sql_34_3 = """select "0", cast(cast("0" as largeint) as decimalv3(27, 26));"""
    qt_sql_34_3_strict "${const_sql_34_3}"
    testFoldConst("${const_sql_34_3}")
    def const_sql_34_4 = """select "1", cast(cast("1" as largeint) as decimalv3(27, 26));"""
    qt_sql_34_4_strict "${const_sql_34_4}"
    testFoldConst("${const_sql_34_4}")
    def const_sql_34_5 = """select "8", cast(cast("8" as largeint) as decimalv3(27, 26));"""
    qt_sql_34_5_strict "${const_sql_34_5}"
    testFoldConst("${const_sql_34_5}")
    def const_sql_34_6 = """select "9", cast(cast("9" as largeint) as decimalv3(27, 26));"""
    qt_sql_34_6_strict "${const_sql_34_6}"
    testFoldConst("${const_sql_34_6}")

    sql "set enable_strict_cast=false;"
    qt_sql_34_0_non_strict "${const_sql_34_0}"
    testFoldConst("${const_sql_34_0}")
    qt_sql_34_1_non_strict "${const_sql_34_1}"
    testFoldConst("${const_sql_34_1}")
    qt_sql_34_2_non_strict "${const_sql_34_2}"
    testFoldConst("${const_sql_34_2}")
    qt_sql_34_3_non_strict "${const_sql_34_3}"
    testFoldConst("${const_sql_34_3}")
    qt_sql_34_4_non_strict "${const_sql_34_4}"
    testFoldConst("${const_sql_34_4}")
    qt_sql_34_5_non_strict "${const_sql_34_5}"
    testFoldConst("${const_sql_34_5}")
    qt_sql_34_6_non_strict "${const_sql_34_6}"
    testFoldConst("${const_sql_34_6}")
}