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


suite("test_cast_to_decimal32_1_1_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_strict_cast=true;"
    def const_sql_34 = """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(1, 1));"""
    qt_sql_34_strict "${const_sql_34}"
    testFoldConst("${const_sql_34}")
    def const_sql_35 = """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(1, 1));"""
    qt_sql_35_strict "${const_sql_35}"
    testFoldConst("${const_sql_35}")
    def const_sql_36 = """select cast(".04" as decimalv3(1, 1));"""
    qt_sql_36_strict "${const_sql_36}"
    testFoldConst("${const_sql_36}")
    def const_sql_37 = """select cast(".14" as decimalv3(1, 1));"""
    qt_sql_37_strict "${const_sql_37}"
    testFoldConst("${const_sql_37}")
    def const_sql_38 = """select cast(".84" as decimalv3(1, 1));"""
    qt_sql_38_strict "${const_sql_38}"
    testFoldConst("${const_sql_38}")
    def const_sql_39 = """select cast(".94" as decimalv3(1, 1));"""
    qt_sql_39_strict "${const_sql_39}"
    testFoldConst("${const_sql_39}")
    def const_sql_40 = """select cast(".05" as decimalv3(1, 1));"""
    qt_sql_40_strict "${const_sql_40}"
    testFoldConst("${const_sql_40}")
    def const_sql_41 = """select cast(".15" as decimalv3(1, 1));"""
    qt_sql_41_strict "${const_sql_41}"
    testFoldConst("${const_sql_41}")
    def const_sql_42 = """select cast(".85" as decimalv3(1, 1));"""
    qt_sql_42_strict "${const_sql_42}"
    testFoldConst("${const_sql_42}")
    def const_sql_43 = """select cast(".94" as decimalv3(1, 1));"""
    qt_sql_43_strict "${const_sql_43}"
    testFoldConst("${const_sql_43}")
    def const_sql_44 = """select cast("-.04" as decimalv3(1, 1));"""
    qt_sql_44_strict "${const_sql_44}"
    testFoldConst("${const_sql_44}")
    def const_sql_45 = """select cast("-.14" as decimalv3(1, 1));"""
    qt_sql_45_strict "${const_sql_45}"
    testFoldConst("${const_sql_45}")
    def const_sql_46 = """select cast("-.84" as decimalv3(1, 1));"""
    qt_sql_46_strict "${const_sql_46}"
    testFoldConst("${const_sql_46}")
    def const_sql_47 = """select cast("-.94" as decimalv3(1, 1));"""
    qt_sql_47_strict "${const_sql_47}"
    testFoldConst("${const_sql_47}")
    def const_sql_48 = """select cast("-.05" as decimalv3(1, 1));"""
    qt_sql_48_strict "${const_sql_48}"
    testFoldConst("${const_sql_48}")
    def const_sql_49 = """select cast("-.15" as decimalv3(1, 1));"""
    qt_sql_49_strict "${const_sql_49}"
    testFoldConst("${const_sql_49}")
    def const_sql_50 = """select cast("-.85" as decimalv3(1, 1));"""
    qt_sql_50_strict "${const_sql_50}"
    testFoldConst("${const_sql_50}")
    def const_sql_51 = """select cast("-.94" as decimalv3(1, 1));"""
    qt_sql_51_strict "${const_sql_51}"
    testFoldConst("${const_sql_51}")
    sql "set enable_strict_cast=false;"
    qt_sql_34_non_strict "${const_sql_34}"
    testFoldConst("${const_sql_34}")
    qt_sql_35_non_strict "${const_sql_35}"
    testFoldConst("${const_sql_35}")
    qt_sql_36_non_strict "${const_sql_36}"
    testFoldConst("${const_sql_36}")
    qt_sql_37_non_strict "${const_sql_37}"
    testFoldConst("${const_sql_37}")
    qt_sql_38_non_strict "${const_sql_38}"
    testFoldConst("${const_sql_38}")
    qt_sql_39_non_strict "${const_sql_39}"
    testFoldConst("${const_sql_39}")
    qt_sql_40_non_strict "${const_sql_40}"
    testFoldConst("${const_sql_40}")
    qt_sql_41_non_strict "${const_sql_41}"
    testFoldConst("${const_sql_41}")
    qt_sql_42_non_strict "${const_sql_42}"
    testFoldConst("${const_sql_42}")
    qt_sql_43_non_strict "${const_sql_43}"
    testFoldConst("${const_sql_43}")
    qt_sql_44_non_strict "${const_sql_44}"
    testFoldConst("${const_sql_44}")
    qt_sql_45_non_strict "${const_sql_45}"
    testFoldConst("${const_sql_45}")
    qt_sql_46_non_strict "${const_sql_46}"
    testFoldConst("${const_sql_46}")
    qt_sql_47_non_strict "${const_sql_47}"
    testFoldConst("${const_sql_47}")
    qt_sql_48_non_strict "${const_sql_48}"
    testFoldConst("${const_sql_48}")
    qt_sql_49_non_strict "${const_sql_49}"
    testFoldConst("${const_sql_49}")
    qt_sql_50_non_strict "${const_sql_50}"
    testFoldConst("${const_sql_50}")
    qt_sql_51_non_strict "${const_sql_51}"
    testFoldConst("${const_sql_51}")
}