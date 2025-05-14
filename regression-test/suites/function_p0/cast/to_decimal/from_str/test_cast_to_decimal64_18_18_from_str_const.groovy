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


suite("test_cast_to_decimal64_18_18_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_strict_cast=true;"
    def const_sql_5134 = """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(18, 18));"""
    qt_sql_5134_strict "${const_sql_5134}"
    testFoldConst("${const_sql_5134}")
    def const_sql_5135 = """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(18, 18));"""
    qt_sql_5135_strict "${const_sql_5135}"
    testFoldConst("${const_sql_5135}")
    def const_sql_5136 = """select cast(".0000000000000000004" as decimalv3(18, 18));"""
    qt_sql_5136_strict "${const_sql_5136}"
    testFoldConst("${const_sql_5136}")
    def const_sql_5137 = """select cast(".0000000000000000014" as decimalv3(18, 18));"""
    qt_sql_5137_strict "${const_sql_5137}"
    testFoldConst("${const_sql_5137}")
    def const_sql_5138 = """select cast(".0000000000000000094" as decimalv3(18, 18));"""
    qt_sql_5138_strict "${const_sql_5138}"
    testFoldConst("${const_sql_5138}")
    def const_sql_5139 = """select cast(".0999999999999999994" as decimalv3(18, 18));"""
    qt_sql_5139_strict "${const_sql_5139}"
    testFoldConst("${const_sql_5139}")
    def const_sql_5140 = """select cast(".9000000000000000004" as decimalv3(18, 18));"""
    qt_sql_5140_strict "${const_sql_5140}"
    testFoldConst("${const_sql_5140}")
    def const_sql_5141 = """select cast(".9000000000000000014" as decimalv3(18, 18));"""
    qt_sql_5141_strict "${const_sql_5141}"
    testFoldConst("${const_sql_5141}")
    def const_sql_5142 = """select cast(".9999999999999999984" as decimalv3(18, 18));"""
    qt_sql_5142_strict "${const_sql_5142}"
    testFoldConst("${const_sql_5142}")
    def const_sql_5143 = """select cast(".9999999999999999994" as decimalv3(18, 18));"""
    qt_sql_5143_strict "${const_sql_5143}"
    testFoldConst("${const_sql_5143}")
    def const_sql_5144 = """select cast(".0000000000000000005" as decimalv3(18, 18));"""
    qt_sql_5144_strict "${const_sql_5144}"
    testFoldConst("${const_sql_5144}")
    def const_sql_5145 = """select cast(".0000000000000000015" as decimalv3(18, 18));"""
    qt_sql_5145_strict "${const_sql_5145}"
    testFoldConst("${const_sql_5145}")
    def const_sql_5146 = """select cast(".0000000000000000095" as decimalv3(18, 18));"""
    qt_sql_5146_strict "${const_sql_5146}"
    testFoldConst("${const_sql_5146}")
    def const_sql_5147 = """select cast(".0999999999999999995" as decimalv3(18, 18));"""
    qt_sql_5147_strict "${const_sql_5147}"
    testFoldConst("${const_sql_5147}")
    def const_sql_5148 = """select cast(".9000000000000000005" as decimalv3(18, 18));"""
    qt_sql_5148_strict "${const_sql_5148}"
    testFoldConst("${const_sql_5148}")
    def const_sql_5149 = """select cast(".9000000000000000015" as decimalv3(18, 18));"""
    qt_sql_5149_strict "${const_sql_5149}"
    testFoldConst("${const_sql_5149}")
    def const_sql_5150 = """select cast(".9999999999999999985" as decimalv3(18, 18));"""
    qt_sql_5150_strict "${const_sql_5150}"
    testFoldConst("${const_sql_5150}")
    def const_sql_5151 = """select cast(".9999999999999999994" as decimalv3(18, 18));"""
    qt_sql_5151_strict "${const_sql_5151}"
    testFoldConst("${const_sql_5151}")
    def const_sql_5152 = """select cast("-.0000000000000000004" as decimalv3(18, 18));"""
    qt_sql_5152_strict "${const_sql_5152}"
    testFoldConst("${const_sql_5152}")
    def const_sql_5153 = """select cast("-.0000000000000000014" as decimalv3(18, 18));"""
    qt_sql_5153_strict "${const_sql_5153}"
    testFoldConst("${const_sql_5153}")
    def const_sql_5154 = """select cast("-.0000000000000000094" as decimalv3(18, 18));"""
    qt_sql_5154_strict "${const_sql_5154}"
    testFoldConst("${const_sql_5154}")
    def const_sql_5155 = """select cast("-.0999999999999999994" as decimalv3(18, 18));"""
    qt_sql_5155_strict "${const_sql_5155}"
    testFoldConst("${const_sql_5155}")
    def const_sql_5156 = """select cast("-.9000000000000000004" as decimalv3(18, 18));"""
    qt_sql_5156_strict "${const_sql_5156}"
    testFoldConst("${const_sql_5156}")
    def const_sql_5157 = """select cast("-.9000000000000000014" as decimalv3(18, 18));"""
    qt_sql_5157_strict "${const_sql_5157}"
    testFoldConst("${const_sql_5157}")
    def const_sql_5158 = """select cast("-.9999999999999999984" as decimalv3(18, 18));"""
    qt_sql_5158_strict "${const_sql_5158}"
    testFoldConst("${const_sql_5158}")
    def const_sql_5159 = """select cast("-.9999999999999999994" as decimalv3(18, 18));"""
    qt_sql_5159_strict "${const_sql_5159}"
    testFoldConst("${const_sql_5159}")
    def const_sql_5160 = """select cast("-.0000000000000000005" as decimalv3(18, 18));"""
    qt_sql_5160_strict "${const_sql_5160}"
    testFoldConst("${const_sql_5160}")
    def const_sql_5161 = """select cast("-.0000000000000000015" as decimalv3(18, 18));"""
    qt_sql_5161_strict "${const_sql_5161}"
    testFoldConst("${const_sql_5161}")
    def const_sql_5162 = """select cast("-.0000000000000000095" as decimalv3(18, 18));"""
    qt_sql_5162_strict "${const_sql_5162}"
    testFoldConst("${const_sql_5162}")
    def const_sql_5163 = """select cast("-.0999999999999999995" as decimalv3(18, 18));"""
    qt_sql_5163_strict "${const_sql_5163}"
    testFoldConst("${const_sql_5163}")
    def const_sql_5164 = """select cast("-.9000000000000000005" as decimalv3(18, 18));"""
    qt_sql_5164_strict "${const_sql_5164}"
    testFoldConst("${const_sql_5164}")
    def const_sql_5165 = """select cast("-.9000000000000000015" as decimalv3(18, 18));"""
    qt_sql_5165_strict "${const_sql_5165}"
    testFoldConst("${const_sql_5165}")
    def const_sql_5166 = """select cast("-.9999999999999999985" as decimalv3(18, 18));"""
    qt_sql_5166_strict "${const_sql_5166}"
    testFoldConst("${const_sql_5166}")
    def const_sql_5167 = """select cast("-.9999999999999999994" as decimalv3(18, 18));"""
    qt_sql_5167_strict "${const_sql_5167}"
    testFoldConst("${const_sql_5167}")
    sql "set enable_strict_cast=false;"
    qt_sql_5134_non_strict "${const_sql_5134}"
    testFoldConst("${const_sql_5134}")
    qt_sql_5135_non_strict "${const_sql_5135}"
    testFoldConst("${const_sql_5135}")
    qt_sql_5136_non_strict "${const_sql_5136}"
    testFoldConst("${const_sql_5136}")
    qt_sql_5137_non_strict "${const_sql_5137}"
    testFoldConst("${const_sql_5137}")
    qt_sql_5138_non_strict "${const_sql_5138}"
    testFoldConst("${const_sql_5138}")
    qt_sql_5139_non_strict "${const_sql_5139}"
    testFoldConst("${const_sql_5139}")
    qt_sql_5140_non_strict "${const_sql_5140}"
    testFoldConst("${const_sql_5140}")
    qt_sql_5141_non_strict "${const_sql_5141}"
    testFoldConst("${const_sql_5141}")
    qt_sql_5142_non_strict "${const_sql_5142}"
    testFoldConst("${const_sql_5142}")
    qt_sql_5143_non_strict "${const_sql_5143}"
    testFoldConst("${const_sql_5143}")
    qt_sql_5144_non_strict "${const_sql_5144}"
    testFoldConst("${const_sql_5144}")
    qt_sql_5145_non_strict "${const_sql_5145}"
    testFoldConst("${const_sql_5145}")
    qt_sql_5146_non_strict "${const_sql_5146}"
    testFoldConst("${const_sql_5146}")
    qt_sql_5147_non_strict "${const_sql_5147}"
    testFoldConst("${const_sql_5147}")
    qt_sql_5148_non_strict "${const_sql_5148}"
    testFoldConst("${const_sql_5148}")
    qt_sql_5149_non_strict "${const_sql_5149}"
    testFoldConst("${const_sql_5149}")
    qt_sql_5150_non_strict "${const_sql_5150}"
    testFoldConst("${const_sql_5150}")
    qt_sql_5151_non_strict "${const_sql_5151}"
    testFoldConst("${const_sql_5151}")
    qt_sql_5152_non_strict "${const_sql_5152}"
    testFoldConst("${const_sql_5152}")
    qt_sql_5153_non_strict "${const_sql_5153}"
    testFoldConst("${const_sql_5153}")
    qt_sql_5154_non_strict "${const_sql_5154}"
    testFoldConst("${const_sql_5154}")
    qt_sql_5155_non_strict "${const_sql_5155}"
    testFoldConst("${const_sql_5155}")
    qt_sql_5156_non_strict "${const_sql_5156}"
    testFoldConst("${const_sql_5156}")
    qt_sql_5157_non_strict "${const_sql_5157}"
    testFoldConst("${const_sql_5157}")
    qt_sql_5158_non_strict "${const_sql_5158}"
    testFoldConst("${const_sql_5158}")
    qt_sql_5159_non_strict "${const_sql_5159}"
    testFoldConst("${const_sql_5159}")
    qt_sql_5160_non_strict "${const_sql_5160}"
    testFoldConst("${const_sql_5160}")
    qt_sql_5161_non_strict "${const_sql_5161}"
    testFoldConst("${const_sql_5161}")
    qt_sql_5162_non_strict "${const_sql_5162}"
    testFoldConst("${const_sql_5162}")
    qt_sql_5163_non_strict "${const_sql_5163}"
    testFoldConst("${const_sql_5163}")
    qt_sql_5164_non_strict "${const_sql_5164}"
    testFoldConst("${const_sql_5164}")
    qt_sql_5165_non_strict "${const_sql_5165}"
    testFoldConst("${const_sql_5165}")
    qt_sql_5166_non_strict "${const_sql_5166}"
    testFoldConst("${const_sql_5166}")
    qt_sql_5167_non_strict "${const_sql_5167}"
    testFoldConst("${const_sql_5167}")
}