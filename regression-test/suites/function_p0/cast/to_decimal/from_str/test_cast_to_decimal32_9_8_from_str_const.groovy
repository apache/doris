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


suite("test_cast_to_decimal32_9_8_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_strict_cast=true;"
    def const_sql_5008 = """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 8));"""
    qt_sql_5008_strict "${const_sql_5008}"
    testFoldConst("${const_sql_5008}")
    def const_sql_5009 = """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(9, 8));"""
    qt_sql_5009_strict "${const_sql_5009}"
    testFoldConst("${const_sql_5009}")
    def const_sql_5010 = """select cast("0" as decimalv3(9, 8));"""
    qt_sql_5010_strict "${const_sql_5010}"
    testFoldConst("${const_sql_5010}")
    def const_sql_5011 = """select cast("1" as decimalv3(9, 8));"""
    qt_sql_5011_strict "${const_sql_5011}"
    testFoldConst("${const_sql_5011}")
    def const_sql_5012 = """select cast("8" as decimalv3(9, 8));"""
    qt_sql_5012_strict "${const_sql_5012}"
    testFoldConst("${const_sql_5012}")
    def const_sql_5013 = """select cast("9" as decimalv3(9, 8));"""
    qt_sql_5013_strict "${const_sql_5013}"
    testFoldConst("${const_sql_5013}")
    def const_sql_5014 = """select cast("0." as decimalv3(9, 8));"""
    qt_sql_5014_strict "${const_sql_5014}"
    testFoldConst("${const_sql_5014}")
    def const_sql_5015 = """select cast("1." as decimalv3(9, 8));"""
    qt_sql_5015_strict "${const_sql_5015}"
    testFoldConst("${const_sql_5015}")
    def const_sql_5016 = """select cast("8." as decimalv3(9, 8));"""
    qt_sql_5016_strict "${const_sql_5016}"
    testFoldConst("${const_sql_5016}")
    def const_sql_5017 = """select cast("9." as decimalv3(9, 8));"""
    qt_sql_5017_strict "${const_sql_5017}"
    testFoldConst("${const_sql_5017}")
    def const_sql_5018 = """select cast("-0" as decimalv3(9, 8));"""
    qt_sql_5018_strict "${const_sql_5018}"
    testFoldConst("${const_sql_5018}")
    def const_sql_5019 = """select cast("-1" as decimalv3(9, 8));"""
    qt_sql_5019_strict "${const_sql_5019}"
    testFoldConst("${const_sql_5019}")
    def const_sql_5020 = """select cast("-8" as decimalv3(9, 8));"""
    qt_sql_5020_strict "${const_sql_5020}"
    testFoldConst("${const_sql_5020}")
    def const_sql_5021 = """select cast("-9" as decimalv3(9, 8));"""
    qt_sql_5021_strict "${const_sql_5021}"
    testFoldConst("${const_sql_5021}")
    def const_sql_5022 = """select cast("-0." as decimalv3(9, 8));"""
    qt_sql_5022_strict "${const_sql_5022}"
    testFoldConst("${const_sql_5022}")
    def const_sql_5023 = """select cast("-1." as decimalv3(9, 8));"""
    qt_sql_5023_strict "${const_sql_5023}"
    testFoldConst("${const_sql_5023}")
    def const_sql_5024 = """select cast("-8." as decimalv3(9, 8));"""
    qt_sql_5024_strict "${const_sql_5024}"
    testFoldConst("${const_sql_5024}")
    def const_sql_5025 = """select cast("-9." as decimalv3(9, 8));"""
    qt_sql_5025_strict "${const_sql_5025}"
    testFoldConst("${const_sql_5025}")
    def const_sql_5026 = """select cast(".000000004" as decimalv3(9, 8));"""
    qt_sql_5026_strict "${const_sql_5026}"
    testFoldConst("${const_sql_5026}")
    def const_sql_5027 = """select cast(".000000014" as decimalv3(9, 8));"""
    qt_sql_5027_strict "${const_sql_5027}"
    testFoldConst("${const_sql_5027}")
    def const_sql_5028 = """select cast(".000000094" as decimalv3(9, 8));"""
    qt_sql_5028_strict "${const_sql_5028}"
    testFoldConst("${const_sql_5028}")
    def const_sql_5029 = """select cast(".099999994" as decimalv3(9, 8));"""
    qt_sql_5029_strict "${const_sql_5029}"
    testFoldConst("${const_sql_5029}")
    def const_sql_5030 = """select cast(".900000004" as decimalv3(9, 8));"""
    qt_sql_5030_strict "${const_sql_5030}"
    testFoldConst("${const_sql_5030}")
    def const_sql_5031 = """select cast(".900000014" as decimalv3(9, 8));"""
    qt_sql_5031_strict "${const_sql_5031}"
    testFoldConst("${const_sql_5031}")
    def const_sql_5032 = """select cast(".999999984" as decimalv3(9, 8));"""
    qt_sql_5032_strict "${const_sql_5032}"
    testFoldConst("${const_sql_5032}")
    def const_sql_5033 = """select cast(".999999994" as decimalv3(9, 8));"""
    qt_sql_5033_strict "${const_sql_5033}"
    testFoldConst("${const_sql_5033}")
    def const_sql_5034 = """select cast(".000000005" as decimalv3(9, 8));"""
    qt_sql_5034_strict "${const_sql_5034}"
    testFoldConst("${const_sql_5034}")
    def const_sql_5035 = """select cast(".000000015" as decimalv3(9, 8));"""
    qt_sql_5035_strict "${const_sql_5035}"
    testFoldConst("${const_sql_5035}")
    def const_sql_5036 = """select cast(".000000095" as decimalv3(9, 8));"""
    qt_sql_5036_strict "${const_sql_5036}"
    testFoldConst("${const_sql_5036}")
    def const_sql_5037 = """select cast(".099999995" as decimalv3(9, 8));"""
    qt_sql_5037_strict "${const_sql_5037}"
    testFoldConst("${const_sql_5037}")
    def const_sql_5038 = """select cast(".900000005" as decimalv3(9, 8));"""
    qt_sql_5038_strict "${const_sql_5038}"
    testFoldConst("${const_sql_5038}")
    def const_sql_5039 = """select cast(".900000015" as decimalv3(9, 8));"""
    qt_sql_5039_strict "${const_sql_5039}"
    testFoldConst("${const_sql_5039}")
    def const_sql_5040 = """select cast(".999999985" as decimalv3(9, 8));"""
    qt_sql_5040_strict "${const_sql_5040}"
    testFoldConst("${const_sql_5040}")
    def const_sql_5041 = """select cast(".999999994" as decimalv3(9, 8));"""
    qt_sql_5041_strict "${const_sql_5041}"
    testFoldConst("${const_sql_5041}")
    def const_sql_5042 = """select cast("-.000000004" as decimalv3(9, 8));"""
    qt_sql_5042_strict "${const_sql_5042}"
    testFoldConst("${const_sql_5042}")
    def const_sql_5043 = """select cast("-.000000014" as decimalv3(9, 8));"""
    qt_sql_5043_strict "${const_sql_5043}"
    testFoldConst("${const_sql_5043}")
    def const_sql_5044 = """select cast("-.000000094" as decimalv3(9, 8));"""
    qt_sql_5044_strict "${const_sql_5044}"
    testFoldConst("${const_sql_5044}")
    def const_sql_5045 = """select cast("-.099999994" as decimalv3(9, 8));"""
    qt_sql_5045_strict "${const_sql_5045}"
    testFoldConst("${const_sql_5045}")
    def const_sql_5046 = """select cast("-.900000004" as decimalv3(9, 8));"""
    qt_sql_5046_strict "${const_sql_5046}"
    testFoldConst("${const_sql_5046}")
    def const_sql_5047 = """select cast("-.900000014" as decimalv3(9, 8));"""
    qt_sql_5047_strict "${const_sql_5047}"
    testFoldConst("${const_sql_5047}")
    def const_sql_5048 = """select cast("-.999999984" as decimalv3(9, 8));"""
    qt_sql_5048_strict "${const_sql_5048}"
    testFoldConst("${const_sql_5048}")
    def const_sql_5049 = """select cast("-.999999994" as decimalv3(9, 8));"""
    qt_sql_5049_strict "${const_sql_5049}"
    testFoldConst("${const_sql_5049}")
    def const_sql_5050 = """select cast("-.000000005" as decimalv3(9, 8));"""
    qt_sql_5050_strict "${const_sql_5050}"
    testFoldConst("${const_sql_5050}")
    def const_sql_5051 = """select cast("-.000000015" as decimalv3(9, 8));"""
    qt_sql_5051_strict "${const_sql_5051}"
    testFoldConst("${const_sql_5051}")
    def const_sql_5052 = """select cast("-.000000095" as decimalv3(9, 8));"""
    qt_sql_5052_strict "${const_sql_5052}"
    testFoldConst("${const_sql_5052}")
    def const_sql_5053 = """select cast("-.099999995" as decimalv3(9, 8));"""
    qt_sql_5053_strict "${const_sql_5053}"
    testFoldConst("${const_sql_5053}")
    def const_sql_5054 = """select cast("-.900000005" as decimalv3(9, 8));"""
    qt_sql_5054_strict "${const_sql_5054}"
    testFoldConst("${const_sql_5054}")
    def const_sql_5055 = """select cast("-.900000015" as decimalv3(9, 8));"""
    qt_sql_5055_strict "${const_sql_5055}"
    testFoldConst("${const_sql_5055}")
    def const_sql_5056 = """select cast("-.999999985" as decimalv3(9, 8));"""
    qt_sql_5056_strict "${const_sql_5056}"
    testFoldConst("${const_sql_5056}")
    def const_sql_5057 = """select cast("-.999999994" as decimalv3(9, 8));"""
    qt_sql_5057_strict "${const_sql_5057}"
    testFoldConst("${const_sql_5057}")
    def const_sql_5058 = """select cast("00000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5058_strict "${const_sql_5058}"
    testFoldConst("${const_sql_5058}")
    def const_sql_5059 = """select cast("00000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5059_strict "${const_sql_5059}"
    testFoldConst("${const_sql_5059}")
    def const_sql_5060 = """select cast("00000000940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5060_strict "${const_sql_5060}"
    testFoldConst("${const_sql_5060}")
    def const_sql_5061 = """select cast("00999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5061_strict "${const_sql_5061}"
    testFoldConst("${const_sql_5061}")
    def const_sql_5062 = """select cast("09000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5062_strict "${const_sql_5062}"
    testFoldConst("${const_sql_5062}")
    def const_sql_5063 = """select cast("09000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5063_strict "${const_sql_5063}"
    testFoldConst("${const_sql_5063}")
    def const_sql_5064 = """select cast("09999999840000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5064_strict "${const_sql_5064}"
    testFoldConst("${const_sql_5064}")
    def const_sql_5065 = """select cast("09999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5065_strict "${const_sql_5065}"
    testFoldConst("${const_sql_5065}")
    def const_sql_5066 = """select cast("10000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5066_strict "${const_sql_5066}"
    testFoldConst("${const_sql_5066}")
    def const_sql_5067 = """select cast("10000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5067_strict "${const_sql_5067}"
    testFoldConst("${const_sql_5067}")
    def const_sql_5068 = """select cast("10000000940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5068_strict "${const_sql_5068}"
    testFoldConst("${const_sql_5068}")
    def const_sql_5069 = """select cast("10999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5069_strict "${const_sql_5069}"
    testFoldConst("${const_sql_5069}")
    def const_sql_5070 = """select cast("19000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5070_strict "${const_sql_5070}"
    testFoldConst("${const_sql_5070}")
    def const_sql_5071 = """select cast("19000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5071_strict "${const_sql_5071}"
    testFoldConst("${const_sql_5071}")
    def const_sql_5072 = """select cast("19999999840000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5072_strict "${const_sql_5072}"
    testFoldConst("${const_sql_5072}")
    def const_sql_5073 = """select cast("19999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5073_strict "${const_sql_5073}"
    testFoldConst("${const_sql_5073}")
    def const_sql_5074 = """select cast("80000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5074_strict "${const_sql_5074}"
    testFoldConst("${const_sql_5074}")
    def const_sql_5075 = """select cast("80000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5075_strict "${const_sql_5075}"
    testFoldConst("${const_sql_5075}")
    def const_sql_5076 = """select cast("80000000940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5076_strict "${const_sql_5076}"
    testFoldConst("${const_sql_5076}")
    def const_sql_5077 = """select cast("80999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5077_strict "${const_sql_5077}"
    testFoldConst("${const_sql_5077}")
    def const_sql_5078 = """select cast("89000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5078_strict "${const_sql_5078}"
    testFoldConst("${const_sql_5078}")
    def const_sql_5079 = """select cast("89000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5079_strict "${const_sql_5079}"
    testFoldConst("${const_sql_5079}")
    def const_sql_5080 = """select cast("89999999840000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5080_strict "${const_sql_5080}"
    testFoldConst("${const_sql_5080}")
    def const_sql_5081 = """select cast("89999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5081_strict "${const_sql_5081}"
    testFoldConst("${const_sql_5081}")
    def const_sql_5082 = """select cast("90000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5082_strict "${const_sql_5082}"
    testFoldConst("${const_sql_5082}")
    def const_sql_5083 = """select cast("90000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5083_strict "${const_sql_5083}"
    testFoldConst("${const_sql_5083}")
    def const_sql_5084 = """select cast("90000000940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5084_strict "${const_sql_5084}"
    testFoldConst("${const_sql_5084}")
    def const_sql_5085 = """select cast("90999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5085_strict "${const_sql_5085}"
    testFoldConst("${const_sql_5085}")
    def const_sql_5086 = """select cast("99000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5086_strict "${const_sql_5086}"
    testFoldConst("${const_sql_5086}")
    def const_sql_5087 = """select cast("99000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5087_strict "${const_sql_5087}"
    testFoldConst("${const_sql_5087}")
    def const_sql_5088 = """select cast("99999999840000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5088_strict "${const_sql_5088}"
    testFoldConst("${const_sql_5088}")
    def const_sql_5089 = """select cast("99999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5089_strict "${const_sql_5089}"
    testFoldConst("${const_sql_5089}")
    def const_sql_5090 = """select cast("00000000050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5090_strict "${const_sql_5090}"
    testFoldConst("${const_sql_5090}")
    def const_sql_5091 = """select cast("00000000150000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5091_strict "${const_sql_5091}"
    testFoldConst("${const_sql_5091}")
    def const_sql_5092 = """select cast("00000000950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5092_strict "${const_sql_5092}"
    testFoldConst("${const_sql_5092}")
    def const_sql_5093 = """select cast("00999999950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5093_strict "${const_sql_5093}"
    testFoldConst("${const_sql_5093}")
    def const_sql_5094 = """select cast("09000000050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5094_strict "${const_sql_5094}"
    testFoldConst("${const_sql_5094}")
    def const_sql_5095 = """select cast("09000000150000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5095_strict "${const_sql_5095}"
    testFoldConst("${const_sql_5095}")
    def const_sql_5096 = """select cast("09999999850000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5096_strict "${const_sql_5096}"
    testFoldConst("${const_sql_5096}")
    def const_sql_5097 = """select cast("09999999950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5097_strict "${const_sql_5097}"
    testFoldConst("${const_sql_5097}")
    def const_sql_5098 = """select cast("10000000050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5098_strict "${const_sql_5098}"
    testFoldConst("${const_sql_5098}")
    def const_sql_5099 = """select cast("10000000150000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5099_strict "${const_sql_5099}"
    testFoldConst("${const_sql_5099}")
    def const_sql_5100 = """select cast("10000000950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5100_strict "${const_sql_5100}"
    testFoldConst("${const_sql_5100}")
    def const_sql_5101 = """select cast("10999999950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5101_strict "${const_sql_5101}"
    testFoldConst("${const_sql_5101}")
    def const_sql_5102 = """select cast("19000000050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5102_strict "${const_sql_5102}"
    testFoldConst("${const_sql_5102}")
    def const_sql_5103 = """select cast("19000000150000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5103_strict "${const_sql_5103}"
    testFoldConst("${const_sql_5103}")
    def const_sql_5104 = """select cast("19999999850000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5104_strict "${const_sql_5104}"
    testFoldConst("${const_sql_5104}")
    def const_sql_5105 = """select cast("19999999950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5105_strict "${const_sql_5105}"
    testFoldConst("${const_sql_5105}")
    def const_sql_5106 = """select cast("80000000050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5106_strict "${const_sql_5106}"
    testFoldConst("${const_sql_5106}")
    def const_sql_5107 = """select cast("80000000150000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5107_strict "${const_sql_5107}"
    testFoldConst("${const_sql_5107}")
    def const_sql_5108 = """select cast("80000000950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5108_strict "${const_sql_5108}"
    testFoldConst("${const_sql_5108}")
    def const_sql_5109 = """select cast("80999999950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5109_strict "${const_sql_5109}"
    testFoldConst("${const_sql_5109}")
    def const_sql_5110 = """select cast("89000000050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5110_strict "${const_sql_5110}"
    testFoldConst("${const_sql_5110}")
    def const_sql_5111 = """select cast("89000000150000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5111_strict "${const_sql_5111}"
    testFoldConst("${const_sql_5111}")
    def const_sql_5112 = """select cast("89999999850000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5112_strict "${const_sql_5112}"
    testFoldConst("${const_sql_5112}")
    def const_sql_5113 = """select cast("89999999950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5113_strict "${const_sql_5113}"
    testFoldConst("${const_sql_5113}")
    def const_sql_5114 = """select cast("90000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5114_strict "${const_sql_5114}"
    testFoldConst("${const_sql_5114}")
    def const_sql_5115 = """select cast("90000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5115_strict "${const_sql_5115}"
    testFoldConst("${const_sql_5115}")
    def const_sql_5116 = """select cast("90000000940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5116_strict "${const_sql_5116}"
    testFoldConst("${const_sql_5116}")
    def const_sql_5117 = """select cast("90999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5117_strict "${const_sql_5117}"
    testFoldConst("${const_sql_5117}")
    def const_sql_5118 = """select cast("99000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5118_strict "${const_sql_5118}"
    testFoldConst("${const_sql_5118}")
    def const_sql_5119 = """select cast("99000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5119_strict "${const_sql_5119}"
    testFoldConst("${const_sql_5119}")
    def const_sql_5120 = """select cast("99999999840000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5120_strict "${const_sql_5120}"
    testFoldConst("${const_sql_5120}")
    def const_sql_5121 = """select cast("99999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5121_strict "${const_sql_5121}"
    testFoldConst("${const_sql_5121}")
    def const_sql_5122 = """select cast("-00000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5122_strict "${const_sql_5122}"
    testFoldConst("${const_sql_5122}")
    def const_sql_5123 = """select cast("-00000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5123_strict "${const_sql_5123}"
    testFoldConst("${const_sql_5123}")
    def const_sql_5124 = """select cast("-00000000940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5124_strict "${const_sql_5124}"
    testFoldConst("${const_sql_5124}")
    def const_sql_5125 = """select cast("-00999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5125_strict "${const_sql_5125}"
    testFoldConst("${const_sql_5125}")
    def const_sql_5126 = """select cast("-09000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5126_strict "${const_sql_5126}"
    testFoldConst("${const_sql_5126}")
    def const_sql_5127 = """select cast("-09000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5127_strict "${const_sql_5127}"
    testFoldConst("${const_sql_5127}")
    def const_sql_5128 = """select cast("-09999999840000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5128_strict "${const_sql_5128}"
    testFoldConst("${const_sql_5128}")
    def const_sql_5129 = """select cast("-09999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5129_strict "${const_sql_5129}"
    testFoldConst("${const_sql_5129}")
    def const_sql_5130 = """select cast("-10000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5130_strict "${const_sql_5130}"
    testFoldConst("${const_sql_5130}")
    def const_sql_5131 = """select cast("-10000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5131_strict "${const_sql_5131}"
    testFoldConst("${const_sql_5131}")
    def const_sql_5132 = """select cast("-10000000940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5132_strict "${const_sql_5132}"
    testFoldConst("${const_sql_5132}")
    def const_sql_5133 = """select cast("-10999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5133_strict "${const_sql_5133}"
    testFoldConst("${const_sql_5133}")
    def const_sql_5134 = """select cast("-19000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5134_strict "${const_sql_5134}"
    testFoldConst("${const_sql_5134}")
    def const_sql_5135 = """select cast("-19000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5135_strict "${const_sql_5135}"
    testFoldConst("${const_sql_5135}")
    def const_sql_5136 = """select cast("-19999999840000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5136_strict "${const_sql_5136}"
    testFoldConst("${const_sql_5136}")
    def const_sql_5137 = """select cast("-19999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5137_strict "${const_sql_5137}"
    testFoldConst("${const_sql_5137}")
    def const_sql_5138 = """select cast("-80000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5138_strict "${const_sql_5138}"
    testFoldConst("${const_sql_5138}")
    def const_sql_5139 = """select cast("-80000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5139_strict "${const_sql_5139}"
    testFoldConst("${const_sql_5139}")
    def const_sql_5140 = """select cast("-80000000940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5140_strict "${const_sql_5140}"
    testFoldConst("${const_sql_5140}")
    def const_sql_5141 = """select cast("-80999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5141_strict "${const_sql_5141}"
    testFoldConst("${const_sql_5141}")
    def const_sql_5142 = """select cast("-89000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5142_strict "${const_sql_5142}"
    testFoldConst("${const_sql_5142}")
    def const_sql_5143 = """select cast("-89000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5143_strict "${const_sql_5143}"
    testFoldConst("${const_sql_5143}")
    def const_sql_5144 = """select cast("-89999999840000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5144_strict "${const_sql_5144}"
    testFoldConst("${const_sql_5144}")
    def const_sql_5145 = """select cast("-89999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5145_strict "${const_sql_5145}"
    testFoldConst("${const_sql_5145}")
    def const_sql_5146 = """select cast("-90000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5146_strict "${const_sql_5146}"
    testFoldConst("${const_sql_5146}")
    def const_sql_5147 = """select cast("-90000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5147_strict "${const_sql_5147}"
    testFoldConst("${const_sql_5147}")
    def const_sql_5148 = """select cast("-90000000940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5148_strict "${const_sql_5148}"
    testFoldConst("${const_sql_5148}")
    def const_sql_5149 = """select cast("-90999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5149_strict "${const_sql_5149}"
    testFoldConst("${const_sql_5149}")
    def const_sql_5150 = """select cast("-99000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5150_strict "${const_sql_5150}"
    testFoldConst("${const_sql_5150}")
    def const_sql_5151 = """select cast("-99000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5151_strict "${const_sql_5151}"
    testFoldConst("${const_sql_5151}")
    def const_sql_5152 = """select cast("-99999999840000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5152_strict "${const_sql_5152}"
    testFoldConst("${const_sql_5152}")
    def const_sql_5153 = """select cast("-99999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5153_strict "${const_sql_5153}"
    testFoldConst("${const_sql_5153}")
    def const_sql_5154 = """select cast("-00000000050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5154_strict "${const_sql_5154}"
    testFoldConst("${const_sql_5154}")
    def const_sql_5155 = """select cast("-00000000150000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5155_strict "${const_sql_5155}"
    testFoldConst("${const_sql_5155}")
    def const_sql_5156 = """select cast("-00000000950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5156_strict "${const_sql_5156}"
    testFoldConst("${const_sql_5156}")
    def const_sql_5157 = """select cast("-00999999950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5157_strict "${const_sql_5157}"
    testFoldConst("${const_sql_5157}")
    def const_sql_5158 = """select cast("-09000000050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5158_strict "${const_sql_5158}"
    testFoldConst("${const_sql_5158}")
    def const_sql_5159 = """select cast("-09000000150000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5159_strict "${const_sql_5159}"
    testFoldConst("${const_sql_5159}")
    def const_sql_5160 = """select cast("-09999999850000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5160_strict "${const_sql_5160}"
    testFoldConst("${const_sql_5160}")
    def const_sql_5161 = """select cast("-09999999950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5161_strict "${const_sql_5161}"
    testFoldConst("${const_sql_5161}")
    def const_sql_5162 = """select cast("-10000000050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5162_strict "${const_sql_5162}"
    testFoldConst("${const_sql_5162}")
    def const_sql_5163 = """select cast("-10000000150000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5163_strict "${const_sql_5163}"
    testFoldConst("${const_sql_5163}")
    def const_sql_5164 = """select cast("-10000000950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5164_strict "${const_sql_5164}"
    testFoldConst("${const_sql_5164}")
    def const_sql_5165 = """select cast("-10999999950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5165_strict "${const_sql_5165}"
    testFoldConst("${const_sql_5165}")
    def const_sql_5166 = """select cast("-19000000050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5166_strict "${const_sql_5166}"
    testFoldConst("${const_sql_5166}")
    def const_sql_5167 = """select cast("-19000000150000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5167_strict "${const_sql_5167}"
    testFoldConst("${const_sql_5167}")
    def const_sql_5168 = """select cast("-19999999850000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5168_strict "${const_sql_5168}"
    testFoldConst("${const_sql_5168}")
    def const_sql_5169 = """select cast("-19999999950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5169_strict "${const_sql_5169}"
    testFoldConst("${const_sql_5169}")
    def const_sql_5170 = """select cast("-80000000050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5170_strict "${const_sql_5170}"
    testFoldConst("${const_sql_5170}")
    def const_sql_5171 = """select cast("-80000000150000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5171_strict "${const_sql_5171}"
    testFoldConst("${const_sql_5171}")
    def const_sql_5172 = """select cast("-80000000950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5172_strict "${const_sql_5172}"
    testFoldConst("${const_sql_5172}")
    def const_sql_5173 = """select cast("-80999999950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5173_strict "${const_sql_5173}"
    testFoldConst("${const_sql_5173}")
    def const_sql_5174 = """select cast("-89000000050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5174_strict "${const_sql_5174}"
    testFoldConst("${const_sql_5174}")
    def const_sql_5175 = """select cast("-89000000150000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5175_strict "${const_sql_5175}"
    testFoldConst("${const_sql_5175}")
    def const_sql_5176 = """select cast("-89999999850000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5176_strict "${const_sql_5176}"
    testFoldConst("${const_sql_5176}")
    def const_sql_5177 = """select cast("-89999999950000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5177_strict "${const_sql_5177}"
    testFoldConst("${const_sql_5177}")
    def const_sql_5178 = """select cast("-90000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5178_strict "${const_sql_5178}"
    testFoldConst("${const_sql_5178}")
    def const_sql_5179 = """select cast("-90000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5179_strict "${const_sql_5179}"
    testFoldConst("${const_sql_5179}")
    def const_sql_5180 = """select cast("-90000000940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5180_strict "${const_sql_5180}"
    testFoldConst("${const_sql_5180}")
    def const_sql_5181 = """select cast("-90999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5181_strict "${const_sql_5181}"
    testFoldConst("${const_sql_5181}")
    def const_sql_5182 = """select cast("-99000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5182_strict "${const_sql_5182}"
    testFoldConst("${const_sql_5182}")
    def const_sql_5183 = """select cast("-99000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5183_strict "${const_sql_5183}"
    testFoldConst("${const_sql_5183}")
    def const_sql_5184 = """select cast("-99999999840000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5184_strict "${const_sql_5184}"
    testFoldConst("${const_sql_5184}")
    def const_sql_5185 = """select cast("-99999999940000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(9, 8));"""
    qt_sql_5185_strict "${const_sql_5185}"
    testFoldConst("${const_sql_5185}")
    sql "set enable_strict_cast=false;"
    qt_sql_5008_non_strict "${const_sql_5008}"
    testFoldConst("${const_sql_5008}")
    qt_sql_5009_non_strict "${const_sql_5009}"
    testFoldConst("${const_sql_5009}")
    qt_sql_5010_non_strict "${const_sql_5010}"
    testFoldConst("${const_sql_5010}")
    qt_sql_5011_non_strict "${const_sql_5011}"
    testFoldConst("${const_sql_5011}")
    qt_sql_5012_non_strict "${const_sql_5012}"
    testFoldConst("${const_sql_5012}")
    qt_sql_5013_non_strict "${const_sql_5013}"
    testFoldConst("${const_sql_5013}")
    qt_sql_5014_non_strict "${const_sql_5014}"
    testFoldConst("${const_sql_5014}")
    qt_sql_5015_non_strict "${const_sql_5015}"
    testFoldConst("${const_sql_5015}")
    qt_sql_5016_non_strict "${const_sql_5016}"
    testFoldConst("${const_sql_5016}")
    qt_sql_5017_non_strict "${const_sql_5017}"
    testFoldConst("${const_sql_5017}")
    qt_sql_5018_non_strict "${const_sql_5018}"
    testFoldConst("${const_sql_5018}")
    qt_sql_5019_non_strict "${const_sql_5019}"
    testFoldConst("${const_sql_5019}")
    qt_sql_5020_non_strict "${const_sql_5020}"
    testFoldConst("${const_sql_5020}")
    qt_sql_5021_non_strict "${const_sql_5021}"
    testFoldConst("${const_sql_5021}")
    qt_sql_5022_non_strict "${const_sql_5022}"
    testFoldConst("${const_sql_5022}")
    qt_sql_5023_non_strict "${const_sql_5023}"
    testFoldConst("${const_sql_5023}")
    qt_sql_5024_non_strict "${const_sql_5024}"
    testFoldConst("${const_sql_5024}")
    qt_sql_5025_non_strict "${const_sql_5025}"
    testFoldConst("${const_sql_5025}")
    qt_sql_5026_non_strict "${const_sql_5026}"
    testFoldConst("${const_sql_5026}")
    qt_sql_5027_non_strict "${const_sql_5027}"
    testFoldConst("${const_sql_5027}")
    qt_sql_5028_non_strict "${const_sql_5028}"
    testFoldConst("${const_sql_5028}")
    qt_sql_5029_non_strict "${const_sql_5029}"
    testFoldConst("${const_sql_5029}")
    qt_sql_5030_non_strict "${const_sql_5030}"
    testFoldConst("${const_sql_5030}")
    qt_sql_5031_non_strict "${const_sql_5031}"
    testFoldConst("${const_sql_5031}")
    qt_sql_5032_non_strict "${const_sql_5032}"
    testFoldConst("${const_sql_5032}")
    qt_sql_5033_non_strict "${const_sql_5033}"
    testFoldConst("${const_sql_5033}")
    qt_sql_5034_non_strict "${const_sql_5034}"
    testFoldConst("${const_sql_5034}")
    qt_sql_5035_non_strict "${const_sql_5035}"
    testFoldConst("${const_sql_5035}")
    qt_sql_5036_non_strict "${const_sql_5036}"
    testFoldConst("${const_sql_5036}")
    qt_sql_5037_non_strict "${const_sql_5037}"
    testFoldConst("${const_sql_5037}")
    qt_sql_5038_non_strict "${const_sql_5038}"
    testFoldConst("${const_sql_5038}")
    qt_sql_5039_non_strict "${const_sql_5039}"
    testFoldConst("${const_sql_5039}")
    qt_sql_5040_non_strict "${const_sql_5040}"
    testFoldConst("${const_sql_5040}")
    qt_sql_5041_non_strict "${const_sql_5041}"
    testFoldConst("${const_sql_5041}")
    qt_sql_5042_non_strict "${const_sql_5042}"
    testFoldConst("${const_sql_5042}")
    qt_sql_5043_non_strict "${const_sql_5043}"
    testFoldConst("${const_sql_5043}")
    qt_sql_5044_non_strict "${const_sql_5044}"
    testFoldConst("${const_sql_5044}")
    qt_sql_5045_non_strict "${const_sql_5045}"
    testFoldConst("${const_sql_5045}")
    qt_sql_5046_non_strict "${const_sql_5046}"
    testFoldConst("${const_sql_5046}")
    qt_sql_5047_non_strict "${const_sql_5047}"
    testFoldConst("${const_sql_5047}")
    qt_sql_5048_non_strict "${const_sql_5048}"
    testFoldConst("${const_sql_5048}")
    qt_sql_5049_non_strict "${const_sql_5049}"
    testFoldConst("${const_sql_5049}")
    qt_sql_5050_non_strict "${const_sql_5050}"
    testFoldConst("${const_sql_5050}")
    qt_sql_5051_non_strict "${const_sql_5051}"
    testFoldConst("${const_sql_5051}")
    qt_sql_5052_non_strict "${const_sql_5052}"
    testFoldConst("${const_sql_5052}")
    qt_sql_5053_non_strict "${const_sql_5053}"
    testFoldConst("${const_sql_5053}")
    qt_sql_5054_non_strict "${const_sql_5054}"
    testFoldConst("${const_sql_5054}")
    qt_sql_5055_non_strict "${const_sql_5055}"
    testFoldConst("${const_sql_5055}")
    qt_sql_5056_non_strict "${const_sql_5056}"
    testFoldConst("${const_sql_5056}")
    qt_sql_5057_non_strict "${const_sql_5057}"
    testFoldConst("${const_sql_5057}")
    qt_sql_5058_non_strict "${const_sql_5058}"
    testFoldConst("${const_sql_5058}")
    qt_sql_5059_non_strict "${const_sql_5059}"
    testFoldConst("${const_sql_5059}")
    qt_sql_5060_non_strict "${const_sql_5060}"
    testFoldConst("${const_sql_5060}")
    qt_sql_5061_non_strict "${const_sql_5061}"
    testFoldConst("${const_sql_5061}")
    qt_sql_5062_non_strict "${const_sql_5062}"
    testFoldConst("${const_sql_5062}")
    qt_sql_5063_non_strict "${const_sql_5063}"
    testFoldConst("${const_sql_5063}")
    qt_sql_5064_non_strict "${const_sql_5064}"
    testFoldConst("${const_sql_5064}")
    qt_sql_5065_non_strict "${const_sql_5065}"
    testFoldConst("${const_sql_5065}")
    qt_sql_5066_non_strict "${const_sql_5066}"
    testFoldConst("${const_sql_5066}")
    qt_sql_5067_non_strict "${const_sql_5067}"
    testFoldConst("${const_sql_5067}")
    qt_sql_5068_non_strict "${const_sql_5068}"
    testFoldConst("${const_sql_5068}")
    qt_sql_5069_non_strict "${const_sql_5069}"
    testFoldConst("${const_sql_5069}")
    qt_sql_5070_non_strict "${const_sql_5070}"
    testFoldConst("${const_sql_5070}")
    qt_sql_5071_non_strict "${const_sql_5071}"
    testFoldConst("${const_sql_5071}")
    qt_sql_5072_non_strict "${const_sql_5072}"
    testFoldConst("${const_sql_5072}")
    qt_sql_5073_non_strict "${const_sql_5073}"
    testFoldConst("${const_sql_5073}")
    qt_sql_5074_non_strict "${const_sql_5074}"
    testFoldConst("${const_sql_5074}")
    qt_sql_5075_non_strict "${const_sql_5075}"
    testFoldConst("${const_sql_5075}")
    qt_sql_5076_non_strict "${const_sql_5076}"
    testFoldConst("${const_sql_5076}")
    qt_sql_5077_non_strict "${const_sql_5077}"
    testFoldConst("${const_sql_5077}")
    qt_sql_5078_non_strict "${const_sql_5078}"
    testFoldConst("${const_sql_5078}")
    qt_sql_5079_non_strict "${const_sql_5079}"
    testFoldConst("${const_sql_5079}")
    qt_sql_5080_non_strict "${const_sql_5080}"
    testFoldConst("${const_sql_5080}")
    qt_sql_5081_non_strict "${const_sql_5081}"
    testFoldConst("${const_sql_5081}")
    qt_sql_5082_non_strict "${const_sql_5082}"
    testFoldConst("${const_sql_5082}")
    qt_sql_5083_non_strict "${const_sql_5083}"
    testFoldConst("${const_sql_5083}")
    qt_sql_5084_non_strict "${const_sql_5084}"
    testFoldConst("${const_sql_5084}")
    qt_sql_5085_non_strict "${const_sql_5085}"
    testFoldConst("${const_sql_5085}")
    qt_sql_5086_non_strict "${const_sql_5086}"
    testFoldConst("${const_sql_5086}")
    qt_sql_5087_non_strict "${const_sql_5087}"
    testFoldConst("${const_sql_5087}")
    qt_sql_5088_non_strict "${const_sql_5088}"
    testFoldConst("${const_sql_5088}")
    qt_sql_5089_non_strict "${const_sql_5089}"
    testFoldConst("${const_sql_5089}")
    qt_sql_5090_non_strict "${const_sql_5090}"
    testFoldConst("${const_sql_5090}")
    qt_sql_5091_non_strict "${const_sql_5091}"
    testFoldConst("${const_sql_5091}")
    qt_sql_5092_non_strict "${const_sql_5092}"
    testFoldConst("${const_sql_5092}")
    qt_sql_5093_non_strict "${const_sql_5093}"
    testFoldConst("${const_sql_5093}")
    qt_sql_5094_non_strict "${const_sql_5094}"
    testFoldConst("${const_sql_5094}")
    qt_sql_5095_non_strict "${const_sql_5095}"
    testFoldConst("${const_sql_5095}")
    qt_sql_5096_non_strict "${const_sql_5096}"
    testFoldConst("${const_sql_5096}")
    qt_sql_5097_non_strict "${const_sql_5097}"
    testFoldConst("${const_sql_5097}")
    qt_sql_5098_non_strict "${const_sql_5098}"
    testFoldConst("${const_sql_5098}")
    qt_sql_5099_non_strict "${const_sql_5099}"
    testFoldConst("${const_sql_5099}")
    qt_sql_5100_non_strict "${const_sql_5100}"
    testFoldConst("${const_sql_5100}")
    qt_sql_5101_non_strict "${const_sql_5101}"
    testFoldConst("${const_sql_5101}")
    qt_sql_5102_non_strict "${const_sql_5102}"
    testFoldConst("${const_sql_5102}")
    qt_sql_5103_non_strict "${const_sql_5103}"
    testFoldConst("${const_sql_5103}")
    qt_sql_5104_non_strict "${const_sql_5104}"
    testFoldConst("${const_sql_5104}")
    qt_sql_5105_non_strict "${const_sql_5105}"
    testFoldConst("${const_sql_5105}")
    qt_sql_5106_non_strict "${const_sql_5106}"
    testFoldConst("${const_sql_5106}")
    qt_sql_5107_non_strict "${const_sql_5107}"
    testFoldConst("${const_sql_5107}")
    qt_sql_5108_non_strict "${const_sql_5108}"
    testFoldConst("${const_sql_5108}")
    qt_sql_5109_non_strict "${const_sql_5109}"
    testFoldConst("${const_sql_5109}")
    qt_sql_5110_non_strict "${const_sql_5110}"
    testFoldConst("${const_sql_5110}")
    qt_sql_5111_non_strict "${const_sql_5111}"
    testFoldConst("${const_sql_5111}")
    qt_sql_5112_non_strict "${const_sql_5112}"
    testFoldConst("${const_sql_5112}")
    qt_sql_5113_non_strict "${const_sql_5113}"
    testFoldConst("${const_sql_5113}")
    qt_sql_5114_non_strict "${const_sql_5114}"
    testFoldConst("${const_sql_5114}")
    qt_sql_5115_non_strict "${const_sql_5115}"
    testFoldConst("${const_sql_5115}")
    qt_sql_5116_non_strict "${const_sql_5116}"
    testFoldConst("${const_sql_5116}")
    qt_sql_5117_non_strict "${const_sql_5117}"
    testFoldConst("${const_sql_5117}")
    qt_sql_5118_non_strict "${const_sql_5118}"
    testFoldConst("${const_sql_5118}")
    qt_sql_5119_non_strict "${const_sql_5119}"
    testFoldConst("${const_sql_5119}")
    qt_sql_5120_non_strict "${const_sql_5120}"
    testFoldConst("${const_sql_5120}")
    qt_sql_5121_non_strict "${const_sql_5121}"
    testFoldConst("${const_sql_5121}")
    qt_sql_5122_non_strict "${const_sql_5122}"
    testFoldConst("${const_sql_5122}")
    qt_sql_5123_non_strict "${const_sql_5123}"
    testFoldConst("${const_sql_5123}")
    qt_sql_5124_non_strict "${const_sql_5124}"
    testFoldConst("${const_sql_5124}")
    qt_sql_5125_non_strict "${const_sql_5125}"
    testFoldConst("${const_sql_5125}")
    qt_sql_5126_non_strict "${const_sql_5126}"
    testFoldConst("${const_sql_5126}")
    qt_sql_5127_non_strict "${const_sql_5127}"
    testFoldConst("${const_sql_5127}")
    qt_sql_5128_non_strict "${const_sql_5128}"
    testFoldConst("${const_sql_5128}")
    qt_sql_5129_non_strict "${const_sql_5129}"
    testFoldConst("${const_sql_5129}")
    qt_sql_5130_non_strict "${const_sql_5130}"
    testFoldConst("${const_sql_5130}")
    qt_sql_5131_non_strict "${const_sql_5131}"
    testFoldConst("${const_sql_5131}")
    qt_sql_5132_non_strict "${const_sql_5132}"
    testFoldConst("${const_sql_5132}")
    qt_sql_5133_non_strict "${const_sql_5133}"
    testFoldConst("${const_sql_5133}")
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
    qt_sql_5168_non_strict "${const_sql_5168}"
    testFoldConst("${const_sql_5168}")
    qt_sql_5169_non_strict "${const_sql_5169}"
    testFoldConst("${const_sql_5169}")
    qt_sql_5170_non_strict "${const_sql_5170}"
    testFoldConst("${const_sql_5170}")
    qt_sql_5171_non_strict "${const_sql_5171}"
    testFoldConst("${const_sql_5171}")
    qt_sql_5172_non_strict "${const_sql_5172}"
    testFoldConst("${const_sql_5172}")
    qt_sql_5173_non_strict "${const_sql_5173}"
    testFoldConst("${const_sql_5173}")
    qt_sql_5174_non_strict "${const_sql_5174}"
    testFoldConst("${const_sql_5174}")
    qt_sql_5175_non_strict "${const_sql_5175}"
    testFoldConst("${const_sql_5175}")
    qt_sql_5176_non_strict "${const_sql_5176}"
    testFoldConst("${const_sql_5176}")
    qt_sql_5177_non_strict "${const_sql_5177}"
    testFoldConst("${const_sql_5177}")
    qt_sql_5178_non_strict "${const_sql_5178}"
    testFoldConst("${const_sql_5178}")
    qt_sql_5179_non_strict "${const_sql_5179}"
    testFoldConst("${const_sql_5179}")
    qt_sql_5180_non_strict "${const_sql_5180}"
    testFoldConst("${const_sql_5180}")
    qt_sql_5181_non_strict "${const_sql_5181}"
    testFoldConst("${const_sql_5181}")
    qt_sql_5182_non_strict "${const_sql_5182}"
    testFoldConst("${const_sql_5182}")
    qt_sql_5183_non_strict "${const_sql_5183}"
    testFoldConst("${const_sql_5183}")
    qt_sql_5184_non_strict "${const_sql_5184}"
    testFoldConst("${const_sql_5184}")
    qt_sql_5185_non_strict "${const_sql_5185}"
    testFoldConst("${const_sql_5185}")
}