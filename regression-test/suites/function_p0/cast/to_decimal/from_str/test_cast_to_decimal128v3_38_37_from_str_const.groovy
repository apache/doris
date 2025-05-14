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


suite("test_cast_to_decimal128v3_38_37_from_str_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_strict_cast=true;"
    def const_sql_4956 = """select cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(38, 37));"""
    qt_sql_4956_strict "${const_sql_4956}"
    testFoldConst("${const_sql_4956}")
    def const_sql_4957 = """select cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e2147483647" as decimalv3(38, 37));"""
    qt_sql_4957_strict "${const_sql_4957}"
    testFoldConst("${const_sql_4957}")
    def const_sql_4958 = """select cast("0" as decimalv3(38, 37));"""
    qt_sql_4958_strict "${const_sql_4958}"
    testFoldConst("${const_sql_4958}")
    def const_sql_4959 = """select cast("1" as decimalv3(38, 37));"""
    qt_sql_4959_strict "${const_sql_4959}"
    testFoldConst("${const_sql_4959}")
    def const_sql_4960 = """select cast("8" as decimalv3(38, 37));"""
    qt_sql_4960_strict "${const_sql_4960}"
    testFoldConst("${const_sql_4960}")
    def const_sql_4961 = """select cast("9" as decimalv3(38, 37));"""
    qt_sql_4961_strict "${const_sql_4961}"
    testFoldConst("${const_sql_4961}")
    def const_sql_4962 = """select cast("0." as decimalv3(38, 37));"""
    qt_sql_4962_strict "${const_sql_4962}"
    testFoldConst("${const_sql_4962}")
    def const_sql_4963 = """select cast("1." as decimalv3(38, 37));"""
    qt_sql_4963_strict "${const_sql_4963}"
    testFoldConst("${const_sql_4963}")
    def const_sql_4964 = """select cast("8." as decimalv3(38, 37));"""
    qt_sql_4964_strict "${const_sql_4964}"
    testFoldConst("${const_sql_4964}")
    def const_sql_4965 = """select cast("9." as decimalv3(38, 37));"""
    qt_sql_4965_strict "${const_sql_4965}"
    testFoldConst("${const_sql_4965}")
    def const_sql_4966 = """select cast("-0" as decimalv3(38, 37));"""
    qt_sql_4966_strict "${const_sql_4966}"
    testFoldConst("${const_sql_4966}")
    def const_sql_4967 = """select cast("-1" as decimalv3(38, 37));"""
    qt_sql_4967_strict "${const_sql_4967}"
    testFoldConst("${const_sql_4967}")
    def const_sql_4968 = """select cast("-8" as decimalv3(38, 37));"""
    qt_sql_4968_strict "${const_sql_4968}"
    testFoldConst("${const_sql_4968}")
    def const_sql_4969 = """select cast("-9" as decimalv3(38, 37));"""
    qt_sql_4969_strict "${const_sql_4969}"
    testFoldConst("${const_sql_4969}")
    def const_sql_4970 = """select cast("-0." as decimalv3(38, 37));"""
    qt_sql_4970_strict "${const_sql_4970}"
    testFoldConst("${const_sql_4970}")
    def const_sql_4971 = """select cast("-1." as decimalv3(38, 37));"""
    qt_sql_4971_strict "${const_sql_4971}"
    testFoldConst("${const_sql_4971}")
    def const_sql_4972 = """select cast("-8." as decimalv3(38, 37));"""
    qt_sql_4972_strict "${const_sql_4972}"
    testFoldConst("${const_sql_4972}")
    def const_sql_4973 = """select cast("-9." as decimalv3(38, 37));"""
    qt_sql_4973_strict "${const_sql_4973}"
    testFoldConst("${const_sql_4973}")
    def const_sql_4974 = """select cast(".00000000000000000000000000000000000004" as decimalv3(38, 37));"""
    qt_sql_4974_strict "${const_sql_4974}"
    testFoldConst("${const_sql_4974}")
    def const_sql_4975 = """select cast(".00000000000000000000000000000000000014" as decimalv3(38, 37));"""
    qt_sql_4975_strict "${const_sql_4975}"
    testFoldConst("${const_sql_4975}")
    def const_sql_4976 = """select cast(".00000000000000000000000000000000000094" as decimalv3(38, 37));"""
    qt_sql_4976_strict "${const_sql_4976}"
    testFoldConst("${const_sql_4976}")
    def const_sql_4977 = """select cast(".09999999999999999999999999999999999994" as decimalv3(38, 37));"""
    qt_sql_4977_strict "${const_sql_4977}"
    testFoldConst("${const_sql_4977}")
    def const_sql_4978 = """select cast(".90000000000000000000000000000000000004" as decimalv3(38, 37));"""
    qt_sql_4978_strict "${const_sql_4978}"
    testFoldConst("${const_sql_4978}")
    def const_sql_4979 = """select cast(".90000000000000000000000000000000000014" as decimalv3(38, 37));"""
    qt_sql_4979_strict "${const_sql_4979}"
    testFoldConst("${const_sql_4979}")
    def const_sql_4980 = """select cast(".99999999999999999999999999999999999984" as decimalv3(38, 37));"""
    qt_sql_4980_strict "${const_sql_4980}"
    testFoldConst("${const_sql_4980}")
    def const_sql_4981 = """select cast(".99999999999999999999999999999999999994" as decimalv3(38, 37));"""
    qt_sql_4981_strict "${const_sql_4981}"
    testFoldConst("${const_sql_4981}")
    def const_sql_4982 = """select cast(".00000000000000000000000000000000000005" as decimalv3(38, 37));"""
    qt_sql_4982_strict "${const_sql_4982}"
    testFoldConst("${const_sql_4982}")
    def const_sql_4983 = """select cast(".00000000000000000000000000000000000015" as decimalv3(38, 37));"""
    qt_sql_4983_strict "${const_sql_4983}"
    testFoldConst("${const_sql_4983}")
    def const_sql_4984 = """select cast(".00000000000000000000000000000000000095" as decimalv3(38, 37));"""
    qt_sql_4984_strict "${const_sql_4984}"
    testFoldConst("${const_sql_4984}")
    def const_sql_4985 = """select cast(".09999999999999999999999999999999999995" as decimalv3(38, 37));"""
    qt_sql_4985_strict "${const_sql_4985}"
    testFoldConst("${const_sql_4985}")
    def const_sql_4986 = """select cast(".90000000000000000000000000000000000005" as decimalv3(38, 37));"""
    qt_sql_4986_strict "${const_sql_4986}"
    testFoldConst("${const_sql_4986}")
    def const_sql_4987 = """select cast(".90000000000000000000000000000000000015" as decimalv3(38, 37));"""
    qt_sql_4987_strict "${const_sql_4987}"
    testFoldConst("${const_sql_4987}")
    def const_sql_4988 = """select cast(".99999999999999999999999999999999999985" as decimalv3(38, 37));"""
    qt_sql_4988_strict "${const_sql_4988}"
    testFoldConst("${const_sql_4988}")
    def const_sql_4989 = """select cast(".99999999999999999999999999999999999994" as decimalv3(38, 37));"""
    qt_sql_4989_strict "${const_sql_4989}"
    testFoldConst("${const_sql_4989}")
    def const_sql_4990 = """select cast("-.00000000000000000000000000000000000004" as decimalv3(38, 37));"""
    qt_sql_4990_strict "${const_sql_4990}"
    testFoldConst("${const_sql_4990}")
    def const_sql_4991 = """select cast("-.00000000000000000000000000000000000014" as decimalv3(38, 37));"""
    qt_sql_4991_strict "${const_sql_4991}"
    testFoldConst("${const_sql_4991}")
    def const_sql_4992 = """select cast("-.00000000000000000000000000000000000094" as decimalv3(38, 37));"""
    qt_sql_4992_strict "${const_sql_4992}"
    testFoldConst("${const_sql_4992}")
    def const_sql_4993 = """select cast("-.09999999999999999999999999999999999994" as decimalv3(38, 37));"""
    qt_sql_4993_strict "${const_sql_4993}"
    testFoldConst("${const_sql_4993}")
    def const_sql_4994 = """select cast("-.90000000000000000000000000000000000004" as decimalv3(38, 37));"""
    qt_sql_4994_strict "${const_sql_4994}"
    testFoldConst("${const_sql_4994}")
    def const_sql_4995 = """select cast("-.90000000000000000000000000000000000014" as decimalv3(38, 37));"""
    qt_sql_4995_strict "${const_sql_4995}"
    testFoldConst("${const_sql_4995}")
    def const_sql_4996 = """select cast("-.99999999999999999999999999999999999984" as decimalv3(38, 37));"""
    qt_sql_4996_strict "${const_sql_4996}"
    testFoldConst("${const_sql_4996}")
    def const_sql_4997 = """select cast("-.99999999999999999999999999999999999994" as decimalv3(38, 37));"""
    qt_sql_4997_strict "${const_sql_4997}"
    testFoldConst("${const_sql_4997}")
    def const_sql_4998 = """select cast("-.00000000000000000000000000000000000005" as decimalv3(38, 37));"""
    qt_sql_4998_strict "${const_sql_4998}"
    testFoldConst("${const_sql_4998}")
    def const_sql_4999 = """select cast("-.00000000000000000000000000000000000015" as decimalv3(38, 37));"""
    qt_sql_4999_strict "${const_sql_4999}"
    testFoldConst("${const_sql_4999}")
    def const_sql_5000 = """select cast("-.00000000000000000000000000000000000095" as decimalv3(38, 37));"""
    qt_sql_5000_strict "${const_sql_5000}"
    testFoldConst("${const_sql_5000}")
    def const_sql_5001 = """select cast("-.09999999999999999999999999999999999995" as decimalv3(38, 37));"""
    qt_sql_5001_strict "${const_sql_5001}"
    testFoldConst("${const_sql_5001}")
    def const_sql_5002 = """select cast("-.90000000000000000000000000000000000005" as decimalv3(38, 37));"""
    qt_sql_5002_strict "${const_sql_5002}"
    testFoldConst("${const_sql_5002}")
    def const_sql_5003 = """select cast("-.90000000000000000000000000000000000015" as decimalv3(38, 37));"""
    qt_sql_5003_strict "${const_sql_5003}"
    testFoldConst("${const_sql_5003}")
    def const_sql_5004 = """select cast("-.99999999999999999999999999999999999985" as decimalv3(38, 37));"""
    qt_sql_5004_strict "${const_sql_5004}"
    testFoldConst("${const_sql_5004}")
    def const_sql_5005 = """select cast("-.99999999999999999999999999999999999994" as decimalv3(38, 37));"""
    qt_sql_5005_strict "${const_sql_5005}"
    testFoldConst("${const_sql_5005}")
    def const_sql_5006 = """select cast("00000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5006_strict "${const_sql_5006}"
    testFoldConst("${const_sql_5006}")
    def const_sql_5007 = """select cast("00000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5007_strict "${const_sql_5007}"
    testFoldConst("${const_sql_5007}")
    def const_sql_5008 = """select cast("00000000000000000000000000000000000009400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5008_strict "${const_sql_5008}"
    testFoldConst("${const_sql_5008}")
    def const_sql_5009 = """select cast("00999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5009_strict "${const_sql_5009}"
    testFoldConst("${const_sql_5009}")
    def const_sql_5010 = """select cast("09000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5010_strict "${const_sql_5010}"
    testFoldConst("${const_sql_5010}")
    def const_sql_5011 = """select cast("09000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5011_strict "${const_sql_5011}"
    testFoldConst("${const_sql_5011}")
    def const_sql_5012 = """select cast("09999999999999999999999999999999999998400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5012_strict "${const_sql_5012}"
    testFoldConst("${const_sql_5012}")
    def const_sql_5013 = """select cast("09999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5013_strict "${const_sql_5013}"
    testFoldConst("${const_sql_5013}")
    def const_sql_5014 = """select cast("10000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5014_strict "${const_sql_5014}"
    testFoldConst("${const_sql_5014}")
    def const_sql_5015 = """select cast("10000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5015_strict "${const_sql_5015}"
    testFoldConst("${const_sql_5015}")
    def const_sql_5016 = """select cast("10000000000000000000000000000000000009400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5016_strict "${const_sql_5016}"
    testFoldConst("${const_sql_5016}")
    def const_sql_5017 = """select cast("10999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5017_strict "${const_sql_5017}"
    testFoldConst("${const_sql_5017}")
    def const_sql_5018 = """select cast("19000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5018_strict "${const_sql_5018}"
    testFoldConst("${const_sql_5018}")
    def const_sql_5019 = """select cast("19000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5019_strict "${const_sql_5019}"
    testFoldConst("${const_sql_5019}")
    def const_sql_5020 = """select cast("19999999999999999999999999999999999998400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5020_strict "${const_sql_5020}"
    testFoldConst("${const_sql_5020}")
    def const_sql_5021 = """select cast("19999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5021_strict "${const_sql_5021}"
    testFoldConst("${const_sql_5021}")
    def const_sql_5022 = """select cast("80000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5022_strict "${const_sql_5022}"
    testFoldConst("${const_sql_5022}")
    def const_sql_5023 = """select cast("80000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5023_strict "${const_sql_5023}"
    testFoldConst("${const_sql_5023}")
    def const_sql_5024 = """select cast("80000000000000000000000000000000000009400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5024_strict "${const_sql_5024}"
    testFoldConst("${const_sql_5024}")
    def const_sql_5025 = """select cast("80999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5025_strict "${const_sql_5025}"
    testFoldConst("${const_sql_5025}")
    def const_sql_5026 = """select cast("89000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5026_strict "${const_sql_5026}"
    testFoldConst("${const_sql_5026}")
    def const_sql_5027 = """select cast("89000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5027_strict "${const_sql_5027}"
    testFoldConst("${const_sql_5027}")
    def const_sql_5028 = """select cast("89999999999999999999999999999999999998400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5028_strict "${const_sql_5028}"
    testFoldConst("${const_sql_5028}")
    def const_sql_5029 = """select cast("89999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5029_strict "${const_sql_5029}"
    testFoldConst("${const_sql_5029}")
    def const_sql_5030 = """select cast("90000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5030_strict "${const_sql_5030}"
    testFoldConst("${const_sql_5030}")
    def const_sql_5031 = """select cast("90000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5031_strict "${const_sql_5031}"
    testFoldConst("${const_sql_5031}")
    def const_sql_5032 = """select cast("90000000000000000000000000000000000009400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5032_strict "${const_sql_5032}"
    testFoldConst("${const_sql_5032}")
    def const_sql_5033 = """select cast("90999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5033_strict "${const_sql_5033}"
    testFoldConst("${const_sql_5033}")
    def const_sql_5034 = """select cast("99000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5034_strict "${const_sql_5034}"
    testFoldConst("${const_sql_5034}")
    def const_sql_5035 = """select cast("99000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5035_strict "${const_sql_5035}"
    testFoldConst("${const_sql_5035}")
    def const_sql_5036 = """select cast("99999999999999999999999999999999999998400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5036_strict "${const_sql_5036}"
    testFoldConst("${const_sql_5036}")
    def const_sql_5037 = """select cast("99999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5037_strict "${const_sql_5037}"
    testFoldConst("${const_sql_5037}")
    def const_sql_5038 = """select cast("00000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5038_strict "${const_sql_5038}"
    testFoldConst("${const_sql_5038}")
    def const_sql_5039 = """select cast("00000000000000000000000000000000000001500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5039_strict "${const_sql_5039}"
    testFoldConst("${const_sql_5039}")
    def const_sql_5040 = """select cast("00000000000000000000000000000000000009500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5040_strict "${const_sql_5040}"
    testFoldConst("${const_sql_5040}")
    def const_sql_5041 = """select cast("00999999999999999999999999999999999999500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5041_strict "${const_sql_5041}"
    testFoldConst("${const_sql_5041}")
    def const_sql_5042 = """select cast("09000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5042_strict "${const_sql_5042}"
    testFoldConst("${const_sql_5042}")
    def const_sql_5043 = """select cast("09000000000000000000000000000000000001500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5043_strict "${const_sql_5043}"
    testFoldConst("${const_sql_5043}")
    def const_sql_5044 = """select cast("09999999999999999999999999999999999998500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5044_strict "${const_sql_5044}"
    testFoldConst("${const_sql_5044}")
    def const_sql_5045 = """select cast("09999999999999999999999999999999999999500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5045_strict "${const_sql_5045}"
    testFoldConst("${const_sql_5045}")
    def const_sql_5046 = """select cast("10000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5046_strict "${const_sql_5046}"
    testFoldConst("${const_sql_5046}")
    def const_sql_5047 = """select cast("10000000000000000000000000000000000001500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5047_strict "${const_sql_5047}"
    testFoldConst("${const_sql_5047}")
    def const_sql_5048 = """select cast("10000000000000000000000000000000000009500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5048_strict "${const_sql_5048}"
    testFoldConst("${const_sql_5048}")
    def const_sql_5049 = """select cast("10999999999999999999999999999999999999500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5049_strict "${const_sql_5049}"
    testFoldConst("${const_sql_5049}")
    def const_sql_5050 = """select cast("19000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5050_strict "${const_sql_5050}"
    testFoldConst("${const_sql_5050}")
    def const_sql_5051 = """select cast("19000000000000000000000000000000000001500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5051_strict "${const_sql_5051}"
    testFoldConst("${const_sql_5051}")
    def const_sql_5052 = """select cast("19999999999999999999999999999999999998500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5052_strict "${const_sql_5052}"
    testFoldConst("${const_sql_5052}")
    def const_sql_5053 = """select cast("19999999999999999999999999999999999999500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5053_strict "${const_sql_5053}"
    testFoldConst("${const_sql_5053}")
    def const_sql_5054 = """select cast("80000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5054_strict "${const_sql_5054}"
    testFoldConst("${const_sql_5054}")
    def const_sql_5055 = """select cast("80000000000000000000000000000000000001500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5055_strict "${const_sql_5055}"
    testFoldConst("${const_sql_5055}")
    def const_sql_5056 = """select cast("80000000000000000000000000000000000009500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5056_strict "${const_sql_5056}"
    testFoldConst("${const_sql_5056}")
    def const_sql_5057 = """select cast("80999999999999999999999999999999999999500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5057_strict "${const_sql_5057}"
    testFoldConst("${const_sql_5057}")
    def const_sql_5058 = """select cast("89000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5058_strict "${const_sql_5058}"
    testFoldConst("${const_sql_5058}")
    def const_sql_5059 = """select cast("89000000000000000000000000000000000001500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5059_strict "${const_sql_5059}"
    testFoldConst("${const_sql_5059}")
    def const_sql_5060 = """select cast("89999999999999999999999999999999999998500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5060_strict "${const_sql_5060}"
    testFoldConst("${const_sql_5060}")
    def const_sql_5061 = """select cast("89999999999999999999999999999999999999500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5061_strict "${const_sql_5061}"
    testFoldConst("${const_sql_5061}")
    def const_sql_5062 = """select cast("90000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5062_strict "${const_sql_5062}"
    testFoldConst("${const_sql_5062}")
    def const_sql_5063 = """select cast("90000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5063_strict "${const_sql_5063}"
    testFoldConst("${const_sql_5063}")
    def const_sql_5064 = """select cast("90000000000000000000000000000000000009400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5064_strict "${const_sql_5064}"
    testFoldConst("${const_sql_5064}")
    def const_sql_5065 = """select cast("90999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5065_strict "${const_sql_5065}"
    testFoldConst("${const_sql_5065}")
    def const_sql_5066 = """select cast("99000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5066_strict "${const_sql_5066}"
    testFoldConst("${const_sql_5066}")
    def const_sql_5067 = """select cast("99000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5067_strict "${const_sql_5067}"
    testFoldConst("${const_sql_5067}")
    def const_sql_5068 = """select cast("99999999999999999999999999999999999998400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5068_strict "${const_sql_5068}"
    testFoldConst("${const_sql_5068}")
    def const_sql_5069 = """select cast("99999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5069_strict "${const_sql_5069}"
    testFoldConst("${const_sql_5069}")
    def const_sql_5070 = """select cast("-00000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5070_strict "${const_sql_5070}"
    testFoldConst("${const_sql_5070}")
    def const_sql_5071 = """select cast("-00000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5071_strict "${const_sql_5071}"
    testFoldConst("${const_sql_5071}")
    def const_sql_5072 = """select cast("-00000000000000000000000000000000000009400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5072_strict "${const_sql_5072}"
    testFoldConst("${const_sql_5072}")
    def const_sql_5073 = """select cast("-00999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5073_strict "${const_sql_5073}"
    testFoldConst("${const_sql_5073}")
    def const_sql_5074 = """select cast("-09000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5074_strict "${const_sql_5074}"
    testFoldConst("${const_sql_5074}")
    def const_sql_5075 = """select cast("-09000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5075_strict "${const_sql_5075}"
    testFoldConst("${const_sql_5075}")
    def const_sql_5076 = """select cast("-09999999999999999999999999999999999998400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5076_strict "${const_sql_5076}"
    testFoldConst("${const_sql_5076}")
    def const_sql_5077 = """select cast("-09999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5077_strict "${const_sql_5077}"
    testFoldConst("${const_sql_5077}")
    def const_sql_5078 = """select cast("-10000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5078_strict "${const_sql_5078}"
    testFoldConst("${const_sql_5078}")
    def const_sql_5079 = """select cast("-10000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5079_strict "${const_sql_5079}"
    testFoldConst("${const_sql_5079}")
    def const_sql_5080 = """select cast("-10000000000000000000000000000000000009400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5080_strict "${const_sql_5080}"
    testFoldConst("${const_sql_5080}")
    def const_sql_5081 = """select cast("-10999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5081_strict "${const_sql_5081}"
    testFoldConst("${const_sql_5081}")
    def const_sql_5082 = """select cast("-19000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5082_strict "${const_sql_5082}"
    testFoldConst("${const_sql_5082}")
    def const_sql_5083 = """select cast("-19000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5083_strict "${const_sql_5083}"
    testFoldConst("${const_sql_5083}")
    def const_sql_5084 = """select cast("-19999999999999999999999999999999999998400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5084_strict "${const_sql_5084}"
    testFoldConst("${const_sql_5084}")
    def const_sql_5085 = """select cast("-19999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5085_strict "${const_sql_5085}"
    testFoldConst("${const_sql_5085}")
    def const_sql_5086 = """select cast("-80000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5086_strict "${const_sql_5086}"
    testFoldConst("${const_sql_5086}")
    def const_sql_5087 = """select cast("-80000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5087_strict "${const_sql_5087}"
    testFoldConst("${const_sql_5087}")
    def const_sql_5088 = """select cast("-80000000000000000000000000000000000009400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5088_strict "${const_sql_5088}"
    testFoldConst("${const_sql_5088}")
    def const_sql_5089 = """select cast("-80999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5089_strict "${const_sql_5089}"
    testFoldConst("${const_sql_5089}")
    def const_sql_5090 = """select cast("-89000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5090_strict "${const_sql_5090}"
    testFoldConst("${const_sql_5090}")
    def const_sql_5091 = """select cast("-89000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5091_strict "${const_sql_5091}"
    testFoldConst("${const_sql_5091}")
    def const_sql_5092 = """select cast("-89999999999999999999999999999999999998400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5092_strict "${const_sql_5092}"
    testFoldConst("${const_sql_5092}")
    def const_sql_5093 = """select cast("-89999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5093_strict "${const_sql_5093}"
    testFoldConst("${const_sql_5093}")
    def const_sql_5094 = """select cast("-90000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5094_strict "${const_sql_5094}"
    testFoldConst("${const_sql_5094}")
    def const_sql_5095 = """select cast("-90000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5095_strict "${const_sql_5095}"
    testFoldConst("${const_sql_5095}")
    def const_sql_5096 = """select cast("-90000000000000000000000000000000000009400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5096_strict "${const_sql_5096}"
    testFoldConst("${const_sql_5096}")
    def const_sql_5097 = """select cast("-90999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5097_strict "${const_sql_5097}"
    testFoldConst("${const_sql_5097}")
    def const_sql_5098 = """select cast("-99000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5098_strict "${const_sql_5098}"
    testFoldConst("${const_sql_5098}")
    def const_sql_5099 = """select cast("-99000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5099_strict "${const_sql_5099}"
    testFoldConst("${const_sql_5099}")
    def const_sql_5100 = """select cast("-99999999999999999999999999999999999998400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5100_strict "${const_sql_5100}"
    testFoldConst("${const_sql_5100}")
    def const_sql_5101 = """select cast("-99999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5101_strict "${const_sql_5101}"
    testFoldConst("${const_sql_5101}")
    def const_sql_5102 = """select cast("-00000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5102_strict "${const_sql_5102}"
    testFoldConst("${const_sql_5102}")
    def const_sql_5103 = """select cast("-00000000000000000000000000000000000001500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5103_strict "${const_sql_5103}"
    testFoldConst("${const_sql_5103}")
    def const_sql_5104 = """select cast("-00000000000000000000000000000000000009500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5104_strict "${const_sql_5104}"
    testFoldConst("${const_sql_5104}")
    def const_sql_5105 = """select cast("-00999999999999999999999999999999999999500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5105_strict "${const_sql_5105}"
    testFoldConst("${const_sql_5105}")
    def const_sql_5106 = """select cast("-09000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5106_strict "${const_sql_5106}"
    testFoldConst("${const_sql_5106}")
    def const_sql_5107 = """select cast("-09000000000000000000000000000000000001500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5107_strict "${const_sql_5107}"
    testFoldConst("${const_sql_5107}")
    def const_sql_5108 = """select cast("-09999999999999999999999999999999999998500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5108_strict "${const_sql_5108}"
    testFoldConst("${const_sql_5108}")
    def const_sql_5109 = """select cast("-09999999999999999999999999999999999999500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5109_strict "${const_sql_5109}"
    testFoldConst("${const_sql_5109}")
    def const_sql_5110 = """select cast("-10000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5110_strict "${const_sql_5110}"
    testFoldConst("${const_sql_5110}")
    def const_sql_5111 = """select cast("-10000000000000000000000000000000000001500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5111_strict "${const_sql_5111}"
    testFoldConst("${const_sql_5111}")
    def const_sql_5112 = """select cast("-10000000000000000000000000000000000009500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5112_strict "${const_sql_5112}"
    testFoldConst("${const_sql_5112}")
    def const_sql_5113 = """select cast("-10999999999999999999999999999999999999500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5113_strict "${const_sql_5113}"
    testFoldConst("${const_sql_5113}")
    def const_sql_5114 = """select cast("-19000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5114_strict "${const_sql_5114}"
    testFoldConst("${const_sql_5114}")
    def const_sql_5115 = """select cast("-19000000000000000000000000000000000001500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5115_strict "${const_sql_5115}"
    testFoldConst("${const_sql_5115}")
    def const_sql_5116 = """select cast("-19999999999999999999999999999999999998500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5116_strict "${const_sql_5116}"
    testFoldConst("${const_sql_5116}")
    def const_sql_5117 = """select cast("-19999999999999999999999999999999999999500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5117_strict "${const_sql_5117}"
    testFoldConst("${const_sql_5117}")
    def const_sql_5118 = """select cast("-80000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5118_strict "${const_sql_5118}"
    testFoldConst("${const_sql_5118}")
    def const_sql_5119 = """select cast("-80000000000000000000000000000000000001500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5119_strict "${const_sql_5119}"
    testFoldConst("${const_sql_5119}")
    def const_sql_5120 = """select cast("-80000000000000000000000000000000000009500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5120_strict "${const_sql_5120}"
    testFoldConst("${const_sql_5120}")
    def const_sql_5121 = """select cast("-80999999999999999999999999999999999999500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5121_strict "${const_sql_5121}"
    testFoldConst("${const_sql_5121}")
    def const_sql_5122 = """select cast("-89000000000000000000000000000000000000500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5122_strict "${const_sql_5122}"
    testFoldConst("${const_sql_5122}")
    def const_sql_5123 = """select cast("-89000000000000000000000000000000000001500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5123_strict "${const_sql_5123}"
    testFoldConst("${const_sql_5123}")
    def const_sql_5124 = """select cast("-89999999999999999999999999999999999998500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5124_strict "${const_sql_5124}"
    testFoldConst("${const_sql_5124}")
    def const_sql_5125 = """select cast("-89999999999999999999999999999999999999500000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5125_strict "${const_sql_5125}"
    testFoldConst("${const_sql_5125}")
    def const_sql_5126 = """select cast("-90000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5126_strict "${const_sql_5126}"
    testFoldConst("${const_sql_5126}")
    def const_sql_5127 = """select cast("-90000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5127_strict "${const_sql_5127}"
    testFoldConst("${const_sql_5127}")
    def const_sql_5128 = """select cast("-90000000000000000000000000000000000009400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5128_strict "${const_sql_5128}"
    testFoldConst("${const_sql_5128}")
    def const_sql_5129 = """select cast("-90999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5129_strict "${const_sql_5129}"
    testFoldConst("${const_sql_5129}")
    def const_sql_5130 = """select cast("-99000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5130_strict "${const_sql_5130}"
    testFoldConst("${const_sql_5130}")
    def const_sql_5131 = """select cast("-99000000000000000000000000000000000001400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5131_strict "${const_sql_5131}"
    testFoldConst("${const_sql_5131}")
    def const_sql_5132 = """select cast("-99999999999999999999999999999999999998400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5132_strict "${const_sql_5132}"
    testFoldConst("${const_sql_5132}")
    def const_sql_5133 = """select cast("-99999999999999999999999999999999999999400000000000000000000000000000000000000000000000000000000000000.e-100" as decimalv3(38, 37));"""
    qt_sql_5133_strict "${const_sql_5133}"
    testFoldConst("${const_sql_5133}")
    sql "set enable_strict_cast=false;"
    qt_sql_4956_non_strict "${const_sql_4956}"
    testFoldConst("${const_sql_4956}")
    qt_sql_4957_non_strict "${const_sql_4957}"
    testFoldConst("${const_sql_4957}")
    qt_sql_4958_non_strict "${const_sql_4958}"
    testFoldConst("${const_sql_4958}")
    qt_sql_4959_non_strict "${const_sql_4959}"
    testFoldConst("${const_sql_4959}")
    qt_sql_4960_non_strict "${const_sql_4960}"
    testFoldConst("${const_sql_4960}")
    qt_sql_4961_non_strict "${const_sql_4961}"
    testFoldConst("${const_sql_4961}")
    qt_sql_4962_non_strict "${const_sql_4962}"
    testFoldConst("${const_sql_4962}")
    qt_sql_4963_non_strict "${const_sql_4963}"
    testFoldConst("${const_sql_4963}")
    qt_sql_4964_non_strict "${const_sql_4964}"
    testFoldConst("${const_sql_4964}")
    qt_sql_4965_non_strict "${const_sql_4965}"
    testFoldConst("${const_sql_4965}")
    qt_sql_4966_non_strict "${const_sql_4966}"
    testFoldConst("${const_sql_4966}")
    qt_sql_4967_non_strict "${const_sql_4967}"
    testFoldConst("${const_sql_4967}")
    qt_sql_4968_non_strict "${const_sql_4968}"
    testFoldConst("${const_sql_4968}")
    qt_sql_4969_non_strict "${const_sql_4969}"
    testFoldConst("${const_sql_4969}")
    qt_sql_4970_non_strict "${const_sql_4970}"
    testFoldConst("${const_sql_4970}")
    qt_sql_4971_non_strict "${const_sql_4971}"
    testFoldConst("${const_sql_4971}")
    qt_sql_4972_non_strict "${const_sql_4972}"
    testFoldConst("${const_sql_4972}")
    qt_sql_4973_non_strict "${const_sql_4973}"
    testFoldConst("${const_sql_4973}")
    qt_sql_4974_non_strict "${const_sql_4974}"
    testFoldConst("${const_sql_4974}")
    qt_sql_4975_non_strict "${const_sql_4975}"
    testFoldConst("${const_sql_4975}")
    qt_sql_4976_non_strict "${const_sql_4976}"
    testFoldConst("${const_sql_4976}")
    qt_sql_4977_non_strict "${const_sql_4977}"
    testFoldConst("${const_sql_4977}")
    qt_sql_4978_non_strict "${const_sql_4978}"
    testFoldConst("${const_sql_4978}")
    qt_sql_4979_non_strict "${const_sql_4979}"
    testFoldConst("${const_sql_4979}")
    qt_sql_4980_non_strict "${const_sql_4980}"
    testFoldConst("${const_sql_4980}")
    qt_sql_4981_non_strict "${const_sql_4981}"
    testFoldConst("${const_sql_4981}")
    qt_sql_4982_non_strict "${const_sql_4982}"
    testFoldConst("${const_sql_4982}")
    qt_sql_4983_non_strict "${const_sql_4983}"
    testFoldConst("${const_sql_4983}")
    qt_sql_4984_non_strict "${const_sql_4984}"
    testFoldConst("${const_sql_4984}")
    qt_sql_4985_non_strict "${const_sql_4985}"
    testFoldConst("${const_sql_4985}")
    qt_sql_4986_non_strict "${const_sql_4986}"
    testFoldConst("${const_sql_4986}")
    qt_sql_4987_non_strict "${const_sql_4987}"
    testFoldConst("${const_sql_4987}")
    qt_sql_4988_non_strict "${const_sql_4988}"
    testFoldConst("${const_sql_4988}")
    qt_sql_4989_non_strict "${const_sql_4989}"
    testFoldConst("${const_sql_4989}")
    qt_sql_4990_non_strict "${const_sql_4990}"
    testFoldConst("${const_sql_4990}")
    qt_sql_4991_non_strict "${const_sql_4991}"
    testFoldConst("${const_sql_4991}")
    qt_sql_4992_non_strict "${const_sql_4992}"
    testFoldConst("${const_sql_4992}")
    qt_sql_4993_non_strict "${const_sql_4993}"
    testFoldConst("${const_sql_4993}")
    qt_sql_4994_non_strict "${const_sql_4994}"
    testFoldConst("${const_sql_4994}")
    qt_sql_4995_non_strict "${const_sql_4995}"
    testFoldConst("${const_sql_4995}")
    qt_sql_4996_non_strict "${const_sql_4996}"
    testFoldConst("${const_sql_4996}")
    qt_sql_4997_non_strict "${const_sql_4997}"
    testFoldConst("${const_sql_4997}")
    qt_sql_4998_non_strict "${const_sql_4998}"
    testFoldConst("${const_sql_4998}")
    qt_sql_4999_non_strict "${const_sql_4999}"
    testFoldConst("${const_sql_4999}")
    qt_sql_5000_non_strict "${const_sql_5000}"
    testFoldConst("${const_sql_5000}")
    qt_sql_5001_non_strict "${const_sql_5001}"
    testFoldConst("${const_sql_5001}")
    qt_sql_5002_non_strict "${const_sql_5002}"
    testFoldConst("${const_sql_5002}")
    qt_sql_5003_non_strict "${const_sql_5003}"
    testFoldConst("${const_sql_5003}")
    qt_sql_5004_non_strict "${const_sql_5004}"
    testFoldConst("${const_sql_5004}")
    qt_sql_5005_non_strict "${const_sql_5005}"
    testFoldConst("${const_sql_5005}")
    qt_sql_5006_non_strict "${const_sql_5006}"
    testFoldConst("${const_sql_5006}")
    qt_sql_5007_non_strict "${const_sql_5007}"
    testFoldConst("${const_sql_5007}")
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
}