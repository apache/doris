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

suite("nereids_scalar_fn_P") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	sql "select parse_url(kvchrs1, 'HOST') from fn_test order by kvchrs1, kvchrs1"
	sql "select parse_url(kvchrs1, 'HOST') from fn_test_not_nullable order by kvchrs1, kvchrs1"
	sql "select parse_url(kstr, 'HOST') from fn_test order by kstr, kstr"
	sql "select parse_url(kstr, 'HOST') from fn_test_not_nullable order by kstr, kstr"
	sql "select parse_url(kvchrs1, 'HOST', 'PROTOCOL') from fn_test order by kvchrs1, kvchrs1, kvchrs1"
	sql "select parse_url(kvchrs1, 'HOST', 'PROTOCOL') from fn_test_not_nullable order by kvchrs1, kvchrs1, kvchrs1"
	sql "select parse_url(kstr, 'HOST', 'PROTOCOL') from fn_test order by kstr, kstr, kstr"
	sql "select parse_url(kstr, 'HOST', 'PROTOCOL') from fn_test_not_nullable order by kstr, kstr, kstr"
	qt_sql_pwd """select password("123")"""
	qt_sql_pmod_BigInt_BigInt "select pmod(kbint, kbint) from fn_test order by kbint, kbint"
	qt_sql_pmod_BigInt_BigInt_notnull "select pmod(kbint, kbint) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_pmod_Double_Double "select pmod(kdbl, kdbl) from fn_test order by kdbl, kdbl"
	qt_sql_pmod_Double_Double_notnull "select pmod(kdbl, kdbl) from fn_test_not_nullable order by kdbl, kdbl"
	qt_sql_position_Varchar_Varchar_In "select position(kvchrs1 in kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_position_Varchar_Varchar_notnull_In "select position(kvchrs1 in kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	qt_sql_position_String_String_In "select position(kstr in kstr) from fn_test order by kstr, kstr"
	qt_sql_position_String_String_notnull_In "select position(kstr in kstr) from fn_test_not_nullable order by kstr, kstr"
	qt_sql_position_Varchar_Varchar "select position(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_position_Varchar_Varchar_notnull "select position(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	qt_sql_position_String_String "select position(kstr, kstr) from fn_test order by kstr, kstr"
	qt_sql_position_String_String_notnull "select position(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	qt_sql_position_Varchar_Varchar_Integer "select position(kvchrs1, kvchrs1, kint) from fn_test order by kvchrs1, kvchrs1, kint"
	qt_sql_position_Varchar_Varchar_Integer_notnull "select position(kvchrs1, kvchrs1, kint) from fn_test_not_nullable order by kvchrs1, kvchrs1, kint"
	qt_sql_position_String_String_Integer "select position(kstr, kstr, kint) from fn_test order by kstr, kstr, kint"
	qt_sql_position_String_String_Integer_notnull "select position(kstr, kstr, kint) from fn_test_not_nullable order by kstr, kstr, kint"
	qt_sql_positive_BigInt "select positive(kbint) from fn_test order by kbint"
	qt_sql_positive_BigInt_notnull "select positive(kbint) from fn_test_not_nullable order by kbint"
	qt_sql_positive_Double "select positive(kdbl) from fn_test order by kdbl"
	qt_sql_positive_Double_notnull "select positive(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_positive_DecimalV2 "select positive(kdcmls1) from fn_test order by kdcmls1"
	qt_sql_positive_DecimalV2_notnull "select positive(kdcmls1) from fn_test_not_nullable order by kdcmls1"
	qt_sql_pow_Double_Double "select pow(kdbl, kdbl) from fn_test order by kdbl, kdbl"
	qt_sql_pow_Double_Double_notnull "select pow(kdbl, kdbl) from fn_test_not_nullable order by kdbl, kdbl"
	qt_sql_power_Double_Double "select power(kdbl, kdbl) from fn_test order by kdbl, kdbl"
	qt_sql_power_Double_Double_notnull "select power(kdbl, kdbl) from fn_test_not_nullable order by kdbl, kdbl"
	qt_sql_protocol_String "select protocol(kstr) from fn_test order by kstr"
	qt_sql_protocol_String_notnull "select protocol(kstr) from fn_test_not_nullable order by kstr"
}