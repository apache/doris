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

suite("nereids_scalar_fn_L") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_last_day_DateTime "select last_day(kdtm) from fn_test order by kdtm"
	qt_sql_last_day_DateTime_notnull "select last_day(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_last_day_Date "select last_day(kdt) from fn_test order by kdt"
	qt_sql_last_day_Date_notnull "select last_day(kdt) from fn_test_not_nullable order by kdt"
	qt_sql_last_day_DateTimeV2 "select last_day(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_last_day_DateTimeV2_notnull "select last_day(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_last_day_DateV2 "select last_day(kdtv2) from fn_test order by kdtv2"
	qt_sql_last_day_DateV2_notnull "select last_day(kdtv2) from fn_test_not_nullable order by kdtv2"
	qt_sql_least_TinyInt "select least(ktint) from fn_test order by ktint"
	qt_sql_least_TinyInt_notnull "select least(ktint) from fn_test_not_nullable order by ktint"
	qt_sql_least_SmallInt "select least(ksint) from fn_test order by ksint"
	qt_sql_least_SmallInt_notnull "select least(ksint) from fn_test_not_nullable order by ksint"
	qt_sql_least_Integer "select least(kint) from fn_test order by kint"
	qt_sql_least_Integer_notnull "select least(kint) from fn_test_not_nullable order by kint"
	qt_sql_least_BigInt "select least(kbint) from fn_test order by kbint"
	qt_sql_least_BigInt_notnull "select least(kbint) from fn_test_not_nullable order by kbint"
	qt_sql_least_LargeInt "select least(klint) from fn_test order by klint"
	qt_sql_least_LargeInt_notnull "select least(klint) from fn_test_not_nullable order by klint"
	qt_sql_least_Float "select least(kfloat) from fn_test order by kfloat"
	qt_sql_least_Float_notnull "select least(kfloat) from fn_test_not_nullable order by kfloat"
	qt_sql_least_Double "select least(kdbl) from fn_test order by kdbl"
	qt_sql_least_Double_notnull "select least(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_least_Date "select least(kdt) from fn_test order by kdt"
	qt_sql_least_Date_notnull "select least(kdt) from fn_test_not_nullable order by kdt"
	qt_sql_least_DateV2 "select least(kdtv2) from fn_test order by kdtv2"
	qt_sql_least_DateV2_notnull "select least(kdtv2) from fn_test_not_nullable order by kdtv2"
	qt_sql_least_DateTime "select least(kdtm) from fn_test order by kdtm"
	qt_sql_least_DateTime_notnull "select least(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_least_DateTimeV2 "select least(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_least_DateTimeV2_notnull "select least(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_least_DecimalV2 "select least(kdcmls1) from fn_test order by kdcmls1"
	qt_sql_least_DecimalV2_notnull "select least(kdcmls1) from fn_test_not_nullable order by kdcmls1"
	qt_sql_least_Varchar "select least(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_least_Varchar_notnull "select least(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_least_String "select least(kstr) from fn_test order by kstr"
	qt_sql_least_String_notnull "select least(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_left_Varchar_Integer "select left(kvchrs1, kint) from fn_test order by kvchrs1, kint"
	qt_sql_left_Varchar_Integer_notnull "select left(kvchrs1, kint) from fn_test_not_nullable order by kvchrs1, kint"
	qt_sql_left_String_Integer "select left(kstr, kint) from fn_test order by kstr, kint"
	qt_sql_left_String_Integer_notnull "select left(kstr, kint) from fn_test_not_nullable order by kstr, kint"
	qt_sql_length_Varchar "select length(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_length_Varchar_notnull "select length(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_length_String "select length(kstr) from fn_test order by kstr"
	qt_sql_length_String_notnull "select length(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_like_Varchar_Varchar "select like(kvchrs1, kvchrs2) from fn_test order by kvchrs1"
	qt_sql_like_Varchar_Varchar_not_null "select like(kvchrs1, kvchrs2) from fn_test_not_nullable order by kvchrs1"
	qt_sql_ln_Double "select ln(kdbl) from fn_test order by kdbl"
	qt_sql_ln_Double_notnull "select ln(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_locate_Varchar_Varchar "select locate(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_locate_Varchar_Varchar_notnull "select locate(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	qt_sql_locate_String_String "select locate(kstr, kstr) from fn_test order by kstr, kstr"
	qt_sql_locate_String_String_notnull "select locate(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	qt_sql_locate_Varchar_Varchar_Integer "select locate(kvchrs1, kvchrs1, kint) from fn_test order by kvchrs1, kvchrs1, kint"
	qt_sql_locate_Varchar_Varchar_Integer_notnull "select locate(kvchrs1, kvchrs1, kint) from fn_test_not_nullable order by kvchrs1, kvchrs1, kint"
	qt_sql_locate_String_String_Integer "select locate(kstr, kstr, kint) from fn_test order by kstr, kstr, kint"
	qt_sql_locate_String_String_Integer_notnull "select locate(kstr, kstr, kint) from fn_test_not_nullable order by kstr, kstr, kint"
	qt_sql_log_Double_Double "select log(kdbl, kdbl) from fn_test order by kdbl, kdbl"
	qt_sql_log_Double_Double_notnull "select log(kdbl, kdbl) from fn_test_not_nullable order by kdbl, kdbl"
	qt_sql_log10_Double "select log10(kdbl) from fn_test order by kdbl"
	qt_sql_log10_Double_notnull "select log10(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_log2_Double "select log2(kdbl) from fn_test order by kdbl"
	qt_sql_log2_Double_notnull "select log2(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_lower_Varchar "select lower(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_lower_Varchar_notnull "select lower(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_lower_String "select lower(kstr) from fn_test order by kstr"
	qt_sql_lower_String_notnull "select lower(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_lpad_Varchar_Integer_Varchar "select lpad(kvchrs1, kint, kvchrs1) from fn_test order by kvchrs1, kint, kvchrs1"
	qt_sql_lpad_Varchar_Integer_Varchar_notnull "select lpad(kvchrs1, kint, kvchrs1) from fn_test_not_nullable order by kvchrs1, kint, kvchrs1"
	qt_sql_lpad_String_Integer_String "select lpad(kstr, kint, kstr) from fn_test order by kstr, kint, kstr"
	qt_sql_lpad_String_Integer_String_notnull "select lpad(kstr, kint, kstr) from fn_test_not_nullable order by kstr, kint, kstr"
	qt_sql_ltrim_Varchar "select ltrim(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_ltrim_Varchar_notnull "select ltrim(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_ltrim_String "select ltrim(kstr) from fn_test order by kstr"
	qt_sql_ltrim_String_notnull "select ltrim(kstr) from fn_test_not_nullable order by kstr"
}