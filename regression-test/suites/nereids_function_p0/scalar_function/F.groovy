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

suite("nereids_scalar_fn_F") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_field_TinyInt "select field(ktint, 1, 2) from fn_test order by ktint"
	qt_sql_field_TinyInt_notnull "select field(ktint, 1, 2) from fn_test_not_nullable order by ktint"
	qt_sql_field_SmallInt "select field(ksint, 1, 2) from fn_test order by ksint"
	qt_sql_field_SmallInt_notnull "select field(ksint, 1, 2) from fn_test_not_nullable order by ksint"
	qt_sql_field_Integer "select field(kint, 1, 2) from fn_test order by kint"
	qt_sql_field_Integer_notnull "select field(kint, 1, 2) from fn_test_not_nullable order by kint"
	qt_sql_field_BigInt "select field(kbint, 1, 2) from fn_test order by kbint"
	qt_sql_field_BigInt_notnull "select field(kbint, 1, 2) from fn_test_not_nullable order by kbint"
	qt_sql_field_LargeInt "select field(klint, 1, 2) from fn_test order by klint"
	qt_sql_field_LargeInt_notnull "select field(klint, 1, 2) from fn_test_not_nullable order by klint"
	qt_sql_field_Float "select field(kfloat, 1, 2) from fn_test order by kfloat"
	qt_sql_field_Float_notnull "select field(kfloat, 1, 2) from fn_test_not_nullable order by kfloat"
	qt_sql_field_Double "select field(kdbl, 1, 2) from fn_test order by kdbl"
	qt_sql_field_Double_notnull "select field(kdbl, 1, 2) from fn_test_not_nullable order by kdbl"
	qt_sql_field_DecimalV2 "select field(kdcmls1, 1, 2) from fn_test order by kdcmls1"
	qt_sql_field_DecimalV2_notnull "select field(kdcmls1, 1, 2) from fn_test_not_nullable order by kdcmls1"
	qt_sql_field_DateV2 "select field(kdtv2, 1, 2) from fn_test order by kdtv2"
	qt_sql_field_DateV2_notnull "select field(kdtv2, 1, 2) from fn_test_not_nullable order by kdtv2"
	qt_sql_field_DateTimeV2 "select field(kdtmv2s1, 1, 2) from fn_test order by kdtmv2s1"
	qt_sql_field_DateTimeV2_notnull "select field(kdtmv2s1, 1, 2) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_field_Varchar "select field(kvchrs1, 1, 2) from fn_test order by kvchrs1"
	qt_sql_field_Varchar_notnull "select field(kvchrs1, 1, 2) from fn_test_not_nullable order by kvchrs1"
	qt_sql_field_String "select field(kstr, 1, 2) from fn_test order by kstr"
	qt_sql_field_String_notnull "select field(kstr, 1, 2) from fn_test_not_nullable order by kstr"
	qt_sql_find_in_set_Varchar_Varchar "select find_in_set(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_find_in_set_Varchar_Varchar_notnull "select find_in_set(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	qt_sql_find_in_set_String_String "select find_in_set(kstr, kstr) from fn_test order by kstr, kstr"
	qt_sql_find_in_set_String_String_notnull "select find_in_set(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	qt_sql_floor_Double "select floor(kdbl) from fn_test order by kdbl"
	qt_sql_floor_Double_notnull "select floor(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_floor_DecimalV3S1 "select floor(kdcmlv3s1) from fn_test order by kdcmlv3s1"
	qt_sql_floor_DecimalV3S1_notnull "select floor(kdcmlv3s1) from fn_test_not_nullable order by kdcmlv3s1"
	qt_sql_floor_DecimalV3S2 "select floor(kdcmlv3s2) from fn_test order by kdcmlv3s2"
	qt_sql_floor_DecimalV3S2_notnull "select floor(kdcmlv3s2) from fn_test_not_nullable order by kdcmlv3s2"
	qt_sql_floor_DecimalV3S3 "select floor(kdcmlv3s3) from fn_test order by kdcmlv3s3"
	qt_sql_floor_DecimalV3S3_notnull "select floor(kdcmlv3s3) from fn_test_not_nullable order by kdcmlv3s3"
	qt_sql_floor_DecimalV3S1_Int "select floor(kdcmlv3s1, 1) from fn_test order by kdcmlv3s1"
	qt_sql_floor_DecimalV3S1_Int_notnull "select floor(kdcmlv3s1, 1) from fn_test_not_nullable order by kdcmlv3s1"
	qt_sql_floor_DecimalV3S2_Int "select floor(kdcmlv3s2, 1) from fn_test order by kdcmlv3s2"
	qt_sql_floor_DecimalV3S2_Int_notnull "select floor(kdcmlv3s2, 1) from fn_test_not_nullable order by kdcmlv3s2"
	qt_sql_floor_DecimalV3S3_Int "select floor(kdcmlv3s3, 1) from fn_test order by kdcmlv3s3"
	qt_sql_floor_DecimalV3S3_Int_notnull "select floor(kdcmlv3s3, 1) from fn_test_not_nullable order by kdcmlv3s3"
	qt_sql_fmod_Float_Float "select fmod(kfloat, kfloat) from fn_test order by kfloat, kfloat"
	qt_sql_fmod_Float_Float_notnull "select fmod(kfloat, kfloat) from fn_test_not_nullable order by kfloat, kfloat"
	qt_sql_fmod_Double_Double "select fmod(kdbl, kdbl) from fn_test order by kdbl, kdbl"
	qt_sql_fmod_Double_Double_notnull "select fmod(kdbl, kdbl) from fn_test_not_nullable order by kdbl, kdbl"
	qt_sql_fpow_Double_Double "select fpow(kdbl, kdbl) from fn_test order by kdbl, kdbl"
	qt_sql_fpow_Double_Double_notnull "select fpow(kdbl, kdbl) from fn_test_not_nullable order by kdbl, kdbl"
	sql "select from_base64(kvchrs1) from fn_test order by kvchrs1"
	sql "select from_base64(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	sql "select from_base64(kstr) from fn_test order by kstr"
	sql "select from_base64(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_from_days_Integer "select from_days(kint) from fn_test order by kint"
	qt_sql_from_days_Integer_notnull "select from_days(kint) from fn_test_not_nullable order by kint"
	qt_sql_from_unixtime_Integer "select from_unixtime(kint) from fn_test order by kint"
	qt_sql_from_unixtime_Integer_notnull "select from_unixtime(kint) from fn_test_not_nullable order by kint"
	qt_sql_from_unixtime_Integer_Varchar "select from_unixtime(kint, 'varchar') from fn_test order by kint"
	qt_sql_from_unixtime_Integer_Varchar_notnull "select from_unixtime(kint, 'varchar') from fn_test_not_nullable order by kint"
	qt_sql_from_unixtime_Integer_String "select from_unixtime(kint, 'string') from fn_test order by kint"
	qt_sql_from_unixtime_Integer_String_notnull "select from_unixtime(kint, 'string') from fn_test_not_nullable order by kint"
}
