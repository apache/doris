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

suite("nereids_scalar_fn_C") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_cbrt_Double "select cbrt(kdbl) from fn_test order by kdbl"
	qt_sql_cbrt_Double_notnull "select cbrt(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_ceil_Double "select ceil(kdbl) from fn_test order by kdbl"
	qt_sql_ceil_Double_notnull "select ceil(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_ceil_DecimalV3S1 "select ceil(kdcmlv3s1) from fn_test order by kdcmlv3s1"
	qt_sql_ceil_DecimalV3S1_notnull "select ceil(kdcmlv3s1) from fn_test_not_nullable order by kdcmlv3s1"
	qt_sql_ceil_DecimalV3S2 "select ceil(kdcmlv3s2) from fn_test order by kdcmlv3s2"
	qt_sql_ceil_DecimalV3S2_notnull "select ceil(kdcmlv3s2) from fn_test_not_nullable order by kdcmlv3s2"
	qt_sql_ceil_DecimalV3S3 "select ceil(kdcmlv3s3) from fn_test order by kdcmlv3s3"
	qt_sql_ceil_DecimalV3S3_notnull "select ceil(kdcmlv3s3) from fn_test_not_nullable order by kdcmlv3s3"
	qt_sql_ceil_DecimalV3S1_Int "select ceil(kdcmlv3s1, 1) from fn_test order by kdcmlv3s1"
	qt_sql_ceil_DecimalV3S1_Int_notnull "select ceil(kdcmlv3s1, 1) from fn_test_not_nullable order by kdcmlv3s1"
	qt_sql_ceil_DecimalV3S2_Int "select ceil(kdcmlv3s2, 1) from fn_test order by kdcmlv3s2"
	qt_sql_ceil_DecimalV3S2_Int_notnull "select ceil(kdcmlv3s2, 1) from fn_test_not_nullable order by kdcmlv3s2"
	qt_sql_ceil_DecimalV3S3_Int "select ceil(kdcmlv3s3, 1) from fn_test order by kdcmlv3s3"
	qt_sql_ceil_DecimalV3S3_Int_notnull "select ceil(kdcmlv3s3, 1) from fn_test_not_nullable order by kdcmlv3s3"
	qt_sql_character_length_Varchar "select character_length(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_character_length_Varchar_notnull "select character_length(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_character_length_String "select character_length(kstr) from fn_test order by kstr"
	qt_sql_character_length_String_notnull "select character_length(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_coalesce_Boolean "select coalesce(kbool) from fn_test order by kbool"
	qt_sql_coalesce_Boolean_notnull "select coalesce(kbool) from fn_test_not_nullable order by kbool"
	qt_sql_coalesce_TinyInt "select coalesce(ktint) from fn_test order by ktint"
	qt_sql_coalesce_TinyInt_notnull "select coalesce(ktint) from fn_test_not_nullable order by ktint"
	qt_sql_coalesce_SmallInt "select coalesce(ksint) from fn_test order by ksint"
	qt_sql_coalesce_SmallInt_notnull "select coalesce(ksint) from fn_test_not_nullable order by ksint"
	qt_sql_coalesce_Integer "select coalesce(kint) from fn_test order by kint"
	qt_sql_coalesce_Integer_notnull "select coalesce(kint) from fn_test_not_nullable order by kint"
	qt_sql_coalesce_BigInt "select coalesce(kbint) from fn_test order by kbint"
	qt_sql_coalesce_BigInt_notnull "select coalesce(kbint) from fn_test_not_nullable order by kbint"
	qt_sql_coalesce_LargeInt "select coalesce(klint) from fn_test order by klint"
	qt_sql_coalesce_LargeInt_notnull "select coalesce(klint) from fn_test_not_nullable order by klint"
	qt_sql_coalesce_Float "select coalesce(kfloat) from fn_test order by kfloat"
	qt_sql_coalesce_Float_notnull "select coalesce(kfloat) from fn_test_not_nullable order by kfloat"
	qt_sql_coalesce_Double "select coalesce(kdbl) from fn_test order by kdbl"
	qt_sql_coalesce_Double_notnull "select coalesce(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_coalesce_DateTime "select coalesce(kdtm) from fn_test order by kdtm"
	qt_sql_coalesce_DateTime_notnull "select coalesce(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_coalesce_Date "select coalesce(kdt) from fn_test order by kdt"
	qt_sql_coalesce_Date_notnull "select coalesce(kdt) from fn_test_not_nullable order by kdt"
	qt_sql_coalesce_DateTimeV2 "select coalesce(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_coalesce_DateTimeV2_notnull "select coalesce(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_coalesce_DateV2 "select coalesce(kdtv2) from fn_test order by kdtv2"
	qt_sql_coalesce_DateV2_notnull "select coalesce(kdtv2) from fn_test_not_nullable order by kdtv2"
	qt_sql_coalesce_DecimalV2 "select coalesce(kdcmls1) from fn_test order by kdcmls1"
	qt_sql_coalesce_DecimalV2_notnull "select coalesce(kdcmls1) from fn_test_not_nullable order by kdcmls1"
	qt_sql_coalesce_Bitmap "select coalesce(to_bitmap(kbint)) from fn_test order by kbint"
	qt_sql_coalesce_Bitmap_notnull "select coalesce(to_bitmap(kbint)) from fn_test_not_nullable order by kbint"
	qt_sql_coalesce_Varchar "select coalesce(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_coalesce_Varchar_notnull "select coalesce(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_coalesce_String "select coalesce(kstr) from fn_test order by kstr"
	qt_sql_coalesce_String_notnull "select coalesce(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_concat_Varchar "select concat(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_concat_Varchar_notnull "select concat(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_concat_String "select concat(kstr) from fn_test order by kstr"
	qt_sql_concat_String_notnull "select concat(kstr) from fn_test_not_nullable order by kstr"
	sql "select connection_id() from fn_test"
	sql "select connection_id() from fn_test_not_nullable"
	qt_sql_conv_BigInt_TinyInt_TinyInt "select conv(kbint, ktint, ktint) from fn_test order by kbint, ktint, ktint"
	qt_sql_conv_BigInt_TinyInt_TinyInt_notnull "select conv(kbint, ktint, ktint) from fn_test_not_nullable order by kbint, ktint, ktint"
	qt_sql_conv_Varchar_TinyInt_TinyInt "select conv(kvchrs1, ktint, ktint) from fn_test order by kvchrs1, ktint, ktint"
	qt_sql_conv_Varchar_TinyInt_TinyInt_notnull "select conv(kvchrs1, ktint, ktint) from fn_test_not_nullable order by kvchrs1, ktint, ktint"
	qt_sql_conv_String_TinyInt_TinyInt "select conv(kstr, ktint, ktint) from fn_test order by kstr, ktint, ktint"
	qt_sql_conv_String_TinyInt_TinyInt_notnull "select conv(kstr, ktint, ktint) from fn_test_not_nullable order by kstr, ktint, ktint"
	qt_sql_convert_to_Varchar_Varchar "select convert_to(kvchrs1, 'gbk') from fn_test order by kvchrs1"
	qt_sql_convert_to_Varchar_Varchar_notnull "select convert_to(kvchrs1, 'gbk') from fn_test_not_nullable order by kvchrs1"
	qt_sql_convert_tz_DateTime_Varchar_Varchar "select convert_tz(kdtm, 'Asia/Shanghai', 'Europe/Sofia') from fn_test order by kdtm"
	qt_sql_convert_tz_DateTime_Varchar_Varchar_notnull "select convert_tz(kdtm, 'Asia/Shanghai', 'Europe/Sofia') from fn_test_not_nullable order by kdtm"
	qt_sql_convert_tz_DateTimeV2_Varchar_Varchar "select convert_tz(kdtmv2s1, 'Asia/Shanghai', 'Europe/Sofia') from fn_test order by kdtmv2s1"
	qt_sql_convert_tz_DateTimeV2_Varchar_Varchar_notnull "select convert_tz(kdtmv2s1, 'Asia/Shanghai', 'Europe/Sofia') from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_cos_Double "select cos(kdbl) from fn_test order by kdbl"
	qt_sql_cos_Double_notnull "select cos(kdbl) from fn_test_not_nullable order by kdbl"
	sql "select current_user() from fn_test"
	sql "select current_user() from fn_test_not_nullable"

	qt_char "select char(68, 111, 114, 105, 115), char(68, 111, 114, 105, 115 using utf8);"
	qt_convert "select convert(1 using gbk), convert(1, string);"

}
