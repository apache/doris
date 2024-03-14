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

suite("nereids_scalar_fn_N") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_negative_BigInt "select negative(kbint) from fn_test order by kbint"
	qt_sql_negative_BigInt_notnull "select negative(kbint) from fn_test_not_nullable order by kbint"
	qt_sql_negative_Double "select negative(kdbl) from fn_test order by kdbl"
	qt_sql_negative_Double_notnull "select negative(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_negative_DecimalV2 "select negative(kdcmls1) from fn_test order by kdcmls1"
	qt_sql_negative_DecimalV2_notnull "select negative(kdcmls1) from fn_test_not_nullable order by kdcmls1"
	qt_sql_not_null_or_empty_Varchar "select not_null_or_empty(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_not_null_or_empty_Varchar_notnull "select not_null_or_empty(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_not_null_or_empty_String "select not_null_or_empty(kstr) from fn_test order by kstr"
	qt_sql_not_null_or_empty_String_notnull "select not_null_or_empty(kstr) from fn_test_not_nullable order by kstr"
	sql "select now() from fn_test"
	sql "select now() from fn_test_not_nullable"
	sql "select now(kint) from fn_test where kint is not null order by kint"
	sql "select now(kint) from fn_test_not_nullable where kint is not null order by kint"
	qt_sql_null_or_empty_Varchar "select null_or_empty(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_null_or_empty_Varchar_notnull "select null_or_empty(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_null_or_empty_String "select null_or_empty(kstr) from fn_test order by kstr"
	qt_sql_null_or_empty_String_notnull "select null_or_empty(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_nullif_Boolean_Boolean "select nullif(kbool, kbool) from fn_test order by kbool, kbool"
	qt_sql_nullif_Boolean_Boolean_notnull "select nullif(kbool, kbool) from fn_test_not_nullable order by kbool, kbool"
	qt_sql_nullif_TinyInt_TinyInt "select nullif(ktint, ktint) from fn_test order by ktint, ktint"
	qt_sql_nullif_TinyInt_TinyInt_notnull "select nullif(ktint, ktint) from fn_test_not_nullable order by ktint, ktint"
	qt_sql_nullif_SmallInt_SmallInt "select nullif(ksint, ksint) from fn_test order by ksint, ksint"
	qt_sql_nullif_SmallInt_SmallInt_notnull "select nullif(ksint, ksint) from fn_test_not_nullable order by ksint, ksint"
	qt_sql_nullif_Integer_Integer "select nullif(kint, kint) from fn_test order by kint, kint"
	qt_sql_nullif_Integer_Integer_notnull "select nullif(kint, kint) from fn_test_not_nullable order by kint, kint"
	qt_sql_nullif_BigInt_BigInt "select nullif(kbint, kbint) from fn_test order by kbint, kbint"
	qt_sql_nullif_BigInt_BigInt_notnull "select nullif(kbint, kbint) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_nullif_LargeInt_LargeInt "select nullif(klint, klint) from fn_test order by klint, klint"
	qt_sql_nullif_LargeInt_LargeInt_notnull "select nullif(klint, klint) from fn_test_not_nullable order by klint, klint"
	qt_sql_nullif_Float_Float "select nullif(kfloat, kfloat) from fn_test order by kfloat, kfloat"
	qt_sql_nullif_Float_Float_notnull "select nullif(kfloat, kfloat) from fn_test_not_nullable order by kfloat, kfloat"
	qt_sql_nullif_Double_Double "select nullif(kdbl, kdbl) from fn_test order by kdbl, kdbl"
	qt_sql_nullif_Double_Double_notnull "select nullif(kdbl, kdbl) from fn_test_not_nullable order by kdbl, kdbl"
	qt_sql_nullif_DateTime_DateTime "select nullif(kdtm, kdtm) from fn_test order by kdtm, kdtm"
	qt_sql_nullif_DateTime_DateTime_notnull "select nullif(kdtm, kdtm) from fn_test_not_nullable order by kdtm, kdtm"
	qt_sql_nullif_Date_Date "select nullif(kdt, kdt) from fn_test order by kdt, kdt"
	qt_sql_nullif_Date_Date_notnull "select nullif(kdt, kdt) from fn_test_not_nullable order by kdt, kdt"
	qt_sql_nullif_DateTimeV2_DateTimeV2 "select nullif(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
	qt_sql_nullif_DateTimeV2_DateTimeV2_notnull "select nullif(kdtmv2s1, kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1, kdtmv2s1"
	qt_sql_nullif_DateV2_DateV2 "select nullif(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
	qt_sql_nullif_DateV2_DateV2_notnull "select nullif(kdtv2, kdtv2) from fn_test_not_nullable order by kdtv2, kdtv2"
	qt_sql_nullif_DecimalV2_DecimalV2 "select nullif(kdcmls1, kdcmls1) from fn_test order by kdcmls1, kdcmls1"
	qt_sql_nullif_DecimalV2_DecimalV2_notnull "select nullif(kdcmls1, kdcmls1) from fn_test_not_nullable order by kdcmls1, kdcmls1"
	qt_sql_nullif_Varchar_Varchar "select nullif(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_nullif_Varchar_Varchar_notnull "select nullif(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	qt_sql_nullif_String_String "select nullif(kstr, kstr) from fn_test order by kstr, kstr"
	qt_sql_nullif_String_String_notnull "select nullif(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	qt_sql_nvl_Boolean_Boolean "select nvl(kbool, kbool) from fn_test order by kbool, kbool"
	qt_sql_nvl_Boolean_Boolean_notnull "select nvl(kbool, kbool) from fn_test_not_nullable order by kbool, kbool"
	qt_sql_nvl_TinyInt_TinyInt "select nvl(ktint, ktint) from fn_test order by ktint, ktint"
	qt_sql_nvl_TinyInt_TinyInt_notnull "select nvl(ktint, ktint) from fn_test_not_nullable order by ktint, ktint"
	qt_sql_nvl_SmallInt_SmallInt "select nvl(ksint, ksint) from fn_test order by ksint, ksint"
	qt_sql_nvl_SmallInt_SmallInt_notnull "select nvl(ksint, ksint) from fn_test_not_nullable order by ksint, ksint"
	qt_sql_nvl_Integer_Integer "select nvl(kint, kint) from fn_test order by kint, kint"
	qt_sql_nvl_Integer_Integer_notnull "select nvl(kint, kint) from fn_test_not_nullable order by kint, kint"
	qt_sql_nvl_BigInt_BigInt "select nvl(kbint, kbint) from fn_test order by kbint, kbint"
	qt_sql_nvl_BigInt_BigInt_notnull "select nvl(kbint, kbint) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_nvl_LargeInt_LargeInt "select nvl(klint, klint) from fn_test order by klint, klint"
	qt_sql_nvl_LargeInt_LargeInt_notnull "select nvl(klint, klint) from fn_test_not_nullable order by klint, klint"
	qt_sql_nvl_Float_Float "select nvl(kfloat, kfloat) from fn_test order by kfloat, kfloat"
	qt_sql_nvl_Float_Float_notnull "select nvl(kfloat, kfloat) from fn_test_not_nullable order by kfloat, kfloat"
	qt_sql_nvl_Double_Double "select nvl(kdbl, kdbl) from fn_test order by kdbl, kdbl"
	qt_sql_nvl_Double_Double_notnull "select nvl(kdbl, kdbl) from fn_test_not_nullable order by kdbl, kdbl"
	qt_sql_nvl_Date_Date "select nvl(kdt, kdt) from fn_test order by kdt, kdt"
	qt_sql_nvl_Date_Date_notnull "select nvl(kdt, kdt) from fn_test_not_nullable order by kdt, kdt"
	qt_sql_nvl_DateTime_DateTime "select nvl(kdtm, kdtm) from fn_test order by kdtm, kdtm"
	qt_sql_nvl_DateTime_DateTime_notnull "select nvl(kdtm, kdtm) from fn_test_not_nullable order by kdtm, kdtm"
	qt_sql_nvl_Date_DateTime "select nvl(kdt, kdtm) from fn_test order by kdt, kdtm"
	qt_sql_nvl_Date_DateTime_notnull "select nvl(kdt, kdtm) from fn_test_not_nullable order by kdt, kdtm"
	qt_sql_nvl_DateTime_Date "select nvl(kdtm, kdt) from fn_test order by kdtm, kdt"
	qt_sql_nvl_DateTime_Date_notnull "select nvl(kdtm, kdt) from fn_test_not_nullable order by kdtm, kdt"
	qt_sql_nvl_DateTimeV2_DateTimeV2 "select nvl(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
	qt_sql_nvl_DateTimeV2_DateTimeV2_notnull "select nvl(kdtmv2s1, kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1, kdtmv2s1"
	qt_sql_nvl_DateV2_DateV2 "select nvl(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
	qt_sql_nvl_DateV2_DateV2_notnull "select nvl(kdtv2, kdtv2) from fn_test_not_nullable order by kdtv2, kdtv2"
	qt_sql_nvl_DecimalV2_DecimalV2 "select nvl(kdcmls1, kdcmls1) from fn_test order by kdcmls1, kdcmls1"
	qt_sql_nvl_DecimalV2_DecimalV2_notnull "select nvl(kdcmls1, kdcmls1) from fn_test_not_nullable order by kdcmls1, kdcmls1"
	qt_sql_nvl_Bitmap_Bitmap "select nvl(to_bitmap(kbint), to_bitmap(kbint)) from fn_test order by kbint, kbint"
	qt_sql_nvl_Bitmap_Bitmap_notnull "select nvl(to_bitmap(kbint), to_bitmap(kbint)) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_nvl_Varchar_Varchar "select nvl(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_nvl_Varchar_Varchar_notnull "select nvl(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	qt_sql_nvl_String_String "select nvl(kstr, kstr) from fn_test order by kstr, kstr"
	qt_sql_nvl_String_String_notnull "select nvl(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	test{
		sql"""select ifnull(kstr) from fn_test"""
		check {result, exception, startTime, endTime ->
			assertTrue(exception != null)
			logger.info(exception.message)
		}
	}
	test{
		sql"""select nvl(kstr) from fn_test"""
		check {result, exception, startTime, endTime ->
			assertTrue(exception != null)
			logger.info(exception.message)
		}
	}
}
