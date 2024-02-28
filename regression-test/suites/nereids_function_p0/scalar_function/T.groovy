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

suite("nereids_scalar_fn_T") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_tan_Double "select tan(kdbl) from fn_test order by kdbl"
	qt_sql_tan_Double_notnull "select tan(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_tanh_Double "select tanh(kdbl) from fn_test order by kdbl"
	qt_sql_tanh_Double_notnull "select tanh(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_timediff_DateTime_DateTime "select timediff(kdtm, kdtm) from fn_test order by kdtm, kdtm"
	qt_sql_timediff_DateTime_DateTime_notnull "select timediff(kdtm, kdtm) from fn_test_not_nullable order by kdtm, kdtm"
	qt_sql_timediff_DateTimeV2_DateTimeV2 "select timediff(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
	qt_sql_timediff_DateTimeV2_DateTimeV2_notnull "select timediff(kdtmv2s1, kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1, kdtmv2s1"
	qt_sql_timediff_DateTimeV2_DateV2 "select timediff(kdtmv2s1, kdtv2) from fn_test order by kdtmv2s1, kdtv2"
	qt_sql_timediff_DateTimeV2_DateV2_notnull "select timediff(kdtmv2s1, kdtv2) from fn_test_not_nullable order by kdtmv2s1, kdtv2"
	qt_sql_timediff_DateV2_DateTimeV2 "select timediff(kdtv2, kdtmv2s1) from fn_test order by kdtv2, kdtmv2s1"
	qt_sql_timediff_DateV2_DateTimeV2_notnull "select timediff(kdtv2, kdtmv2s1) from fn_test_not_nullable order by kdtv2, kdtmv2s1"
	qt_sql_timediff_DateV2_DateV2 "select timediff(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
	qt_sql_timediff_DateV2_DateV2_notnull "select timediff(kdtv2, kdtv2) from fn_test_not_nullable order by kdtv2, kdtv2"
	qt_sql_timediff_DateTimeV2_DateTime "select timediff(kdtmv2s1, kdtm) from fn_test order by kdtmv2s1, kdtm"
	qt_sql_timediff_DateTimeV2_DateTime_notnull "select timediff(kdtmv2s1, kdtm) from fn_test_not_nullable order by kdtmv2s1, kdtm"
	qt_sql_timediff_DateV2_DateTime "select timediff(kdtv2, kdtm) from fn_test order by kdtv2, kdtm"
	qt_sql_timediff_DateV2_DateTime_notnull "select timediff(kdtv2, kdtm) from fn_test_not_nullable order by kdtv2, kdtm"
	qt_sql_timestamp_DateTimeV2 "select timestamp(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_timestamp_DateTimeV2_notnull "select timestamp(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_timestamp_DateTime "select timestamp(kdtm) from fn_test order by kdtm"
	qt_sql_timestamp_DateTime_notnull "select timestamp(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_timestamp_DateTime "select timestamp(kdtm) from fn_test order by kdtm"
	qt_sql_timestamp_DateTime_notnull "select timestamp(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_timestamp_DateTimeV2 "select timestamp(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_timestamp_DateTimeV2_notnull "select timestamp(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_to_base64_String "select to_base64(kstr) from fn_test order by kstr"
	qt_sql_to_base64_String_notnull "select to_base64(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_to_bitmap_Varchar "select to_bitmap(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_to_bitmap_Varchar_notnull "select to_bitmap(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_to_bitmap_String "select to_bitmap(kstr) from fn_test order by kstr"
	qt_sql_to_bitmap_String_notnull "select to_bitmap(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_to_bitmap_with_check_BigInt "select to_bitmap_with_check(kbint) from fn_test order by kbint"
	qt_sql_to_bitmap_with_check_BigInt_notnull "select to_bitmap_with_check(kbint) from fn_test_not_nullable order by kbint"
	qt_sql_to_date_DateTime "select to_date(kdtm) from fn_test order by kdtm"
	qt_sql_to_date_DateTime_notnull "select to_date(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_to_date_DateTimeV2 "select to_date(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_to_date_DateTimeV2_notnull "select to_date(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_to_datev2_DateTimeV2 "select to_datev2(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_to_datev2_DateTimeV2_notnull "select to_datev2(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_to_days_Date "select to_days(kdt) from fn_test order by kdt"
	qt_sql_to_days_Date_notnull "select to_days(kdt) from fn_test_not_nullable order by kdt"
	qt_sql_to_days_DateV2 "select to_days(kdtv2) from fn_test order by kdtv2"
	qt_sql_to_days_DateV2_notnull "select to_days(kdtv2) from fn_test_not_nullable order by kdtv2"
	qt_sql_to_monday_DateTimeV2 "select to_monday(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_to_monday_DateTimeV2_notnull "select to_monday(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_to_monday_DateV2 "select to_monday(kdtv2) from fn_test order by kdtv2"
	qt_sql_to_monday_DateV2_notnull "select to_monday(kdtv2) from fn_test_not_nullable order by kdtv2"
	qt_sql_to_monday_DateTime "select to_monday(kdtm) from fn_test order by kdtm"
	qt_sql_to_monday_DateTime_notnull "select to_monday(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_to_monday_Date "select to_monday(kdt) from fn_test order by kdt"
	qt_sql_to_monday_Date_notnull "select to_monday(kdt) from fn_test_not_nullable order by kdt"
	qt_sql_trim_Varchar "select trim(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_trim_Varchar_notnull "select trim(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_trim_String "select trim(kstr) from fn_test order by kstr"
	qt_sql_trim_String_notnull "select trim(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_trim_Varchar_Varchar "select trim(kvchrs1, 'var') from fn_test order by kvchrs1"
	qt_sql_trim_Varchar_Varchar_notnull "select trim(kvchrs1, 'var') from fn_test_not_nullable order by kvchrs1"
	qt_sql_trim_String_String "select trim(kstr, 'str') from fn_test order by kstr"
	qt_sql_trim_String_String_notnull "select trim(kstr, 'str') from fn_test_not_nullable order by kstr"
	qt_sql_truncate_Double_Integer "select truncate(kdbl, 2) from fn_test order by kdbl"
	qt_sql_truncate_Double_Integer_notnull "select truncate(kdbl, 2) from fn_test_not_nullable order by kdbl"
	qt_sql_truncate_DecimalV3S1_Int "select truncate(kdcmlv3s1, 1) from fn_test order by kdcmlv3s1"
	qt_sql_truncate_DecimalV3S1_Int_notnull "select truncate(kdcmlv3s1, 1) from fn_test_not_nullable order by kdcmlv3s1"
	qt_sql_truncate_DecimalV3S2_Int "select truncate(kdcmlv3s2, 1) from fn_test order by kdcmlv3s2"
	qt_sql_truncate_DecimalV3S2_Int_notnull "select truncate(kdcmlv3s2, 1) from fn_test_not_nullable order by kdcmlv3s2"
	qt_sql_truncate_DecimalV3S3_Int "select truncate(kdcmlv3s3, 1) from fn_test order by kdcmlv3s3"
	qt_sql_truncate_DecimalV3S3_Int_notnull "select truncate(kdcmlv3s3, 1) from fn_test_not_nullable order by kdcmlv3s3"
}
