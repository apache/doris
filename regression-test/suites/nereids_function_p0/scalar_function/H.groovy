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

suite("nereids_scalar_fn_H") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_hex_BigInt "select hex(kbint) from fn_test order by kbint"
	qt_sql_hex_BigInt_notnull "select hex(kbint) from fn_test_not_nullable order by kbint"
	qt_sql_hex_Varchar "select hex(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_hex_Varchar_notnull "select hex(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_hex_String "select hex(kstr) from fn_test order by kstr"
	qt_sql_hex_String_notnull "select hex(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_hll_from_base64_1 "select hll_cardinality(hll_from_base64(hll_to_base64(hll_hash(kvchrs1)))) from fn_test order by kvchrs1"
	qt_sql_hll_from_base64_notnull_1 "select hll_cardinality(hll_from_base64(hll_to_base64(hll_hash(kvchrs1)))) from fn_test_not_nullable order by kvchrs1"
	qt_sql_hll_from_base64_2 "select hll_cardinality(hll_from_base64(hll_to_base64(hll_hash(kstr)))) from fn_test_not_nullable order by kstr"
	qt_sql_hll_from_base64_notnull_2 "select hll_cardinality(hll_from_base64(hll_to_base64(hll_hash(kstr)))) from fn_test order by kstr" 
	qt_sql_hll_hash_Varchar "select hll_hash(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_hll_hash_Varchar_notnull "select hll_hash(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_hll_hash_String "select hll_hash(kstr) from fn_test order by kstr"
	qt_sql_hll_hash_String_notnull "select hll_hash(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_hll_to_base64_1 "select hll_to_base64(hll_hash(kvchrs1)) from fn_test order by kvchrs1"
	qt_sql_hll_to_base64_notnull_1 "select hll_to_base64(hll_hash(kvchrs1)) from fn_test_not_nullable order by kvchrs1"
	qt_sql_hll_to_base64_2 "select hll_to_base64(hll_hash(kstr)) from fn_test_not_nullable order by kstr"
	qt_sql_hll_to_base64_notnull_2 "select hll_to_base64(hll_hash(kstr)) from fn_test order by kstr"
	qt_sql_hour_DateTime "select hour(kdtm) from fn_test order by kdtm"
	qt_sql_hour_DateTime_notnull "select hour(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_hour_DateTimeV2 "select hour(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_hour_DateTimeV2_notnull "select hour(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_hour_DateV2 "select hour(kdtv2) from fn_test order by kdtv2"
	qt_sql_hour_DateV2_notnull "select hour(kdtv2) from fn_test_not_nullable order by kdtv2"
	qt_sql_hour_ceil_DateTime "select hour_ceil(kdtm) from fn_test order by kdtm"
	qt_sql_hour_ceil_DateTime_notnull "select hour_ceil(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_hour_ceil_DateTimeV2 "select hour_ceil(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_hour_ceil_DateTimeV2_notnull "select hour_ceil(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_hour_ceil_DateTime_DateTime "select hour_ceil(kdtm, kdtm) from fn_test order by kdtm, kdtm"
	qt_sql_hour_ceil_DateTime_DateTime_notnull "select hour_ceil(kdtm, kdtm) from fn_test_not_nullable order by kdtm, kdtm"
	qt_sql_hour_ceil_DateTime_Integer "select hour_ceil(kdtm, kint) from fn_test order by kdtm, kint"
	qt_sql_hour_ceil_DateTime_Integer_notnull "select hour_ceil(kdtm, kint) from fn_test_not_nullable order by kdtm, kint"
	qt_sql_hour_ceil_DateTimeV2_DateTimeV2 "select hour_ceil(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
	qt_sql_hour_ceil_DateTimeV2_DateTimeV2_notnull "select hour_ceil(kdtmv2s1, kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1, kdtmv2s1"
	qt_sql_hour_ceil_DateTimeV2_Integer "select hour_ceil(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
	qt_sql_hour_ceil_DateTimeV2_Integer_notnull "select hour_ceil(kdtmv2s1, kint) from fn_test_not_nullable order by kdtmv2s1, kint"
	qt_sql_hour_ceil_DateTime_Integer_DateTime "select hour_ceil(kdtm, kint, kdtm) from fn_test order by kdtm, kint, kdtm"
	qt_sql_hour_ceil_DateTime_Integer_DateTime_notnull "select hour_ceil(kdtm, kint, kdtm) from fn_test_not_nullable order by kdtm, kint, kdtm"
	qt_sql_hour_ceil_DateTimeV2_Integer_DateTimeV2 "select hour_ceil(kdtmv2s1, kint, kdtmv2s1) from fn_test order by kdtmv2s1, kint, kdtmv2s1"
	qt_sql_hour_ceil_DateTimeV2_Integer_DateTimeV2_notnull "select hour_ceil(kdtmv2s1, kint, kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1, kint, kdtmv2s1"
	qt_sql_hour_floor_DateTime "select hour_floor(kdtm) from fn_test order by kdtm"
	qt_sql_hour_floor_DateTime_notnull "select hour_floor(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_hour_floor_DateTimeV2 "select hour_floor(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_hour_floor_DateTimeV2_notnull "select hour_floor(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_hour_floor_DateTime_DateTime "select hour_floor(kdtm, kdtm) from fn_test order by kdtm, kdtm"
	qt_sql_hour_floor_DateTime_DateTime_notnull "select hour_floor(kdtm, kdtm) from fn_test_not_nullable order by kdtm, kdtm"
	qt_sql_hour_floor_DateTime_Integer "select hour_floor(kdtm, kint) from fn_test order by kdtm, kint"
	qt_sql_hour_floor_DateTime_Integer_notnull "select hour_floor(kdtm, kint) from fn_test_not_nullable order by kdtm, kint"
	qt_sql_hour_floor_DateTimeV2_DateTimeV2 "select hour_floor(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
	qt_sql_hour_floor_DateTimeV2_DateTimeV2_notnull "select hour_floor(kdtmv2s1, kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1, kdtmv2s1"
	qt_sql_hour_floor_DateTimeV2_Integer "select hour_floor(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
	qt_sql_hour_floor_DateTimeV2_Integer_notnull "select hour_floor(kdtmv2s1, kint) from fn_test_not_nullable order by kdtmv2s1, kint"
	qt_sql_hour_floor_DateTime_Integer_DateTime "select hour_floor(kdtm, kint, kdtm) from fn_test order by kdtm, kint, kdtm"
	qt_sql_hour_floor_DateTime_Integer_DateTime_notnull "select hour_floor(kdtm, kint, kdtm) from fn_test_not_nullable order by kdtm, kint, kdtm"
	qt_sql_hour_floor_DateTimeV2_Integer_DateTimeV2 "select hour_floor(kdtmv2s1, kint, kdtmv2s1) from fn_test order by kdtmv2s1, kint, kdtmv2s1"
	qt_sql_hour_floor_DateTimeV2_Integer_DateTimeV2_notnull "select hour_floor(kdtmv2s1, kint, kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1, kint, kdtmv2s1"
	qt_sql_hours_add_DateTime_Integer "select hours_add(kdtm, kint) from fn_test order by kdtm, kint"
	qt_sql_hours_add_DateTime_Integer_notnull "select hours_add(kdtm, kint) from fn_test_not_nullable order by kdtm, kint"
	qt_sql_hours_add_DateTimeV2_Integer "select hours_add(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
	qt_sql_hours_add_DateTimeV2_Integer_notnull "select hours_add(kdtmv2s1, kint) from fn_test_not_nullable order by kdtmv2s1, kint"
	qt_sql_hours_add_Date_Integer "select hours_add(kdt, kint) from fn_test order by kdt, kint"
	qt_sql_hours_add_Date_Integer_notnull "select hours_add(kdt, kint) from fn_test_not_nullable order by kdt, kint"
	qt_sql_hours_add_DateV2_Integer "select hours_add(kdtv2, kint) from fn_test order by kdtv2, kint"
	qt_sql_hours_add_DateV2_Integer_notnull "select hours_add(kdtv2, kint) from fn_test_not_nullable order by kdtv2, kint"
	qt_sql_hours_diff_DateTime_DateTime "select hours_diff(kdtm, kdtm) from fn_test order by kdtm, kdtm"
	qt_sql_hours_diff_DateTime_DateTime_notnull "select hours_diff(kdtm, kdtm) from fn_test_not_nullable order by kdtm, kdtm"
	qt_sql_hours_diff_DateTimeV2_DateTimeV2 "select hours_diff(kdtmv2s1, kdtmv2s1) from fn_test order by kdtmv2s1, kdtmv2s1"
	qt_sql_hours_diff_DateTimeV2_DateTimeV2_notnull "select hours_diff(kdtmv2s1, kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1, kdtmv2s1"
	qt_sql_hours_diff_DateV2_DateTimeV2 "select hours_diff(kdtv2, kdtmv2s1) from fn_test order by kdtv2, kdtmv2s1"
	qt_sql_hours_diff_DateV2_DateTimeV2_notnull "select hours_diff(kdtv2, kdtmv2s1) from fn_test_not_nullable order by kdtv2, kdtmv2s1"
	qt_sql_hours_diff_DateTimeV2_DateV2 "select hours_diff(kdtmv2s1, kdtv2) from fn_test order by kdtmv2s1, kdtv2"
	qt_sql_hours_diff_DateTimeV2_DateV2_notnull "select hours_diff(kdtmv2s1, kdtv2) from fn_test_not_nullable order by kdtmv2s1, kdtv2"
	qt_sql_hours_diff_DateV2_DateV2 "select hours_diff(kdtv2, kdtv2) from fn_test order by kdtv2, kdtv2"
	qt_sql_hours_diff_DateV2_DateV2_notnull "select hours_diff(kdtv2, kdtv2) from fn_test_not_nullable order by kdtv2, kdtv2"
	qt_sql_hours_diff_DateV2_DateTime "select hours_diff(kdtv2, kdtm) from fn_test order by kdtv2, kdtm"
	qt_sql_hours_diff_DateV2_DateTime_notnull "select hours_diff(kdtv2, kdtm) from fn_test_not_nullable order by kdtv2, kdtm"
	qt_sql_hours_diff_DateTime_DateV2 "select hours_diff(kdtm, kdtv2) from fn_test order by kdtm, kdtv2"
	qt_sql_hours_diff_DateTime_DateV2_notnull "select hours_diff(kdtm, kdtv2) from fn_test_not_nullable order by kdtm, kdtv2"
	qt_sql_hours_diff_DateTimeV2_DateTime "select hours_diff(kdtmv2s1, kdtm) from fn_test order by kdtmv2s1, kdtm"
	qt_sql_hours_diff_DateTimeV2_DateTime_notnull "select hours_diff(kdtmv2s1, kdtm) from fn_test_not_nullable order by kdtmv2s1, kdtm"
	qt_sql_hours_diff_DateTime_DateTimeV2 "select hours_diff(kdtm, kdtmv2s1) from fn_test order by kdtm, kdtmv2s1"
	qt_sql_hours_diff_DateTime_DateTimeV2_notnull "select hours_diff(kdtm, kdtmv2s1) from fn_test_not_nullable order by kdtm, kdtmv2s1"
	qt_sql_hours_sub_DateTime_Integer "select hours_sub(kdtm, kint) from fn_test order by kdtm, kint"
	qt_sql_hours_sub_DateTime_Integer_notnull "select hours_sub(kdtm, kint) from fn_test_not_nullable order by kdtm, kint"
	qt_sql_hours_sub_DateTimeV2_Integer "select hours_sub(kdtmv2s1, kint) from fn_test order by kdtmv2s1, kint"
	qt_sql_hours_sub_DateTimeV2_Integer_notnull "select hours_sub(kdtmv2s1, kint) from fn_test_not_nullable order by kdtmv2s1, kint"
	qt_sql_hours_sub_Date_Integer "select hours_sub(kdt, kint) from fn_test order by kdt, kint"
	qt_sql_hours_sub_Date_Integer_notnull "select hours_sub(kdt, kint) from fn_test_not_nullable order by kdt, kint"
	qt_sql_hours_sub_DateV2_Integer "select hours_sub(kdtv2, kint) from fn_test order by kdtv2, kint"
	qt_sql_hours_sub_DateV2_Integer_notnull "select hours_sub(kdtv2, kint) from fn_test_not_nullable order by kdtv2, kint"
}