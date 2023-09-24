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

suite("nereids_scalar_fn_B") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_bin_BigInt "select bin(kbint) from fn_test order by kbint"
	qt_sql_bin_BigInt_notnull "select bin(kbint) from fn_test_not_nullable order by kbint"
	qt_sql_bit_length_Varchar "select bit_length(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_bit_length_Varchar_notnull "select bit_length(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_bit_length_String "select bit_length(kstr) from fn_test order by kstr"
	qt_sql_bit_length_String_notnull "select bit_length(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_bitmap_and_Bitmap_Bitmap "select bitmap_and(to_bitmap(kbint), to_bitmap(kbint)) from fn_test order by kbint, kbint"
	qt_sql_bitmap_and_Bitmap_Bitmap_notnull "select bitmap_and(to_bitmap(kbint), to_bitmap(kbint)) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_bitmap_and_count_Bitmap_Bitmap "select bitmap_and_count(to_bitmap(kbint), to_bitmap(kbint)) from fn_test order by kbint, kbint"
	qt_sql_bitmap_and_count_Bitmap_Bitmap_notnull "select bitmap_and_count(to_bitmap(kbint), to_bitmap(kbint)) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_bitmap_and_not_Bitmap_Bitmap "select bitmap_and_not(to_bitmap(kbint), to_bitmap(kbint)) from fn_test order by kbint, kbint"
	qt_sql_bitmap_and_not_Bitmap_Bitmap_notnull "select bitmap_and_not(to_bitmap(kbint), to_bitmap(kbint)) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_bitmap_and_not_count_Bitmap_Bitmap "select bitmap_and_not_count(to_bitmap(kbint), to_bitmap(kbint)) from fn_test order by kbint, kbint"
	qt_sql_bitmap_and_not_count_Bitmap_Bitmap_notnull "select bitmap_and_not_count(to_bitmap(kbint), to_bitmap(kbint)) from fn_test_not_nullable order by kbint, kbint"

	qt_sql_bitmap_andnot_Bitmap_Bitmap "select bitmap_andnot(to_bitmap(kbint), to_bitmap(kbint)) from fn_test order by kbint, kbint"
	qt_sql_bitmap_andnot_Bitmap_Bitmap_notnull "select bitmap_andnot(to_bitmap(kbint), to_bitmap(kbint)) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_bitmap_andnot_count_Bitmap_Bitmap "select bitmap_andnot_count(to_bitmap(kbint), to_bitmap(kbint)) from fn_test order by kbint, kbint"
	qt_sql_bitmap_andnot_count_Bitmap_Bitmap_notnull "select bitmap_andnot_count(to_bitmap(kbint), to_bitmap(kbint)) from fn_test_not_nullable order by kbint, kbint"

	qt_sql_bitmap_contains_Bitmap_BigInt "select bitmap_contains(to_bitmap(kbint), kbint) from fn_test order by kbint, kbint"
	qt_sql_bitmap_contains_Bitmap_BigInt_notnull "select bitmap_contains(to_bitmap(kbint), kbint) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_bitmap_count_Bitmap "select bitmap_count(to_bitmap(kbint)) from fn_test order by kbint"
	qt_sql_bitmap_count_Bitmap_notnull "select bitmap_count(to_bitmap(kbint)) from fn_test_not_nullable order by kbint"
	qt_sql_bitmap_from_string_Varchar "select bitmap_from_string(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_bitmap_from_string_Varchar_notnull "select bitmap_from_string(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_bitmap_from_string_String "select bitmap_from_string(kstr) from fn_test order by kstr"
	qt_sql_bitmap_from_string_String_notnull "select bitmap_from_string(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_bitmap_has_all_Bitmap_Bitmap "select bitmap_has_all(to_bitmap(kbint), to_bitmap(kbint)) from fn_test order by kbint, kbint"
	qt_sql_bitmap_has_all_Bitmap_Bitmap_notnull "select bitmap_has_all(to_bitmap(kbint), to_bitmap(kbint)) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_bitmap_has_any_Bitmap_Bitmap "select bitmap_has_any(to_bitmap(kbint), to_bitmap(kbint)) from fn_test order by kbint, kbint"
	qt_sql_bitmap_has_any_Bitmap_Bitmap_notnull "select bitmap_has_any(to_bitmap(kbint), to_bitmap(kbint)) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_bitmap_hash_Varchar "select bitmap_hash(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_bitmap_hash_Varchar_notnull "select bitmap_hash(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_bitmap_hash_String "select bitmap_hash(kstr) from fn_test order by kstr"
	qt_sql_bitmap_hash_String_notnull "select bitmap_hash(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_bitmap_hash64_Varchar "select bitmap_hash64(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_bitmap_hash64_Varchar_notnull "select bitmap_hash64(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_bitmap_hash64_String "select bitmap_hash64(kstr) from fn_test order by kstr"
	qt_sql_bitmap_hash64_String_notnull "select bitmap_hash64(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_bitmap_max_Bitmap "select bitmap_max(to_bitmap(kbint)) from fn_test order by kbint"
	qt_sql_bitmap_max_Bitmap_notnull "select bitmap_max(to_bitmap(kbint)) from fn_test_not_nullable order by kbint"
	qt_sql_bitmap_min_Bitmap "select bitmap_min(to_bitmap(kbint)) from fn_test order by kbint"
	qt_sql_bitmap_min_Bitmap_notnull "select bitmap_min(to_bitmap(kbint)) from fn_test_not_nullable order by kbint"
	qt_sql_bitmap_not_Bitmap_Bitmap "select bitmap_not(to_bitmap(kbint), to_bitmap(kbint)) from fn_test order by kbint, kbint"
	qt_sql_bitmap_not_Bitmap_Bitmap_notnull "select bitmap_not(to_bitmap(kbint), to_bitmap(kbint)) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_bitmap_or_Bitmap_Bitmap "select bitmap_or(to_bitmap(kbint), to_bitmap(kbint)) from fn_test order by kbint, kbint"
	qt_sql_bitmap_or_Bitmap_Bitmap_notnull "select bitmap_or(to_bitmap(kbint), to_bitmap(kbint)) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_bitmap_or_count_Bitmap_Bitmap "select bitmap_or_count(to_bitmap(kbint), to_bitmap(kbint)) from fn_test order by kbint, kbint"
	qt_sql_bitmap_or_count_Bitmap_Bitmap_notnull "select bitmap_or_count(to_bitmap(kbint), to_bitmap(kbint)) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_bitmap_subset_in_range_Bitmap_BigInt_BigInt "select bitmap_subset_in_range(to_bitmap(kbint), kbint, kbint) from fn_test order by kbint, kbint, kbint"
	qt_sql_bitmap_subset_in_range_Bitmap_BigInt_BigInt_notnull "select bitmap_subset_in_range(to_bitmap(kbint), kbint, kbint) from fn_test_not_nullable order by kbint, kbint, kbint"
	qt_sql_bitmap_subset_limit_Bitmap_BigInt_BigInt "select bitmap_subset_limit(to_bitmap(kbint), kbint, kbint) from fn_test order by kbint, kbint, kbint"
	qt_sql_bitmap_subset_limit_Bitmap_BigInt_BigInt_notnull "select bitmap_subset_limit(to_bitmap(kbint), kbint, kbint) from fn_test_not_nullable order by kbint, kbint, kbint"
	qt_sql_bitmap_to_string_Bitmap "select bitmap_to_string(to_bitmap(kbint)) from fn_test order by kbint"
	qt_sql_bitmap_to_string_Bitmap_notnull "select bitmap_to_string(to_bitmap(kbint)) from fn_test_not_nullable order by kbint"
	qt_sql_bitmap_xor_Bitmap_Bitmap "select bitmap_xor(to_bitmap(kbint), to_bitmap(kbint)) from fn_test order by kbint, kbint"
	qt_sql_bitmap_xor_Bitmap_Bitmap_notnull "select bitmap_xor(to_bitmap(kbint), to_bitmap(kbint)) from fn_test_not_nullable order by kbint, kbint"
	qt_sql_bitmap_xor_count_Bitmap_Bitmap "select bitmap_xor_count(to_bitmap(kbint), to_bitmap(kbint)) from fn_test order by kbint, kbint"
	qt_sql_bitmap_xor_count_Bitmap_Bitmap_notnull "select bitmap_xor_count(to_bitmap(kbint), to_bitmap(kbint)) from fn_test_not_nullable order by kbint, kbint"
}