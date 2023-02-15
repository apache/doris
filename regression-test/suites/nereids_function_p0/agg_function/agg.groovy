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

suite("nereids_agg_fn") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_any_value_AnyData_gb "select any_value(kint) from fn_test group by kbool order by kbool"
	qt_sql_any_value_AnyData_gb "select any_value(kint) from fn_test"
	qt_sql_any_value_AnyData_gb_notnull_gb "select any_value(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_any_value_AnyData_gb_notnull_gb_notnull "select any_value(kint) from fn_test_not_nullable"

	qt_sql_avg_TinyInt_gb "select avg(ktint) from fn_test group by kbool order by kbool"
	qt_sql_avg_TinyInt_gb "select avg(ktint) from fn_test"
	qt_sql_avg_TinyInt_gb_notnull_gb "select avg(ktint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_avg_TinyInt_gb_notnull_gb_notnull "select avg(ktint) from fn_test_not_nullable"

	qt_sql_avg_SmallInt_gb "select avg(ksint) from fn_test group by kbool order by kbool"
	qt_sql_avg_SmallInt_gb "select avg(ksint) from fn_test"
	qt_sql_avg_SmallInt_gb_notnull_gb "select avg(ksint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_avg_SmallInt_gb_notnull_gb_notnull "select avg(ksint) from fn_test_not_nullable"

	qt_sql_avg_Integer_gb "select avg(kint) from fn_test group by kbool order by kbool"
	qt_sql_avg_Integer_gb "select avg(kint) from fn_test"
	qt_sql_avg_Integer_gb_notnull_gb "select avg(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_avg_Integer_gb_notnull_gb_notnull "select avg(kint) from fn_test_not_nullable"

	qt_sql_avg_BigInt_gb "select avg(kbint) from fn_test group by kbool order by kbool"
	qt_sql_avg_BigInt_gb "select avg(kbint) from fn_test"
	qt_sql_avg_BigInt_gb_notnull_gb "select avg(kbint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_avg_BigInt_gb_notnull_gb_notnull "select avg(kbint) from fn_test_not_nullable"

	qt_sql_avg_Double_gb "select avg(kdbl) from fn_test group by kbool order by kbool"
	qt_sql_avg_Double_gb "select avg(kdbl) from fn_test"
	qt_sql_avg_Double_gb_notnull_gb "select avg(kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_avg_Double_gb_notnull_gb_notnull "select avg(kdbl) from fn_test_not_nullable"

	qt_sql_avg_DecimalV2_gb "select avg(kdcmls1) from fn_test group by kbool order by kbool"
	qt_sql_avg_DecimalV2_gb "select avg(kdcmls1) from fn_test"
	qt_sql_avg_DecimalV2_gb_notnull_gb "select avg(kdcmls1) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_avg_DecimalV2_gb_notnull_gb_notnull "select avg(kdcmls1) from fn_test_not_nullable"

	qt_sql_avg_weighted_TinyInt_Double_gb "select avg_weighted(ktint, kdbl) from fn_test group by kbool order by kbool"
	qt_sql_avg_weighted_TinyInt_Double_gb "select avg_weighted(ktint, kdbl) from fn_test"
	qt_sql_avg_weighted_TinyInt_Double_gb_notnull_gb "select avg_weighted(ktint, kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_avg_weighted_TinyInt_Double_gb_notnull_gb_notnull "select avg_weighted(ktint, kdbl) from fn_test_not_nullable"

	qt_sql_avg_weighted_SmallInt_Double_gb "select avg_weighted(ksint, kdbl) from fn_test group by kbool order by kbool"
	qt_sql_avg_weighted_SmallInt_Double_gb "select avg_weighted(ksint, kdbl) from fn_test"
	qt_sql_avg_weighted_SmallInt_Double_gb_notnull_gb "select avg_weighted(ksint, kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_avg_weighted_SmallInt_Double_gb_notnull_gb_notnull "select avg_weighted(ksint, kdbl) from fn_test_not_nullable"

	qt_sql_avg_weighted_Integer_Double_gb "select avg_weighted(kint, kdbl) from fn_test group by kbool order by kbool"
	qt_sql_avg_weighted_Integer_Double_gb "select avg_weighted(kint, kdbl) from fn_test"
	qt_sql_avg_weighted_Integer_Double_gb_notnull_gb "select avg_weighted(kint, kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_avg_weighted_Integer_Double_gb_notnull_gb_notnull "select avg_weighted(kint, kdbl) from fn_test_not_nullable"

	qt_sql_avg_weighted_BigInt_Double_gb "select avg_weighted(kbint, kdbl) from fn_test group by kbool order by kbool"
	qt_sql_avg_weighted_BigInt_Double_gb "select avg_weighted(kbint, kdbl) from fn_test"
	qt_sql_avg_weighted_BigInt_Double_gb_notnull_gb "select avg_weighted(kbint, kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_avg_weighted_BigInt_Double_gb_notnull_gb_notnull "select avg_weighted(kbint, kdbl) from fn_test_not_nullable"

	qt_sql_avg_weighted_Float_Double_gb "select avg_weighted(kfloat, kdbl) from fn_test group by kbool order by kbool"
	qt_sql_avg_weighted_Float_Double_gb "select avg_weighted(kfloat, kdbl) from fn_test"
	qt_sql_avg_weighted_Float_Double_gb_notnull_gb "select avg_weighted(kfloat, kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_avg_weighted_Float_Double_gb_notnull_gb_notnull "select avg_weighted(kfloat, kdbl) from fn_test_not_nullable"

	qt_sql_avg_weighted_Double_Double_gb "select avg_weighted(kdbl, kdbl) from fn_test group by kbool order by kbool"
	qt_sql_avg_weighted_Double_Double_gb "select avg_weighted(kdbl, kdbl) from fn_test"
	qt_sql_avg_weighted_Double_Double_gb_notnull_gb "select avg_weighted(kdbl, kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_avg_weighted_Double_Double_gb_notnull_gb_notnull "select avg_weighted(kdbl, kdbl) from fn_test_not_nullable"

	qt_sql_avg_weighted_DecimalV2_Double_gb "select avg_weighted(kdcmls1, kdbl) from fn_test group by kbool order by kbool"
	qt_sql_avg_weighted_DecimalV2_Double_gb "select avg_weighted(kdcmls1, kdbl) from fn_test"
	qt_sql_avg_weighted_DecimalV2_Double_gb_notnull_gb "select avg_weighted(kdcmls1, kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_avg_weighted_DecimalV2_Double_gb_notnull_gb_notnull "select avg_weighted(kdcmls1, kdbl) from fn_test_not_nullable"

	qt_sql_bitmap_intersect_Bitmap_gb "select bitmap_intersect(to_bitmap(kbint)) from fn_test group by kbool order by kbool"
	qt_sql_bitmap_intersect_Bitmap_gb "select bitmap_intersect(to_bitmap(kbint)) from fn_test"
	qt_sql_bitmap_intersect_Bitmap_gb_notnull_gb "select bitmap_intersect(to_bitmap(kbint)) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_bitmap_intersect_Bitmap_gb_notnull_gb_notnull "select bitmap_intersect(to_bitmap(kbint)) from fn_test_not_nullable"

	qt_sql_bitmap_union_Bitmap_gb "select bitmap_union(to_bitmap(kbint)) from fn_test group by kbool order by kbool"
	qt_sql_bitmap_union_Bitmap_gb "select bitmap_union(to_bitmap(kbint)) from fn_test"
	qt_sql_bitmap_union_Bitmap_gb_notnull_gb "select bitmap_union(to_bitmap(kbint)) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_bitmap_union_Bitmap_gb_notnull_gb_notnull "select bitmap_union(to_bitmap(kbint)) from fn_test_not_nullable"

	qt_sql_bitmap_union_count_Bitmap_gb "select bitmap_union_count(to_bitmap(kbint)) from fn_test group by kbool order by kbool"
	qt_sql_bitmap_union_count_Bitmap_gb "select bitmap_union_count(to_bitmap(kbint)) from fn_test"
	qt_sql_bitmap_union_count_Bitmap_gb_notnull_gb "select bitmap_union_count(to_bitmap(kbint)) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_bitmap_union_count_Bitmap_gb_notnull_gb_notnull "select bitmap_union_count(to_bitmap(kbint)) from fn_test_not_nullable"

	qt_sql_bitmap_union_int_SmallInt_gb "select bitmap_union_int(ksint) from fn_test group by kbool order by kbool"
	qt_sql_bitmap_union_int_SmallInt_gb "select bitmap_union_int(ksint) from fn_test"
	qt_sql_bitmap_union_int_SmallInt_gb_notnull_gb "select bitmap_union_int(ksint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_bitmap_union_int_SmallInt_gb_notnull_gb_notnull "select bitmap_union_int(ksint) from fn_test_not_nullable"

	qt_sql_bitmap_union_int_TinyInt_gb "select bitmap_union_int(ktint) from fn_test group by kbool order by kbool"
	qt_sql_bitmap_union_int_TinyInt_gb "select bitmap_union_int(ktint) from fn_test"
	qt_sql_bitmap_union_int_TinyInt_gb_notnull_gb "select bitmap_union_int(ktint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_bitmap_union_int_TinyInt_gb_notnull_gb_notnull "select bitmap_union_int(ktint) from fn_test_not_nullable"

	qt_sql_bitmap_union_int_Integer_gb "select bitmap_union_int(kint) from fn_test group by kbool order by kbool"
	qt_sql_bitmap_union_int_Integer_gb "select bitmap_union_int(kint) from fn_test"
	qt_sql_bitmap_union_int_Integer_gb_notnull_gb "select bitmap_union_int(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_bitmap_union_int_Integer_gb_notnull_gb_notnull "select bitmap_union_int(kint) from fn_test_not_nullable"

	qt_sql_bitmap_union_int_BigInt_gb "select bitmap_union_int(kbint) from fn_test group by kbool order by kbool"
	qt_sql_bitmap_union_int_BigInt_gb "select bitmap_union_int(kbint) from fn_test"
	qt_sql_bitmap_union_int_BigInt_gb_notnull_gb "select bitmap_union_int(kbint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_bitmap_union_int_BigInt_gb_notnull_gb_notnull "select bitmap_union_int(kbint) from fn_test_not_nullable"

	qt_sql_count_gb "select count() from fn_test group by kbool order by kbool"
	qt_sql_count_gb "select count() from fn_test"
	qt_sql_count_gb_notnull_gb "select count() from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_count_gb_notnull_gb_notnull "select count() from fn_test_not_nullable"

	qt_sql_count_AnyData_gb "select count(kint) from fn_test group by kbool order by kbool"
	qt_sql_count_AnyData_gb "select count(kint) from fn_test"
	qt_sql_count_AnyData_gb_notnull_gb "select count(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_count_AnyData_gb_notnull_gb_notnull "select count(kint) from fn_test_not_nullable"

	qt_sql_group_bit_and_TinyInt_gb "select group_bit_and(ktint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_and_TinyInt_gb "select group_bit_and(ktint) from fn_test"
	qt_sql_group_bit_and_TinyInt_gb_notnull_gb "select group_bit_and(ktint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_and_TinyInt_gb_notnull_gb_notnull "select group_bit_and(ktint) from fn_test_not_nullable"

	qt_sql_group_bit_and_SmallInt_gb "select group_bit_and(ksint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_and_SmallInt_gb "select group_bit_and(ksint) from fn_test"
	qt_sql_group_bit_and_SmallInt_gb_notnull_gb "select group_bit_and(ksint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_and_SmallInt_gb_notnull_gb_notnull "select group_bit_and(ksint) from fn_test_not_nullable"

	qt_sql_group_bit_and_Integer_gb "select group_bit_and(kint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_and_Integer_gb "select group_bit_and(kint) from fn_test"
	qt_sql_group_bit_and_Integer_gb_notnull_gb "select group_bit_and(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_and_Integer_gb_notnull_gb_notnull "select group_bit_and(kint) from fn_test_not_nullable"

	qt_sql_group_bit_and_BigInt_gb "select group_bit_and(kbint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_and_BigInt_gb "select group_bit_and(kbint) from fn_test"
	qt_sql_group_bit_and_BigInt_gb_notnull_gb "select group_bit_and(kbint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_and_BigInt_gb_notnull_gb_notnull "select group_bit_and(kbint) from fn_test_not_nullable"

	qt_sql_group_bit_and_LargeInt_gb "select group_bit_and(klint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_and_LargeInt_gb "select group_bit_and(klint) from fn_test"
	qt_sql_group_bit_and_LargeInt_gb_notnull_gb "select group_bit_and(klint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_and_LargeInt_gb_notnull_gb_notnull "select group_bit_and(klint) from fn_test_not_nullable"

	qt_sql_group_bit_or_TinyInt_gb "select group_bit_or(ktint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_or_TinyInt_gb "select group_bit_or(ktint) from fn_test"
	qt_sql_group_bit_or_TinyInt_gb_notnull_gb "select group_bit_or(ktint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_or_TinyInt_gb_notnull_gb_notnull "select group_bit_or(ktint) from fn_test_not_nullable"

	qt_sql_group_bit_or_SmallInt_gb "select group_bit_or(ksint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_or_SmallInt_gb "select group_bit_or(ksint) from fn_test"
	qt_sql_group_bit_or_SmallInt_gb_notnull_gb "select group_bit_or(ksint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_or_SmallInt_gb_notnull_gb_notnull "select group_bit_or(ksint) from fn_test_not_nullable"

	qt_sql_group_bit_or_Integer_gb "select group_bit_or(kint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_or_Integer_gb "select group_bit_or(kint) from fn_test"
	qt_sql_group_bit_or_Integer_gb_notnull_gb "select group_bit_or(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_or_Integer_gb_notnull_gb_notnull "select group_bit_or(kint) from fn_test_not_nullable"

	qt_sql_group_bit_or_BigInt_gb "select group_bit_or(kbint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_or_BigInt_gb "select group_bit_or(kbint) from fn_test"
	qt_sql_group_bit_or_BigInt_gb_notnull_gb "select group_bit_or(kbint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_or_BigInt_gb_notnull_gb_notnull "select group_bit_or(kbint) from fn_test_not_nullable"

	qt_sql_group_bit_or_LargeInt_gb "select group_bit_or(klint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_or_LargeInt_gb "select group_bit_or(klint) from fn_test"
	qt_sql_group_bit_or_LargeInt_gb_notnull_gb "select group_bit_or(klint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_or_LargeInt_gb_notnull_gb_notnull "select group_bit_or(klint) from fn_test_not_nullable"

	qt_sql_group_bit_xor_TinyInt_gb "select group_bit_xor(ktint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_xor_TinyInt_gb "select group_bit_xor(ktint) from fn_test"
	qt_sql_group_bit_xor_TinyInt_gb_notnull_gb "select group_bit_xor(ktint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_xor_TinyInt_gb_notnull_gb_notnull "select group_bit_xor(ktint) from fn_test_not_nullable"

	qt_sql_group_bit_xor_SmallInt_gb "select group_bit_xor(ksint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_xor_SmallInt_gb "select group_bit_xor(ksint) from fn_test"
	qt_sql_group_bit_xor_SmallInt_gb_notnull_gb "select group_bit_xor(ksint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_xor_SmallInt_gb_notnull_gb_notnull "select group_bit_xor(ksint) from fn_test_not_nullable"

	qt_sql_group_bit_xor_Integer_gb "select group_bit_xor(kint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_xor_Integer_gb "select group_bit_xor(kint) from fn_test"
	qt_sql_group_bit_xor_Integer_gb_notnull_gb "select group_bit_xor(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_xor_Integer_gb_notnull_gb_notnull "select group_bit_xor(kint) from fn_test_not_nullable"

	qt_sql_group_bit_xor_BigInt_gb "select group_bit_xor(kbint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_xor_BigInt_gb "select group_bit_xor(kbint) from fn_test"
	qt_sql_group_bit_xor_BigInt_gb_notnull_gb "select group_bit_xor(kbint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_xor_BigInt_gb_notnull_gb_notnull "select group_bit_xor(kbint) from fn_test_not_nullable"

	qt_sql_group_bit_xor_LargeInt_gb "select group_bit_xor(klint) from fn_test group by kbool order by kbool"
	qt_sql_group_bit_xor_LargeInt_gb "select group_bit_xor(klint) from fn_test"
	qt_sql_group_bit_xor_LargeInt_gb_notnull_gb "select group_bit_xor(klint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bit_xor_LargeInt_gb_notnull_gb_notnull "select group_bit_xor(klint) from fn_test_not_nullable"

	qt_sql_group_bitmap_xor_Bitmap_gb "select group_bitmap_xor(to_bitmap(kbint)) from fn_test group by kbool order by kbool"
	qt_sql_group_bitmap_xor_Bitmap_gb "select group_bitmap_xor(to_bitmap(kbint)) from fn_test"
	qt_sql_group_bitmap_xor_Bitmap_gb_notnull_gb "select group_bitmap_xor(to_bitmap(kbint)) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_bitmap_xor_Bitmap_gb_notnull_gb_notnull "select group_bitmap_xor(to_bitmap(kbint)) from fn_test_not_nullable"

	qt_sql_group_concat_Varchar_gb "select group_concat(kvchrs1) from fn_test group by kbool order by kbool"
	qt_sql_group_concat_Varchar_gb "select group_concat(kvchrs1) from fn_test"
	qt_sql_group_concat_Varchar_gb_notnull_gb "select group_concat(kvchrs1) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_concat_Varchar_gb_notnull_gb_notnull "select group_concat(kvchrs1) from fn_test_not_nullable"

	qt_sql_group_concat_Varchar_AnyData_gb "select group_concat(distinct cast(abs(kint) as varchar) order by abs(ksint), kdt) from fn_test group by kbool order by kbool"
	qt_sql_group_concat_Varchar_AnyData_gb "select group_concat(distinct cast(abs(kint) as varchar) order by abs(ksint), kdt) from fn_test"
	qt_sql_group_concat_Varchar_AnyData_gb_notnull_gb "select group_concat(distinct cast(abs(kint) as varchar) order by abs(ksint), kdt) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_concat_Varchar_AnyData_gb_notnull_gb_notnull "select group_concat(distinct cast(abs(kint) as varchar) order by abs(ksint), kdt) from fn_test_not_nullable"

	qt_sql_group_concat_Varchar_Varchar_AnyData_gb "select group_concat(distinct cast(abs(kint) as varchar) order by abs(ksint), kdt) from fn_test group by kbool order by kbool"
	qt_sql_group_concat_Varchar_Varchar_AnyData_gb "select group_concat(distinct cast(abs(kint) as varchar) order by abs(ksint), kdt) from fn_test"
	qt_sql_group_concat_Varchar_Varchar_AnyData_gb_notnull_gb "select group_concat(distinct cast(abs(kint) as varchar) order by abs(ksint), kdt) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_group_concat_Varchar_Varchar_AnyData_gb_notnull_gb_notnull "select group_concat(distinct cast(abs(kint) as varchar) order by abs(ksint), kdt) from fn_test_not_nullable"

	qt_sql_histogram_Boolean_gb "select histogram(kbool) from fn_test group by kbool order by kbool"
	qt_sql_histogram_Boolean_gb "select histogram(kbool) from fn_test"
	qt_sql_histogram_Boolean_gb_notnull_gb "select histogram(kbool) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_Boolean_gb_notnull_gb_notnull "select histogram(kbool) from fn_test_not_nullable"

	qt_sql_histogram_TinyInt_gb "select histogram(ktint) from fn_test group by kbool order by kbool"
	qt_sql_histogram_TinyInt_gb "select histogram(ktint) from fn_test"
	qt_sql_histogram_TinyInt_gb_notnull_gb "select histogram(ktint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_TinyInt_gb_notnull_gb_notnull "select histogram(ktint) from fn_test_not_nullable"

	qt_sql_histogram_SmallInt_gb "select histogram(ksint) from fn_test group by kbool order by kbool"
	qt_sql_histogram_SmallInt_gb "select histogram(ksint) from fn_test"
	qt_sql_histogram_SmallInt_gb_notnull_gb "select histogram(ksint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_SmallInt_gb_notnull_gb_notnull "select histogram(ksint) from fn_test_not_nullable"

	qt_sql_histogram_Integer_gb "select histogram(kint) from fn_test group by kbool order by kbool"
	qt_sql_histogram_Integer_gb "select histogram(kint) from fn_test"
	qt_sql_histogram_Integer_gb_notnull_gb "select histogram(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_Integer_gb_notnull_gb_notnull "select histogram(kint) from fn_test_not_nullable"

	qt_sql_histogram_BigInt_gb "select histogram(kbint) from fn_test group by kbool order by kbool"
	qt_sql_histogram_BigInt_gb "select histogram(kbint) from fn_test"
	qt_sql_histogram_BigInt_gb_notnull_gb "select histogram(kbint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_BigInt_gb_notnull_gb_notnull "select histogram(kbint) from fn_test_not_nullable"

	qt_sql_histogram_LargeInt_gb "select histogram(klint) from fn_test group by kbool order by kbool"
	qt_sql_histogram_LargeInt_gb "select histogram(klint) from fn_test"
	qt_sql_histogram_LargeInt_gb_notnull_gb "select histogram(klint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_LargeInt_gb_notnull_gb_notnull "select histogram(klint) from fn_test_not_nullable"

	qt_sql_histogram_Float_gb "select histogram(kfloat) from fn_test group by kbool order by kbool"
	qt_sql_histogram_Float_gb "select histogram(kfloat) from fn_test"
	qt_sql_histogram_Float_gb_notnull_gb "select histogram(kfloat) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_Float_gb_notnull_gb_notnull "select histogram(kfloat) from fn_test_not_nullable"

	qt_sql_histogram_Double_gb "select histogram(kdbl) from fn_test group by kbool order by kbool"
	qt_sql_histogram_Double_gb "select histogram(kdbl) from fn_test"
	qt_sql_histogram_Double_gb_notnull_gb "select histogram(kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_Double_gb_notnull_gb_notnull "select histogram(kdbl) from fn_test_not_nullable"

	qt_sql_histogram_DecimalV2_gb "select histogram(kdcmls1) from fn_test group by kbool order by kbool"
	qt_sql_histogram_DecimalV2_gb "select histogram(kdcmls1) from fn_test"
	qt_sql_histogram_DecimalV2_gb_notnull_gb "select histogram(kdcmls1) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_DecimalV2_gb_notnull_gb_notnull "select histogram(kdcmls1) from fn_test_not_nullable"

	qt_sql_histogram_Date_gb "select histogram(kdt) from fn_test group by kbool order by kbool"
	qt_sql_histogram_Date_gb "select histogram(kdt) from fn_test"
	qt_sql_histogram_Date_gb_notnull_gb "select histogram(kdt) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_Date_gb_notnull_gb_notnull "select histogram(kdt) from fn_test_not_nullable"

	qt_sql_histogram_DateTime_gb "select histogram(kdtm) from fn_test group by kbool order by kbool"
	qt_sql_histogram_DateTime_gb "select histogram(kdtm) from fn_test"
	qt_sql_histogram_DateTime_gb_notnull_gb "select histogram(kdtm) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_DateTime_gb_notnull_gb_notnull "select histogram(kdtm) from fn_test_not_nullable"

	qt_sql_histogram_DateV2_gb "select histogram(kdtv2) from fn_test group by kbool order by kbool"
	qt_sql_histogram_DateV2_gb "select histogram(kdtv2) from fn_test"
	qt_sql_histogram_DateV2_gb_notnull_gb "select histogram(kdtv2) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_DateV2_gb_notnull_gb_notnull "select histogram(kdtv2) from fn_test_not_nullable"

	qt_sql_histogram_DateTimeV2_gb "select histogram(kdtmv2s1) from fn_test group by kbool order by kbool"
	qt_sql_histogram_DateTimeV2_gb "select histogram(kdtmv2s1) from fn_test"
	qt_sql_histogram_DateTimeV2_gb_notnull_gb "select histogram(kdtmv2s1) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_DateTimeV2_gb_notnull_gb_notnull "select histogram(kdtmv2s1) from fn_test_not_nullable"

	qt_sql_histogram_Char_gb "select histogram(kchrs1) from fn_test group by kbool order by kbool"
	qt_sql_histogram_Char_gb "select histogram(kchrs1) from fn_test"
	qt_sql_histogram_Char_gb_notnull_gb "select histogram(kchrs1) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_Char_gb_notnull_gb_notnull "select histogram(kchrs1) from fn_test_not_nullable"

	qt_sql_histogram_String_gb "select histogram(kstr) from fn_test group by kbool order by kbool"
	qt_sql_histogram_String_gb "select histogram(kstr) from fn_test"
	qt_sql_histogram_String_gb_notnull_gb "select histogram(kstr) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_histogram_String_gb_notnull_gb_notnull "select histogram(kstr) from fn_test_not_nullable"

	qt_sql_max_by_AnyData_AnyData_gb "select max_by(kint, kint) from fn_test group by kbool order by kbool"
	qt_sql_max_by_AnyData_AnyData_gb "select max_by(kint, kint) from fn_test"
	qt_sql_max_by_AnyData_AnyData_gb_notnull_gb "select max_by(kint, kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_max_by_AnyData_AnyData_gb_notnull_gb_notnull "select max_by(kint, kint) from fn_test_not_nullable"

	qt_sql_min_by_AnyData_AnyData_gb "select min_by(kint, kint) from fn_test group by kbool order by kbool"
	qt_sql_min_by_AnyData_AnyData_gb "select min_by(kint, kint) from fn_test"
	qt_sql_min_by_AnyData_AnyData_gb_notnull_gb "select min_by(kint, kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_min_by_AnyData_AnyData_gb_notnull_gb_notnull "select min_by(kint, kint) from fn_test_not_nullable"

	qt_sql_multi_distinct_count_AnyData_gb "select multi_distinct_count(kint) from fn_test group by kbool order by kbool"
	qt_sql_multi_distinct_count_AnyData_gb "select multi_distinct_count(kint) from fn_test"
	qt_sql_multi_distinct_count_AnyData_gb_notnull_gb "select multi_distinct_count(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_multi_distinct_count_AnyData_gb_notnull_gb_notnull "select multi_distinct_count(kint) from fn_test_not_nullable"

	qt_sql_multi_distinct_sum_BigInt_gb "select multi_distinct_sum(kbint) from fn_test group by kbool order by kbool"
	qt_sql_multi_distinct_sum_BigInt_gb "select multi_distinct_sum(kbint) from fn_test"
	qt_sql_multi_distinct_sum_BigInt_gb_notnull_gb "select multi_distinct_sum(kbint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_multi_distinct_sum_BigInt_gb_notnull_gb_notnull "select multi_distinct_sum(kbint) from fn_test_not_nullable"

	qt_sql_multi_distinct_sum_Double_gb "select multi_distinct_sum(kdbl) from fn_test group by kbool order by kbool"
	qt_sql_multi_distinct_sum_Double_gb "select multi_distinct_sum(kdbl) from fn_test"
	qt_sql_multi_distinct_sum_Double_gb_notnull_gb "select multi_distinct_sum(kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_multi_distinct_sum_Double_gb_notnull_gb_notnull "select multi_distinct_sum(kdbl) from fn_test_not_nullable"

	qt_sql_multi_distinct_sum_LargeInt_gb "select multi_distinct_sum(klint) from fn_test group by kbool order by kbool"
	qt_sql_multi_distinct_sum_LargeInt_gb "select multi_distinct_sum(klint) from fn_test"
	qt_sql_multi_distinct_sum_LargeInt_gb_notnull_gb "select multi_distinct_sum(klint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_multi_distinct_sum_LargeInt_gb_notnull_gb_notnull "select multi_distinct_sum(klint) from fn_test_not_nullable"

	qt_sql_ndv_AnyData_gb "select ndv(kint) from fn_test group by kbool order by kbool"
	qt_sql_ndv_AnyData_gb "select ndv(kint) from fn_test"
	qt_sql_ndv_AnyData_gb_notnull_gb "select ndv(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_ndv_AnyData_gb_notnull_gb_notnull "select ndv(kint) from fn_test_not_nullable"

	qt_sql_orthogonal_bitmap_union_count_Bitmap_gb "select orthogonal_bitmap_union_count(to_bitmap(kbint)) from fn_test group by kbool order by kbool"
	qt_sql_orthogonal_bitmap_union_count_Bitmap_gb "select orthogonal_bitmap_union_count(to_bitmap(kbint)) from fn_test"
	qt_sql_orthogonal_bitmap_union_count_Bitmap_gb_notnull_gb "select orthogonal_bitmap_union_count(to_bitmap(kbint)) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_orthogonal_bitmap_union_count_Bitmap_gb_notnull_gb_notnull "select orthogonal_bitmap_union_count(to_bitmap(kbint)) from fn_test_not_nullable"

	qt_sql_percentile_BigInt_Double_gb "select percentile(kbint, 0.6) from fn_test group by kbool order by kbool"
	qt_sql_percentile_BigInt_Double_gb "select percentile(kbint, 0.6) from fn_test"
	qt_sql_percentile_BigInt_Double_gb_notnull_gb "select percentile(kbint, 0.6) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_percentile_BigInt_Double_gb_notnull_gb_notnull "select percentile(kbint, 0.6) from fn_test_not_nullable"

	qt_sql_percentile_approx_Double_Double_gb "select percentile_approx(kdbl, 0.6) from fn_test group by kbool order by kbool"
	qt_sql_percentile_approx_Double_Double_gb "select percentile_approx(kdbl, 0.6) from fn_test"
	qt_sql_percentile_approx_Double_Double_gb_notnull_gb "select percentile_approx(kdbl, 0.6) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_percentile_approx_Double_Double_gb_notnull_gb_notnull "select percentile_approx(kdbl, 0.6) from fn_test_not_nullable"

	qt_sql_percentile_approx_Double_Double_Double_gb "select percentile_approx(kdbl, 0.6, 4096.0) from fn_test group by kbool order by kbool"
	qt_sql_percentile_approx_Double_Double_Double_gb "select percentile_approx(kdbl, 0.6, 4096.0) from fn_test"
	qt_sql_percentile_approx_Double_Double_Double_gb_notnull_gb "select percentile_approx(kdbl, 0.6, 4096.0) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_percentile_approx_Double_Double_Double_gb_notnull_gb_notnull "select percentile_approx(kdbl, 0.6, 4096.0) from fn_test_not_nullable"

	qt_sql_sequence_count_String_DateV2_Boolean_gb "select sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from fn_test group by kbool order by kbool"
	qt_sql_sequence_count_String_DateV2_Boolean_gb "select sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from fn_test"
	qt_sql_sequence_count_String_DateV2_Boolean_gb_notnull_gb "select sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_sequence_count_String_DateV2_Boolean_gb_notnull_gb_notnull "select sequence_count('(?1)(?2)', kdtv2, kint = 1, kint = 2) from fn_test_not_nullable"

	qt_sql_sequence_count_String_DateTime_Boolean_gb "select sequence_count('(?1)(?2)', kdtm, kint = 1, kint = 2) from fn_test group by kbool order by kbool"
	qt_sql_sequence_count_String_DateTime_Boolean_gb "select sequence_count('(?1)(?2)', kdtm, kint = 1, kint = 2) from fn_test"
	qt_sql_sequence_count_String_DateTime_Boolean_gb_notnull_gb "select sequence_count('(?1)(?2)', kdtm, kint = 1, kint = 2) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_sequence_count_String_DateTime_Boolean_gb_notnull_gb_notnull "select sequence_count('(?1)(?2)', kdtm, kint = 1, kint = 2) from fn_test_not_nullable"

	qt_sql_sequence_count_String_DateTimeV2_Boolean_gb "select sequence_count('(?1)(?2)', kdtmv2s1, kint = 1, kint = 5) from fn_test group by kbool order by kbool"
	qt_sql_sequence_count_String_DateTimeV2_Boolean_gb "select sequence_count('(?1)(?2)', kdtmv2s1, kint = 1, kint = 5) from fn_test"
	qt_sql_sequence_count_String_DateTimeV2_Boolean_gb_notnull_gb "select sequence_count('(?1)(?2)', kdtmv2s1, kint = 1, kint = 5) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_sequence_count_String_DateTimeV2_Boolean_gb_notnull_gb_notnull "select sequence_count('(?1)(?2)', kdtmv2s1, kint = 1, kint = 5) from fn_test_not_nullable"

	qt_sql_sequence_match_String_DateV2_Boolean_gb "select sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from fn_test group by kbool order by kbool"
	qt_sql_sequence_match_String_DateV2_Boolean_gb "select sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from fn_test"
	qt_sql_sequence_match_String_DateV2_Boolean_gb_notnull_gb "select sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_sequence_match_String_DateV2_Boolean_gb_notnull_gb_notnull "select sequence_match('(?1)(?2)', kdtv2, kint = 1, kint = 2) from fn_test_not_nullable"

	qt_sql_sequence_match_String_DateTime_Boolean_gb "select sequence_match('(?1)(?2)', kdtm, kint = 1, kint = 2) from fn_test group by kbool order by kbool"
	qt_sql_sequence_match_String_DateTime_Boolean_gb "select sequence_match('(?1)(?2)', kdtm, kint = 1, kint = 2) from fn_test"
	qt_sql_sequence_match_String_DateTime_Boolean_gb_notnull_gb "select sequence_match('(?1)(?2)', kdtm, kint = 1, kint = 2) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_sequence_match_String_DateTime_Boolean_gb_notnull_gb_notnull "select sequence_match('(?1)(?2)', kdtm, kint = 1, kint = 2) from fn_test_not_nullable"

	qt_sql_sequence_match_String_DateTimeV2_Boolean_gb "select sequence_match('(?1)(?2)', kdtmv2s1, kint = 1, kint = 2) from fn_test group by kbool order by kbool"
	qt_sql_sequence_match_String_DateTimeV2_Boolean_gb "select sequence_match('(?1)(?2)', kdtmv2s1, kint = 1, kint = 2) from fn_test"
	qt_sql_sequence_match_String_DateTimeV2_Boolean_gb_notnull_gb "select sequence_match('(?1)(?2)', kdtmv2s1, kint = 1, kint = 2) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_sequence_match_String_DateTimeV2_Boolean_gb_notnull_gb_notnull "select sequence_match('(?1)(?2)', kdtmv2s1, kint = 1, kint = 2) from fn_test_not_nullable"

	qt_sql_stddev_TinyInt_gb "select stddev(ktint) from fn_test group by kbool order by kbool"
	qt_sql_stddev_TinyInt_gb "select stddev(ktint) from fn_test"
	qt_sql_stddev_TinyInt_gb_notnull_gb "select stddev(ktint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_stddev_TinyInt_gb_notnull_gb_notnull "select stddev(ktint) from fn_test_not_nullable"

	qt_sql_stddev_SmallInt_gb "select stddev(ksint) from fn_test group by kbool order by kbool"
	qt_sql_stddev_SmallInt_gb "select stddev(ksint) from fn_test"
	qt_sql_stddev_SmallInt_gb_notnull_gb "select stddev(ksint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_stddev_SmallInt_gb_notnull_gb_notnull "select stddev(ksint) from fn_test_not_nullable"

	qt_sql_stddev_Integer_gb "select stddev(kint) from fn_test group by kbool order by kbool"
	qt_sql_stddev_Integer_gb "select stddev(kint) from fn_test"
	qt_sql_stddev_Integer_gb_notnull_gb "select stddev(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_stddev_Integer_gb_notnull_gb_notnull "select stddev(kint) from fn_test_not_nullable"

	qt_sql_stddev_BigInt_gb "select stddev(kbint) from fn_test group by kbool order by kbool"
	qt_sql_stddev_BigInt_gb "select stddev(kbint) from fn_test"
	qt_sql_stddev_BigInt_gb_notnull_gb "select stddev(kbint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_stddev_BigInt_gb_notnull_gb_notnull "select stddev(kbint) from fn_test_not_nullable"

	qt_sql_stddev_Float_gb "select stddev(kfloat) from fn_test group by kbool order by kbool"
	qt_sql_stddev_Float_gb "select stddev(kfloat) from fn_test"
	qt_sql_stddev_Float_gb_notnull_gb "select stddev(kfloat) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_stddev_Float_gb_notnull_gb_notnull "select stddev(kfloat) from fn_test_not_nullable"

	qt_sql_stddev_Double_gb "select stddev(kdbl) from fn_test group by kbool order by kbool"
	qt_sql_stddev_Double_gb "select stddev(kdbl) from fn_test"
	qt_sql_stddev_Double_gb_notnull_gb "select stddev(kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_stddev_Double_gb_notnull_gb_notnull "select stddev(kdbl) from fn_test_not_nullable"

	qt_sql_stddev_DecimalV2_gb "select stddev(kdcmls1) from fn_test group by kbool order by kbool"
	qt_sql_stddev_DecimalV2_gb "select stddev(kdcmls1) from fn_test"
	qt_sql_stddev_DecimalV2_gb_notnull_gb "select stddev(kdcmls1) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_stddev_DecimalV2_gb_notnull_gb_notnull "select stddev(kdcmls1) from fn_test_not_nullable"

	qt_sql_stddev_samp_TinyInt_gb "select stddev_samp(ktint) from fn_test group by kbool order by kbool"
	qt_sql_stddev_samp_TinyInt_gb "select stddev_samp(ktint) from fn_test"
	qt_sql_stddev_samp_TinyInt_gb_notnull_gb "select stddev_samp(ktint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_stddev_samp_TinyInt_gb_notnull_gb_notnull "select stddev_samp(ktint) from fn_test_not_nullable"

	qt_sql_stddev_samp_SmallInt_gb "select stddev_samp(ksint) from fn_test group by kbool order by kbool"
	qt_sql_stddev_samp_SmallInt_gb "select stddev_samp(ksint) from fn_test"
	qt_sql_stddev_samp_SmallInt_gb_notnull_gb "select stddev_samp(ksint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_stddev_samp_SmallInt_gb_notnull_gb_notnull "select stddev_samp(ksint) from fn_test_not_nullable"

	qt_sql_stddev_samp_Integer_gb "select stddev_samp(kint) from fn_test group by kbool order by kbool"
	qt_sql_stddev_samp_Integer_gb "select stddev_samp(kint) from fn_test"
	qt_sql_stddev_samp_Integer_gb_notnull_gb "select stddev_samp(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_stddev_samp_Integer_gb_notnull_gb_notnull "select stddev_samp(kint) from fn_test_not_nullable"

	qt_sql_stddev_samp_BigInt_gb "select stddev_samp(kbint) from fn_test group by kbool order by kbool"
	qt_sql_stddev_samp_BigInt_gb "select stddev_samp(kbint) from fn_test"
	qt_sql_stddev_samp_BigInt_gb_notnull_gb "select stddev_samp(kbint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_stddev_samp_BigInt_gb_notnull_gb_notnull "select stddev_samp(kbint) from fn_test_not_nullable"

	qt_sql_stddev_samp_Float_gb "select stddev_samp(kfloat) from fn_test group by kbool order by kbool"
	qt_sql_stddev_samp_Float_gb "select stddev_samp(kfloat) from fn_test"
	qt_sql_stddev_samp_Float_gb_notnull_gb "select stddev_samp(kfloat) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_stddev_samp_Float_gb_notnull_gb_notnull "select stddev_samp(kfloat) from fn_test_not_nullable"

	qt_sql_stddev_samp_Double_gb "select stddev_samp(kdbl) from fn_test group by kbool order by kbool"
	qt_sql_stddev_samp_Double_gb "select stddev_samp(kdbl) from fn_test"
	qt_sql_stddev_samp_Double_gb_notnull_gb "select stddev_samp(kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_stddev_samp_Double_gb_notnull_gb_notnull "select stddev_samp(kdbl) from fn_test_not_nullable"

	qt_sql_stddev_samp_DecimalV2_gb "select stddev_samp(kdcmls1) from fn_test group by kbool order by kbool"
	qt_sql_stddev_samp_DecimalV2_gb "select stddev_samp(kdcmls1) from fn_test"
	qt_sql_stddev_samp_DecimalV2_gb_notnull_gb "select stddev_samp(kdcmls1) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_stddev_samp_DecimalV2_gb_notnull_gb_notnull "select stddev_samp(kdcmls1) from fn_test_not_nullable"

	qt_sql_sum_TinyInt_gb "select sum(ktint) from fn_test group by kbool order by kbool"
	qt_sql_sum_TinyInt_gb "select sum(ktint) from fn_test"
	qt_sql_sum_TinyInt_gb_notnull_gb "select sum(ktint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_sum_TinyInt_gb_notnull_gb_notnull "select sum(ktint) from fn_test_not_nullable"

	qt_sql_sum_SmallInt_gb "select sum(ksint) from fn_test group by kbool order by kbool"
	qt_sql_sum_SmallInt_gb "select sum(ksint) from fn_test"
	qt_sql_sum_SmallInt_gb_notnull_gb "select sum(ksint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_sum_SmallInt_gb_notnull_gb_notnull "select sum(ksint) from fn_test_not_nullable"

	qt_sql_sum_Integer_gb "select sum(kint) from fn_test group by kbool order by kbool"
	qt_sql_sum_Integer_gb "select sum(kint) from fn_test"
	qt_sql_sum_Integer_gb_notnull_gb "select sum(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_sum_Integer_gb_notnull_gb_notnull "select sum(kint) from fn_test_not_nullable"

	qt_sql_sum_BigInt_gb "select sum(kbint) from fn_test group by kbool order by kbool"
	qt_sql_sum_BigInt_gb "select sum(kbint) from fn_test"
	qt_sql_sum_BigInt_gb_notnull_gb "select sum(kbint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_sum_BigInt_gb_notnull_gb_notnull "select sum(kbint) from fn_test_not_nullable"

	qt_sql_sum_Double_gb "select sum(kdbl) from fn_test group by kbool order by kbool"
	qt_sql_sum_Double_gb "select sum(kdbl) from fn_test"
	qt_sql_sum_Double_gb_notnull_gb "select sum(kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_sum_Double_gb_notnull_gb_notnull "select sum(kdbl) from fn_test_not_nullable"

	qt_sql_sum_DecimalV2_gb "select sum(kdcmls1) from fn_test group by kbool order by kbool"
	qt_sql_sum_DecimalV2_gb "select sum(kdcmls1) from fn_test"
	qt_sql_sum_DecimalV2_gb_notnull_gb "select sum(kdcmls1) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_sum_DecimalV2_gb_notnull_gb_notnull "select sum(kdcmls1) from fn_test_not_nullable"

	qt_sql_sum_LargeInt_gb "select sum(klint) from fn_test group by kbool order by kbool"
	qt_sql_sum_LargeInt_gb "select sum(klint) from fn_test"
	qt_sql_sum_LargeInt_gb_notnull_gb "select sum(klint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_sum_LargeInt_gb_notnull_gb_notnull "select sum(klint) from fn_test_not_nullable"

	qt_sql_topn_Varchar_Integer_gb "select topn(kvchrs1, 3) from fn_test group by kbool order by kbool"
	qt_sql_topn_Varchar_Integer_gb "select topn(kvchrs1, 3) from fn_test"
	qt_sql_topn_Varchar_Integer_gb_notnull_gb "select topn(kvchrs1, 3) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_topn_Varchar_Integer_gb_notnull_gb_notnull "select topn(kvchrs1, 3) from fn_test_not_nullable"

	qt_sql_topn_String_Integer_gb "select topn(kstr, 3) from fn_test group by kbool order by kbool"
	qt_sql_topn_String_Integer_gb "select topn(kstr, 3) from fn_test"
	qt_sql_topn_String_Integer_gb_notnull_gb "select topn(kstr, 3) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_topn_String_Integer_gb_notnull_gb_notnull "select topn(kstr, 3) from fn_test_not_nullable"

	qt_sql_topn_Varchar_Integer_Integer_gb "select topn(kvchrs1, 3, 100) from fn_test group by kbool order by kbool"
	qt_sql_topn_Varchar_Integer_Integer_gb "select topn(kvchrs1, 3, 100) from fn_test"
	qt_sql_topn_Varchar_Integer_Integer_gb_notnull_gb "select topn(kvchrs1, 3, 100) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_topn_Varchar_Integer_Integer_gb_notnull_gb_notnull "select topn(kvchrs1, 3, 100) from fn_test_not_nullable"

	qt_sql_topn_String_Integer_Integer_gb "select topn(kstr, 3, 100) from fn_test group by kbool order by kbool"
	qt_sql_topn_String_Integer_Integer_gb "select topn(kstr, 3, 100) from fn_test"
	qt_sql_topn_String_Integer_Integer_gb_notnull_gb "select topn(kstr, 3, 100) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_topn_String_Integer_Integer_gb_notnull_gb_notnull "select topn(kstr, 3, 100) from fn_test_not_nullable"

	qt_sql_variance_TinyInt_gb "select variance(ktint) from fn_test group by kbool order by kbool"
	qt_sql_variance_TinyInt_gb "select variance(ktint) from fn_test"
	qt_sql_variance_TinyInt_gb_notnull_gb "select variance(ktint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_variance_TinyInt_gb_notnull_gb_notnull "select variance(ktint) from fn_test_not_nullable"

	qt_sql_variance_SmallInt_gb "select variance(ksint) from fn_test group by kbool order by kbool"
	qt_sql_variance_SmallInt_gb "select variance(ksint) from fn_test"
	qt_sql_variance_SmallInt_gb_notnull_gb "select variance(ksint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_variance_SmallInt_gb_notnull_gb_notnull "select variance(ksint) from fn_test_not_nullable"

	qt_sql_variance_Integer_gb "select variance(kint) from fn_test group by kbool order by kbool"
	qt_sql_variance_Integer_gb "select variance(kint) from fn_test"
	qt_sql_variance_Integer_gb_notnull_gb "select variance(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_variance_Integer_gb_notnull_gb_notnull "select variance(kint) from fn_test_not_nullable"

	qt_sql_variance_BigInt_gb "select variance(kbint) from fn_test group by kbool order by kbool"
	qt_sql_variance_BigInt_gb "select variance(kbint) from fn_test"
	qt_sql_variance_BigInt_gb_notnull_gb "select variance(kbint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_variance_BigInt_gb_notnull_gb_notnull "select variance(kbint) from fn_test_not_nullable"

	qt_sql_variance_Float_gb "select variance(kfloat) from fn_test group by kbool order by kbool"
	qt_sql_variance_Float_gb "select variance(kfloat) from fn_test"
	qt_sql_variance_Float_gb_notnull_gb "select variance(kfloat) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_variance_Float_gb_notnull_gb_notnull "select variance(kfloat) from fn_test_not_nullable"

	qt_sql_variance_Double_gb "select variance(kdbl) from fn_test group by kbool order by kbool"
	qt_sql_variance_Double_gb "select variance(kdbl) from fn_test"
	qt_sql_variance_Double_gb_notnull_gb "select variance(kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_variance_Double_gb_notnull_gb_notnull "select variance(kdbl) from fn_test_not_nullable"

	qt_sql_variance_DecimalV2_gb "select variance(kdcmls1) from fn_test group by kbool order by kbool"
	qt_sql_variance_DecimalV2_gb "select variance(kdcmls1) from fn_test"
	qt_sql_variance_DecimalV2_gb_notnull_gb "select variance(kdcmls1) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_variance_DecimalV2_gb_notnull_gb_notnull "select variance(kdcmls1) from fn_test_not_nullable"

	qt_sql_variance_samp_TinyInt_gb "select variance_samp(ktint) from fn_test group by kbool order by kbool"
	qt_sql_variance_samp_TinyInt_gb "select variance_samp(ktint) from fn_test"
	qt_sql_variance_samp_TinyInt_gb_notnull_gb "select variance_samp(ktint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_variance_samp_TinyInt_gb_notnull_gb_notnull "select variance_samp(ktint) from fn_test_not_nullable"

	qt_sql_variance_samp_SmallInt_gb "select variance_samp(ksint) from fn_test group by kbool order by kbool"
	qt_sql_variance_samp_SmallInt_gb "select variance_samp(ksint) from fn_test"
	qt_sql_variance_samp_SmallInt_gb_notnull_gb "select variance_samp(ksint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_variance_samp_SmallInt_gb_notnull_gb_notnull "select variance_samp(ksint) from fn_test_not_nullable"

	qt_sql_variance_samp_Integer_gb "select variance_samp(kint) from fn_test group by kbool order by kbool"
	qt_sql_variance_samp_Integer_gb "select variance_samp(kint) from fn_test"
	qt_sql_variance_samp_Integer_gb_notnull_gb "select variance_samp(kint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_variance_samp_Integer_gb_notnull_gb_notnull "select variance_samp(kint) from fn_test_not_nullable"

	qt_sql_variance_samp_BigInt_gb "select variance_samp(kbint) from fn_test group by kbool order by kbool"
	qt_sql_variance_samp_BigInt_gb "select variance_samp(kbint) from fn_test"
	qt_sql_variance_samp_BigInt_gb_notnull_gb "select variance_samp(kbint) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_variance_samp_BigInt_gb_notnull_gb_notnull "select variance_samp(kbint) from fn_test_not_nullable"

	qt_sql_variance_samp_Float_gb "select variance_samp(kfloat) from fn_test group by kbool order by kbool"
	qt_sql_variance_samp_Float_gb "select variance_samp(kfloat) from fn_test"
	qt_sql_variance_samp_Float_gb_notnull_gb "select variance_samp(kfloat) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_variance_samp_Float_gb_notnull_gb_notnull "select variance_samp(kfloat) from fn_test_not_nullable"

	qt_sql_variance_samp_Double_gb "select variance_samp(kdbl) from fn_test group by kbool order by kbool"
	qt_sql_variance_samp_Double_gb "select variance_samp(kdbl) from fn_test"
	qt_sql_variance_samp_Double_gb_notnull_gb "select variance_samp(kdbl) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_variance_samp_Double_gb_notnull_gb_notnull "select variance_samp(kdbl) from fn_test_not_nullable"

	qt_sql_variance_samp_DecimalV2_gb "select variance_samp(kdcmls1) from fn_test group by kbool order by kbool"
	qt_sql_variance_samp_DecimalV2_gb "select variance_samp(kdcmls1) from fn_test"
	qt_sql_variance_samp_DecimalV2_gb_notnull_gb "select variance_samp(kdcmls1) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_variance_samp_DecimalV2_gb_notnull_gb_notnull "select variance_samp(kdcmls1) from fn_test_not_nullable"

	qt_sql_window_funnel_BigInt_String_DateTime_Boolean_gb "select window_funnel(kbint, kstr, kdtm, kbool) from fn_test group by kbool order by kbool"
	qt_sql_window_funnel_BigInt_String_DateTime_Boolean_gb "select window_funnel(kbint, kstr, kdtm, kbool) from fn_test"
	qt_sql_window_funnel_BigInt_String_DateTime_Boolean_gb_notnull_gb "select window_funnel(kbint, kstr, kdtm, kbool) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_window_funnel_BigInt_String_DateTime_Boolean_gb_notnull_gb_notnull "select window_funnel(kbint, kstr, kdtm, kbool) from fn_test_not_nullable"

	qt_sql_window_funnel_BigInt_String_DateTimeV2_Boolean_gb "select window_funnel(kbint, kstr, kdtmv2s1, kbool) from fn_test group by kbool order by kbool"
	qt_sql_window_funnel_BigInt_String_DateTimeV2_Boolean_gb "select window_funnel(kbint, kstr, kdtmv2s1, kbool) from fn_test"
	qt_sql_window_funnel_BigInt_String_DateTimeV2_Boolean_gb_notnull_gb "select window_funnel(kbint, kstr, kdtmv2s1, kbool) from fn_test_not_nullable group by kbool order by kbool"
	qt_sql_window_funnel_BigInt_String_DateTimeV2_Boolean_gb_notnull_gb_notnull "select window_funnel(kbint, kstr, kdtmv2s1, kbool) from fn_test_not_nullable"

}