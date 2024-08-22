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

suite("nereids_gen_fn") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'

	qt_sql_explode_bitmap_Bitmap '''
		select id, e from fn_test lateral view explode_bitmap(to_bitmap(kbint)) lv as e order by id, e'''
	qt_sql_explode_bitmap_Bitmap_notnull '''
		select id, e from fn_test_not_nullable lateral view explode_bitmap(to_bitmap(kbint)) lv as e order by id, e'''

	qt_sql_explode_bitmap_outer_Bitmap '''
		select id, e from fn_test lateral view explode_bitmap_outer(to_bitmap(kbint)) lv as e order by id, e'''
	qt_sql_explode_bitmap_outer_Bitmap_notnull '''
		select id, e from fn_test_not_nullable lateral view explode_bitmap_outer(to_bitmap(kbint)) lv as e order by id, e'''

	qt_sql_explode_numbers_Integer '''
		select id, e from fn_test lateral view explode_numbers(kint) lv as e order by id, e'''
	qt_sql_explode_numbers_Integer_notnull '''
		select id, e from fn_test_not_nullable lateral view explode_numbers(kint) lv as e order by id, e'''

	qt_sql_explode_numbers_outer_Integer '''
		select id, e from fn_test lateral view explode_numbers_outer(kint) lv as e order by id, e'''
	qt_sql_explode_numbers_outer_Integer_notnull '''
		select id, e from fn_test_not_nullable lateral view explode_numbers_outer(kint) lv as e order by id, e'''

	qt_sql_explode_split_Varchar_Varchar '''
		select id, e from fn_test lateral view explode_split('a, b, c, d', ',') lv as e order by id, e'''
	qt_sql_explode_split_Varchar_Varchar_notnull '''
		select id, e from fn_test lateral view explode_split('a, b, c, d', ',') lv as e order by id, e'''

	qt_sql_explode_split_outer_Varchar_Varchar '''
		select id, e from fn_test lateral view explode_split_outer('a, b, c, d', ',') lv as e order by id, e'''
	qt_sql_explode_split_outer_Varchar_Varchar_notnull '''
		select id, e from fn_test lateral view explode_split_outer('a, b, c, d', ',') lv as e order by id, e'''

	qt_sql_explode_json_array_int_Varchar '''
		select id, e from fn_test lateral view explode_json_array_int('[1, 2, 3]') lv as e order by id, e'''

	qt_sql_explode_json_array_double_Varchar '''
		select id, e from fn_test lateral view explode_json_array_double('[1.1, 2.2, 3.3]') lv as e order by id, e'''

	qt_sql_explode_json_array_string_Varchar '''
		select id, e from fn_test lateral view explode_json_array_string('["1", "2", "3"]') lv as e order by id, e'''

	qt_sql_explode_json_array_json_Varchar '''
		select id, e from fn_test lateral view explode_json_array_json('[{"id":1,"name":"John"},{"id":2,"name":"Mary"},{"id":3,"name":"Bob"}]') lv as e order by id, e'''

	qt_sql_explode_json_array_json_Json '''
		select id, e from fn_test lateral view explode_json_array_json(cast('[{"id":1,"name":"John"},{"id":2,"name":"Mary"},{"id":3,"name":"Bob"}]' as json)) lv as e order by id, cast(e as string); '''

	// explode
	order_qt_sql_explode_Double "select id, e from fn_test lateral view explode(kadbl) lv as e order by id, e"
	order_qt_sql_explode_Double_notnull "select id, e from fn_test_not_nullable lateral view explode(kadbl) lv as e order by id, e"
	order_qt_sql_explode_Float "select id, e from fn_test lateral view explode(kafloat) lv as e order by id, e"
	order_qt_sql_explode_Float_notnull "select id, e from fn_test_not_nullable lateral view explode(kafloat) lv as e order by id, e"
	order_qt_sql_explode_LargeInt "select id, e from fn_test lateral view explode(kalint) lv as e order by id, e"
	order_qt_sql_explode_LargeInt_notnull "select id, e from fn_test_not_nullable lateral view explode(kalint) lv as e order by id, e"
	order_qt_sql_explode_BigInt "select id, e from fn_test lateral view explode(kabint) lv as e order by id, e"
	order_qt_sql_explode_BigInt_notnull "select id, e from fn_test_not_nullable lateral view explode(kabint) lv as e order by id, e"
	order_qt_sql_explode_SmallInt "select id, e from fn_test lateral view explode(kasint) lv as e order by id, e"
	order_qt_sql_explode_SmallInt_notnull "select id, e from fn_test_not_nullable lateral view explode(kasint) lv as e order by id, e"
	order_qt_sql_explode_Integer "select id, e from fn_test lateral view explode(kaint) lv as e order by id, e"
	order_qt_sql_explode_Integer_notnull "select id, e from fn_test_not_nullable lateral view explode(kaint) lv as e order by id, e"
	order_qt_sql_explode_TinyInt "select id, e from fn_test lateral view explode(katint) lv as e order by id, e"
	order_qt_sql_explode_TinyInt_notnull "select id, e from fn_test_not_nullable lateral view explode(katint) lv as e order by id, e"
	order_qt_sql_explode_DecimalV3 "select id, e from fn_test lateral view explode(kadcml) lv as e order by id, e"
	order_qt_sql_explode_DecimalV3_notnull "select id, e from fn_test_not_nullable lateral view explode(kadcml) lv as e order by id, e"

	order_qt_sql_explode_Boolean "select id, e from fn_test lateral view explode(kabool) lv as e order by id, e"
	order_qt_sql_explode_Boolean_notnull "select id, e from fn_test_not_nullable lateral view explode(kabool) lv as e order by id, e"

	order_qt_sql_explode_Char "select id, e from fn_test lateral view explode(kachr) lv as e order by id, e"
	order_qt_sql_explode_Char_notnull "select id, e from fn_test_not_nullable lateral view explode(kachr) lv as e order by id, e"
	order_qt_sql_explode_Varchar "select id, e from fn_test lateral view explode(kavchr) lv as e order by id, e"
	order_qt_sql_explode_Varchar_notnull "select id, e from fn_test_not_nullable lateral view explode(kavchr) lv as e order by id, e"
	order_qt_sql_explode_String "select id, e from fn_test lateral view explode(kastr) lv as e order by id, e"
	order_qt_sql_explode_String_notnull "select id, e from fn_test_not_nullable lateral view explode(kastr) lv as e order by id, e"

	order_qt_sql_explode_DatetimeV2 "select id, e from fn_test lateral view explode(kadtmv2) lv as e order by id, e"
	order_qt_sql_explode_DatetimeV2_notnull "select id, e from fn_test_not_nullable lateral view explode(kadtmv2) lv as e order by id, e"
	order_qt_sql_explode_DateV2 "select id, e from fn_test lateral view explode(kadtv2) lv as e order by id, e"
	order_qt_sql_explode_DateV2_notnull "select id, e from fn_test_not_nullable lateral view explode(kadtv2) lv as e order by id, e"
}
