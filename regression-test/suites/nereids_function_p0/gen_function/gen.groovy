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

}