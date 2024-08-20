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

suite("nereids_scalar_fn_A") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_abs_Double "select abs(kdbl) from fn_test order by kdbl"
	qt_sql_abs_Double_notnull "select abs(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_abs_Float "select abs(kfloat) from fn_test order by kfloat"
	qt_sql_abs_Float_notnull "select abs(kfloat) from fn_test_not_nullable order by kfloat"
	qt_sql_abs_LargeInt "select abs(klint) from fn_test order by klint"
	qt_sql_abs_LargeInt_notnull "select abs(klint) from fn_test_not_nullable order by klint"
	qt_sql_abs_BigInt "select abs(kbint) from fn_test order by kbint"
	qt_sql_abs_BigInt_notnull "select abs(kbint) from fn_test_not_nullable order by kbint"
	qt_sql_abs_SmallInt "select abs(ksint) from fn_test order by ksint"
	qt_sql_abs_SmallInt_notnull "select abs(ksint) from fn_test_not_nullable order by ksint"
	qt_sql_abs_Integer "select abs(kint) from fn_test order by kint"
	qt_sql_abs_Integer_notnull "select abs(kint) from fn_test_not_nullable order by kint"
	qt_sql_abs_TinyInt "select abs(ktint) from fn_test order by ktint"
	qt_sql_abs_TinyInt_notnull "select abs(ktint) from fn_test_not_nullable order by ktint"
	qt_sql_abs_DecimalV2 "select abs(kdcmls1) from fn_test order by kdcmls1"
	qt_sql_abs_DecimalV2_notnull "select abs(kdcmls1) from fn_test_not_nullable order by kdcmls1"
	qt_sql_acos_Double "select acos(kdbl) from fn_test order by kdbl"
	qt_sql_acos_Double_notnull "select acos(kdbl) from fn_test_not_nullable order by kdbl"
    qt_sql_acos_Double_NAN "select acos(cast(1.1 as double))"
	qt_sql_acos_Double_NULL "select acos(null)"
	sql "select aes_decrypt(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	sql "select aes_decrypt(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	sql "select aes_decrypt(kstr, kstr) from fn_test order by kstr, kstr"
	sql "select aes_decrypt(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	sql "select aes_decrypt(kvchrs1, kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1, kvchrs1"
	sql "select aes_decrypt(kvchrs1, kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1, kvchrs1"
	sql "select aes_decrypt(kstr, kstr, kstr) from fn_test order by kstr, kstr, kstr"
	sql "select aes_decrypt(kstr, kstr, kstr) from fn_test_not_nullable order by kstr, kstr, kstr"
	sql "select aes_decrypt(kvchrs1, kvchrs1, kvchrs1, 'AES_128_ECB') from fn_test order by kvchrs1, kvchrs1, kvchrs1"
	sql "select aes_decrypt(kvchrs1, kvchrs1, kvchrs1, 'AES_128_ECB') from fn_test_not_nullable order by kvchrs1, kvchrs1, kvchrs1"
	sql "select aes_decrypt(kstr, kstr, kstr, 'AES_128_ECB') from fn_test order by kstr, kstr, kstr, kstr"
	sql "select aes_decrypt(kstr, kstr, kstr, 'AES_128_ECB') from fn_test_not_nullable order by kstr, kstr, kstr, kstr"
	sql "select aes_encrypt(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	sql "select aes_encrypt(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	sql "select aes_encrypt(kstr, kstr) from fn_test order by kstr, kstr"
	sql "select aes_encrypt(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	sql "select aes_encrypt(kvchrs1, kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1, kvchrs1"
	sql "select aes_encrypt(kvchrs1, kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1, kvchrs1"
	sql "select aes_encrypt(kstr, kstr, kstr) from fn_test order by kstr, kstr, kstr"
	sql "select aes_encrypt(kstr, kstr, kstr) from fn_test_not_nullable order by kstr, kstr, kstr"
	sql "select aes_encrypt(kvchrs1, kvchrs1, kvchrs1, 'AES_128_ECB') from fn_test order by kvchrs1, kvchrs1, kvchrs1"
	sql "select aes_encrypt(kvchrs1, kvchrs1, kvchrs1, 'AES_128_ECB') from fn_test_not_nullable order by kvchrs1, kvchrs1, kvchrs1"
	sql "select aes_encrypt(kstr, kstr, kstr, 'AES_128_ECB') from fn_test order by kstr, kstr, kstr, kstr"
	sql "select aes_encrypt(kstr, kstr, kstr, 'AES_128_ECB') from fn_test_not_nullable order by kstr, kstr, kstr, kstr"
	qt_sql_append_trailing_char_if_absent_Varchar_Varchar "select append_trailing_char_if_absent(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_append_trailing_char_if_absent_Varchar_Varchar_notnull "select append_trailing_char_if_absent(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	qt_sql_append_trailing_char_if_absent_String_String "select append_trailing_char_if_absent(kstr, kstr) from fn_test order by kstr, kstr"
	qt_sql_append_trailing_char_if_absent_String_String_notnull "select append_trailing_char_if_absent(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	qt_sql_ascii_Varchar "select ascii(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_ascii_Varchar_notnull "select ascii(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_ascii_String "select ascii(kstr) from fn_test order by kstr"
	qt_sql_ascii_String_notnull "select ascii(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_asin_Double "select asin(kdbl) from fn_test order by kdbl"
	qt_sql_asin_Double_notnull "select asin(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_atan_Double "select atan(kdbl) from fn_test order by kdbl"
	qt_sql_atan_Double_notnull "select atan(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_atan2_Double "select atan2(kdbl, kdbl*kdbl) from fn_test order by kdbl"
	qt_sql_atan2_Double_notnull "select atan2(kdbl, kdbl*kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_auto_partition_name_list_column_type_mixed "select auto_partition_name('list', kchrs1, kbool) from fn_test_not_nullable where id < 3 order by kchrs1"
	qt_sql_auto_partition_name_list_column_literal_mixed "select auto_partition_name('list', kstr, 'hello') from fn_test_not_nullable where id < 3 order by kstr"
	qt_sql_auto_partition_name_list_column "select auto_partition_name('list', kchrs1, kvchrs1) from fn_test_not_nullable where id < 3 order by kchrs1"
	qt_sql_auto_partition_name_list_literal_empty "select auto_partition_name('list', '')"
	qt_sql_auto_partition_name_list_literal_mixed "select auto_partition_name('list', '你好', true, false)"
	qt_sql_auto_partition_name_list_literal_mixed "select auto_partition_name('list', '-hello')"
	qt_sql_auto_partition_name_list_literal_mixed "select auto_partition_name('list', '@#￥%~|world11111....')"
	qt_sql_auto_partition_name_range_literal_notnull "select auto_partition_name('range', 'day', '2022-12-12 19:20:30')"
	qt_sql_auto_partition_name_range_literal_notnull "select auto_partition_name('range', 'month', '2022-12-12 19:20:30')"
	qt_sql_auto_partition_name_range_literal_notnull "select auto_partition_name('range', 'year', '2022-12-12 19:20:30')"
	qt_sql_auto_partition_name_range_literal_notnull "select auto_partition_name('range', 'hour', '2022-12-12 19:20:30')"
	qt_sql_auto_partition_name_range_literal_notnull "select auto_partition_name('range', 'minute', '2022-12-12 19:20:30')"
	qt_sql_auto_partition_name_range_literal_notnull "select auto_partition_name('range', 'second', '2022-12-12 19:20:30')"
	qt_sql_auto_partition_name_range_notnull "select auto_partition_name('range', 'day', kdt) from fn_test_not_nullable order by kdt"
	qt_sql_auto_partition_name_range_notnull "select auto_partition_name('range', 'month', kdt) from fn_test_not_nullable order by kdt"
	qt_sql_auto_partition_name_range_notnull "select auto_partition_name('range', 'year', kdt) from fn_test_not_nullable order by kdt"
	qt_sql_auto_partition_name_range_notnull "select auto_partition_name('range', 'hour', kdt) from fn_test_not_nullable order by kdt"
	qt_sql_auto_partition_name_range_notnull "select auto_partition_name('range', 'minute', kdt) from fn_test_not_nullable order by kdt"
	qt_sql_auto_partition_name_range_notnull "select auto_partition_name('range', 'second', kdt) from fn_test_not_nullable order by kdt"

	test{
		sql """select auto_partition_name('hello');"""
		exception "function auto_partition_name must contains at least two arguments"
	}
	test{
		sql """select auto_partition_name('range', 'day', '123-12-12 19:20:30', '123-12-12 19:20:30');"""
		exception "range auto_partition_name must contains three arguments"
	}
	test{
		sql """select auto_partition_name(kdt, 'day', kdt) from fn_test_not_nullable order by kdt"""
		exception "auto_partition_name must accept literal for 1nd argument"
	}
	test{
		sql """select auto_partition_name('range', kdt, kdt) from fn_test_not_nullable order by kdt"""
		exception "auto_partition_name must accept literal for 2nd argument"
	}
	test{
		sql """select auto_partition_name('range', 'second', '')"""
		exception "The range partition only support DATE|DATETIME"
	}
	test{
		sql """select auto_partition_name('range', 'second', '123-12-12 19:20:30')"""
		exception "The range partition only support DATE|DATETIME"
	}
	test{
		sql """select auto_partition_name('range', 'second', '123-12-12')"""
		exception "The range partition only support DATE|DATETIME"
	}
	test{
		sql """select auto_partition_name('range', 'second', '2011-12-12 123:12:12')"""
		exception "The range partition only support DATE|DATETIME"
	}
	test{
		sql """select auto_partition_name('range', 'day', 1);"""
		exception "The range partition only support DATE|DATETIME"
	}
	test{
		sql """select auto_partition_name('range', 'year', 'hello');"""
		exception "The range partition only support DATE|DATETIME"
	}
	test{
		sql """select auto_partition_name('ranges', 'year', 'hello');"""
		exception "function auto_partition_name must accept range|list for 1nd argument"
	}
	test{
		sql """select auto_partition_name('range', 'years', 'hello');"""
		exception "range auto_partition_name must accept year|month|day|hour|minute|second for 2nd argument"
	}
	test{
		sql "select auto_partition_name('list', '你好', 'hello!@#￥%~|world11111....', '世界')"
		exception "The list partition name cannot exceed 50 characters"
	}
}
