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

suite("nereids_scalar_fn_U") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_unhex_Varchar "select unhex(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_unhex_Varchar_notnull "select unhex(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_unhex_String "select unhex(kstr) from fn_test order by kstr"
	qt_sql_unhex_String_notnull "select unhex(kstr) from fn_test_not_nullable order by kstr"
	sql "select unix_timestamp() from fn_test"
	sql "select unix_timestamp() from fn_test_not_nullable"
	qt_sql_unix_timestamp_DateTime "select unix_timestamp(kdtm) from fn_test order by kdtm"
	qt_sql_unix_timestamp_DateTime_notnull "select unix_timestamp(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_unix_timestamp_Date "select unix_timestamp(kdt) from fn_test order by kdt"
	qt_sql_unix_timestamp_Date_notnull "select unix_timestamp(kdt) from fn_test_not_nullable order by kdt"
	qt_sql_unix_timestamp_DateTimeV2 "select unix_timestamp(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_unix_timestamp_DateTimeV2_notnull "select unix_timestamp(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_unix_timestamp_DateV2 "select unix_timestamp(kdtv2) from fn_test order by kdtv2"
	qt_sql_unix_timestamp_DateV2_notnull "select unix_timestamp(kdtv2) from fn_test_not_nullable order by kdtv2"
	qt_sql_unix_timestamp_Varchar_Varchar "select unix_timestamp(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_unix_timestamp_Varchar_Varchar_notnull "select unix_timestamp(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	qt_sql_unix_timestamp_String_String "select unix_timestamp(kstr, kstr) from fn_test order by kstr, kstr"
	qt_sql_unix_timestamp_String_String_notnull "select unix_timestamp(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	qt_sql_upper_Varchar "select upper(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_upper_Varchar_notnull "select upper(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_upper_String "select upper(kstr) from fn_test order by kstr"
	qt_sql_upper_String_notnull "select upper(kstr) from fn_test_not_nullable order by kstr"
	sql "select user() from fn_test"
	sql "select user() from fn_test_not_nullable"
    qt_sql_url_decode "select url_decode('https%3A%2F%2Fdoris.apache.org%2Fzh-CN%2Fdocs%2Fsql-manual%2Fsql-functions%2Fstring-functions')"
    qt_sql_url_decode_empty "select url_decode('');"
    qt_sql_url_decode_null "select url_decode(null);"
    qt_sql_url_decode_invalid_url "select url_decode('This is not a url');"
}