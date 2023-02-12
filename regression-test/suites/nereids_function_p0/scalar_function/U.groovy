suite("nereids_scalar_fn_U") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=false'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_unhex_Varchar "select unhex(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_unhex_Varchar "select unhex(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_unhex_String "select unhex(kstr) from fn_test order by kstr"
	qt_sql_unhex_String "select unhex(kstr) from fn_test_not_nullable order by kstr"
	qt_sql_unix_timestamp "select unix_timestamp() from fn_test"
	qt_sql_unix_timestamp "select unix_timestamp() from fn_test_not_nullable"
	qt_sql_unix_timestamp_DateTime "select unix_timestamp(kdtm) from fn_test order by kdtm"
	qt_sql_unix_timestamp_DateTime "select unix_timestamp(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_unix_timestamp_Date "select unix_timestamp(kdt) from fn_test order by kdt"
	qt_sql_unix_timestamp_Date "select unix_timestamp(kdt) from fn_test_not_nullable order by kdt"
	qt_sql_unix_timestamp_DateTimeV2 "select unix_timestamp(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_unix_timestamp_DateTimeV2 "select unix_timestamp(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_unix_timestamp_DateV2 "select unix_timestamp(kdtv2) from fn_test order by kdtv2"
	qt_sql_unix_timestamp_DateV2 "select unix_timestamp(kdtv2) from fn_test_not_nullable order by kdtv2"
	qt_sql_unix_timestamp_Varchar_Varchar "select unix_timestamp(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_unix_timestamp_Varchar_Varchar "select unix_timestamp(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	qt_sql_unix_timestamp_String_String "select unix_timestamp(kstr, kstr) from fn_test order by kstr, kstr"
	qt_sql_unix_timestamp_String_String "select unix_timestamp(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	qt_sql_upper_Varchar "select upper(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_upper_Varchar "select upper(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_upper_String "select upper(kstr) from fn_test order by kstr"
	qt_sql_upper_String "select upper(kstr) from fn_test_not_nullable order by kstr"
	sql "select user() from fn_test"
	sql "select user() from fn_test_not_nullable"
}