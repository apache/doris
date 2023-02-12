suite("nereids_scalar_fn_Q") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_quarter_DateTime "select quarter(kdtm) from fn_test order by kdtm"
	qt_sql_quarter_DateTime "select quarter(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_quarter_DateTimeV2 "select quarter(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_quarter_DateTimeV2 "select quarter(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_quarter_DateV2 "select quarter(kdtv2) from fn_test order by kdtv2"
	qt_sql_quarter_DateV2 "select quarter(kdtv2) from fn_test_not_nullable order by kdtv2"
}