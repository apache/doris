suite("nereids_scalar_fn_E") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=false'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_elt_Integer_Varchar "select elt(kint, kvchrs1) from fn_test order by kint, kvchrs1"
	qt_sql_elt_Integer_Varchar "select elt(kint, kvchrs1) from fn_test_not_nullable order by kint, kvchrs1"
	qt_sql_elt_Integer_String "select elt(kint, kstr) from fn_test order by kint, kstr"
	qt_sql_elt_Integer_String "select elt(kint, kstr) from fn_test_not_nullable order by kint, kstr"
	qt_sql_ends_with_Varchar_Varchar "select ends_with(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_ends_with_Varchar_Varchar "select ends_with(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	qt_sql_ends_with_String_String "select ends_with(kstr, kstr) from fn_test order by kstr, kstr"
	qt_sql_ends_with_String_String "select ends_with(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	qt_sql_exp_Double "select exp(kdbl) from fn_test order by kdbl"
	qt_sql_exp_Double "select exp(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_extract_url_parameter_Varchar_Varchar "select extract_url_parameter(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_extract_url_parameter_Varchar_Varchar "select extract_url_parameter(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
}