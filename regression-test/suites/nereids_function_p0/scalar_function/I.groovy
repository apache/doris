suite("nereids_scalar_fn_I") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=false'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_initcap_Varchar "select initcap(kvchrs1) from fn_test order by kvchrs1"
	qt_sql_initcap_Varchar "select initcap(kvchrs1) from fn_test_not_nullable order by kvchrs1"
	qt_sql_instr_Varchar_Varchar "select instr(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_instr_Varchar_Varchar "select instr(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	qt_sql_instr_String_String "select instr(kstr, kstr) from fn_test order by kstr, kstr"
	qt_sql_instr_String_String "select instr(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
}