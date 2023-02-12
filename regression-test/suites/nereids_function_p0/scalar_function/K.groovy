suite("nereids_scalar_fn_K") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=false'
	sql 'set enable_fallback_to_original_planner=false'
}