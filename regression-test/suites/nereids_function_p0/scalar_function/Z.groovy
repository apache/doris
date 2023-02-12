suite('nereids_fn_test_new') {
    sql 'use regression_test_nereids_function_p0'
    sql 'set enable_nereids_planner=false'
    sql 'set enable_fallback_to_original_planner=false'
}