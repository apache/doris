set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT test_is_null_simple(-100); # error: errCode = 2, detailMessage = Can not found function 'test_is_null_simple'
-- SELECT test_is_null_simple(23); # error: errCode = 2, detailMessage = Can not found function 'test_is_null_simple'
-- SELECT test_is_null_simple(null); # error: errCode = 2, detailMessage = Can not found function 'test_is_null_simple'
-- SELECT test_is_null_simple(cast(null as bigint)); # error: errCode = 2, detailMessage = Can not found function 'test_is_null_simple'
-- SELECT test_is_null(23, 'aaa', 'bbb', 'ccc'); # error: errCode = 2, detailMessage = Can not found function 'test_is_null'
-- SELECT test_is_null(null, 'aaa', 'bbb', 'ccc'); # error: errCode = 2, detailMessage = Can not found function 'test_is_null'
-- SELECT test_is_null(null, 'aaa', null, 'ccc'); # error: errCode = 2, detailMessage = Can not found function 'test_is_null'
-- SELECT test_is_null(23, 'aaa', null, null); # error: errCode = 2, detailMessage = Can not found function 'test_is_null'
-- SELECT test_is_null(23, null, 'bbb', 'ccc'); # error: errCode = 2, detailMessage = Can not found function 'test_is_null'
-- SELECT test_is_null_void(null); # error: errCode = 2, detailMessage = Can not found function 'test_is_null_void'
set debug_skip_fold_constant=true;
-- SELECT test_is_null_simple(-100); # error: errCode = 2, detailMessage = Can not found function 'test_is_null_simple'
-- SELECT test_is_null_simple(23); # error: errCode = 2, detailMessage = Can not found function 'test_is_null_simple'
-- SELECT test_is_null_simple(null); # error: errCode = 2, detailMessage = Can not found function 'test_is_null_simple'
-- SELECT test_is_null_simple(cast(null as bigint)); # error: errCode = 2, detailMessage = Can not found function 'test_is_null_simple'
-- SELECT test_is_null(23, 'aaa', 'bbb', 'ccc'); # error: errCode = 2, detailMessage = Can not found function 'test_is_null'
-- SELECT test_is_null(null, 'aaa', 'bbb', 'ccc'); # error: errCode = 2, detailMessage = Can not found function 'test_is_null'
-- SELECT test_is_null(null, 'aaa', null, 'ccc'); # error: errCode = 2, detailMessage = Can not found function 'test_is_null'
-- SELECT test_is_null(23, 'aaa', null, null); # error: errCode = 2, detailMessage = Can not found function 'test_is_null'
-- SELECT test_is_null(23, null, 'bbb', 'ccc'); # error: errCode = 2, detailMessage = Can not found function 'test_is_null'
-- SELECT test_is_null_void(null) # error: errCode = 2, detailMessage = Can not found function 'test_is_null_void'
