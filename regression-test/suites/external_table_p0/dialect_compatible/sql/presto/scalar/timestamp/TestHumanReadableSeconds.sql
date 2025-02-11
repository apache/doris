set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT human_readable_seconds(535333.9513888889); # error: errCode = 2, detailMessage = Can not found function 'human_readable_seconds'
-- SELECT human_readable_seconds(535333.2513888889); # error: errCode = 2, detailMessage = Can not found function 'human_readable_seconds'
set debug_skip_fold_constant=true;
-- SELECT human_readable_seconds(535333.9513888889); # error: errCode = 2, detailMessage = Can not found function 'human_readable_seconds'
-- SELECT human_readable_seconds(535333.2513888889) # error: errCode = 2, detailMessage = Can not found function 'human_readable_seconds'
