set sql_dialect='presto';
set enable_fallback_to_original_planner=false;

set debug_skip_fold_constant=false;

SELECT human_readable_seconds(535333.9513888889);
SELECT human_readable_seconds(535333.2513888889);

set debug_skip_fold_constant=true;

SELECT human_readable_seconds(535333.9513888889);
SELECT human_readable_seconds(535333.2513888889);