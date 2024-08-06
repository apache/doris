set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT concat(ARRAY[1], ARRAY[2], ARRAY[3]); # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<TINYINT> to target type=VARCHAR(65533)
set debug_skip_fold_constant=true;
-- SELECT concat(ARRAY[1], ARRAY[2], ARRAY[3]) # error: errCode = 2, detailMessage = can not cast from origin type ARRAY<TINYINT> to target type=VARCHAR(65533)
