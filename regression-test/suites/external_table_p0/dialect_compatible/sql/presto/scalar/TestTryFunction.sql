set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
SELECT try(42);
SELECT try(DOUBLE'4.5');
-- SELECT try(DECIMAL'4.5'); # differ: doris : 4.500000000, presto : 4.5
SELECT try(TRUE);
SELECT try('hello');
-- SELECT try(JSON'[true, false, 12, 12.7, \"12\", null]'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [true, false, 12, 12.7, \"12\", null]
-- SELECT try(ARRAY[1, 2]); # differ: doris : [1, 2], presto : [1, 2]
SELECT try(NULL);
set debug_skip_fold_constant=true;
SELECT try(42);
SELECT try(DOUBLE'4.5');
-- SELECT try(DECIMAL'4.5'); # differ: doris : 4.500000000, presto : 4.5
SELECT try(TRUE);
SELECT try('hello');
-- SELECT try(JSON'[true, false, 12, 12.7, \"12\", null]'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [true, false, 12, 12.7, \"12\", null]
-- SELECT try(ARRAY[1, 2]); # differ: doris : [1, 2], presto : [1, 2]
SELECT try(NULL);
