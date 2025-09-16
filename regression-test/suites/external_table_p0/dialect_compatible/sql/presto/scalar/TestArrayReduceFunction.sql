set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT reduce(ARRAY [], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT REDUCE(ARRAY<ARRAY>, CAST(0 AS BIGINT), (s, x) ...	                          ^	Encountered: COMMA	Expected: IDENTIFIER	
-- SELECT reduce(ARRAY [5, 20, 50], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s); # error: errCode = 2, detailMessage = Invalid call to s.getDataType() on unbound object
-- SELECT reduce(ARRAY [5 + RANDOM(1), 20, 50], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s); # error: errCode = 2, detailMessage = Invalid call to s.getDataType() on unbound object
-- SELECT reduce(ARRAY [5, 6, 10, 20], 0.0E0, (s, x) -> s + x, s -> s); # error: errCode = 2, detailMessage = Invalid call to s.getDataType() on unbound object
-- SELECT reduce(ARRAY [NULL], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s); # error: errCode = 2, detailMessage = Invalid call to s.getDataType() on unbound object
-- SELECT reduce(ARRAY [NULL, NULL, NULL], CAST (0 AS BIGINT), (s, x) -> coalesce(x, 1) + s, s -> s); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT reduce(ARRAY [5, NULL, 50], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s); # error: errCode = 2, detailMessage = Invalid call to s.getDataType() on unbound object
-- SELECT reduce(ARRAY [5, NULL, 50], CAST (0 AS BIGINT), (s, x) -> coalesce(x, 0) + s, s -> s); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT reduce(ARRAY [], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT REDUCE(ARRAY<ARRAY>, CAST(NULL AS BIGINT), (s, ...	                          ^	Encountered: COMMA	Expected: IDENTIFIER	
-- SELECT reduce(ARRAY [NULL], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT reduce(ARRAY [NULL, NULL, NULL], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT reduce(ARRAY [NULL, 6, 10, NULL, 8], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT reduce(ARRAY [5, NULL, 6, 10, NULL, 8], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT reduce(ARRAY[ARRAY[1, 2], ARRAY[3, 4], ARRAY[5, NULL, 7]], CAST(ARRAY[] AS ARRAY(INTEGER)), (s, x) -> concat(s, x), s -> s); # error: errCode = 2, detailMessage = Syntax error in line 1:	...L, 7)), CAST(ARRAY<ARRAY> AS ARRAY<INT>), (s, x) -> CO...	                             ^	Encountered: AS	Expected: AS is keyword, maybe `AS`	
-- SELECT reduce(ARRAY [123456789012345, NULL, 54321], 0, (s, x) -> s + coalesce(x, 0), s -> s); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
set debug_skip_fold_constant=true;
-- SELECT reduce(ARRAY [], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT REDUCE(ARRAY<ARRAY>, CAST(0 AS BIGINT), (s, x) ...	                          ^	Encountered: COMMA	Expected: IDENTIFIER	
-- SELECT reduce(ARRAY [5, 20, 50], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s); # error: errCode = 2, detailMessage = Invalid call to s.getDataType() on unbound object
-- SELECT reduce(ARRAY [5 + RANDOM(1), 20, 50], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s); # error: errCode = 2, detailMessage = Invalid call to s.getDataType() on unbound object
-- SELECT reduce(ARRAY [5, 6, 10, 20], 0.0E0, (s, x) -> s + x, s -> s); # error: errCode = 2, detailMessage = Invalid call to s.getDataType() on unbound object
-- SELECT reduce(ARRAY [NULL], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s); # error: errCode = 2, detailMessage = Invalid call to s.getDataType() on unbound object
-- SELECT reduce(ARRAY [NULL, NULL, NULL], CAST (0 AS BIGINT), (s, x) -> coalesce(x, 1) + s, s -> s); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT reduce(ARRAY [5, NULL, 50], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s); # error: errCode = 2, detailMessage = Invalid call to s.getDataType() on unbound object
-- SELECT reduce(ARRAY [5, NULL, 50], CAST (0 AS BIGINT), (s, x) -> coalesce(x, 0) + s, s -> s); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT reduce(ARRAY [], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT REDUCE(ARRAY<ARRAY>, CAST(NULL AS BIGINT), (s, ...	                          ^	Encountered: COMMA	Expected: IDENTIFIER	
-- SELECT reduce(ARRAY [NULL], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT reduce(ARRAY [NULL, NULL, NULL], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT reduce(ARRAY [NULL, 6, 10, NULL, 8], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT reduce(ARRAY [5, NULL, 6, 10, NULL, 8], CAST (NULL AS BIGINT), (s, x) -> IF(s IS NULL OR x > s, x, s), s -> s); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT reduce(ARRAY[ARRAY[1, 2], ARRAY[3, 4], ARRAY[5, NULL, 7]], CAST(ARRAY[] AS ARRAY(INTEGER)), (s, x) -> concat(s, x), s -> s); # error: errCode = 2, detailMessage = Syntax error in line 1:	...L, 7)), CAST(ARRAY<ARRAY> AS ARRAY<INT>), (s, x) -> CO...	                             ^	Encountered: AS	Expected: AS is keyword, maybe `AS`	
-- SELECT reduce(ARRAY [123456789012345, NULL, 54321], 0, (s, x) -> s + coalesce(x, 0), s -> s) # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
