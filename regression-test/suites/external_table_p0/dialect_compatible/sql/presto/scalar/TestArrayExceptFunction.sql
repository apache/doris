set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT array_except(ARRAY[1, 5, 3], ARRAY[3]); # differ: doris : [1, 5], presto : [1, 5]
-- SELECT array_except(ARRAY[BIGINT '1', 5, 3], ARRAY[5]); # differ: doris : [1, 3], presto : [1, 3]
-- SELECT array_except(ARRAY[VARCHAR 'x', 'y', 'z'], ARRAY['x']); # differ: doris : ["y", "z"], presto : ['y', 'z']
-- SELECT array_except(ARRAY[true, false, null], ARRAY[true]); # differ: doris : [0, null], presto : [False, None]
-- SELECT array_except(ARRAY[1.1E0, 5.4E0, 3.9E0], ARRAY[5, 5.4E0]); # differ: doris : [1.1, 3.9], presto : [1.1, 3.9]
-- SELECT array_except(ARRAY[], ARRAY[]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT ARRAY_EXCEPT(ARRAY<ARRAY>, ARRAY<ARRAY>)	                                ^	Encountered: COMMA	Expected: IDENTIFIER	
-- SELECT array_except(ARRAY[], ARRAY[1, 3]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT ARRAY_EXCEPT(ARRAY<ARRAY>, ARRAY(1, 3))	                                ^	Encountered: COMMA	Expected: IDENTIFIER	
-- SELECT array_except(ARRAY[VARCHAR 'abc'], ARRAY[]); # error: errCode = 2, detailMessage = Syntax error in line 1:	...AS VARCHAR)), ARRAY<ARRAY>)	                             ^	Encountered: )	Expected: IDENTIFIER	
SELECT array_except(ARRAY[NULL], NULL);
SELECT array_except(NULL, NULL);
SELECT array_except(NULL, ARRAY[NULL]);
-- SELECT array_except(ARRAY[NULL], ARRAY[NULL]); # differ: doris : [], presto : []
-- SELECT array_except(ARRAY[], ARRAY[NULL]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT ARRAY_EXCEPT(ARRAY<ARRAY>, ARRAY(NULL))	                                ^	Encountered: COMMA	Expected: IDENTIFIER	
-- SELECT array_except(ARRAY[NULL], ARRAY[]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT ARRAY_EXCEPT(ARRAY(NULL), ARRAY<ARRAY>)	                                             ^	Encountered: )	Expected: IDENTIFIER	
-- SELECT array_except(ARRAY[1, 5, 3, 5, 1], ARRAY[3]); # differ: doris : [1, 5], presto : [1, 5]
-- SELECT array_except(ARRAY[BIGINT '1', 5, 5, 3, 3, 3, 1], ARRAY[3, 5]); # differ: doris : [1], presto : [1]
-- SELECT array_except(ARRAY[VARCHAR 'x', 'x', 'y', 'z'], ARRAY['x', 'y', 'x']); # differ: doris : ["z"], presto : ['z']
-- SELECT array_except(ARRAY[true, false, null, true, false, null], ARRAY[true, true, true]); # differ: doris : [0, null], presto : [False, None]
-- SELECT array_except(ARRAY[NaN()], ARRAY[NaN()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT array_except(ARRAY[1, NaN(), 3], ARRAY[NaN(), 3]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT array_except(ARRAY[1, NaN(), 3], ARRAY[NaN(), 3]);	                           ^	Encountered: COMMA	Expected: ||	
set debug_skip_fold_constant=true;
-- SELECT array_except(ARRAY[1, 5, 3], ARRAY[3]); # differ: doris : [1, 5], presto : [1, 5]
-- SELECT array_except(ARRAY[BIGINT '1', 5, 3], ARRAY[5]); # differ: doris : [1, 3], presto : [1, 3]
-- SELECT array_except(ARRAY[VARCHAR 'x', 'y', 'z'], ARRAY['x']); # differ: doris : ["y", "z"], presto : ['y', 'z']
-- SELECT array_except(ARRAY[true, false, null], ARRAY[true]); # differ: doris : [0, null], presto : [False, None]
-- SELECT array_except(ARRAY[1.1E0, 5.4E0, 3.9E0], ARRAY[5, 5.4E0]); # differ: doris : [1.1, 3.9], presto : [1.1, 3.9]
-- SELECT array_except(ARRAY[], ARRAY[]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT ARRAY_EXCEPT(ARRAY<ARRAY>, ARRAY<ARRAY>)	                                ^	Encountered: COMMA	Expected: IDENTIFIER	
-- SELECT array_except(ARRAY[], ARRAY[1, 3]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT ARRAY_EXCEPT(ARRAY<ARRAY>, ARRAY(1, 3))	                                ^	Encountered: COMMA	Expected: IDENTIFIER	
-- SELECT array_except(ARRAY[VARCHAR 'abc'], ARRAY[]); # error: errCode = 2, detailMessage = Syntax error in line 1:	...AS VARCHAR)), ARRAY<ARRAY>)	                             ^	Encountered: )	Expected: IDENTIFIER	
SELECT array_except(ARRAY[NULL], NULL);
SELECT array_except(NULL, NULL);
SELECT array_except(NULL, ARRAY[NULL]);
-- SELECT array_except(ARRAY[NULL], ARRAY[NULL]); # differ: doris : [], presto : []
-- SELECT array_except(ARRAY[], ARRAY[NULL]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT ARRAY_EXCEPT(ARRAY<ARRAY>, ARRAY(NULL))	                                ^	Encountered: COMMA	Expected: IDENTIFIER	
-- SELECT array_except(ARRAY[NULL], ARRAY[]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT ARRAY_EXCEPT(ARRAY(NULL), ARRAY<ARRAY>)	                                             ^	Encountered: )	Expected: IDENTIFIER	
-- SELECT array_except(ARRAY[1, 5, 3, 5, 1], ARRAY[3]); # differ: doris : [1, 5], presto : [1, 5]
-- SELECT array_except(ARRAY[BIGINT '1', 5, 5, 3, 3, 3, 1], ARRAY[3, 5]); # differ: doris : [1], presto : [1]
-- SELECT array_except(ARRAY[VARCHAR 'x', 'x', 'y', 'z'], ARRAY['x', 'y', 'x']); # differ: doris : ["z"], presto : ['z']
-- SELECT array_except(ARRAY[true, false, null, true, false, null], ARRAY[true, true, true]); # differ: doris : [0, null], presto : [False, None]
-- SELECT array_except(ARRAY[NaN()], ARRAY[NaN()]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT array_except(ARRAY[1, NaN(), 3], ARRAY[NaN(), 3]) # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT array_except(ARRAY[1, NaN(), 3], ARRAY[NaN(), 3]);	                           ^	Encountered: COMMA	Expected: ||	
