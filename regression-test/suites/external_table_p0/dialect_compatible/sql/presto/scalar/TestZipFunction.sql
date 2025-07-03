set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT zip(ARRAY[1, 2], ARRAY['a', 'b']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[1, 2], ARRAY['a', 'b']);	                  ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[1, 2], ARRAY['a', VARCHAR 'b']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[1, 2], ARRAY['a', VARCHAR 'b']);	                  ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[1, 2, 3, 4], ARRAY['a', 'b', 'c', 'd']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[1, 2, 3, 4], ARRAY['a', 'b', 'c', 'd']);	                  ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[1, 2], ARRAY['a', 'b'],  ARRAY['c', 'd']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[1, 2], ARRAY['a', 'b'],  ARRAY['c', 'd']);	                  ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[1, 2], ARRAY['a', 'b'], ARRAY['c', 'd'], ARRAY['e', 'f']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[1, 2], ARRAY['a', 'b'], ARRAY...	                  ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[], ARRAY[]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[], ARRAY[]);	                 ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT zip(ARRAY[], ARRAY[], ARRAY[]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[], ARRAY[], ARRAY[]);	                 ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT zip(ARRAY[], ARRAY[], ARRAY[], ARRAY[]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[], ARRAY[], ARRAY[], ARRAY[]);	                 ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT zip(ARRAY[NULL], ARRAY[NULL]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT zip(ARRAY[ARRAY[1, 1], ARRAY[1, 2]], ARRAY[ARRAY[2, 1], ARRAY[2, 2]]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[ARRAY[1, 1], ARRAY[1, 2]], ARRAY[AR...	                        ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[1], ARRAY['a', 'b']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[1], ARRAY['a', 'b']);	                              ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[NULL, 2], ARRAY['a']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[NULL, 2], ARRAY['a']);	                     ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[], ARRAY[1], ARRAY[1, 2], ARRAY[1, 2, 3]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[], ARRAY[1], ARRAY[1, 2], ARRAY[1, 2, 3]);	                 ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT zip(ARRAY[], ARRAY[NULL], ARRAY[NULL, NULL]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[], ARRAY[NULL], ARRAY[NULL, NULL]);	                 ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT zip(CAST(NULL AS ARRAY(UNKNOWN)), ARRAY[], ARRAY[1]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(CAST(NULL AS ARRAY(UNKNOWN)), ARRAY[], ARRAY[1]);	                             ^	Encountered: (	Expected	
set debug_skip_fold_constant=true;
-- SELECT zip(ARRAY[1, 2], ARRAY['a', 'b']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[1, 2], ARRAY['a', 'b']);	                  ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[1, 2], ARRAY['a', VARCHAR 'b']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[1, 2], ARRAY['a', VARCHAR 'b']);	                  ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[1, 2, 3, 4], ARRAY['a', 'b', 'c', 'd']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[1, 2, 3, 4], ARRAY['a', 'b', 'c', 'd']);	                  ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[1, 2], ARRAY['a', 'b'],  ARRAY['c', 'd']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[1, 2], ARRAY['a', 'b'],  ARRAY['c', 'd']);	                  ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[1, 2], ARRAY['a', 'b'], ARRAY['c', 'd'], ARRAY['e', 'f']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[1, 2], ARRAY['a', 'b'], ARRAY...	                  ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[], ARRAY[]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[], ARRAY[]);	                 ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT zip(ARRAY[], ARRAY[], ARRAY[]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[], ARRAY[], ARRAY[]);	                 ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT zip(ARRAY[], ARRAY[], ARRAY[], ARRAY[]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[], ARRAY[], ARRAY[], ARRAY[]);	                 ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT zip(ARRAY[NULL], ARRAY[NULL]); # error: errCode = 2, detailMessage = Unknown column 'ARRAY' in 'table list' in PROJECT clause
-- SELECT zip(ARRAY[ARRAY[1, 1], ARRAY[1, 2]], ARRAY[ARRAY[2, 1], ARRAY[2, 2]]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[ARRAY[1, 1], ARRAY[1, 2]], ARRAY[AR...	                        ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[1], ARRAY['a', 'b']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[1], ARRAY['a', 'b']);	                              ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[NULL, 2], ARRAY['a']); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[NULL, 2], ARRAY['a']);	                     ^	Encountered: COMMA	Expected: ||	
-- SELECT zip(ARRAY[], ARRAY[1], ARRAY[1, 2], ARRAY[1, 2, 3]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[], ARRAY[1], ARRAY[1, 2], ARRAY[1, 2, 3]);	                 ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT zip(ARRAY[], ARRAY[NULL], ARRAY[NULL, NULL]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(ARRAY[], ARRAY[NULL], ARRAY[NULL, NULL]);	                 ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT zip(CAST(NULL AS ARRAY(UNKNOWN)), ARRAY[], ARRAY[1]) # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT zip(CAST(NULL AS ARRAY(UNKNOWN)), ARRAY[], ARRAY[1]);	                             ^	Encountered: (	Expected	
