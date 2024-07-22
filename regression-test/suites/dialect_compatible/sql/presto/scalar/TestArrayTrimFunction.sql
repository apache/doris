set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT trim_array(ARRAY[1, 2, 3, 4], 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[1, 2, 3, 4], 2);	                         ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY[1, 2, 3, 4], 0); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[1, 2, 3, 4], 0);	                         ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY[1, 2, 3, 4], 1); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[1, 2, 3, 4], 1);	                         ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY[1, 2, 3, 4], 3); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[1, 2, 3, 4], 3);	                         ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY[1, 2, 3, 4], 4); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[1, 2, 3, 4], 4);	                         ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY['a', 'b', 'c', 'd'], 1); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY['a', 'b', 'c', 'd'], 1);	                           ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY['a', 'b', null, 'd'], 1); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY['a', 'b', null, 'd'], 1);	                           ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY[ARRAY[1, 2, 3], ARRAY[4, 5, 6]], 1); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[ARRAY[1, 2, 3], ARRAY[4, 5, 6]], 1);	                               ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY[1, 2, 3, 4], 5); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[1, 2, 3, 4], 5);	                         ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY[1, 2, 3, 4], -1); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[1, 2, 3, 4], -1);	                         ^	Encountered: COMMA	Expected: ||	
set debug_skip_fold_constant=true;
-- SELECT trim_array(ARRAY[1, 2, 3, 4], 2); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[1, 2, 3, 4], 2);	                         ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY[1, 2, 3, 4], 0); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[1, 2, 3, 4], 0);	                         ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY[1, 2, 3, 4], 1); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[1, 2, 3, 4], 1);	                         ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY[1, 2, 3, 4], 3); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[1, 2, 3, 4], 3);	                         ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY[1, 2, 3, 4], 4); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[1, 2, 3, 4], 4);	                         ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY['a', 'b', 'c', 'd'], 1); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY['a', 'b', 'c', 'd'], 1);	                           ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY['a', 'b', null, 'd'], 1); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY['a', 'b', null, 'd'], 1);	                           ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY[ARRAY[1, 2, 3], ARRAY[4, 5, 6]], 1); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[ARRAY[1, 2, 3], ARRAY[4, 5, 6]], 1);	                               ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY[1, 2, 3, 4], 5); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[1, 2, 3, 4], 5);	                         ^	Encountered: COMMA	Expected: ||	
-- SELECT trim_array(ARRAY[1, 2, 3, 4], -1) # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT trim_array(ARRAY[1, 2, 3, 4], -1);	                         ^	Encountered: COMMA	Expected: ||	
