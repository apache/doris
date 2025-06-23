set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
SELECT concat_ws('abc', 'def');
SELECT concat_ws(',', 'def');
SELECT concat_ws(',', 'def', 'pqr', 'mno');
SELECT concat_ws('abc', 'def', 'pqr');
SELECT concat_ws('', 'def');
SELECT concat_ws('', 'def', 'pqr');
SELECT concat_ws('', '', 'pqr');
SELECT concat_ws('', 'def', '');
SELECT concat_ws('', '', '');
SELECT concat_ws(',', 'def', '');
SELECT concat_ws(',', 'def', '', 'pqr');
SELECT concat_ws(',', '', 'pqr');
SELECT concat_ws(NULL, 'def');
SELECT concat_ws(NULL, cast(NULL as VARCHAR));
SELECT concat_ws(NULL, 'def', 'pqr');
SELECT concat_ws(',', cast(NULL as VARCHAR));
SELECT concat_ws(',', NULL, 'pqr');
SELECT concat_ws(',', 'def', NULL);
SELECT concat_ws(',', 'def', NULL, 'pqr');
SELECT concat_ws(',', 'def', NULL, NULL, 'mno', 'xyz', NULL, 'box');
-- SELECT concat_ws(',', ARRAY[]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT_WS(',', ARRAY<ARRAY>)	                                  ^	Encountered: )	Expected: IDENTIFIER	
SELECT concat_ws(',', ARRAY['abc']);
SELECT concat_ws(',', ARRAY['abc', 'def', 'pqr', 'xyz']);
SELECT concat_ws(null, ARRAY['abc']);
-- SELECT concat_ws(',', cast(NULL as array(varchar))); # differ: doris : , presto : None
SELECT concat_ws(',', ARRAY['abc', null, null, 'xyz']);
SELECT concat_ws(',', ARRAY['abc', '', '', 'xyz','abcdefghi']);
-- SELECT concat_ws(',', transform(sequence(0, ); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT concat_ws(',', transform(sequence(0, );	                                            ^	Encountered: )	Expected: IDENTIFIER	
-- SELECT concat_ws(line 1:8: Too many arguments for function call concat_ws()); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT concat_ws(line 1:8: Too many arguments for ...	                      ^	Encountered: INTEGER LITERAL	Expected: ||, COMMA, .	
-- SELECT concat_ws(','); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT_WS(',', )	                      ^	Encountered: )	Expected: IDENTIFIER	
set debug_skip_fold_constant=true;
SELECT concat_ws('abc', 'def');
SELECT concat_ws(',', 'def');
SELECT concat_ws(',', 'def', 'pqr', 'mno');
SELECT concat_ws('abc', 'def', 'pqr');
SELECT concat_ws('', 'def');
SELECT concat_ws('', 'def', 'pqr');
SELECT concat_ws('', '', 'pqr');
SELECT concat_ws('', 'def', '');
SELECT concat_ws('', '', '');
SELECT concat_ws(',', 'def', '');
SELECT concat_ws(',', 'def', '', 'pqr');
SELECT concat_ws(',', '', 'pqr');
SELECT concat_ws(NULL, 'def');
SELECT concat_ws(NULL, cast(NULL as VARCHAR));
SELECT concat_ws(NULL, 'def', 'pqr');
SELECT concat_ws(',', cast(NULL as VARCHAR));
SELECT concat_ws(',', NULL, 'pqr');
SELECT concat_ws(',', 'def', NULL);
SELECT concat_ws(',', 'def', NULL, 'pqr');
SELECT concat_ws(',', 'def', NULL, NULL, 'mno', 'xyz', NULL, 'box');
-- SELECT concat_ws(',', ARRAY[]); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT_WS(',', ARRAY<ARRAY>)	                                  ^	Encountered: )	Expected: IDENTIFIER	
SELECT concat_ws(',', ARRAY['abc']);
SELECT concat_ws(',', ARRAY['abc', 'def', 'pqr', 'xyz']);
SELECT concat_ws(null, ARRAY['abc']);
-- SELECT concat_ws(',', cast(NULL as array(varchar))); # differ: doris : , presto : None
SELECT concat_ws(',', ARRAY['abc', null, null, 'xyz']);
SELECT concat_ws(',', ARRAY['abc', '', '', 'xyz','abcdefghi']);
-- SELECT concat_ws(',', transform(sequence(0, ); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT concat_ws(',', transform(sequence(0, );	                                            ^	Encountered: )	Expected: IDENTIFIER	
-- SELECT concat_ws(line 1:8: Too many arguments for function call concat_ws()); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT concat_ws(line 1:8: Too many arguments for ...	                      ^	Encountered: INTEGER LITERAL	Expected: ||, COMMA, .	
-- SELECT concat_ws(',') # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT CONCAT_WS(',', )	                      ^	Encountered: )	Expected: IDENTIFIER	
