set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT JSON_EXTRACT_SCALAR(from_utf8(X'00 00 00 00 7b 22 72 22'), '$.x'); # error: errCode = 2, detailMessage = Syntax error in line 1:	...EXTRACT_SCALAR(from_utf8(X'00 00 00 00 7b 22 72 22'), ...	                             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
set debug_skip_fold_constant=true;
-- SELECT JSON_EXTRACT_SCALAR(from_utf8(X'00 00 00 00 7b 22 72 22'), '$.x') # error: errCode = 2, detailMessage = Syntax error in line 1:	...EXTRACT_SCALAR(from_utf8(X'00 00 00 00 7b 22 72 22'), ...	                             ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
