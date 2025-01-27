set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT INTERVAL '124-30' YEAR TO MONTH; # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT INTERVAL '124-30' YEAR TO MONTH	                              ^	Encountered: TO	Expected	
-- SELECT INTERVAL '124' YEAR TO MONTH; # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT INTERVAL '124' YEAR TO MONTH	                           ^	Encountered: TO	Expected	
-- SELECT INTERVAL '30' MONTH; # error: errCode = 2, detailMessage = Invalid call to sql on unbound object
-- SELECT INTERVAL '32767' YEAR; # error: errCode = 2, detailMessage = Invalid call to sql on unbound object
-- SELECT INTERVAL '32767' MONTH; # error: errCode = 2, detailMessage = Invalid call to sql on unbound object
-- SELECT INTERVAL '32767-32767' YEAR TO MONTH; # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT INTERVAL '32767-32767' YEAR TO MONTH	                                   ^	Encountered: TO	Expected	
set debug_skip_fold_constant=true;
-- SELECT INTERVAL '124-30' YEAR TO MONTH; # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT INTERVAL '124-30' YEAR TO MONTH	                              ^	Encountered: TO	Expected	
-- SELECT INTERVAL '124' YEAR TO MONTH; # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT INTERVAL '124' YEAR TO MONTH	                           ^	Encountered: TO	Expected	
-- SELECT INTERVAL '30' MONTH; # error: errCode = 2, detailMessage = Invalid call to sql on unbound object
-- SELECT INTERVAL '32767' YEAR; # error: errCode = 2, detailMessage = Invalid call to sql on unbound object
-- SELECT INTERVAL '32767' MONTH; # error: errCode = 2, detailMessage = Invalid call to sql on unbound object
-- SELECT INTERVAL '32767-32767' YEAR TO MONTH # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT INTERVAL '32767-32767' YEAR TO MONTH	                                   ^	Encountered: TO	Expected	
