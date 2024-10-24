set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT is_json_scalar(null); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar(JSON 'null'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_json_scalar(JSON 'null');	                           ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT is_json_scalar(JSON 'true'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_json_scalar(JSON 'true');	                           ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT is_json_scalar(JSON '1'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_json_scalar(JSON '1');	                           ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT is_json_scalar(JSON '\, '); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_json_scalar(JSON '\, ');	                           ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT is_json_scalar('null'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar('true'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar('1'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar('\, '); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar(JSON '[1, 2, 3]'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_json_scalar(JSON '[1, 2, 3]');	                           ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT is_json_scalar(JSON '{\, : 1, \, : 2}'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_json_scalar(JSON '{\, : 1, \, : 2}');	                           ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT is_json_scalar('[1, 2, 3]'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar('{\, : 1, \, : 2}'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar(''); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar('[1'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar('1 trailing'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar('[1, 2] trailing'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
SELECT json_array_length('[]');
SELECT json_array_length('[1]');
SELECT json_array_length('[1, \, , null]');
SELECT json_array_length('[2, 4, {\, : [8, 9]}, [], [5], 4]');
SELECT json_array_length(JSON '[]');
SELECT json_array_length(JSON '[1]');
-- SELECT json_array_length(JSON '[1, \, , null]'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [1, \, , null]
-- SELECT json_array_length(JSON '[2, 4, {\, : [8, 9]}, [], [5], 4]'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: [2, 4, {\, : [8, 9]}, [], [5], 4]
SELECT json_array_length(null);
-- SELECT json_array_contains('[]', true); # differ: doris : None, presto : False
-- SELECT json_array_contains('[true]', true); # differ: doris : None, presto : True
-- SELECT json_array_contains('[false]', false); # differ: doris : None, presto : True
-- SELECT json_array_contains('[true, false]', false); # differ: doris : None, presto : True
-- SELECT json_array_contains('[false, true]', true); # differ: doris : None, presto : True
-- SELECT json_array_contains('[1]', true); # differ: doris : None, presto : False
-- SELECT json_array_contains('[[true]]', true); # differ: doris : None, presto : False
SELECT json_array_contains('[1, \, , null, \, ]', true);
SELECT json_array_contains('[2, 4, {\, : [8, 9]}, [], [5], false]', false);
-- SELECT json_array_contains(JSON '[]', true); # differ: doris : None, presto : False
-- SELECT json_array_contains(JSON '[true]', true); # differ: doris : None, presto : True
-- SELECT json_array_contains(JSON '[false]', false); # differ: doris : None, presto : True
-- SELECT json_array_contains(JSON '[true, false]', false); # differ: doris : None, presto : True
-- SELECT json_array_contains(JSON '[false, true]', true); # differ: doris : None, presto : True
-- SELECT json_array_contains(JSON '[1]', true); # differ: doris : None, presto : False
-- SELECT json_array_contains(JSON '[[true]]', true); # differ: doris : None, presto : False
-- SELECT json_array_contains(JSON '[1, \, , null, \, ]', true); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [1, \, , null, \, ]
-- SELECT json_array_contains(JSON '[2, 4, {\, : [8, 9]}, [], [5], false]', false); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: [2, 4, {\, : [8, 9]}, [], [5], false]
SELECT json_array_contains(null, true);
SELECT json_array_contains(null, null);
SELECT json_array_contains('[]', null);
SELECT json_array_contains('[]', 1);
SELECT json_array_contains('[3]', 3);
SELECT json_array_contains('[-4]', -4);
SELECT json_array_contains('[1.0]', 1);
-- SELECT json_array_contains('[[2]]', 2); # differ: doris : 1, presto : False
SELECT json_array_contains('[1, \, , null, \, ]', 8);
SELECT json_array_contains('[2, 4, {\, : [8, 9]}, [], [5], 6]', 6);
-- SELECT json_array_contains('[92233720368547758071]', -9); # differ: doris : None, presto : False
SELECT json_array_contains(JSON '[]', 1);
SELECT json_array_contains(JSON '[3]', 3);
SELECT json_array_contains(JSON '[-4]', -4);
SELECT json_array_contains(JSON '[1.0]', 1);
-- SELECT json_array_contains(JSON '[[2]]', 2); # differ: doris : 1, presto : False
-- SELECT json_array_contains(JSON '[1, \, , null, \, ]', 8); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [1, \, , null, \, ]
-- SELECT json_array_contains(JSON '[2, 4, {\, : [8, 9]}, [], [5], 6]', 6); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: [2, 4, {\, : [8, 9]}, [], [5], 6]
-- SELECT json_array_contains(JSON '[92233720368547758071]', -9); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [92233720368547758071]
SELECT json_array_contains(null, 1);
SELECT json_array_contains(null, null);
SELECT json_array_contains('[3]', null);
SELECT json_array_contains('[]', 1);
SELECT json_array_contains('[1.5]', 1.5);
SELECT json_array_contains('[-9.5]', -9.5);
SELECT json_array_contains('[1]', 1.0);
-- SELECT json_array_contains('[[2.5]]', 2.5); # differ: doris : 1, presto : False
SELECT json_array_contains('[1, \, , null, \, ]', 8.2);
SELECT json_array_contains('[2, 4, {\, : [8, 9]}, [], [5], 6.1]', 6.1);
SELECT json_array_contains('[9.6E400]', 4.2);
SELECT json_array_contains(JSON '[]', 1);
SELECT json_array_contains(JSON '[1.5]', 1.5);
SELECT json_array_contains(JSON '[-9.5]', -9.5);
SELECT json_array_contains(JSON '[1]', 1.0);
-- SELECT json_array_contains(JSON '[[2.5]]', 2.5); # differ: doris : 1, presto : False
-- SELECT json_array_contains(JSON '[1, \, , null, \, ]', 8.2); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [1, \, , null, \, ]
-- SELECT json_array_contains(JSON '[2, 4, {\, : [8, 9]}, [], [5], 6.1]', 6.1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: [2, 4, {\, : [8, 9]}, [], [5], 6.1]
-- SELECT json_array_contains(JSON '[9.6E400]', 4.2); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [9.6E400]
SELECT json_array_contains(null, 1.5);
SELECT json_array_contains(null, null);
SELECT json_array_contains('[3.5]', null);
-- SELECT json_array_contains('[]', 'x'); # differ: doris : None, presto : False
SELECT json_array_contains('[\, ]', 'foo');
SELECT json_array_contains('[\, , null]', cast(null as varchar));
SELECT json_array_contains('[\, ]', '8');
SELECT json_array_contains('[1, \, , null]', 'foo');
-- SELECT json_array_contains('[1, 5]', '5'); # differ: doris : None, presto : False
SELECT json_array_contains('[2, 4, {\, : [8, 9]}, [], [5], \, ]', '6');
-- SELECT json_array_contains(JSON '[]', 'x'); # differ: doris : None, presto : False
-- SELECT json_array_contains(JSON '[\, ]', 'foo'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [\, ]
-- SELECT json_array_contains(JSON '[\, , null]', cast(null as varchar)); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [\, , null]
-- SELECT json_array_contains(JSON '[\, ]', '8'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [\, ]
-- SELECT json_array_contains(JSON '[1, \, , null]', 'foo'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [1, \, , null]
-- SELECT json_array_contains(JSON '[1, 5]', '5'); # differ: doris : None, presto : False
-- SELECT json_array_contains(JSON '[2, 4, {\, : [8, 9]}, [], [5], \, ]', '6'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: [2, 4, {\, : [8, 9]}, [], [5], \, ]
SELECT json_array_contains(null, 'x');
SELECT json_array_contains(null, '');
SELECT json_array_contains(null, null);
SELECT json_array_contains('[\, ]', null);
SELECT json_array_contains('[\, ]', '');
SELECT json_array_contains('[\, ]', 'x');
SELECT json_array_get('[1]', 0);
SELECT json_array_get('[2, 7, 4]', 1);
SELECT json_array_get('[2, 7, 4, 6, 8, 1, 0]', 6);
SELECT json_array_get('[]', 0);
SELECT json_array_get('[1, 3, 2]', 3);
-- SELECT json_array_get('[2, 7, 4, 6, 8, 1, 0]', -1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -1
-- SELECT json_array_get('[2, 7, 4, 6, 8, 1, 0]', -2); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -2
-- SELECT json_array_get('[2, 7, 4, 6, 8, 1, 0]', -7); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -7
-- SELECT json_array_get('[2, 7, 4, 6, 8, 1, 0]', -8); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -8
SELECT json_array_get(JSON '[1]', 0);
SELECT json_array_get(JSON '[2, 7, 4]', 1);
SELECT json_array_get(JSON '[2, 7, 4, 6, 8, 1, 0]', 6);
SELECT json_array_get(JSON '[]', 0);
SELECT json_array_get(JSON '[1, 3, 2]', 3);
-- SELECT json_array_get(JSON '[2, 7, 4, 6, 8, 1, 0]', -1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -1
-- SELECT json_array_get(JSON '[2, 7, 4, 6, 8, 1, 0]', -2); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -2
-- SELECT json_array_get(JSON '[2, 7, 4, 6, 8, 1, 0]', -7); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -7
-- SELECT json_array_get(JSON '[2, 7, 4, 6, 8, 1, 0]', -8); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -8
SELECT json_array_get('[]', null);
SELECT json_array_get('[1]', null);
SELECT json_array_get('', null);
SELECT json_array_get('', 1);
-- SELECT json_array_get('', -1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -1
-- SELECT json_array_get('[1]', -9223372036854775807 - 1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -9223372036854775808
SELECT json_array_get('[\, ]', 0);
SELECT json_array_get('[\, , null]', 1);
SELECT json_array_get('[\, , \, , \, ]', 1);
SELECT json_array_get('[\, , \, , \, , \, , \, ]', 4);
-- SELECT json_array_get(JSON '[\, ]', 0); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [\, ]
-- SELECT json_array_get(JSON '[\, , null]', 1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [\, , null]
-- SELECT json_array_get(JSON '[\, , \, , \, ]', 1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [\, , \, , \, ]
-- SELECT json_array_get(JSON '[\, , \, , \, , \, , \, ]', 4); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [\, , \, , \, , \, , \, ]
SELECT json_array_get('[\, ]', 0);
SELECT json_array_get('[]', 0);
-- SELECT json_array_get('[null]', 0); # differ: doris : null, presto : None
SELECT json_array_get('[]', null);
-- SELECT json_array_get('[1]', -9223372036854775807 - 1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -9223372036854775808
SELECT json_array_get('[3.14]', 0);
-- SELECT json_array_get('[3.14, null]', 1); # differ: doris : null, presto : None
SELECT json_array_get('[1.12, 3.54, 2.89]', 1);
SELECT json_array_get('[0.58, 9.7, 7.6, 11.2, 5.02]', 4);
SELECT json_array_get(JSON '[3.14]', 0);
-- SELECT json_array_get(JSON '[3.14, null]', 1); # differ: doris : null, presto : None
SELECT json_array_get(JSON '[1.12, 3.54, 2.89]', 1);
SELECT json_array_get(JSON '[0.58, 9.7, 7.6, 11.2, 5.02]', 4);
-- SELECT json_array_get('[1.0]', -1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -1
SELECT json_array_get('[1.0]', null);
SELECT json_array_get('[true]', 0);
-- SELECT json_array_get('[true, null]', 1); # differ: doris : null, presto : None
SELECT json_array_get('[false, false, true]', 1);
SELECT json_array_get('[true, false, false, true, true, false]', 5);
SELECT json_array_get(JSON '[true]', 0);
-- SELECT json_array_get(JSON '[true, null]', 1); # differ: doris : null, presto : None
SELECT json_array_get(JSON '[false, false, true]', 1);
SELECT json_array_get(JSON '[true, false, false, true, true, false]', 5);
-- SELECT json_array_get('[true]', -1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -1
SELECT json_array_get('[{\, :\, }]', 0);
SELECT json_array_get('[{\, :\, }, [1,2,3]]', 1);
SELECT json_array_get('[{\, :\, }, [1,2, {\,  : 2} ]]', 1);
SELECT json_array_get('[{\, :\, }, {\, :[{\, :99}]}]', 1);
SELECT json_array_get('[{\, :\, }, {\, :[{\, :99}]}]', -1);
SELECT json_array_get('[{\, : null}]', 0);
SELECT json_array_get('[{\, :\, }]', 0);
SELECT json_array_get('[{null:null}]', 0);
SELECT json_array_get('[{null:\, }]', 0);
SELECT json_array_get('[{\, :null}]', 0);
SELECT json_array_get('[{\, :null}]', -1);
-- SELECT json_parse('INVALID'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: INVALID
-- SELECT json_parse('\, : 1'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: \, : 1
SELECT json_parse('{}{');
SELECT json_parse('{}{abc');
-- SELECT json_parse(''); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Empty document for value: 
-- SELECT json_format(JSON '[\, , \, ]'); # error: errCode = 2, detailMessage = Can not found function 'JSON_FORMAT'
SELECT json_size('{\, : {\,  : 1, \,  : 2} }', '$');
SELECT json_size('{\, : {\,  : 1, \,  : 2} }', '$.x');
SELECT json_size('{\, : {\,  : 1, \,  : [1,2,3], \,  : {\, :9}} }', '$.x');
SELECT json_size('{\, : {\,  : 1, \,  : 2} }', '$.x.a');
SELECT json_size('[1,2,3]', '$');
SELECT json_size('[1,2,3]', CHAR '$');
SELECT json_size(null, '$');
SELECT json_size('INVALID_JSON', '$');
SELECT json_size('[1,2,3]', null);
-- SELECT json_size(JSON '{\, : {\,  : 1, \,  : 2} }', '$'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: {\, : {\,  : 1, \,  : 2} }
-- SELECT json_size(JSON '{\, : {\,  : 1, \,  : 2} }', '$.x'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: {\, : {\,  : 1, \,  : 2} }
-- SELECT json_size(JSON '{\, : {\,  : 1, \,  : [1,2,3], \,  : {\, :9}} }', '$.x'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: {\, : {\,  : 1, \,  : [1,2,3], \,  : {\, :9}} }
-- SELECT json_size(JSON '{\, : {\,  : 1, \,  : 2} }', '$.x.a'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: {\, : {\,  : 1, \,  : 2} }
SELECT json_size(JSON '[1,2,3]', '$');
SELECT json_size(JSON '[1,2,3]', null);
set debug_skip_fold_constant=true;
-- SELECT is_json_scalar(null); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar(JSON 'null'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_json_scalar(JSON 'null');	                           ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT is_json_scalar(JSON 'true'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_json_scalar(JSON 'true');	                           ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT is_json_scalar(JSON '1'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_json_scalar(JSON '1');	                           ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT is_json_scalar(JSON '\, '); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_json_scalar(JSON '\, ');	                           ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT is_json_scalar('null'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar('true'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar('1'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar('\, '); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar(JSON '[1, 2, 3]'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_json_scalar(JSON '[1, 2, 3]');	                           ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT is_json_scalar(JSON '{\, : 1, \, : 2}'); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT is_json_scalar(JSON '{\, : 1, \, : 2}');	                           ^	Encountered: STRING LITERAL	Expected: ||, COMMA, .	
-- SELECT is_json_scalar('[1, 2, 3]'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar('{\, : 1, \, : 2}'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar(''); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar('[1'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar('1 trailing'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
-- SELECT is_json_scalar('[1, 2] trailing'); # error: errCode = 2, detailMessage = Can not found function 'is_json_scalar'
SELECT json_array_length('[]');
SELECT json_array_length('[1]');
SELECT json_array_length('[1, \, , null]');
SELECT json_array_length('[2, 4, {\, : [8, 9]}, [], [5], 4]');
SELECT json_array_length(JSON '[]');
SELECT json_array_length(JSON '[1]');
-- SELECT json_array_length(JSON '[1, \, , null]'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [1, \, , null]
-- SELECT json_array_length(JSON '[2, 4, {\, : [8, 9]}, [], [5], 4]'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: [2, 4, {\, : [8, 9]}, [], [5], 4]
SELECT json_array_length(null);
-- SELECT json_array_contains('[]', true); # differ: doris : None, presto : False
-- SELECT json_array_contains('[true]', true); # differ: doris : None, presto : True
-- SELECT json_array_contains('[false]', false); # differ: doris : None, presto : True
-- SELECT json_array_contains('[true, false]', false); # differ: doris : None, presto : True
-- SELECT json_array_contains('[false, true]', true); # differ: doris : None, presto : True
-- SELECT json_array_contains('[1]', true); # differ: doris : None, presto : False
-- SELECT json_array_contains('[[true]]', true); # differ: doris : None, presto : False
SELECT json_array_contains('[1, \, , null, \, ]', true);
SELECT json_array_contains('[2, 4, {\, : [8, 9]}, [], [5], false]', false);
-- SELECT json_array_contains(JSON '[]', true); # differ: doris : None, presto : False
-- SELECT json_array_contains(JSON '[true]', true); # differ: doris : None, presto : True
-- SELECT json_array_contains(JSON '[false]', false); # differ: doris : None, presto : True
-- SELECT json_array_contains(JSON '[true, false]', false); # differ: doris : None, presto : True
-- SELECT json_array_contains(JSON '[false, true]', true); # differ: doris : None, presto : True
-- SELECT json_array_contains(JSON '[1]', true); # differ: doris : None, presto : False
-- SELECT json_array_contains(JSON '[[true]]', true); # differ: doris : None, presto : False
-- SELECT json_array_contains(JSON '[1, \, , null, \, ]', true); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [1, \, , null, \, ]
-- SELECT json_array_contains(JSON '[2, 4, {\, : [8, 9]}, [], [5], false]', false); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: [2, 4, {\, : [8, 9]}, [], [5], false]
SELECT json_array_contains(null, true);
SELECT json_array_contains(null, null);
SELECT json_array_contains('[]', null);
SELECT json_array_contains('[]', 1);
SELECT json_array_contains('[3]', 3);
SELECT json_array_contains('[-4]', -4);
SELECT json_array_contains('[1.0]', 1);
-- SELECT json_array_contains('[[2]]', 2); # differ: doris : 1, presto : False
SELECT json_array_contains('[1, \, , null, \, ]', 8);
SELECT json_array_contains('[2, 4, {\, : [8, 9]}, [], [5], 6]', 6);
-- SELECT json_array_contains('[92233720368547758071]', -9); # differ: doris : None, presto : False
SELECT json_array_contains(JSON '[]', 1);
SELECT json_array_contains(JSON '[3]', 3);
SELECT json_array_contains(JSON '[-4]', -4);
SELECT json_array_contains(JSON '[1.0]', 1);
-- SELECT json_array_contains(JSON '[[2]]', 2); # differ: doris : 1, presto : False
-- SELECT json_array_contains(JSON '[1, \, , null, \, ]', 8); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [1, \, , null, \, ]
-- SELECT json_array_contains(JSON '[2, 4, {\, : [8, 9]}, [], [5], 6]', 6); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: [2, 4, {\, : [8, 9]}, [], [5], 6]
-- SELECT json_array_contains(JSON '[92233720368547758071]', -9); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [92233720368547758071]
SELECT json_array_contains(null, 1);
SELECT json_array_contains(null, null);
SELECT json_array_contains('[3]', null);
SELECT json_array_contains('[]', 1);
SELECT json_array_contains('[1.5]', 1.5);
SELECT json_array_contains('[-9.5]', -9.5);
SELECT json_array_contains('[1]', 1.0);
-- SELECT json_array_contains('[[2.5]]', 2.5); # differ: doris : 1, presto : False
SELECT json_array_contains('[1, \, , null, \, ]', 8.2);
SELECT json_array_contains('[2, 4, {\, : [8, 9]}, [], [5], 6.1]', 6.1);
-- SELECT json_array_contains('[9.6E400]', 4.2); # differ: doris : None, presto : False
SELECT json_array_contains(JSON '[]', 1);
SELECT json_array_contains(JSON '[1.5]', 1.5);
SELECT json_array_contains(JSON '[-9.5]', -9.5);
SELECT json_array_contains(JSON '[1]', 1.0);
-- SELECT json_array_contains(JSON '[[2.5]]', 2.5); # differ: doris : 1, presto : False
-- SELECT json_array_contains(JSON '[1, \, , null, \, ]', 8.2); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [1, \, , null, \, ]
-- SELECT json_array_contains(JSON '[2, 4, {\, : [8, 9]}, [], [5], 6.1]', 6.1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: [2, 4, {\, : [8, 9]}, [], [5], 6.1]
-- SELECT json_array_contains(JSON '[9.6E400]', 4.2); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [9.6E400]
SELECT json_array_contains(null, 1.5);
SELECT json_array_contains(null, null);
SELECT json_array_contains('[3.5]', null);
-- SELECT json_array_contains('[]', 'x'); # differ: doris : None, presto : False
SELECT json_array_contains('[\, ]', 'foo');
SELECT json_array_contains('[\, , null]', cast(null as varchar));
SELECT json_array_contains('[\, ]', '8');
SELECT json_array_contains('[1, \, , null]', 'foo');
-- SELECT json_array_contains('[1, 5]', '5'); # differ: doris : None, presto : False
SELECT json_array_contains('[2, 4, {\, : [8, 9]}, [], [5], \, ]', '6');
-- SELECT json_array_contains(JSON '[]', 'x'); # differ: doris : None, presto : False
-- SELECT json_array_contains(JSON '[\, ]', 'foo'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [\, ]
-- SELECT json_array_contains(JSON '[\, , null]', cast(null as varchar)); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [\, , null]
-- SELECT json_array_contains(JSON '[\, ]', '8'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [\, ]
-- SELECT json_array_contains(JSON '[1, \, , null]', 'foo'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [1, \, , null]
-- SELECT json_array_contains(JSON '[1, 5]', '5'); # differ: doris : None, presto : False
-- SELECT json_array_contains(JSON '[2, 4, {\, : [8, 9]}, [], [5], \, ]', '6'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: [2, 4, {\, : [8, 9]}, [], [5], \, ]
SELECT json_array_contains(null, 'x');
SELECT json_array_contains(null, '');
SELECT json_array_contains(null, null);
SELECT json_array_contains('[\, ]', null);
SELECT json_array_contains('[\, ]', '');
SELECT json_array_contains('[\, ]', 'x');
SELECT json_array_get('[1]', 0);
SELECT json_array_get('[2, 7, 4]', 1);
SELECT json_array_get('[2, 7, 4, 6, 8, 1, 0]', 6);
SELECT json_array_get('[]', 0);
SELECT json_array_get('[1, 3, 2]', 3);
-- SELECT json_array_get('[2, 7, 4, 6, 8, 1, 0]', -1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -1
-- SELECT json_array_get('[2, 7, 4, 6, 8, 1, 0]', -2); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -2
-- SELECT json_array_get('[2, 7, 4, 6, 8, 1, 0]', -7); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -7
-- SELECT json_array_get('[2, 7, 4, 6, 8, 1, 0]', -8); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -8
SELECT json_array_get(JSON '[1]', 0);
SELECT json_array_get(JSON '[2, 7, 4]', 1);
SELECT json_array_get(JSON '[2, 7, 4, 6, 8, 1, 0]', 6);
SELECT json_array_get(JSON '[]', 0);
SELECT json_array_get(JSON '[1, 3, 2]', 3);
-- SELECT json_array_get(JSON '[2, 7, 4, 6, 8, 1, 0]', -1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -1
-- SELECT json_array_get(JSON '[2, 7, 4, 6, 8, 1, 0]', -2); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -2
-- SELECT json_array_get(JSON '[2, 7, 4, 6, 8, 1, 0]', -7); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -7
-- SELECT json_array_get(JSON '[2, 7, 4, 6, 8, 1, 0]', -8); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -8
SELECT json_array_get('[]', null);
SELECT json_array_get('[1]', null);
SELECT json_array_get('', null);
SELECT json_array_get('', 1);
-- SELECT json_array_get('', -1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -1
-- SELECT json_array_get('[1]', -9223372036854775807 - 1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -9223372036854775808
SELECT json_array_get('[\, ]', 0);
SELECT json_array_get('[\, , null]', 1);
SELECT json_array_get('[\, , \, , \, ]', 1);
SELECT json_array_get('[\, , \, , \, , \, , \, ]', 4);
-- SELECT json_array_get(JSON '[\, ]', 0); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [\, ]
-- SELECT json_array_get(JSON '[\, , null]', 1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [\, , null]
-- SELECT json_array_get(JSON '[\, , \, , \, ]', 1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [\, , \, , \, ]
-- SELECT json_array_get(JSON '[\, , \, , \, , \, , \, ]', 4); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: [\, , \, , \, , \, , \, ]
SELECT json_array_get('[\, ]', 0);
SELECT json_array_get('[]', 0);
-- SELECT json_array_get('[null]', 0); # differ: doris : null, presto : None
SELECT json_array_get('[]', null);
-- SELECT json_array_get('[1]', -9223372036854775807 - 1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -9223372036854775808
SELECT json_array_get('[3.14]', 0);
-- SELECT json_array_get('[3.14, null]', 1); # differ: doris : null, presto : None
SELECT json_array_get('[1.12, 3.54, 2.89]', 1);
SELECT json_array_get('[0.58, 9.7, 7.6, 11.2, 5.02]', 4);
SELECT json_array_get(JSON '[3.14]', 0);
-- SELECT json_array_get(JSON '[3.14, null]', 1); # differ: doris : null, presto : None
SELECT json_array_get(JSON '[1.12, 3.54, 2.89]', 1);
SELECT json_array_get(JSON '[0.58, 9.7, 7.6, 11.2, 5.02]', 4);
-- SELECT json_array_get('[1.0]', -1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -1
SELECT json_array_get('[1.0]', null);
SELECT json_array_get('[true]', 0);
-- SELECT json_array_get('[true, null]', 1); # differ: doris : null, presto : None
SELECT json_array_get('[false, false, true]', 1);
SELECT json_array_get('[true, false, false, true, true, false]', 5);
SELECT json_array_get(JSON '[true]', 0);
-- SELECT json_array_get(JSON '[true, null]', 1); # differ: doris : null, presto : None
SELECT json_array_get(JSON '[false, false, true]', 1);
SELECT json_array_get(JSON '[true, false, false, true, true, false]', 5);
-- SELECT json_array_get('[true]', -1); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Json path error: Invalid Json Path for value: -1
SELECT json_array_get('[{\, :\, }]', 0);
SELECT json_array_get('[{\, :\, }, [1,2,3]]', 1);
SELECT json_array_get('[{\, :\, }, [1,2, {\,  : 2} ]]', 1);
SELECT json_array_get('[{\, :\, }, {\, :[{\, :99}]}]', 1);
SELECT json_array_get('[{\, :\, }, {\, :[{\, :99}]}]', -1);
SELECT json_array_get('[{\, : null}]', 0);
SELECT json_array_get('[{\, :\, }]', 0);
SELECT json_array_get('[{null:null}]', 0);
SELECT json_array_get('[{null:\, }]', 0);
SELECT json_array_get('[{\, :null}]', 0);
SELECT json_array_get('[{\, :null}]', -1);
-- SELECT json_parse('INVALID'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: INVALID
-- SELECT json_parse('\, : 1'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Exception throwed for value: \, : 1
SELECT json_parse('{}{');
SELECT json_parse('{}{abc');
-- SELECT json_parse(''); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Empty document for value: 
-- SELECT json_format(JSON '[\, , \, ]'); # error: errCode = 2, detailMessage = Can not found function 'JSON_FORMAT'
SELECT json_size('{\, : {\,  : 1, \,  : 2} }', '$');
SELECT json_size('{\, : {\,  : 1, \,  : 2} }', '$.x');
SELECT json_size('{\, : {\,  : 1, \,  : [1,2,3], \,  : {\, :9}} }', '$.x');
SELECT json_size('{\, : {\,  : 1, \,  : 2} }', '$.x.a');
SELECT json_size('[1,2,3]', '$');
SELECT json_size('[1,2,3]', CHAR '$');
SELECT json_size(null, '$');
SELECT json_size('INVALID_JSON', '$');
SELECT json_size('[1,2,3]', null);
-- SELECT json_size(JSON '{\, : {\,  : 1, \,  : 2} }', '$'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: {\, : {\,  : 1, \,  : 2} }
-- SELECT json_size(JSON '{\, : {\,  : 1, \,  : 2} }', '$.x'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: {\, : {\,  : 1, \,  : 2} }
-- SELECT json_size(JSON '{\, : {\,  : 1, \,  : [1,2,3], \,  : {\, :9}} }', '$.x'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: {\, : {\,  : 1, \,  : [1,2,3], \,  : {\, :9}} }
-- SELECT json_size(JSON '{\, : {\,  : 1, \,  : 2} }', '$.x.a'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]json parse error: Invalid key string for value: {\, : {\,  : 1, \,  : 2} }
SELECT json_size(JSON '[1,2,3]', '$');
SELECT json_size(JSON '[1,2,3]', null);
