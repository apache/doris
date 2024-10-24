set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
SELECT chr(65);
-- SELECT chr(9731); # differ: doris : &, presto : ☃
-- SELECT chr(131210); # differ: doris : None, presto : 𠂊
SELECT chr(0);
SELECT codepoint('x');
-- SELECT codepoint(CHR(128077)); # error: errCode = 2, detailMessage = Can not found function 'CHR'
-- SELECT codepoint(CHR(33804)); # error: errCode = 2, detailMessage = Can not found function 'CHR'
SELECT concat('hello', ' world');
SELECT concat('', '');
SELECT concat('what', '');
SELECT concat('', 'what');
SELECT concat('this', ' is', ' cool');
SELECT concat('this', ' is', ' cool');
SELECT concat('hello na\u00EFve', ' world');
SELECT concat('\uD801\uDC2D', 'end');
SELECT concat('\uD801\uDC2D', 'end', '\uD801\uDC2D');
SELECT concat('\u4FE1\u5FF5', ',\u7231', ',\u5E0C\u671B');
SELECT length('');
SELECT length('hello');
SELECT length('Quadratically');
SELECT length('hello na\u00EFve world');
SELECT length('\uD801\uDC2Dend');
SELECT length('\u4FE1\u5FF5,\u7231,\u5E0C\u671B');
SELECT length(CAST('hello' AS CHAR(5)));
SELECT length(CAST('Quadratically' AS CHAR(13)));
-- SELECT length(CAST('' AS CHAR(20))); # differ: doris : 0, presto : 20
-- SELECT length(CAST('hello' AS CHAR(20))); # differ: doris : 5, presto : 20
-- SELECT length(CAST('Quadratically' AS CHAR(20))); # differ: doris : 13, presto : 20
SELECT length(CAST('hello na\u00EFve world' AS CHAR(17)));
SELECT length(CAST('\uD801\uDC2Dend' AS CHAR(4)));
SELECT length(CAST('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' AS CHAR(7)));
SELECT length(CAST('hello na\u00EFve world' AS CHAR(20)));
-- SELECT length(CAST('\uD801\uDC2Dend' AS CHAR(20))); # differ: doris : 15, presto : 20
SELECT length(CAST('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' AS CHAR(20)));
-- SELECT levenshtein_distance('', ''); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('', 'hello'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello', ''); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello', 'hello'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello', 'hello world'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello world', 'hel wold'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello world', 'hellq wodld'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('helo word', 'hello world'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello word', 'dello world'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello na\u00EFve world', 'hello naive world'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello na\u00EFve world', 'hello na:ive world'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u4FE1\u4EF0,\u7231,\u5E0C\u671B'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('\u4F11\u5FF5,\u7231,\u5E0C\u671B', '\u4FE1\u5FF5,\u7231,\u5E0C\u671B'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u4FE1\u5FF5\u5E0C\u671B'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u4FE1\u5FF5,love,\u5E0C\u671B'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT hamming_distance('', ''); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance('hello', 'hello'); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance('hello', 'jello'); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance('like', 'hate'); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance('hello', 'world'); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance(NULL, NULL); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance('hello', NULL); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance(NULL, 'world'); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u4FE1\u4EF0,\u7231,\u5E0C\u671B'); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance('\u4F11\u5FF5,\u7231,\u5E0C\u671B', '\u4FE1\u5FF5,\u7231,\u5E0C\u671B'); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
SELECT replace('aaa', 'a', 'aa');
SELECT replace('abcdefabcdef', 'cd', 'XX');
SELECT replace('abcdefabcdef', 'cd');
SELECT replace('123123tech', '123');
SELECT replace('123tech123', '123');
SELECT replace('222tech', '2', '3');
SELECT replace('0000123', '0');
SELECT replace('0000123', '0', ' ');
SELECT replace('foo', '');
SELECT replace('foo', '', '');
SELECT replace('foo', 'foo', '');
SELECT replace('abc', '', 'xx');
SELECT replace('', '', 'xx');
SELECT replace('', '');
SELECT replace('', '', '');
SELECT replace('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', '\u2014');
SELECT replace('::\uD801\uDC2D::', ':', '');
SELECT replace('\u00D6sterreich', '\u00D6', 'Oe');
SELECT reverse('');
SELECT reverse('hello');
SELECT reverse('Quadratically');
SELECT reverse('racecar');
SELECT reverse('\u4FE1\u5FF5,\u7231,\u5E0C\u671B');
SELECT reverse('\u00D6sterreich');
SELECT reverse('na\u00EFve');
SELECT reverse('\uD801\uDC2Dend');
SELECT strpos('high', 'ig');
SELECT strpos('high', 'igx');
SELECT strpos('Quadratically', 'a');
SELECT strpos('foobar', 'foobar');
SELECT strpos('foobar', 'obar');
SELECT strpos('zoo!', '!');
SELECT strpos('x', '');
SELECT strpos('', '');
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u7231');
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u5E0C\u671B');
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', 'nice');
SELECT strpos(NULL, '');
SELECT strpos('', NULL);
SELECT strpos(NULL, NULL);
SELECT starts_with('foo', 'foo');
SELECT starts_with('foo', 'bar');
SELECT starts_with('foo', '');
SELECT starts_with('', 'foo');
SELECT starts_with('', '');
SELECT starts_with('foo_bar_baz', 'foo');
SELECT starts_with('foo_bar_baz', 'bar');
SELECT starts_with('foo', 'foo_bar_baz');
SELECT starts_with('信念 爱 希望', '信念');
SELECT starts_with('信念 爱 希望', '爱');
SELECT strpos(NULL, '');
SELECT strpos('', NULL);
SELECT strpos(NULL, NULL);
SELECT strpos('abc/xyz/foo/bar', '/');
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u7231');
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u5E0C\u671B');
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', 'nice');
SELECT strpos('high', 'ig');
SELECT strpos('high', 'igx');
SELECT strpos('Quadratically', 'a');
SELECT strpos('foobar', 'foobar');
SELECT strpos('foobar', 'obar');
SELECT strpos('zoo!', '!');
SELECT strpos('x', '');
SELECT strpos('', '');
SELECT strpos('abc abc abc', 'abc', 1);
SELECT strpos('abc/xyz/foo/bar', '/', 1);
-- SELECT strpos('abc/xyz/foo/bar', '/', 2); # differ: doris : 4, presto : 8
-- SELECT strpos('abc/xyz/foo/bar', '/', 3); # differ: doris : 4, presto : 12
-- SELECT strpos('abc/xyz/foo/bar', '/', 4); # differ: doris : 4, presto : 0
SELECT strpos('highhigh', 'ig', 1);
SELECT strpos('foobarfoo', 'fb', 1);
SELECT strpos('foobarfoo', 'oo', 1);
-- SELECT strpos('abc abc abc', 'abc', -1); # differ: doris : 1, presto : 9
-- SELECT strpos('abc/xyz/foo/bar', '/', -1); # differ: doris : 4, presto : 12
-- SELECT strpos('abc/xyz/foo/bar', '/', -2); # differ: doris : 4, presto : 8
SELECT strpos('abc/xyz/foo/bar', '/', -3);
-- SELECT strpos('abc/xyz/foo/bar', '/', -4); # differ: doris : 4, presto : 0
-- SELECT strpos('highhigh', 'ig', -1); # differ: doris : 2, presto : 6
SELECT strpos('highhigh', 'ig', -2);
SELECT strpos('foobarfoo', 'fb', -1);
-- SELECT strpos('foobarfoo', 'oo', -1); # differ: doris : 2, presto : 8
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u7231', -1);
-- SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u7231\u671B', '\u7231', -1); # differ: doris : 14, presto : 27
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u7231\u671B', '\u7231', -2);
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u5E0C\u671B', -1);
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', 'nice', -1);
SELECT substr('Quadratically', 5);
SELECT substr('Quadratically', 50);
SELECT substr('Quadratically', -5);
SELECT substr('Quadratically', -50);
SELECT substr('Quadratically', 0);
SELECT substr('Quadratically', 5, 6);
SELECT substr('Quadratically', 5, 10);
SELECT substr('Quadratically', 5, 50);
SELECT substr('Quadratically', 50, 10);
SELECT substr('Quadratically', -5, 4);
SELECT substr('Quadratically', -5, 40);
SELECT substr('Quadratically', -50, 4);
SELECT substr('Quadratically', 0, 4);
SELECT substr('Quadratically', 5, 0);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 5);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 50);
SELECT substr(CAST('Quadratically' AS CHAR(13)), -5);
SELECT substr(CAST('Quadratically' AS CHAR(13)), -50);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 0);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 5, 6);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 5, 10);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 5, 50);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 50, 10);
SELECT substr(CAST('Quadratically' AS CHAR(13)), -5, 4);
SELECT substr(CAST('Quadratically' AS CHAR(13)), -5, 40);
SELECT substr(CAST('Quadratically' AS CHAR(13)), -50, 4);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 0, 4);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 5, 0);
SELECT substr(CAST('abc def' AS CHAR(7)), 1, 4);
-- SELECT substr(CAST('keep trailing' AS CHAR(14)), 1); # differ: doris : keep trailing, presto : keep trailing 
-- SELECT substr(CAST('keep trailing' AS CHAR(14)), 1, 14); # differ: doris : keep trailing, presto : keep trailing 
SELECT substring(CAST('Quadratically' AS CHAR(13)), 5);
SELECT substring(CAST('Quadratically' AS CHAR(13)), 5, 6);
-- SELECT split('a.b.c', '.'); # differ: doris : ["a", "b", "c"], presto : ['a', 'b', 'c']
-- SELECT split('ab', '.', 1); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a.b', '.', 1); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a.b.c', '.'); # differ: doris : ["a", "b", "c"], presto : ['a', 'b', 'c']
-- SELECT split('a..b..c', '..'); # differ: doris : ["a", "b", "c"], presto : ['a', 'b', 'c']
-- SELECT split('a.b.c', '.', 2); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a.b.c', '.', 3); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a.b.c', '.', 4); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a.b.c.', '.', 4); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a.b.c.', '.', 3); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('...', '.'); # differ: doris : ["", "", "", ""], presto : ['', '', '', '']
-- SELECT split('..a...a..', '.'); # differ: doris : ["", "", "a", "", "", "a", "", ""], presto : ['', '', 'a', '', '', 'a', '', '']
-- SELECT split('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 3); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('\u8B49\u8BC1\u8A3C', '\u8BC1', 2); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('.a.b.c', '.', 4); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('.a.b.c', '.', 3); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('.a.b.c', '.', 2); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a..b..c', '.', 3); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a.b..', '.', 3); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split_to_map('', ',', '='); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_map('a=123,b=.4,c=,=d', ',', '='); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_map('=', ',', '='); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_map('key=>value', ',', '=>'); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_map('key => value', ',', '=>'); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_map('\u4EA0\u4EFF\u4EA1', '\u4E00', '\u4EFF'); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_map('\u4EC0\u4EFF', '\u4E00', '\u4EFF'); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_map('\u4EFF\u4EC1', '\u4E00', '\u4EFF'); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_multimap('', ',', '='); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('a=123,b=.4,c=,=d', ',', '='); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('=', ',', '='); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('a=123,a=.4,a=5.67', ',', '='); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('key=>value,key=>value', ',', '=>'); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('key => value, key => value', ',', '=>'); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('key => value, key => value', ', ', '=>'); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('\u4EA0\u4EFF\u4EA1', '\u4E00', '\u4EFF'); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('\u4EA0\u4EFF\u4EA1\u4E00\u4EA0\u4EFF\u4EB1', '\u4E00', '\u4EFF'); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
SELECT split_part('abc-@-def-@-ghi', '-@-', 1);
SELECT split_part('abc-@-def-@-ghi', '-@-', 2);
SELECT split_part('abc-@-def-@-ghi', '-@-', 3);
SELECT split_part('abc-@-def-@-ghi', '-@-', 4);
SELECT split_part('abc-@-def-@-ghi', '-@-', 99);
SELECT split_part('abc', 'abc', 1);
SELECT split_part('abc', 'abc', 2);
SELECT split_part('abc', 'abc', 3);
-- SELECT split_part('abc', '-@-', 1); # differ: doris : None, presto : abc
SELECT split_part('abc', '-@-', 2);
-- SELECT split_part('', 'abc', 1); # differ: doris : None, presto : 
-- SELECT split_part('', '', 1); # differ: doris : , presto : None
-- SELECT split_part('abc', '', 1); # differ: doris : , presto : a
-- SELECT split_part('abc', '', 2); # differ: doris : , presto : b
-- SELECT split_part('abc', '', 3); # differ: doris : , presto : c
-- SELECT split_part('abc', '', 4); # differ: doris : , presto : None
-- SELECT split_part('abc', '', 99); # differ: doris : , presto : None
-- SELECT split_part('abc', 'abcd', 1); # differ: doris : None, presto : abc
SELECT split_part('abc', 'abcd', 2);
SELECT split_part('abc--@--def', '-@-', 1);
SELECT split_part('abc--@--def', '-@-', 2);
SELECT split_part('abc-@-@-@-def', '-@-', 1);
SELECT split_part('abc-@-@-@-def', '-@-', 2);
SELECT split_part('abc-@-@-@-def', '-@-', 3);
SELECT split_part(' ', ' ', 1);
SELECT split_part('abcdddddef', 'dd', 1);
SELECT split_part('abcdddddef', 'dd', 2);
SELECT split_part('abcdddddef', 'dd', 3);
SELECT split_part('a/b/c', '/', 4);
SELECT split_part('a/b/c/', '/', 4);
SELECT split_part('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 1);
SELECT split_part('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 2);
SELECT split_part('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 3);
SELECT split_part('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 4);
SELECT split_part('\u8B49\u8BC1\u8A3C', '\u8BC1', 1);
SELECT split_part('\u8B49\u8BC1\u8A3C', '\u8BC1', 2);
SELECT split_part('\u8B49\u8BC1\u8A3C', '\u8BC1', 3);
SELECT ltrim('');
SELECT ltrim('   ');
SELECT ltrim('  hello  ');
SELECT ltrim('  hello');
SELECT ltrim('hello  ');
SELECT ltrim(' hello world ');
SELECT ltrim('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ');
SELECT ltrim(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ');
SELECT ltrim('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B');
SELECT ltrim(' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B');
-- SELECT ltrim(CAST('' AS CHAR(20))); # differ: doris : , presto :                     
-- SELECT ltrim(CAST('  hello  ' AS CHAR(9))); # differ: doris : hello  , presto : hello    
-- SELECT ltrim(CAST('  hello' AS CHAR(7))); # differ: doris : hello, presto : hello  
SELECT ltrim(CAST('hello  ' AS CHAR(7)));
-- SELECT ltrim(CAST(' hello world ' AS CHAR(13))); # differ: doris : hello world , presto : hello world  
SELECT ltrim(CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ' AS CHAR(9)));
-- SELECT ltrim(CAST(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ' AS CHAR(9))); # differ: doris : \u4FE1\u, presto : \u4FE1\u 
-- SELECT ltrim(CAST('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(9))); # differ: doris : \u4FE1\, presto : \u4FE1\  
-- SELECT ltrim(CAST(' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(10))); # differ: doris : \u2028 \u, presto : \u2028 \u 
SELECT rtrim('');
SELECT rtrim('   ');
SELECT rtrim('  hello  ');
SELECT rtrim('  hello');
SELECT rtrim('hello  ');
SELECT rtrim(' hello world ');
SELECT rtrim('\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ');
SELECT rtrim('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ');
SELECT rtrim(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ');
SELECT rtrim('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B');
-- SELECT rtrim(CAST('' AS CHAR(20))); # differ: doris : , presto :                     
-- SELECT rtrim(CAST('  hello  ' AS CHAR(9))); # differ: doris :   hello, presto :   hello  
SELECT rtrim(CAST('  hello' AS CHAR(7)));
-- SELECT rtrim(CAST('hello  ' AS CHAR(7))); # differ: doris : hello, presto : hello  
-- SELECT rtrim(CAST(' hello world ' AS CHAR(13))); # differ: doris :  hello world, presto :  hello world 
SELECT rtrim(CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ' AS CHAR(10)));
SELECT rtrim(CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ' AS CHAR(9)));
SELECT rtrim(CAST(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ' AS CHAR(9)));
SELECT rtrim(CAST('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(9)));
SELECT ltrim('', '');
SELECT ltrim('   ', '');
SELECT ltrim('  hello  ', '');
SELECT ltrim('  hello  ', ' ');
SELECT ltrim('  hello  ', CHAR ' ');
-- SELECT ltrim('  hello  ', 'he '); # differ: doris :   hello  , presto : llo  
SELECT ltrim('  hello', ' ');
-- SELECT ltrim('  hello', 'e h'); # differ: doris :   hello, presto : llo
SELECT ltrim('hello  ', 'l');
SELECT ltrim(' hello world ', ' ');
-- SELECT ltrim(' hello world ', ' eh'); # differ: doris :  hello world , presto : llo world 
-- SELECT ltrim(' hello world ', ' ehlowrd'); # differ: doris :  hello world , presto : 
-- SELECT ltrim(' hello world ', ' x'); # differ: doris :  hello world , presto : hello world 
-- SELECT ltrim('\u017a\u00f3\u0142\u0107', '\u00f3\u017a'); # differ: doris : \u017a\u00f3\u0142\u0107, presto : 42\u0107
-- SELECT ltrim(CAST('' AS CHAR(1)), ''); # differ: doris : , presto :  
SELECT ltrim(CAST('   ' AS CHAR(3)), '');
SELECT ltrim(CAST('  hello  ' AS CHAR(9)), '');
-- SELECT ltrim(CAST('  hello  ' AS CHAR(9)), ' '); # differ: doris : hello  , presto : hello    
-- SELECT ltrim(CAST('  hello  ' AS CHAR(9)), 'he '); # differ: doris :   hello  , presto : llo      
-- SELECT ltrim(CAST('  hello' AS CHAR(7)), ' '); # differ: doris : hello, presto : hello  
-- SELECT ltrim(CAST('  hello' AS CHAR(7)), 'e h'); # differ: doris :   hello, presto : llo    
SELECT ltrim(CAST('hello  ' AS CHAR(7)), 'l');
-- SELECT ltrim(CAST(' hello world ' AS CHAR(13)), ' '); # differ: doris : hello world , presto : hello world  
-- SELECT ltrim(CAST(' hello world ' AS CHAR(13)), ' eh'); # differ: doris :  hello world , presto : llo world    
-- SELECT ltrim(CAST(' hello world ' AS CHAR(13)), ' ehlowrd'); # differ: doris :  hello world , presto :              
-- SELECT ltrim(CAST(' hello world ' AS CHAR(13)), ' x'); # differ: doris :  hello world , presto : hello world  
-- SELECT ltrim(CAST('\u017a\u00f3\u0142\u0107' AS CHAR(4)), '\u00f3\u017a'); # differ: doris : \u01, presto :     
SELECT rtrim('', '');
SELECT rtrim('   ', '');
SELECT rtrim('  hello  ', '');
SELECT rtrim('  hello  ', ' ');
-- SELECT rtrim('  hello  ', 'lo '); # differ: doris :   hello  , presto :   he
SELECT rtrim('hello  ', ' ');
-- SELECT rtrim('hello  ', 'l o'); # differ: doris : hello  , presto : he
SELECT rtrim('hello  ', 'l');
SELECT rtrim(' hello world ', ' ');
-- SELECT rtrim(' hello world ', ' ld'); # differ: doris :  hello world , presto :  hello wor
-- SELECT rtrim(' hello world ', ' ehlowrd'); # differ: doris :  hello world , presto : 
-- SELECT rtrim(' hello world ', ' x'); # differ: doris :  hello world , presto :  hello world
-- SELECT rtrim(CAST('abc def' AS CHAR(7)), 'def'); # differ: doris : abc , presto : abc    
-- SELECT rtrim('\u017a\u00f3\u0142\u0107', '\u0107\u0142'); # differ: doris : \u017a\u00f3\u0142\u0107, presto : \u017a\u00f3
-- SELECT rtrim(CAST('' AS CHAR(1)), ''); # differ: doris : , presto :  
SELECT rtrim(CAST('   ' AS CHAR(3)), '');
SELECT rtrim(CAST('  hello  ' AS CHAR(9)), '');
-- SELECT rtrim(CAST('  hello  ' AS CHAR(9)), ' '); # differ: doris :   hello, presto :   hello  
SELECT rtrim(CAST('  hello  ' AS CHAR(9)), 'he ');
SELECT rtrim(CAST('  hello' AS CHAR(7)), ' ');
SELECT rtrim(CAST('  hello' AS CHAR(7)), 'e h');
SELECT rtrim(CAST('hello  ' AS CHAR(7)), 'l');
-- SELECT rtrim(CAST(' hello world ' AS CHAR(13)), ' '); # differ: doris :  hello world, presto :  hello world 
SELECT rtrim(CAST(' hello world ' AS CHAR(13)), ' eh');
-- SELECT rtrim(CAST(' hello world ' AS CHAR(13)), ' ehlowrd'); # differ: doris :  hello world , presto :              
SELECT rtrim(CAST(' hello world ' AS CHAR(13)), ' x');
-- SELECT rtrim(CAST('\u017a\u00f3\u0142\u0107' AS CHAR(4)), '\u0107\u0142'); # differ: doris : \u01, presto :     
SELECT lower(VARCHAR 'HELLO');
SELECT lower('');
SELECT lower('Hello World');
SELECT lower('WHAT!!');
SELECT lower('\u00D6STERREICH');
SELECT lower('From\uD801\uDC2DTo');
-- SELECT lower(CAST('' AS CHAR(10))); # differ: doris : , presto :           
SELECT lower(CAST('Hello World' AS CHAR(11)));
SELECT lower(CAST('WHAT!!' AS CHAR(6)));
SELECT lower(CAST('\u00D6STERREICH' AS CHAR(10)));
SELECT lower(CAST('From\uD801\uDC2DTo' AS CHAR(7)));
SELECT upper('');
SELECT upper('Hello World');
SELECT upper('what!!');
SELECT upper('\u00D6sterreich');
SELECT upper('From\uD801\uDC2DTo');
-- SELECT upper(CAST('' AS CHAR(10))); # differ: doris : , presto :           
SELECT upper(CAST('Hello World' AS CHAR(11)));
SELECT upper(CAST('what!!' AS CHAR(6)));
SELECT upper(CAST('\u00D6sterreich' AS CHAR(10)));
SELECT upper(CAST('From\uD801\uDC2DTo' AS CHAR(7)));
SELECT lpad('text', 5, 'x');
SELECT lpad('text', 4, 'x');
SELECT lpad('text', 6, 'xy');
SELECT lpad('text', 7, 'xy');
SELECT lpad('text', 9, 'xyz');
SELECT lpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 10, '\u671B');
SELECT lpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 11, '\u671B');
SELECT lpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 12, '\u5E0C\u671B');
SELECT lpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 13, '\u5E0C\u671B');
SELECT lpad('', 3, 'a');
SELECT lpad('abc', 0, 'e');
SELECT lpad('text', 3, 'xy');
SELECT lpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 5, '\u671B');
SELECT rpad('text', 5, 'x');
SELECT rpad('text', 4, 'x');
SELECT rpad('text', 6, 'xy');
SELECT rpad('text', 7, 'xy');
SELECT rpad('text', 9, 'xyz');
SELECT rpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 10, '\u671B');
SELECT rpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 11, '\u671B');
SELECT rpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 12, '\u5E0C\u671B');
SELECT rpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 13, '\u5E0C\u671B');
SELECT rpad('', 3, 'a');
SELECT rpad('abc', 0, 'e');
SELECT rpad('text', 3, 'xy');
SELECT rpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 5, '\u671B');
-- SELECT normalize('sch\u00f6n', NFD); # error: errCode = 2, detailMessage = Unknown column 'NFD' in 'table list' in PROJECT clause
-- SELECT normalize('sch\u00f6n'); # error: errCode = 2, detailMessage = Can not found function 'normalize'
-- SELECT normalize('sch\u00f6n', NFC); # error: errCode = 2, detailMessage = Unknown column 'NFC' in 'table list' in PROJECT clause
-- SELECT normalize('sch\u00f6n', NFKD); # error: errCode = 2, detailMessage = Unknown column 'NFKD' in 'table list' in PROJECT clause
-- SELECT normalize('sch\u00f6n', NFKC); # error: errCode = 2, detailMessage = Unknown column 'NFKC' in 'table list' in PROJECT clause
-- SELECT normalize('\u3231\u3327\u3326\u2162', NFKC); # error: errCode = 2, detailMessage = Unknown column 'NFKC' in 'table list' in PROJECT clause
-- SELECT normalize('\uff8a\uff9d\uff76\uff78\uff76\uff85', NFKC); # error: errCode = 2, detailMessage = Unknown column 'NFKC' in 'table list' in PROJECT clause
-- SELECT from_utf8(to_utf8('hello')); # error: errCode = 2, detailMessage = Can not found function 'ENCODE'
-- SELECT from_utf8(from_hex('58BF')); # error: errCode = 2, detailMessage = Can not found function 'DECODE'
-- SELECT from_utf8(from_hex('58DF')); # error: errCode = 2, detailMessage = Can not found function 'DECODE'
-- SELECT from_utf8(from_hex('58F7')); # error: errCode = 2, detailMessage = Can not found function 'DECODE'
-- SELECT from_utf8(from_hex('58BF'), '#'); # error: errCode = 2, detailMessage = Can not found function 'DECODE'
-- SELECT from_utf8(from_hex('58DF'), 35); # error: errCode = 2, detailMessage = Can not found function 'DECODE'
-- SELECT from_utf8(from_hex('58BF'), ''); # error: errCode = 2, detailMessage = Can not found function 'DECODE'
SELECT concat('ab ', cast(' ' as char(1)));
SELECT concat('ab ', cast(' ' as char(1)));
-- SELECT concat('ab ', cast('a' as char(2))); # differ: doris : ab a, presto : ab a 
-- SELECT concat('ab ', cast('a' as char(2))); # differ: doris : ab a, presto : ab a 
SELECT concat('ab ', cast('' as char(0)));
SELECT concat('ab ', cast('' as char(0)));
SELECT concat('hello na\u00EFve', cast(' world' as char(6)));
SELECT concat(cast(null as char(1)), cast(' ' as char(1)));
-- SELECT translate('abcd', '', ''); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('abcd', 'a', 'z'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('abcda', 'a', 'z'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ', 'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ', 'aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('Goiânia', 'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ', 'aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('São Paulo', 'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ', 'aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('Palhoça', 'ç', 'c'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('Várzea Paulista', 'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ', 'aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('\uD840\uDC00bcd', '\uD840\uDC00', 'z'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('\uD840\uDC00bcd\uD840\uDC00', '\uD840\uDC00', 'z'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('abcd', 'b', '\uD840\uDC00'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('abcd', 'a', ''); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('abcd', 'a', 'zy'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('abcd', 'ac', 'z'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('abcd', 'aac', 'zq'); # error: errCode = 2, detailMessage = Can not found function 'translate'
set debug_skip_fold_constant=true;
SELECT chr(65);
-- SELECT chr(9731); # differ: doris : &, presto : ☃
-- SELECT chr(131210); # differ: doris : None, presto : 𠂊
SELECT chr(0);
SELECT codepoint('x');
-- SELECT codepoint(CHR(128077)); # error: errCode = 2, detailMessage = Can not found function 'CHR'
-- SELECT codepoint(CHR(33804)); # error: errCode = 2, detailMessage = Can not found function 'CHR'
SELECT concat('hello', ' world');
SELECT concat('', '');
SELECT concat('what', '');
SELECT concat('', 'what');
SELECT concat('this', ' is', ' cool');
SELECT concat('this', ' is', ' cool');
SELECT concat('hello na\u00EFve', ' world');
SELECT concat('\uD801\uDC2D', 'end');
SELECT concat('\uD801\uDC2D', 'end', '\uD801\uDC2D');
SELECT concat('\u4FE1\u5FF5', ',\u7231', ',\u5E0C\u671B');
SELECT length('');
SELECT length('hello');
SELECT length('Quadratically');
SELECT length('hello na\u00EFve world');
SELECT length('\uD801\uDC2Dend');
SELECT length('\u4FE1\u5FF5,\u7231,\u5E0C\u671B');
SELECT length(CAST('hello' AS CHAR(5)));
SELECT length(CAST('Quadratically' AS CHAR(13)));
-- SELECT length(CAST('' AS CHAR(20))); # differ: doris : 0, presto : 20
-- SELECT length(CAST('hello' AS CHAR(20))); # differ: doris : 5, presto : 20
-- SELECT length(CAST('Quadratically' AS CHAR(20))); # differ: doris : 13, presto : 20
SELECT length(CAST('hello na\u00EFve world' AS CHAR(17)));
SELECT length(CAST('\uD801\uDC2Dend' AS CHAR(4)));
SELECT length(CAST('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' AS CHAR(7)));
SELECT length(CAST('hello na\u00EFve world' AS CHAR(20)));
-- SELECT length(CAST('\uD801\uDC2Dend' AS CHAR(20))); # differ: doris : 15, presto : 20
SELECT length(CAST('\u4FE1\u5FF5,\u7231,\u5E0C\u671B' AS CHAR(20)));
-- SELECT levenshtein_distance('', ''); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('', 'hello'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello', ''); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello', 'hello'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello', 'hello world'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello world', 'hel wold'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello world', 'hellq wodld'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('helo word', 'hello world'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello word', 'dello world'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello na\u00EFve world', 'hello naive world'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('hello na\u00EFve world', 'hello na:ive world'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u4FE1\u4EF0,\u7231,\u5E0C\u671B'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('\u4F11\u5FF5,\u7231,\u5E0C\u671B', '\u4FE1\u5FF5,\u7231,\u5E0C\u671B'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u4FE1\u5FF5\u5E0C\u671B'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT levenshtein_distance('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u4FE1\u5FF5,love,\u5E0C\u671B'); # error: errCode = 2, detailMessage = Can not found function 'levenshtein_distance'
-- SELECT hamming_distance('', ''); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance('hello', 'hello'); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance('hello', 'jello'); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance('like', 'hate'); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance('hello', 'world'); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance(NULL, NULL); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance('hello', NULL); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance(NULL, 'world'); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u4FE1\u4EF0,\u7231,\u5E0C\u671B'); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
-- SELECT hamming_distance('\u4F11\u5FF5,\u7231,\u5E0C\u671B', '\u4FE1\u5FF5,\u7231,\u5E0C\u671B'); # error: errCode = 2, detailMessage = Can not found function 'hamming_distance'
SELECT replace('aaa', 'a', 'aa');
SELECT replace('abcdefabcdef', 'cd', 'XX');
SELECT replace('abcdefabcdef', 'cd');
SELECT replace('123123tech', '123');
SELECT replace('123tech123', '123');
SELECT replace('222tech', '2', '3');
SELECT replace('0000123', '0');
SELECT replace('0000123', '0', ' ');
SELECT replace('foo', '');
SELECT replace('foo', '', '');
SELECT replace('foo', 'foo', '');
SELECT replace('abc', '', 'xx');
SELECT replace('', '', 'xx');
SELECT replace('', '');
SELECT replace('', '', '');
SELECT replace('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', '\u2014');
SELECT replace('::\uD801\uDC2D::', ':', '');
SELECT replace('\u00D6sterreich', '\u00D6', 'Oe');
SELECT reverse('');
SELECT reverse('hello');
SELECT reverse('Quadratically');
SELECT reverse('racecar');
SELECT reverse('\u4FE1\u5FF5,\u7231,\u5E0C\u671B');
SELECT reverse('\u00D6sterreich');
SELECT reverse('na\u00EFve');
SELECT reverse('\uD801\uDC2Dend');
SELECT strpos('high', 'ig');
SELECT strpos('high', 'igx');
SELECT strpos('Quadratically', 'a');
SELECT strpos('foobar', 'foobar');
SELECT strpos('foobar', 'obar');
SELECT strpos('zoo!', '!');
SELECT strpos('x', '');
SELECT strpos('', '');
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u7231');
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u5E0C\u671B');
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', 'nice');
SELECT strpos(NULL, '');
SELECT strpos('', NULL);
SELECT strpos(NULL, NULL);
SELECT starts_with('foo', 'foo');
SELECT starts_with('foo', 'bar');
SELECT starts_with('foo', '');
SELECT starts_with('', 'foo');
SELECT starts_with('', '');
SELECT starts_with('foo_bar_baz', 'foo');
SELECT starts_with('foo_bar_baz', 'bar');
SELECT starts_with('foo', 'foo_bar_baz');
SELECT starts_with('信念 爱 希望', '信念');
SELECT starts_with('信念 爱 希望', '爱');
SELECT strpos(NULL, '');
SELECT strpos('', NULL);
SELECT strpos(NULL, NULL);
SELECT strpos('abc/xyz/foo/bar', '/');
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u7231');
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u5E0C\u671B');
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', 'nice');
SELECT strpos('high', 'ig');
SELECT strpos('high', 'igx');
SELECT strpos('Quadratically', 'a');
SELECT strpos('foobar', 'foobar');
SELECT strpos('foobar', 'obar');
SELECT strpos('zoo!', '!');
SELECT strpos('x', '');
SELECT strpos('', '');
SELECT strpos('abc abc abc', 'abc', 1);
SELECT strpos('abc/xyz/foo/bar', '/', 1);
-- SELECT strpos('abc/xyz/foo/bar', '/', 2); # differ: doris : 4, presto : 8
-- SELECT strpos('abc/xyz/foo/bar', '/', 3); # differ: doris : 4, presto : 12
-- SELECT strpos('abc/xyz/foo/bar', '/', 4); # differ: doris : 4, presto : 0
SELECT strpos('highhigh', 'ig', 1);
SELECT strpos('foobarfoo', 'fb', 1);
SELECT strpos('foobarfoo', 'oo', 1);
-- SELECT strpos('abc abc abc', 'abc', -1); # differ: doris : 1, presto : 9
-- SELECT strpos('abc/xyz/foo/bar', '/', -1); # differ: doris : 4, presto : 12
-- SELECT strpos('abc/xyz/foo/bar', '/', -2); # differ: doris : 4, presto : 8
SELECT strpos('abc/xyz/foo/bar', '/', -3);
-- SELECT strpos('abc/xyz/foo/bar', '/', -4); # differ: doris : 4, presto : 0
-- SELECT strpos('highhigh', 'ig', -1); # differ: doris : 2, presto : 6
SELECT strpos('highhigh', 'ig', -2);
SELECT strpos('foobarfoo', 'fb', -1);
-- SELECT strpos('foobarfoo', 'oo', -1); # differ: doris : 2, presto : 8
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u7231', -1);
-- SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u7231\u671B', '\u7231', -1); # differ: doris : 14, presto : 27
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u7231\u671B', '\u7231', -2);
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', '\u5E0C\u671B', -1);
SELECT strpos('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', 'nice', -1);
SELECT substr('Quadratically', 5);
SELECT substr('Quadratically', 50);
SELECT substr('Quadratically', -5);
SELECT substr('Quadratically', -50);
SELECT substr('Quadratically', 0);
SELECT substr('Quadratically', 5, 6);
SELECT substr('Quadratically', 5, 10);
SELECT substr('Quadratically', 5, 50);
SELECT substr('Quadratically', 50, 10);
SELECT substr('Quadratically', -5, 4);
SELECT substr('Quadratically', -5, 40);
SELECT substr('Quadratically', -50, 4);
SELECT substr('Quadratically', 0, 4);
SELECT substr('Quadratically', 5, 0);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 5);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 50);
SELECT substr(CAST('Quadratically' AS CHAR(13)), -5);
SELECT substr(CAST('Quadratically' AS CHAR(13)), -50);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 0);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 5, 6);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 5, 10);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 5, 50);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 50, 10);
SELECT substr(CAST('Quadratically' AS CHAR(13)), -5, 4);
SELECT substr(CAST('Quadratically' AS CHAR(13)), -5, 40);
SELECT substr(CAST('Quadratically' AS CHAR(13)), -50, 4);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 0, 4);
SELECT substr(CAST('Quadratically' AS CHAR(13)), 5, 0);
SELECT substr(CAST('abc def' AS CHAR(7)), 1, 4);
-- SELECT substr(CAST('keep trailing' AS CHAR(14)), 1); # differ: doris : keep trailing, presto : keep trailing 
-- SELECT substr(CAST('keep trailing' AS CHAR(14)), 1, 14); # differ: doris : keep trailing, presto : keep trailing 
SELECT substring(CAST('Quadratically' AS CHAR(13)), 5);
SELECT substring(CAST('Quadratically' AS CHAR(13)), 5, 6);
-- SELECT split('a.b.c', '.'); # differ: doris : ["a", "b", "c"], presto : ['a', 'b', 'c']
-- SELECT split('ab', '.', 1); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a.b', '.', 1); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a.b.c', '.'); # differ: doris : ["a", "b", "c"], presto : ['a', 'b', 'c']
-- SELECT split('a..b..c', '..'); # differ: doris : ["a", "b", "c"], presto : ['a', 'b', 'c']
-- SELECT split('a.b.c', '.', 2); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a.b.c', '.', 3); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a.b.c', '.', 4); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a.b.c.', '.', 4); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a.b.c.', '.', 3); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('...', '.'); # differ: doris : ["", "", "", ""], presto : ['', '', '', '']
-- SELECT split('..a...a..', '.'); # differ: doris : ["", "", "a", "", "", "a", "", ""], presto : ['', '', 'a', '', '', 'a', '', '']
-- SELECT split('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 3); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('\u8B49\u8BC1\u8A3C', '\u8BC1', 2); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('.a.b.c', '.', 4); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('.a.b.c', '.', 3); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('.a.b.c', '.', 2); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a..b..c', '.', 3); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split('a.b..', '.', 3); # error: errCode = 2, detailMessage = Can not found function 'SPLIT_BY_STRING' which has 3 arity. Candidate functions are: [SPLIT_BY_STRINGorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@36658cd3]
-- SELECT split_to_map('', ',', '='); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_map('a=123,b=.4,c=,=d', ',', '='); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_map('=', ',', '='); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_map('key=>value', ',', '=>'); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_map('key => value', ',', '=>'); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_map('\u4EA0\u4EFF\u4EA1', '\u4E00', '\u4EFF'); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_map('\u4EC0\u4EFF', '\u4E00', '\u4EFF'); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_map('\u4EFF\u4EC1', '\u4E00', '\u4EFF'); # error: errCode = 2, detailMessage = Can not found function 'STR_TO_MAP'
-- SELECT split_to_multimap('', ',', '='); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('a=123,b=.4,c=,=d', ',', '='); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('=', ',', '='); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('a=123,a=.4,a=5.67', ',', '='); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('key=>value,key=>value', ',', '=>'); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('key => value, key => value', ',', '=>'); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('key => value, key => value', ', ', '=>'); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('\u4EA0\u4EFF\u4EA1', '\u4E00', '\u4EFF'); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
-- SELECT split_to_multimap('\u4EA0\u4EFF\u4EA1\u4E00\u4EA0\u4EFF\u4EB1', '\u4E00', '\u4EFF'); # error: errCode = 2, detailMessage = Can not found function 'split_to_multimap'
SELECT split_part('abc-@-def-@-ghi', '-@-', 1);
SELECT split_part('abc-@-def-@-ghi', '-@-', 2);
SELECT split_part('abc-@-def-@-ghi', '-@-', 3);
SELECT split_part('abc-@-def-@-ghi', '-@-', 4);
SELECT split_part('abc-@-def-@-ghi', '-@-', 99);
SELECT split_part('abc', 'abc', 1);
SELECT split_part('abc', 'abc', 2);
SELECT split_part('abc', 'abc', 3);
-- SELECT split_part('abc', '-@-', 1); # differ: doris : None, presto : abc
SELECT split_part('abc', '-@-', 2);
-- SELECT split_part('', 'abc', 1); # differ: doris : None, presto : 
-- SELECT split_part('', '', 1); # differ: doris : , presto : None
-- SELECT split_part('abc', '', 1); # differ: doris : , presto : a
-- SELECT split_part('abc', '', 2); # differ: doris : , presto : b
-- SELECT split_part('abc', '', 3); # differ: doris : , presto : c
-- SELECT split_part('abc', '', 4); # differ: doris : , presto : None
-- SELECT split_part('abc', '', 99); # differ: doris : , presto : None
-- SELECT split_part('abc', 'abcd', 1); # differ: doris : None, presto : abc
SELECT split_part('abc', 'abcd', 2);
SELECT split_part('abc--@--def', '-@-', 1);
SELECT split_part('abc--@--def', '-@-', 2);
SELECT split_part('abc-@-@-@-def', '-@-', 1);
SELECT split_part('abc-@-@-@-def', '-@-', 2);
SELECT split_part('abc-@-@-@-def', '-@-', 3);
SELECT split_part(' ', ' ', 1);
SELECT split_part('abcdddddef', 'dd', 1);
SELECT split_part('abcdddddef', 'dd', 2);
SELECT split_part('abcdddddef', 'dd', 3);
SELECT split_part('a/b/c', '/', 4);
SELECT split_part('a/b/c/', '/', 4);
SELECT split_part('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 1);
SELECT split_part('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 2);
SELECT split_part('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 3);
SELECT split_part('\u4FE1\u5FF5,\u7231,\u5E0C\u671B', ',', 4);
SELECT split_part('\u8B49\u8BC1\u8A3C', '\u8BC1', 1);
SELECT split_part('\u8B49\u8BC1\u8A3C', '\u8BC1', 2);
SELECT split_part('\u8B49\u8BC1\u8A3C', '\u8BC1', 3);
SELECT ltrim('');
SELECT ltrim('   ');
SELECT ltrim('  hello  ');
SELECT ltrim('  hello');
SELECT ltrim('hello  ');
SELECT ltrim(' hello world ');
SELECT ltrim('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ');
SELECT ltrim(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ');
SELECT ltrim('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B');
SELECT ltrim(' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B');
-- SELECT ltrim(CAST('' AS CHAR(20))); # differ: doris : , presto :                     
-- SELECT ltrim(CAST('  hello  ' AS CHAR(9))); # differ: doris : hello  , presto : hello    
-- SELECT ltrim(CAST('  hello' AS CHAR(7))); # differ: doris : hello, presto : hello  
SELECT ltrim(CAST('hello  ' AS CHAR(7)));
-- SELECT ltrim(CAST(' hello world ' AS CHAR(13))); # differ: doris : hello world , presto : hello world  
SELECT ltrim(CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ' AS CHAR(9)));
-- SELECT ltrim(CAST(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ' AS CHAR(9))); # differ: doris : \u4FE1\u, presto : \u4FE1\u 
-- SELECT ltrim(CAST('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(9))); # differ: doris : \u4FE1\, presto : \u4FE1\  
-- SELECT ltrim(CAST(' \u2028 \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(10))); # differ: doris : \u2028 \u, presto : \u2028 \u 
SELECT rtrim('');
SELECT rtrim('   ');
SELECT rtrim('  hello  ');
SELECT rtrim('  hello');
SELECT rtrim('hello  ');
SELECT rtrim(' hello world ');
SELECT rtrim('\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ');
SELECT rtrim('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ');
SELECT rtrim(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ');
SELECT rtrim('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B');
-- SELECT rtrim(CAST('' AS CHAR(20))); # differ: doris : , presto :                     
-- SELECT rtrim(CAST('  hello  ' AS CHAR(9))); # differ: doris :   hello, presto :   hello  
SELECT rtrim(CAST('  hello' AS CHAR(7)));
-- SELECT rtrim(CAST('hello  ' AS CHAR(7))); # differ: doris : hello, presto : hello  
-- SELECT rtrim(CAST(' hello world ' AS CHAR(13))); # differ: doris :  hello world, presto :  hello world 
SELECT rtrim(CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B \u2028 ' AS CHAR(10)));
SELECT rtrim(CAST('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ' AS CHAR(9)));
SELECT rtrim(CAST(' \u4FE1\u5FF5 \u7231 \u5E0C\u671B ' AS CHAR(9)));
SELECT rtrim(CAST('  \u4FE1\u5FF5 \u7231 \u5E0C\u671B' AS CHAR(9)));
SELECT ltrim('', '');
SELECT ltrim('   ', '');
SELECT ltrim('  hello  ', '');
SELECT ltrim('  hello  ', ' ');
SELECT ltrim('  hello  ', CHAR ' ');
-- SELECT ltrim('  hello  ', 'he '); # differ: doris :   hello  , presto : llo  
SELECT ltrim('  hello', ' ');
-- SELECT ltrim('  hello', 'e h'); # differ: doris :   hello, presto : llo
SELECT ltrim('hello  ', 'l');
SELECT ltrim(' hello world ', ' ');
-- SELECT ltrim(' hello world ', ' eh'); # differ: doris :  hello world , presto : llo world 
-- SELECT ltrim(' hello world ', ' ehlowrd'); # differ: doris :  hello world , presto : 
-- SELECT ltrim(' hello world ', ' x'); # differ: doris :  hello world , presto : hello world 
-- SELECT ltrim('\u017a\u00f3\u0142\u0107', '\u00f3\u017a'); # differ: doris : \u017a\u00f3\u0142\u0107, presto : 42\u0107
-- SELECT ltrim(CAST('' AS CHAR(1)), ''); # differ: doris : , presto :  
SELECT ltrim(CAST('   ' AS CHAR(3)), '');
SELECT ltrim(CAST('  hello  ' AS CHAR(9)), '');
-- SELECT ltrim(CAST('  hello  ' AS CHAR(9)), ' '); # differ: doris : hello  , presto : hello    
-- SELECT ltrim(CAST('  hello  ' AS CHAR(9)), 'he '); # differ: doris :   hello  , presto : llo      
-- SELECT ltrim(CAST('  hello' AS CHAR(7)), ' '); # differ: doris : hello, presto : hello  
-- SELECT ltrim(CAST('  hello' AS CHAR(7)), 'e h'); # differ: doris :   hello, presto : llo    
SELECT ltrim(CAST('hello  ' AS CHAR(7)), 'l');
-- SELECT ltrim(CAST(' hello world ' AS CHAR(13)), ' '); # differ: doris : hello world , presto : hello world  
-- SELECT ltrim(CAST(' hello world ' AS CHAR(13)), ' eh'); # differ: doris :  hello world , presto : llo world    
-- SELECT ltrim(CAST(' hello world ' AS CHAR(13)), ' ehlowrd'); # differ: doris :  hello world , presto :              
-- SELECT ltrim(CAST(' hello world ' AS CHAR(13)), ' x'); # differ: doris :  hello world , presto : hello world  
-- SELECT ltrim(CAST('\u017a\u00f3\u0142\u0107' AS CHAR(4)), '\u00f3\u017a'); # differ: doris : \u01, presto :     
SELECT rtrim('', '');
SELECT rtrim('   ', '');
SELECT rtrim('  hello  ', '');
SELECT rtrim('  hello  ', ' ');
-- SELECT rtrim('  hello  ', 'lo '); # differ: doris :   hello  , presto :   he
SELECT rtrim('hello  ', ' ');
-- SELECT rtrim('hello  ', 'l o'); # differ: doris : hello  , presto : he
SELECT rtrim('hello  ', 'l');
SELECT rtrim(' hello world ', ' ');
-- SELECT rtrim(' hello world ', ' ld'); # differ: doris :  hello world , presto :  hello wor
-- SELECT rtrim(' hello world ', ' ehlowrd'); # differ: doris :  hello world , presto : 
-- SELECT rtrim(' hello world ', ' x'); # differ: doris :  hello world , presto :  hello world
-- SELECT rtrim(CAST('abc def' AS CHAR(7)), 'def'); # differ: doris : abc , presto : abc    
-- SELECT rtrim('\u017a\u00f3\u0142\u0107', '\u0107\u0142'); # differ: doris : \u017a\u00f3\u0142\u0107, presto : \u017a\u00f3
-- SELECT rtrim(CAST('' AS CHAR(1)), ''); # differ: doris : , presto :  
SELECT rtrim(CAST('   ' AS CHAR(3)), '');
SELECT rtrim(CAST('  hello  ' AS CHAR(9)), '');
-- SELECT rtrim(CAST('  hello  ' AS CHAR(9)), ' '); # differ: doris :   hello, presto :   hello  
SELECT rtrim(CAST('  hello  ' AS CHAR(9)), 'he ');
SELECT rtrim(CAST('  hello' AS CHAR(7)), ' ');
SELECT rtrim(CAST('  hello' AS CHAR(7)), 'e h');
SELECT rtrim(CAST('hello  ' AS CHAR(7)), 'l');
-- SELECT rtrim(CAST(' hello world ' AS CHAR(13)), ' '); # differ: doris :  hello world, presto :  hello world 
SELECT rtrim(CAST(' hello world ' AS CHAR(13)), ' eh');
-- SELECT rtrim(CAST(' hello world ' AS CHAR(13)), ' ehlowrd'); # differ: doris :  hello world , presto :              
SELECT rtrim(CAST(' hello world ' AS CHAR(13)), ' x');
-- SELECT rtrim(CAST('\u017a\u00f3\u0142\u0107' AS CHAR(4)), '\u0107\u0142'); # differ: doris : \u01, presto :     
SELECT lower(VARCHAR 'HELLO');
SELECT lower('');
SELECT lower('Hello World');
SELECT lower('WHAT!!');
SELECT lower('\u00D6STERREICH');
SELECT lower('From\uD801\uDC2DTo');
-- SELECT lower(CAST('' AS CHAR(10))); # differ: doris : , presto :           
SELECT lower(CAST('Hello World' AS CHAR(11)));
SELECT lower(CAST('WHAT!!' AS CHAR(6)));
SELECT lower(CAST('\u00D6STERREICH' AS CHAR(10)));
SELECT lower(CAST('From\uD801\uDC2DTo' AS CHAR(7)));
SELECT upper('');
SELECT upper('Hello World');
SELECT upper('what!!');
SELECT upper('\u00D6sterreich');
SELECT upper('From\uD801\uDC2DTo');
-- SELECT upper(CAST('' AS CHAR(10))); # differ: doris : , presto :           
SELECT upper(CAST('Hello World' AS CHAR(11)));
SELECT upper(CAST('what!!' AS CHAR(6)));
SELECT upper(CAST('\u00D6sterreich' AS CHAR(10)));
SELECT upper(CAST('From\uD801\uDC2DTo' AS CHAR(7)));
SELECT lpad('text', 5, 'x');
SELECT lpad('text', 4, 'x');
SELECT lpad('text', 6, 'xy');
SELECT lpad('text', 7, 'xy');
SELECT lpad('text', 9, 'xyz');
SELECT lpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 10, '\u671B');
SELECT lpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 11, '\u671B');
SELECT lpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 12, '\u5E0C\u671B');
SELECT lpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 13, '\u5E0C\u671B');
SELECT lpad('', 3, 'a');
SELECT lpad('abc', 0, 'e');
SELECT lpad('text', 3, 'xy');
SELECT lpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 5, '\u671B');
SELECT rpad('text', 5, 'x');
SELECT rpad('text', 4, 'x');
SELECT rpad('text', 6, 'xy');
SELECT rpad('text', 7, 'xy');
SELECT rpad('text', 9, 'xyz');
SELECT rpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 10, '\u671B');
SELECT rpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 11, '\u671B');
SELECT rpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 12, '\u5E0C\u671B');
SELECT rpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 13, '\u5E0C\u671B');
SELECT rpad('', 3, 'a');
SELECT rpad('abc', 0, 'e');
SELECT rpad('text', 3, 'xy');
SELECT rpad('\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ', 5, '\u671B');
-- SELECT normalize('sch\u00f6n', NFD); # error: errCode = 2, detailMessage = Unknown column 'NFD' in 'table list' in PROJECT clause
-- SELECT normalize('sch\u00f6n'); # error: errCode = 2, detailMessage = Can not found function 'normalize'
-- SELECT normalize('sch\u00f6n', NFC); # error: errCode = 2, detailMessage = Unknown column 'NFC' in 'table list' in PROJECT clause
-- SELECT normalize('sch\u00f6n', NFKD); # error: errCode = 2, detailMessage = Unknown column 'NFKD' in 'table list' in PROJECT clause
-- SELECT normalize('sch\u00f6n', NFKC); # error: errCode = 2, detailMessage = Unknown column 'NFKC' in 'table list' in PROJECT clause
-- SELECT normalize('\u3231\u3327\u3326\u2162', NFKC); # error: errCode = 2, detailMessage = Unknown column 'NFKC' in 'table list' in PROJECT clause
-- SELECT normalize('\uff8a\uff9d\uff76\uff78\uff76\uff85', NFKC); # error: errCode = 2, detailMessage = Unknown column 'NFKC' in 'table list' in PROJECT clause
-- SELECT from_utf8(to_utf8('hello')); # error: errCode = 2, detailMessage = Can not found function 'ENCODE'
-- SELECT from_utf8(from_hex('58BF')); # error: errCode = 2, detailMessage = Can not found function 'DECODE'
-- SELECT from_utf8(from_hex('58DF')); # error: errCode = 2, detailMessage = Can not found function 'DECODE'
-- SELECT from_utf8(from_hex('58F7')); # error: errCode = 2, detailMessage = Can not found function 'DECODE'
-- SELECT from_utf8(from_hex('58BF'), '#'); # error: errCode = 2, detailMessage = Can not found function 'DECODE'
-- SELECT from_utf8(from_hex('58DF'), 35); # error: errCode = 2, detailMessage = Can not found function 'DECODE'
-- SELECT from_utf8(from_hex('58BF'), ''); # error: errCode = 2, detailMessage = Can not found function 'DECODE'
SELECT concat('ab ', cast(' ' as char(1)));
SELECT concat('ab ', cast(' ' as char(1)));
-- SELECT concat('ab ', cast('a' as char(2))); # differ: doris : ab a, presto : ab a 
-- SELECT concat('ab ', cast('a' as char(2))); # differ: doris : ab a, presto : ab a 
SELECT concat('ab ', cast('' as char(0)));
SELECT concat('ab ', cast('' as char(0)));
SELECT concat('hello na\u00EFve', cast(' world' as char(6)));
SELECT concat(cast(null as char(1)), cast(' ' as char(1)));
-- SELECT translate('abcd', '', ''); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('abcd', 'a', 'z'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('abcda', 'a', 'z'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ', 'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ', 'aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('Goiânia', 'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ', 'aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('São Paulo', 'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ', 'aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('Palhoça', 'ç', 'c'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('Várzea Paulista', 'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜâêîôûÂÊÎÔÛãẽĩõũÃẼĨÕŨ', 'aeiouAEIOUaeiouAEIOUaeiouAEIOUaeiouAEIOU'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('\uD840\uDC00bcd', '\uD840\uDC00', 'z'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('\uD840\uDC00bcd\uD840\uDC00', '\uD840\uDC00', 'z'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('abcd', 'b', '\uD840\uDC00'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('abcd', 'a', ''); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('abcd', 'a', 'zy'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('abcd', 'ac', 'z'); # error: errCode = 2, detailMessage = Can not found function 'translate'
-- SELECT translate('abcd', 'aac', 'zq'); # error: errCode = 2, detailMessage = Can not found function 'translate'
