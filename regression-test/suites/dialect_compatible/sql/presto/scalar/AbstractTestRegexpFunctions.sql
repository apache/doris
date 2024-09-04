set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
SELECT regexp_like('Stephen', 'Ste(v|ph)en');
SELECT regexp_like('Stevens', 'Ste(v|ph)en');
SELECT regexp_like('Stephen', '^Ste(v|ph)en$');
SELECT regexp_like('Stevens', '^Ste(v|ph)en$');
SELECT regexp_like('hello world', '[a-z]');
SELECT regexp_like('hello\nworld', '.*hello\nworld.*');
SELECT regexp_like('Hello', '^[a-z]+$');
SELECT regexp_like('Hello', '^(?i)[a-z]+$');
SELECT regexp_like('Hello', '^[a-zA-Z]+$');
SELECT regexp_like('test', 'test\\b');
SELECT regexp_like('ala', CHAR 'ala  ');
SELECT regexp_like('ala  ', CHAR 'ala  ');
SELECT regexp_replace('abc有朋$%X自9远方来', '', 'Y');
SELECT regexp_replace('a有朋💰', '.', 'Y');
-- SELECT regexp_replace('a有朋💰', '.', '1$02'); # differ: doris : 1$021$021$021$02, presto : 1a21有21朋21💰2
SELECT regexp_replace('', '', 'Y');
SELECT regexp_replace('fun stuff.', '[a-z]');
SELECT regexp_replace('fun stuff.', '[a-z]', '*');
SELECT regexp_replace('call 555.123.4444 now', '(\\d{3})\\.(\\d{3}).(\\d{4})');
SELECT regexp_replace('call 555.123.4444 now', '(\\d{3})\\.(\\d{3}).(\\d{4})', '($1) $2-$3');
SELECT regexp_replace('xxx xxx xxx', 'x', 'x');
SELECT regexp_replace('xxx xxx xxx', 'x', '\\x');
SELECT regexp_replace('xxx', '', 'y');
SELECT regexp_replace('xxx', 'x', '\\');
-- SELECT regexp_replace('xxx xxx xxx', 'x', '$0'); # differ: doris : $0$0$0 $0$0$0 $0$0$0, presto : xxx xxx xxx
-- SELECT regexp_replace('xxx', '(x)', '$01'); # differ: doris : $01$01$01, presto : xxx
-- SELECT regexp_replace('xxx', 'x', '$05'); # differ: doris : $05$05$05, presto : x5x5x5
-- SELECT regexp_replace('123456789', '(1)(2)(3)(4)(5)(6)(7)(8)(9)', '$10'); # differ: doris : $10, presto : 10
-- SELECT regexp_replace('1234567890', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)', '$10'); # differ: doris : $10, presto : 0
-- SELECT regexp_replace('1234567890', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)', '$11'); # differ: doris : $11, presto : 11
-- SELECT regexp_replace('1234567890', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)', '$1a'); # differ: doris : $1a, presto : 1a
-- SELECT regexp_replace('wxyz', '(?<xyz>[xyz])', '${xyz}${xyz}'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Could not compile regexp pattern: (?<xyz>[xyz])	Error: invalid perl operator: (?<
-- SELECT regexp_replace('wxyz', '(?<w>w)|(?<xyz>[xyz])', '[${w}](${xyz})'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Could not compile regexp pattern: (?<w>w)|(?<xyz>[xyz])	Error: invalid perl operator: (?<
-- SELECT regexp_replace('xyz', '(?<xyz>[xyz])+', '${xyz}'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Could not compile regexp pattern: (?<xyz>[xyz])+	Error: invalid perl operator: (?<
-- SELECT regexp_replace('xyz', '(?<xyz>[xyz]+)', '${xyz}'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Could not compile regexp pattern: (?<xyz>[xyz]+)	Error: invalid perl operator: (?<
-- SELECT regexp_replace(VARCHAR 'x', '.*', 'xxxxx'); # differ: doris : xxxxx, presto : xxxxxxxxxx
-- SELECT regexp_replace('abc有朋$%X自9远方来', '', x->'Y'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('a有朋💰', '.', x->'Y'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('a有朋💰', '(.)', x->'1'||x[1]||'2'); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('', '', x->'Y'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('new', '(\\w)', x->x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abc abc', '(abc)', x->'m'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('123 456', '([0-9]*)', x->x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('123 456', '(([0-9]*) ([0-9]*))', x->x[2]||x[3]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abbabba', '(abba)', x->'m'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('abbabba', '(abba)', x->'m'||x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abc', '(.)', x->'m'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('abc', '.', x->'m'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('abbabba', 'abba', x->'m'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('abc abc', 'abc', x->'m'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('abc', '', x->'OK'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('abc', '()', x->x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abc', '()', x->'OK'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('new york', '(\\w*)', x->'<'||x[1]||'>'); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('aaa', '(b)?', x->x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abde', 'ab(c)?de', x->x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abde', 'ab(c)?de', x->'OK'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('abde', 'ab(c)?de', x->x[1]||'OK'); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abde', 'ab(c)?de', x->'OK'||x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abde', '(c)', x->x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abde', '(c)', x->'m'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_extract('Hello world bye', '\\b[a-z]([a-z]*)'); # differ: doris : , presto : None
-- SELECT regexp_extract('Hello world bye', '\\b[a-z]([a-z]*)', 1); # differ: doris : , presto : None
-- SELECT regexp_extract('rat cat\nbat dog', 'ra(.)|blah(.)(.)', 2); # differ: doris : , presto : None
-- SELECT regexp_extract('12345', 'x'); # differ: doris : , presto : None
-- SELECT regexp_extract('Baby X', 'by ([A-Z].*)\\b[a-z]'); # differ: doris : , presto : None
-- SELECT regexp_extract_all('abc有朋$%X自9远方来💰', ''); # differ: doris : , presto : ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
-- SELECT regexp_extract_all('a有朋💰', '.'); # differ: doris : ['a','有','朋','💰'], presto : ['a', '有', '朋', '💰']
-- SELECT regexp_extract_all('', ''); # differ: doris : , presto : ['']
-- SELECT regexp_extract_all('rat cat\nbat dog', '.at'); # differ: doris : ['rat','cat','bat'], presto : ['rat', 'cat', 'bat']
-- SELECT regexp_extract_all('rat cat\nbat dog', '(.)at', 1); # error: errCode = 2, detailMessage = Can not found function 'regexp_extract_all' which has 3 arity. Candidate functions are: [regexp_extract_allorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@44ce569]
-- SELECT regexp_extract_all('rat cat\nbat dog', 'ra(.)|blah(.)(.)', 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_extract_all' which has 3 arity. Candidate functions are: [regexp_extract_allorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@44ce569]
-- SELECT regexp_extract_all('12345', ''); # differ: doris : , presto : ['', '', '', '', '', '']
-- SELECT regexp_split('abc有朋$%X自9远方来💰', ''); # differ: doris : ["a", "b", "c", "�", "�", "�", "�", "�", "�", "$", "%", "X", "�", "�", "�", "9", "�", "�", "�", "�", "�", "�", "�", "�", "�", "�", "�", "�", "�"], presto : ['', 'a', 'b', 'c', '有', '朋', '$', '%', 'X', '自', '9', '远', '方', '来', '💰', '']
-- SELECT regexp_split('a有朋💰', '.'); # differ: doris : ["a有朋💰"], presto : ['', '', '', '', '']
-- SELECT regexp_split('', ''); # differ: doris : [], presto : ['', '']
-- SELECT regexp_split('abc', 'a'); # differ: doris : ["", "bc"], presto : ['', 'bc']
-- SELECT regexp_split('a.b:c;d', '[\\.:;]'); # differ: doris : ["a.b:c;d"], presto : ['a', 'b', 'c', 'd']
-- SELECT regexp_split('a.b:c;d', '\\.'); # differ: doris : ["a.b:c;d"], presto : ['a.b:c;d']
-- SELECT regexp_split('a.b:c;d', ':'); # differ: doris : ["a.b", "c;d"], presto : ['a.b', 'c;d']
-- SELECT regexp_split('a,b,c', ','); # differ: doris : ["a", "b", "c"], presto : ['a', 'b', 'c']
-- SELECT regexp_split('a1b2c3d', '\\d'); # differ: doris : ["a1b2c3d"], presto : ['a1b2c3d']
-- SELECT regexp_split('a1b2346c3d', '\\d+'); # differ: doris : ["a1b2346c3d"], presto : ['a1b2346c3d']
-- SELECT regexp_split('abcd', 'x'); # differ: doris : ["abcd"], presto : ['abcd']
-- SELECT regexp_split('abcd', ''); # differ: doris : ["a", "b", "c", "d"], presto : ['', 'a', 'b', 'c', 'd', '']
-- SELECT regexp_split('', 'x'); # differ: doris : [], presto : ['']
-- SELECT regexp_split('a,b,c,d', ','); # differ: doris : ["a", "b", "c", "d"], presto : ['a', 'b', 'c', 'd']
-- SELECT regexp_split(',,a,,,b,c,d,,', ','); # differ: doris : ["", "", "a", "", "", "b", "c", "d", "", ""], presto : ['', '', 'a', '', '', 'b', 'c', 'd', '', '']
-- SELECT regexp_split(',,,', ','); # differ: doris : ["", "", "", ""], presto : ['', '', '', '']
-- SELECT regexp_count('a.b:c;d', '[\\.:;]'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('a.b:c;d', '[\\.:;]')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('a.b:c;d', '\\.'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('a.b:c;d', '\\.')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('a.b:c;d', ':'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('a.b:c;d', ':')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('a,b,c', ','); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('a,b,c', ',')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('a1b2c3d', '\\d'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('a1b2c3d', '\\d')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('a1b2346c3d', '\\d+'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('a1b2346c3d', '\\d+')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('abcd', 'x'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('abcd', 'x')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('Hello world bye', '\\b[a-z]([a-z]*)'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('Hello world bye', '\\b[a-z]([a-z]*)')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('rat cat\nbat dog', 'ra(.)|blah(.)(.)'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('rat cat\nbat dog', 'ra(.)|blah(.)(.)')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('Baby X', 'by ([A-Z].*)\\b[a-z]'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('Baby X', 'by ([A-Z].*)\\b[a-z]')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('rat cat bat dog', '.at'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('rat cat bat dog', '.at')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('', 'x'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('', 'x')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('', ''); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('', '')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('君子矜而不争，党而不群', '不'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('君子矜而不争，党而不群', '不')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('abcd', ''); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('abcd', '')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('1a 2b 14m', '\\s*[a-z]+\\s*'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('1a 2b 14m', '\\s*[a-z]+\\s*')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_position('a.b:c;d', '[\\.:;]'); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a.b:c;d', '\\.'); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a.b:c;d', ':'); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a,b,c', ','); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '\\d'); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a,b,c', ',', 3); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '\\d', 5); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d4e', '\\d', 4, 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '\\d', 4, 3); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', ''); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '', 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '', 2, 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '', 2, 6); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '', 2, 7); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '', 2, 8); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('行成于思str而毁123于随', '于', 3, 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('行成于思str而毁123于随', '', 3, 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('行成于思str而毁123于随', '', 3, 1); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('', ', '); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('', ', ', 4); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('', ', ', 4, 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a,b,c', ',', 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '\\d', 4); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('有朋$%X自9远方来', '\\d', 7); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('有朋$%X自9远方9来', '\\d', 10, 1); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('有朋$%X自9远方9来', '\\d', 10, 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a,b,c', ', ', 1000); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a,b,c', ', ', 8); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('有朋$%X自9远方9来', '来', 999); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('9102, say good bye', '\\s*[a-z]+\\s*'); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('natasha, 9102, miss you', '\\s*[a-z]+\\s*', 10); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('natasha, 9102, miss you', '\\s', 10, 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
set debug_skip_fold_constant=true;
SELECT regexp_like('Stephen', 'Ste(v|ph)en');
SELECT regexp_like('Stevens', 'Ste(v|ph)en');
SELECT regexp_like('Stephen', '^Ste(v|ph)en$');
SELECT regexp_like('Stevens', '^Ste(v|ph)en$');
SELECT regexp_like('hello world', '[a-z]');
SELECT regexp_like('hello\nworld', '.*hello\nworld.*');
SELECT regexp_like('Hello', '^[a-z]+$');
SELECT regexp_like('Hello', '^(?i)[a-z]+$');
SELECT regexp_like('Hello', '^[a-zA-Z]+$');
SELECT regexp_like('test', 'test\\b');
SELECT regexp_like('ala', CHAR 'ala  ');
SELECT regexp_like('ala  ', CHAR 'ala  ');
SELECT regexp_replace('abc有朋$%X自9远方来', '', 'Y');
SELECT regexp_replace('a有朋💰', '.', 'Y');
-- SELECT regexp_replace('a有朋💰', '.', '1$02'); # differ: doris : 1$021$021$021$02, presto : 1a21有21朋21💰2
SELECT regexp_replace('', '', 'Y');
SELECT regexp_replace('fun stuff.', '[a-z]');
SELECT regexp_replace('fun stuff.', '[a-z]', '*');
SELECT regexp_replace('call 555.123.4444 now', '(\\d{3})\\.(\\d{3}).(\\d{4})');
SELECT regexp_replace('call 555.123.4444 now', '(\\d{3})\\.(\\d{3}).(\\d{4})', '($1) $2-$3');
SELECT regexp_replace('xxx xxx xxx', 'x', 'x');
SELECT regexp_replace('xxx xxx xxx', 'x', '\\x');
SELECT regexp_replace('xxx', '', 'y');
SELECT regexp_replace('xxx', 'x', '\\');
-- SELECT regexp_replace('xxx xxx xxx', 'x', '$0'); # differ: doris : $0$0$0 $0$0$0 $0$0$0, presto : xxx xxx xxx
-- SELECT regexp_replace('xxx', '(x)', '$01'); # differ: doris : $01$01$01, presto : xxx
-- SELECT regexp_replace('xxx', 'x', '$05'); # differ: doris : $05$05$05, presto : x5x5x5
-- SELECT regexp_replace('123456789', '(1)(2)(3)(4)(5)(6)(7)(8)(9)', '$10'); # differ: doris : $10, presto : 10
-- SELECT regexp_replace('1234567890', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)', '$10'); # differ: doris : $10, presto : 0
-- SELECT regexp_replace('1234567890', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)', '$11'); # differ: doris : $11, presto : 11
-- SELECT regexp_replace('1234567890', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)', '$1a'); # differ: doris : $1a, presto : 1a
-- SELECT regexp_replace('wxyz', '(?<xyz>[xyz])', '${xyz}${xyz}'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Could not compile regexp pattern: (?<xyz>[xyz])	Error: invalid perl operator: (?<
-- SELECT regexp_replace('wxyz', '(?<w>w)|(?<xyz>[xyz])', '[${w}](${xyz})'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Could not compile regexp pattern: (?<w>w)|(?<xyz>[xyz])	Error: invalid perl operator: (?<
-- SELECT regexp_replace('xyz', '(?<xyz>[xyz])+', '${xyz}'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Could not compile regexp pattern: (?<xyz>[xyz])+	Error: invalid perl operator: (?<
-- SELECT regexp_replace('xyz', '(?<xyz>[xyz]+)', '${xyz}'); # error: errCode = 2, detailMessage = (127.0.0.1)[INVALID_ARGUMENT]Could not compile regexp pattern: (?<xyz>[xyz]+)	Error: invalid perl operator: (?<
-- SELECT regexp_replace(VARCHAR 'x', '.*', 'xxxxx'); # differ: doris : xxxxx, presto : xxxxxxxxxx
-- SELECT regexp_replace('abc有朋$%X自9远方来', '', x->'Y'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('a有朋💰', '.', x->'Y'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('a有朋💰', '(.)', x->'1'||x[1]||'2'); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('', '', x->'Y'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('new', '(\\w)', x->x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abc abc', '(abc)', x->'m'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('123 456', '([0-9]*)', x->x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('123 456', '(([0-9]*) ([0-9]*))', x->x[2]||x[3]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abbabba', '(abba)', x->'m'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('abbabba', '(abba)', x->'m'||x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abc', '(.)', x->'m'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('abc', '.', x->'m'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('abbabba', 'abba', x->'m'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('abc abc', 'abc', x->'m'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('abc', '', x->'OK'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('abc', '()', x->x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abc', '()', x->'OK'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('new york', '(\\w*)', x->'<'||x[1]||'>'); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('aaa', '(b)?', x->x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abde', 'ab(c)?de', x->x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abde', 'ab(c)?de', x->'OK'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_replace('abde', 'ab(c)?de', x->x[1]||'OK'); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abde', 'ab(c)?de', x->'OK'||x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abde', '(c)', x->x[1]); # error: errCode = 2, detailMessage = Invalid call to x.getDataType() on unbound object
-- SELECT regexp_replace('abde', '(c)', x->'m'); # error: errCode = 2, detailMessage = can not cast from origin type Lambda to target type=VARCHAR(65533)
-- SELECT regexp_extract('Hello world bye', '\\b[a-z]([a-z]*)'); # differ: doris : , presto : None
-- SELECT regexp_extract('Hello world bye', '\\b[a-z]([a-z]*)', 1); # differ: doris : , presto : None
-- SELECT regexp_extract('rat cat\nbat dog', 'ra(.)|blah(.)(.)', 2); # differ: doris : , presto : None
-- SELECT regexp_extract('12345', 'x'); # differ: doris : , presto : None
-- SELECT regexp_extract('Baby X', 'by ([A-Z].*)\\b[a-z]'); # differ: doris : , presto : None
-- SELECT regexp_extract_all('abc有朋$%X自9远方来💰', ''); # differ: doris : , presto : ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
-- SELECT regexp_extract_all('a有朋💰', '.'); # differ: doris : ['a','有','朋','💰'], presto : ['a', '有', '朋', '💰']
-- SELECT regexp_extract_all('', ''); # differ: doris : , presto : ['']
-- SELECT regexp_extract_all('rat cat\nbat dog', '.at'); # differ: doris : ['rat','cat','bat'], presto : ['rat', 'cat', 'bat']
-- SELECT regexp_extract_all('rat cat\nbat dog', '(.)at', 1); # error: errCode = 2, detailMessage = Can not found function 'regexp_extract_all' which has 3 arity. Candidate functions are: [regexp_extract_allorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@44ce569]
-- SELECT regexp_extract_all('rat cat\nbat dog', 'ra(.)|blah(.)(.)', 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_extract_all' which has 3 arity. Candidate functions are: [regexp_extract_allorg.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder@44ce569]
-- SELECT regexp_extract_all('12345', ''); # differ: doris : , presto : ['', '', '', '', '', '']
-- SELECT regexp_split('abc有朋$%X自9远方来💰', ''); # differ: doris : ["a", "b", "c", "�", "�", "�", "�", "�", "�", "$", "%", "X", "�", "�", "�", "9", "�", "�", "�", "�", "�", "�", "�", "�", "�", "�", "�", "�", "�"], presto : ['', 'a', 'b', 'c', '有', '朋', '$', '%', 'X', '自', '9', '远', '方', '来', '💰', '']
-- SELECT regexp_split('a有朋💰', '.'); # differ: doris : ["a有朋💰"], presto : ['', '', '', '', '']
-- SELECT regexp_split('', ''); # differ: doris : [], presto : ['', '']
-- SELECT regexp_split('abc', 'a'); # differ: doris : ["", "bc"], presto : ['', 'bc']
-- SELECT regexp_split('a.b:c;d', '[\\.:;]'); # differ: doris : ["a.b:c;d"], presto : ['a', 'b', 'c', 'd']
-- SELECT regexp_split('a.b:c;d', '\\.'); # differ: doris : ["a.b:c;d"], presto : ['a.b:c;d']
-- SELECT regexp_split('a.b:c;d', ':'); # differ: doris : ["a.b", "c;d"], presto : ['a.b', 'c;d']
-- SELECT regexp_split('a,b,c', ','); # differ: doris : ["a", "b", "c"], presto : ['a', 'b', 'c']
-- SELECT regexp_split('a1b2c3d', '\\d'); # differ: doris : ["a1b2c3d"], presto : ['a1b2c3d']
-- SELECT regexp_split('a1b2346c3d', '\\d+'); # differ: doris : ["a1b2346c3d"], presto : ['a1b2346c3d']
-- SELECT regexp_split('abcd', 'x'); # differ: doris : ["abcd"], presto : ['abcd']
-- SELECT regexp_split('abcd', ''); # differ: doris : ["a", "b", "c", "d"], presto : ['', 'a', 'b', 'c', 'd', '']
-- SELECT regexp_split('', 'x'); # differ: doris : [], presto : ['']
-- SELECT regexp_split('a,b,c,d', ','); # differ: doris : ["a", "b", "c", "d"], presto : ['a', 'b', 'c', 'd']
-- SELECT regexp_split(',,a,,,b,c,d,,', ','); # differ: doris : ["", "", "a", "", "", "b", "c", "d", "", ""], presto : ['', '', 'a', '', '', 'b', 'c', 'd', '', '']
-- SELECT regexp_split(',,,', ','); # differ: doris : ["", "", "", ""], presto : ['', '', '', '']
-- SELECT regexp_count('a.b:c;d', '[\\.:;]'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('a.b:c;d', '[\\.:;]')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('a.b:c;d', '\\.'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('a.b:c;d', '\\.')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('a.b:c;d', ':'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('a.b:c;d', ':')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('a,b,c', ','); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('a,b,c', ',')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('a1b2c3d', '\\d'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('a1b2c3d', '\\d')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('a1b2346c3d', '\\d+'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('a1b2346c3d', '\\d+')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('abcd', 'x'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('abcd', 'x')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('Hello world bye', '\\b[a-z]([a-z]*)'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('Hello world bye', '\\b[a-z]([a-z]*)')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('rat cat\nbat dog', 'ra(.)|blah(.)(.)'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('rat cat\nbat dog', 'ra(.)|blah(.)(.)')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('Baby X', 'by ([A-Z].*)\\b[a-z]'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('Baby X', 'by ([A-Z].*)\\b[a-z]')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('rat cat bat dog', '.at'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('rat cat bat dog', '.at')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('', 'x'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('', 'x')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('', ''); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('', '')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('君子矜而不争，党而不群', '不'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('君子矜而不争，党而不群', '不')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('abcd', ''); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('abcd', '')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_count('1a 2b 14m', '\\s*[a-z]+\\s*'); # error: errCode = 2, detailMessage = Can not found function 'regexp_count'
-- SELECT cardinality(regexp_extract_all('1a 2b 14m', '\\s*[a-z]+\\s*')); # error: errCode = 2, detailMessage = Can not find the compatibility function signature: cardinality(VARCHAR(65533))
-- SELECT regexp_position('a.b:c;d', '[\\.:;]'); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a.b:c;d', '\\.'); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a.b:c;d', ':'); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a,b,c', ','); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '\\d'); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a,b,c', ',', 3); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '\\d', 5); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d4e', '\\d', 4, 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '\\d', 4, 3); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', ''); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '', 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '', 2, 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '', 2, 6); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '', 2, 7); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '', 2, 8); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('行成于思str而毁123于随', '于', 3, 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('行成于思str而毁123于随', '', 3, 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('行成于思str而毁123于随', '', 3, 1); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('', ', '); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('', ', ', 4); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('', ', ', 4, 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a,b,c', ',', 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a1b2c3d', '\\d', 4); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('有朋$%X自9远方来', '\\d', 7); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('有朋$%X自9远方9来', '\\d', 10, 1); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('有朋$%X自9远方9来', '\\d', 10, 2); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a,b,c', ', ', 1000); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('a,b,c', ', ', 8); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('有朋$%X自9远方9来', '来', 999); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('9102, say good bye', '\\s*[a-z]+\\s*'); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('natasha, 9102, miss you', '\\s*[a-z]+\\s*', 10); # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
-- SELECT regexp_position('natasha, 9102, miss you', '\\s', 10, 2) # error: errCode = 2, detailMessage = Can not found function 'regexp_position'
