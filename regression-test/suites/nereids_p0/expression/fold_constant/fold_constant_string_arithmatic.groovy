// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("fold_constant_string_arithmatic") {
    def db = "fold_constant_string_arithmatic"
    sql "create database if not exists ${db}"

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_fold_constant_by_be=false"

    testFoldConst("SELECT  Concat('Hello', ' ', 'World')")
    testFoldConst("SELECT  Substring('Hello World', 1, 5)")
    testFoldConst("SELECT  Substring('1', 1, 1)")
    testFoldConst("select  100, 'abc', substring('abc', 1, 2), substring(substring('abcdefg', 4, 3), 1, 2), null")
    testFoldConst("SELECT  Length('Hello World')")
    testFoldConst("SELECT  Lower('Hello World')")
    testFoldConst("SELECT  Upper('Hello World')")
    testFoldConst("SELECT  Trim('  Hello World  ')")
    testFoldConst("SELECT  Trim('11111', 11)")
    testFoldConst("SELECT  Ltrim('  Hello World  ')")
    testFoldConst("SELECT  LTrim(' 11111', 11)")
    testFoldConst("SELECT  LTrim('11111 ', 11)")
    testFoldConst("SELECT  Rtrim('  Hello World  ')")
    testFoldConst("SELECT  RTrim('11111 ', 11)")
    testFoldConst("SELECT  RTrim(' 11111', 11)")
    testFoldConst("SELECT  Replace('Hello World', 'World', 'Everyone')")
    testFoldConst("SELECT  Left('Hello World', 5)")
    testFoldConst("SELECT  Right('Hello World', 5)")
    testFoldConst("SELECT  Locate('World', 'Hello World')")
    testFoldConst("SELECT  Instr('Hello World', 'World')")
    testFoldConst("SELECT  Ascii('A')")
    testFoldConst("SELECT  Bin(5)")
    testFoldConst("SELECT  Hex(255)")
    testFoldConst("SELECT  Unhex('FF')")
    testFoldConst("SELECT  Concat_Ws('-', '2024', '09', '02')")
    testFoldConst("SELECT  Char(65)")
    testFoldConst("SELECT  Character_Length('Hello World')")
    testFoldConst("SELECT  Initcap('hello world')")
    testFoldConst("SELECT  Md5('Hello World')")
    testFoldConst("SELECT  Md5Sum('Hello World')")
//    testFoldConst("SELECT  JsonExtract('{"key": "value"}', '$.key')")
//    testFoldConst("SELECT  JsonbExtractString('{"key": "value"}', '$.key')")
//    testFoldConst("SELECT  JsonContains('{"key": "value"}', '"key"')")
//    testFoldConst("SELECT  JsonLength('{"key1": "value1", "key2": "value2"}')")
//    testFoldConst("SELECT  JsonObject('key', 'value')")
//    testFoldConst("SELECT  JsonArray('value1', 'value2')")
//    testFoldConst("SELECT  JsonKeys('{"key1": "value1", "key2": "value2"}')")
//    testFoldConst("SELECT  JsonInsert('{"key1": "value1"}', '$.key2', 'value2')")
//    testFoldConst("SELECT  JsonReplace('{"key1": "value1"}', '$.key1', 'new_value')")
//    testFoldConst("SELECT  JsonSet('{"key1": "value1"}', '$.key2', 'value2')")
//    testFoldConst("SELECT  Json_Quote('Hello World')")
//    testFoldConst("SELECT  Json_UnQuote('"Hello World"')")
    testFoldConst("SELECT  Field('b', 'a', 'b', 'c')")
    testFoldConst("SELECT  Find_In_Set('b', 'a,b,c')")
    testFoldConst("SELECT  Repeat('Hello', 3)")
    testFoldConst("SELECT  Reverse('Hello')")
    testFoldConst("SELECT  length(Space(10))")
//    testFoldConst("SELECT  Split_By_Char('a,b,c',',')")  has bug in be execution
    testFoldConst("SELECT  Split_By_String('a::b::c', '::')")
    testFoldConst("SELECT  Split_Part('a,b,c', ',', 2)")
    testFoldConst("SELECT  Substring_Index('a,b,c', ',', 2)")
    testFoldConst("SELECT  Strcmp('abc', 'abd')")
    testFoldConst("SELECT  StrLeft('Hello World', 5)")
    testFoldConst("SELECT  StrRight('Hello World', 5)")
    testFoldConst("SELECT  Overlay('abcdef', '123', 3, 2)")
    testFoldConst("SELECT  Parse_Url('http://www.example.com/path?query=abc', 'HOST')")
    testFoldConst("SELECT  Url_Decode('%20Hello%20World%20')")

    // Substring with negative start index
    // Expected behavior: Depending on the SQL engine, might return an empty string or error.
    testFoldConst("SELECT Substring('Hello World', -1, 5)")

    // Substring with length exceeding the string length
    // Expected behavior: Return 'Hello' as the length exceeds the string length.
    testFoldConst("SELECT Substring('Hello', 1, 10)")

    // Left with length greater than string length
    // Expected behavior: Return 'Hello'.
    testFoldConst("SELECT Left('Hello', 10)")

    // Right with length greater than string length
    // Expected behavior: Return 'Hello'.
    testFoldConst("SELECT Right('Hello', 10)")

    // SplitPart with part number greater than the number of parts
    // Expected behavior: Return an empty string or error.
    testFoldConst("SELECT Split_Part('a,b,c', ',', 5)")

    // SplitPart with negative part number
    // Expected behavior: Return an empty string or error.
    testFoldConst("SELECT Split_Part('a,b,c', ',', -1)")

    // Locate with the substring not present
    // Expected behavior: Return 0 as 'World' is not found.
    testFoldConst("SELECT Locate('World', 'Hello')")

    // Instr with the substring not present
    // Expected behavior: Return 0 as 'World' is not found.
    testFoldConst("SELECT Instr('Hello', 'World')")

    // Replace with an empty search string
    // Expected behavior: Some SQL engines may treat this as a no-op, others might throw an error.
    testFoldConst("SELECT Replace('Hello World', '', 'Everyone')")

    // Replace with an empty replacement string
    // Expected behavior: Return 'Hello '.
    testFoldConst("SELECT Replace('Hello World', 'World', '')")

    // Concat with NULL values
    // Expected behavior: Depending on the SQL engine, may return 'HelloWorld' or NULL.
    testFoldConst("SELECT Concat('Hello', NULL, 'World')")

    // Ltrim with a string that has no leading spaces
    // Expected behavior: Return 'Hello'.
    testFoldConst("SELECT Ltrim('Hello')")

    // Rtrim with a string that has no trailing spaces
    // Expected behavior: Return 'Hello'.
    testFoldConst("SELECT Rtrim('Hello')")

    // JsonExtract with an invalid JSON path
    // Expected behavior: Return NULL or an empty string.
//    testFoldConst("SELECT Json_Extract('{"key": "value"}', '$.invalid')")

    // JsonLength with a non-JSON string
    // Expected behavior: Return NULL or error.
    testFoldConst("SELECT Json_Length('Hello World')")

    // Field with a value not present in the list
    // Expected behavior: Return 0 as 'd' is not found.
    testFoldConst("SELECT Field('d', 'a', 'b', 'c')")

    // FindInSet with a value not present in the set
    // Expected behavior: Return 0 as 'd' is not found.
    testFoldConst("SELECT Find_In_Set('d', 'a,b,c')")

    // Repeat with a negative repeat count
    // Expected behavior: Return an empty string or error.
    testFoldConst("SELECT Repeat('Hello', -3)")

    // Space with a negative number of spaces
    // Expected behavior: Return an empty string or error.
    testFoldConst("SELECT Space(-5)")

    // SplitByChar with a delimiter not present in the string
    // Expected behavior: Return the original string in a single element array.
//    testFoldConst("SELECT Split_By_Char('abc', ',')")

    // SplitByString with a delimiter not present in the string
    // Expected behavior: Return the original string in a single element array.
    testFoldConst("SELECT Split_By_String('::', 'abc')")

    // Strcmp with two identical strings
    // Expected behavior: Return 0 as the strings are equal.
    testFoldConst("SELECT Strcmp('abc', 'abc')")

    // Strcmp with a null string
    // Expected behavior: Return NULL or -1 depending on the SQL engine.
    testFoldConst("SELECT Strcmp('abc', NULL)")

    // Hex with a negative number
    // Expected behavior: Return the hexadecimal representation of the two's complement, or an error depending on the SQL engine.
    testFoldConst("SELECT Hex(-255)")

    // Unhex with an invalid hexadecimal string
    // Expected behavior: Return NULL or error as 'GHIJ' is not a valid hex string.
    testFoldConst("SELECT Unhex('GHIJ')")

    // JsonReplace with a path that does not exist
    // Expected behavior: Depending on the engine, might return the original JSON or an error.
//    testFoldConst("SELECT Json_Replace('{"key": "value"}', '$.nonexistent', 'new_value')")

    // UrlDecode with an invalid percent-encoded string
    // Expected behavior: Return NULL or error due to invalid encoding.
    testFoldConst("SELECT Url_Decode('%ZZHello%20World')")

    testFoldConst("select elt(0, \"hello\", \"doris\")")
    testFoldConst("select elt(1, \"hello\", \"doris\")")
    testFoldConst("select elt(2, \"hello\", \"doris\")")
    testFoldConst("select elt(3, \"hello\", \"doris\")")
    testFoldConst("select c1, c2, elt(c1, c2) from (select number as c1, 'varchar' as c2 from numbers('number'='5') where number > 0) a")

    testFoldConst("select append_trailing_char_if_absent('a','c')")
    testFoldConst("select append_trailing_char_if_absent('ac','c')")

    testFoldConst("select ascii('1')")
    testFoldConst("select ascii('a')")
    testFoldConst("select ascii('A')")
    testFoldConst("select ascii('!')")

    testFoldConst("select bit_length(\"abc\")")

    testFoldConst("select char_length(\"abc\")")

    testFoldConst("select concat(\"a\", \"b\")")
    testFoldConst("select concat(\"a\", \"b\", \"c\")")
    testFoldConst("select concat(\"a\", null, \"c\")")

    testFoldConst("select concat_ws(\"or\", \"d\", \"is\")")
    testFoldConst("select concat_ws(NULL, \"d\", \"is\")")
    testFoldConst("select concat_ws(\"or\", \"d\", NULL,\"is\")")
    testFoldConst("select concat_ws(\"or\", [\"d\", \"is\"])")
    testFoldConst("select concat_ws(NULL, [\"d\", \"is\"])")
    testFoldConst("select concat_ws(\"or\", [\"d\", NULL,\"is\"])")
    testFoldConst("select concat_ws(\"or\", [\"d\", \"\",\"is\"])")

    testFoldConst("select ends_with(\"Hello doris\", \"doris\")")
    testFoldConst("select ends_with(\"Hello doris\", \"Hello\")")

    testFoldConst("select find_in_set(\"b\", \"a,b,c\")")
    testFoldConst("select find_in_set(\"d\", \"a,b,c\")")
    testFoldConst("select find_in_set(null, \"a,b,c\")")
    testFoldConst("select find_in_set(\"a\", null)")

    testFoldConst("select hex('1')")
    testFoldConst("select hex('12')")
    testFoldConst("select hex('@')")
    testFoldConst("select hex('A')")
    testFoldConst("select hex(12)")
    testFoldConst("select hex(-1)")
    testFoldConst("select hex('hello,doris')")

    testFoldConst("select unhex('@')")
    testFoldConst("select unhex('68656C6C6F2C646F726973')")
    testFoldConst("select unhex('41')")
    testFoldConst("select unhex('4142')")
    testFoldConst("select unhex('')")
    testFoldConst("select unhex(NULL)")

    testFoldConst("select instr(\"abc\", \"b\")")
    testFoldConst("select instr(\"abc\", \"d\")")
    testFoldConst("select instr(\"abc\", null)")
    testFoldConst("select instr(null, \"a\")")
    testFoldConst("SELECT instr('foobar', '')")
    testFoldConst("SELECT instr('上海天津北京杭州', '北京')")

    testFoldConst("SELECT lcase(\"AbC123\")")
    testFoldConst("SELECT lower(\"AbC123\")")

    testFoldConst("SELECT initcap(\"AbC123abc abc.abc,?|abc\")")

    testFoldConst("select left(\"Hello doris\",5)")
    testFoldConst("select right(\"Hello doris\",5)")

    testFoldConst("select length(\"abc\")")

    testFoldConst("SELECT LOCATE('bar', 'foobarbar')")
    testFoldConst("SELECT LOCATE('xbar', 'foobar')")
    testFoldConst("SELECT LOCATE('', 'foobar')")
    testFoldConst("SELECT LOCATE('北京', '上海天津北京杭州')")

    testFoldConst("SELECT lpad(\"hi\", 5, \"xy\")")
    testFoldConst("SELECT lpad(\"hi\", 1, \"xy\")")
    testFoldConst("SELECT rpad(\"hi\", 5, \"xy\")")
    testFoldConst("SELECT rpad(\"hi\", 1, \"xy\")")

    testFoldConst("SELECT ltrim('   ab d')")

    testFoldConst("select money_format(17014116)")
    testFoldConst("select money_format(1123.456)")
    testFoldConst("select money_format(1123.4)")
    testFoldConst("select money_format(truncate(1000,10))")

    testFoldConst("select null_or_empty(null)")
    testFoldConst("select null_or_empty(\"\")")
    testFoldConst("select null_or_empty(\"a\")")

    testFoldConst("select not_null_or_empty(null)")
    testFoldConst("select not_null_or_empty(\"\")")
    testFoldConst("select not_null_or_empty(\"a\")")

    testFoldConst("SELECT repeat(\"a\", 3)")
    testFoldConst("SELECT repeat(\"a\", -1)")
    testFoldConst("SELECT repeat(\"a\", 0)")
    testFoldConst("SELECT repeat(\"a\",null)")
    testFoldConst("SELECT repeat(null,1)")

    testFoldConst("select replace(\"https://doris.apache.org:9090\", \":9090\", \"\")")
    testFoldConst("select replace(\"https://doris.apache.org:9090\", \"\", \"new_str\")")

    testFoldConst("SELECT REVERSE('hello')")

    testFoldConst("select split_part('hello world', ' ', 1)")
    testFoldConst("select split_part('hello world', ' ', 2)")
    testFoldConst("select split_part('hello world', ' ', 0)")
    testFoldConst("select split_part('hello world', ' ', -1)")
    testFoldConst("select split_part('hello world', ' ', -2)")
    testFoldConst("select split_part('hello world', ' ', -3)")
    testFoldConst("select split_part('abc##123###xyz', '##', 0)")
    testFoldConst("select split_part('abc##123###xyz', '##', 1)")
    testFoldConst("select split_part('abc##123###xyz', '##', 3)")
    testFoldConst("select split_part('abc##123###xyz', '##', 5)")
    testFoldConst("select split_part('abc##123###xyz', '##', -1)")
    testFoldConst("select split_part('abc##123###xyz', '##', -2)")
    testFoldConst("select split_part('abc##123###xyz', '##', -4)")

    testFoldConst("select starts_with(\"hello world\",\"hello\")")
    testFoldConst("select starts_with(\"hello world\",\"world\")")
    testFoldConst("select starts_with(\"hello world\",null)")

    testFoldConst("select strleft(NULL, 1)")
    testFoldConst("select strleft(\"good morning\", NULL)")
    testFoldConst("select left(NULL, 1)")
    testFoldConst("select left(\"good morning\", NULL)")
    testFoldConst("select strleft(\"Hello doris\", 5)")
    testFoldConst("select left(\"Hello doris\", 5)")
    testFoldConst("select strright(NULL, 1)")
    testFoldConst("select strright(\"good morning\", NULL)")
    testFoldConst("select right(NULL, 1)")
    testFoldConst("select right(\"good morning\", NULL)")
    testFoldConst("select strright(\"Hello doris\", 5)")
    testFoldConst("select right(\"Hello doris\", 5)")
    testFoldConst("select strleft(\"good morning\", 120)")
    testFoldConst("select strleft(\"good morning\", -5)")
    testFoldConst("select strright(\"Hello doris\", 120)")
    testFoldConst("select strright(\"Hello doris\", -5)")
    testFoldConst("select left(\"good morning\", 120)")
    testFoldConst("select left(\"good morning\", -5)")
    testFoldConst("select right(\"Hello doris\", 120)")
    testFoldConst("select right(\"Hello doris\", -6)")

    testFoldConst("select substring('abc1', 2)")
    testFoldConst("select substring('abc1', -2)")
    testFoldConst("select substring('abc1', 5)")
    testFoldConst("select substring('abc1def', 2, 2)")
    testFoldConst("select substring('abcdef',3,-1)")
    testFoldConst("select substring('abcdef',-3,-1)")
    testFoldConst("select substring('abcdef',10,1)")

    testFoldConst("select substr('a',3,1)")
    testFoldConst("select substr('a',2,1)")
    testFoldConst("select substr('a',1,1)")
    testFoldConst("select substr('a',0,1)")
    testFoldConst("select substr('a',-1,1)")
    testFoldConst("select substr('a',-2,1)")
    testFoldConst("select substr('a',-3,1)")
    testFoldConst("select substr('abcdef',3,-1)")
    testFoldConst("select substr('abcdef',-3,-1)")

    testFoldConst("select sub_replace(\"this is origin str\",\"NEW-STR\",1)")
    testFoldConst("select sub_replace(\"doris\",\"***\",1,2)")

    testFoldConst("select substring_index(\"hello world\", \" \", 1)")
    testFoldConst("select substring_index(\"hello world\", \" \", 2)")
    testFoldConst("select substring_index(\"hello world\", \" \", 3)")
    testFoldConst("select substring_index(\"hello world\", \" \", -1)")
    testFoldConst("select substring_index(\"hello world\", \" \", -2)")
    testFoldConst("select substring_index(\"hello world\", \" \", -3)")
    testFoldConst("select substring_index(\"prefix__string2\", \"__\", 2)")
    testFoldConst("select substring_index(\"prefix__string2\", \"_\", 2)")
    testFoldConst("select substring_index(\"prefix_string2\", \"__\", 1)")
    testFoldConst("select substring_index(null, \"__\", 1)")
    testFoldConst("select substring_index(\"prefix_string\", null, 1)")
    testFoldConst("select substring_index(\"prefix_string\", \"_\", null)")
    testFoldConst("select substring_index(\"prefix_string\", \"__\", -1)")

    testFoldConst("select elt(0, \"hello\", \"doris\")")
    testFoldConst("select elt(1, \"hello\", \"doris\")")
    testFoldConst("select elt(2, \"hello\", \"doris\")")
    testFoldConst("select elt(3, \"hello\", \"doris\")")

    testFoldConst("select sub_replace(\"this is origin str\",\"NEW-STR\",1)")
    testFoldConst("select sub_replace(\"doris\",\"***\",1,2)")

    testFoldConst("select strcmp('a', 'abc')")
    testFoldConst("select strcmp('abc', 'abc')")
    testFoldConst("select strcmp('abcd', 'abc')")

    testFoldConst("SELECT Concat(cast('Hello' as string), cast(' ' as string), cast('World' as string))")
    testFoldConst("SELECT Substring(cast('Hello World' as string), 1, 5)")
    testFoldConst("SELECT Substring(cast('1' as string), 1, 1)")
    testFoldConst("SELECT 100, cast('abc' as string), Substring(cast('abc' as string), 1, 2), Substring(Substring(cast('abcdefg' as string), 4, 3), 1, 2), null")
    testFoldConst("SELECT Length(cast('Hello World' as string))")
    testFoldConst("SELECT Lower(cast('Hello World' as string))")
    testFoldConst("SELECT Upper(cast('Hello World' as string))")
    testFoldConst("SELECT Trim(cast('  Hello World  ' as string))")
    testFoldConst("SELECT Trim(cast('11111' as string), cast(11 as string))")
    testFoldConst("SELECT Ltrim(cast('  Hello World  ' as string))")
    testFoldConst("SELECT LTrim(cast(' 11111' as string), cast(11 as string))")
    testFoldConst("SELECT LTrim(cast('11111 ' as string), cast(11 as string))")
    testFoldConst("SELECT Rtrim(cast('  Hello World  ' as string))")
    testFoldConst("SELECT RTrim(cast('11111 ' as string), cast(11 as string))")
    testFoldConst("SELECT RTrim(cast(' 11111' as string), cast(11 as string))")
    testFoldConst("SELECT Replace(cast('Hello World' as string), cast('World' as string), cast('Everyone' as string))")
    testFoldConst("SELECT Left(cast('Hello World' as string), 5)")
    testFoldConst("SELECT Right(cast('Hello World' as string), 5)")
    testFoldConst("SELECT Locate(cast('World' as string), cast('Hello World' as string))")
    testFoldConst("SELECT Instr(cast('Hello World' as string), cast('World' as string))")
    testFoldConst("SELECT Ascii(cast('A' as string))")
    testFoldConst("SELECT Bin(5)")
    testFoldConst("SELECT Hex(255)")
    testFoldConst("SELECT Unhex(cast('FF' as string))")
//    testFoldConst("SELECT Concat_Ws(cast('-' as string), cast('2024' as string), cast('09' as string), cast('02' as string))")
    testFoldConst("SELECT Char(65)")
    testFoldConst("SELECT Character_Length(cast('Hello World' as string))")
    testFoldConst("SELECT Initcap(cast('hello world' as string))")
    testFoldConst("SELECT Md5(cast('Hello World' as string))")
//    testFoldConst("SELECT Md5Sum(cast('Hello World' as string))")
// testFoldConst("SELECT JsonExtract(cast('{\"key\": \"value\"}' as string), cast('$.key' as string))")
// testFoldConst("SELECT JsonbExtractString(cast('{\"key\": \"value\"}' as string), cast('$.key' as string))")
// testFoldConst("SELECT JsonContains(cast('{\"key\": \"value\"}' as string), cast('\"key\"' as string))")
// testFoldConst("SELECT JsonLength(cast('{\"key1\": \"value1\", \"key2\": \"value2\"}' as string))")
// testFoldConst("SELECT JsonObject(cast('key' as string), cast('value' as string))")
// testFoldConst("SELECT JsonArray(cast('value1' as string), cast('value2' as string))")
// testFoldConst("SELECT JsonKeys(cast('{\"key1\": \"value1\", \"key2\": \"value2\"}' as string))")
// testFoldConst("SELECT JsonInsert(cast('{\"key1\": \"value1\"}' as string), cast('$.key2' as string), cast('value2' as string))")
// testFoldConst("SELECT JsonReplace(cast('{\"key1\": \"value1\"}' as string), cast('$.key1' as string), cast('new_value' as string))")
// testFoldConst("SELECT JsonSet(cast('{\"key1\": \"value1\"}' as string), cast('$.key2' as string), cast('value2' as string))")
// testFoldConst("SELECT Json_Quote(cast('Hello World' as string))")
// testFoldConst("SELECT Json_UnQuote(cast('\"Hello World\"' as string))")
//    testFoldConst("SELECT Field(cast('b' as string), cast('a' as string), cast('b' as string), cast('c' as string))")
    testFoldConst("SELECT Find_In_Set(cast('b' as string), cast('a,b,c' as string))")
    testFoldConst("SELECT Repeat(cast('Hello' as string), 3)")
    testFoldConst("SELECT Reverse(cast('Hello' as string))")
    testFoldConst("SELECT length(Space(10))")
// testFoldConst("SELECT Split_By_Char(cast('a,b,c' as string), cast(',' as string))")  has bug in be execution
    testFoldConst("SELECT Split_By_String(cast('a::b::c' as string), cast('::' as string))")
    testFoldConst("SELECT Split_Part(cast('a,b,c' as string), cast(',' as string), 2)")
    testFoldConst("SELECT Substring_Index(cast('a,b,c' as string), cast(',' as string), 2)")
    testFoldConst("SELECT Strcmp(cast('abc' as string), cast('abd' as string))")
    testFoldConst("SELECT StrLeft(cast('Hello World' as string), 5)")
    testFoldConst("SELECT StrRight(cast('Hello World' as string), 5)")
    testFoldConst("SELECT Overlay(cast('abcdef' as string), cast('123' as string), 3, 2)")
    testFoldConst("SELECT Parse_Url(cast('http://www.example.com/path?query=abc' as string), cast('HOST' as string))")
    testFoldConst("SELECT Url_Decode(cast('%20Hello%20World%20' as string))")

// Substring with negative start index
// Expected behavior: Depending on the SQL engine, might return an empty string or error.
    testFoldConst("SELECT Substring(cast('Hello World' as string), -1, 5)")

// Substring with length exceeding the string length
// Expected behavior: Return 'Hello' as the length exceeds the string length.
    testFoldConst("SELECT Substring(cast('Hello' as string), 1, 10)")

// Left with length greater than string length
// Expected behavior: Return 'Hello'.
    testFoldConst("SELECT Left(cast('Hello' as string), 10)")

// Right with length greater than string length
// Expected behavior: Return 'Hello'.
    testFoldConst("SELECT Right(cast('Hello' as string), 10)")

// SplitPart with part number greater than the number of parts
// Expected behavior: Return an empty string or error.
    testFoldConst("SELECT Split_Part(cast('a,b,c' as string), cast(',' as string), 5)")

// SplitPart with negative part number
// Expected behavior: Return an empty string or error.
    testFoldConst("SELECT Split_Part(cast('a,b,c' as string), cast(',' as string), -1)")

// Locate with the substring not present
// Expected behavior: Return 0 as 'World' is not found.
    testFoldConst("SELECT Locate(cast('World' as string), cast('Hello' as string))")

// Instr with the substring not present
// Expected behavior: Return 0 as 'World' is not found.
    testFoldConst("SELECT Instr(cast('Hello' as string), cast('World' as string))")

// Replace with an empty search string
// Expected behavior: Some SQL engines may treat this as a no-op, others might throw an error.
    testFoldConst("SELECT Replace(cast('Hello World' as string), '', cast('Everyone' as string))")

// Replace with an empty replacement string
// Expected behavior: Return 'Hello '.
    testFoldConst("SELECT Replace(cast('Hello World' as string), cast('World' as string), '')")

// Concat with NULL values
// Expected behavior: Depending on the SQL engine, may return 'HelloWorld' or NULL.
    testFoldConst("SELECT Concat(cast('Hello' as string), NULL, cast('World' as string))")

// Ltrim with a string that has no leading spaces
// Expected behavior: Return 'Hello'.
    testFoldConst("SELECT Ltrim(cast('Hello' as string))")

// Rtrim with a string that has no trailing spaces
// Expected behavior: Return 'Hello'.
    testFoldConst("SELECT Rtrim(cast('Hello' as string))")

// Testing JSON Length function with a non-JSON string
    testFoldConst("SELECT Json_Length(cast('Hello World' as string))")

// Field with a value not present in the list
//    testFoldConst("SELECT Field(cast('d' as string), cast('a' as string), cast('b' as string), cast('c' as string))")

// FindInSet with a value not present in the set
    testFoldConst("SELECT Find_In_Set(cast('d' as string), cast('a,b,c' as string))")

// Repeat with a negative repeat count
    testFoldConst("SELECT Repeat(cast('Hello' as string), -3)")

// Space with a negative number of spaces
    testFoldConst("SELECT Space(-5)")

// SplitByChar with a delimiter not present in the string
//    testFoldConst("SELECT Split_By_Char(cast('abc' as string), cast(',' as string))")

// SplitByString with a delimiter not present in the string
    testFoldConst("SELECT Split_By_String(cast('abc' as string), cast('::' as string))")

// Strcmp with two identical strings
    testFoldConst("SELECT Strcmp(cast('abc' as string), cast('abc' as string))")

// Strcmp with a null string
    testFoldConst("SELECT Strcmp(cast('abc' as string), NULL)")

// Hex with a negative number
    testFoldConst("SELECT Hex(-255)")

// Unhex with an invalid hexadecimal string
    testFoldConst("SELECT Unhex(cast('GHIJ' as string))")

// UrlDecode with an invalid percent-encoded string
    testFoldConst("SELECT Url_Decode(cast('%ZZHello%20World' as string))")

// Additional function tests
    testFoldConst("SELECT Elt(0, cast('hello' as string), cast('doris' as string))")
    testFoldConst("SELECT Elt(1, cast('hello' as string), cast('doris' as string))")
    testFoldConst("SELECT Elt(2, cast('hello' as string), cast('doris' as string))")
    testFoldConst("SELECT Elt(3, cast('hello' as string), cast('doris' as string))")
    testFoldConst("SELECT Append_Trailing_Char_If_Absent(cast('a' as string), cast('c' as string))")
    testFoldConst("SELECT Append_Trailing_Char_If_Absent(cast('ac' as string), cast('c' as string))")
    testFoldConst("SELECT Ascii(cast('1' as string))")
    testFoldConst("SELECT Ascii(cast('a' as string))")
    testFoldConst("SELECT Ascii(cast('A' as string))")
    testFoldConst("SELECT Ascii(cast('!' as string))")
    testFoldConst("SELECT Bit_Length(cast('abc' as string))")
    testFoldConst("SELECT Char_Length(cast('abc' as string))")
    testFoldConst("SELECT Concat(cast('a' as string), cast('b' as string))")
    testFoldConst("SELECT Concat(cast('a' as string), cast('b' as string), cast('c' as string))")
    testFoldConst("SELECT Concat(cast('a' as string), NULL, cast('c' as string))")
//    testFoldConst("SELECT Concat_Ws(cast('or' as string), cast('d' as string), cast('is' as string))")
//    testFoldConst("SELECT Concat_Ws(NULL, cast('d' as string), cast('is' as string))")
//    testFoldConst("SELECT Concat_Ws(cast('or' as string), cast('d' as string), NULL, cast('is' as string))")
//    testFoldConst("SELECT Concat_Ws(cast('or' as string), cast('d' as string), cast('' as string), cast('is' as string))")
    testFoldConst("SELECT Ends_With(cast('Hello doris' as string), cast('doris' as string))")
    testFoldConst("SELECT Ends_With(cast('Hello doris' as string), cast('Hello' as string))")
    testFoldConst("SELECT Find_In_Set(cast('b' as string), cast('a,b,c' as string))")
    testFoldConst("SELECT Find_In_Set(cast('d' as string), cast('a,b,c' as string))")
    testFoldConst("SELECT Find_In_Set(NULL, cast('a,b,c' as string))")
    testFoldConst("SELECT Find_In_Set(cast('a' as string), NULL)")
    testFoldConst("SELECT Hex(cast('1' as string))")
    testFoldConst("SELECT Hex(cast('12' as string))")
    testFoldConst("SELECT Hex(cast('@' as string))")
    testFoldConst("SELECT Hex(cast('A' as string))")
    testFoldConst("SELECT Hex(12)")
    testFoldConst("SELECT Hex(-1)")
    testFoldConst("SELECT Hex(cast('hello,doris' as string))")
    testFoldConst("SELECT Unhex(cast('@' as string))")
    testFoldConst("SELECT Unhex(cast('68656C6C6F2C646F726973' as string))")
    testFoldConst("SELECT Unhex(cast('41' as string))")
    testFoldConst("SELECT Unhex(cast('4142' as string))")
    testFoldConst("SELECT Unhex(cast('' as string))")
    testFoldConst("SELECT Unhex(NULL)")
    testFoldConst("SELECT Instr(cast('abc' as string), cast('b' as string))")
    testFoldConst("SELECT Instr(cast('abc' as string), cast('d' as string))")
    testFoldConst("SELECT Instr(cast('abc' as string), NULL)")
    testFoldConst("SELECT Instr(NULL, cast('a' as string))")
    testFoldConst("SELECT Lcase(cast('AbC123' as string))")
    testFoldConst("SELECT Lower(cast('AbC123' as string))")
    testFoldConst("SELECT Initcap(cast('AbC123abc abc.abc,?|abc' as string))")
    testFoldConst("SELECT Left(cast('Hello doris' as string), 5)")
    testFoldConst("SELECT Right(cast('Hello doris' as string), 5)")
    testFoldConst("SELECT Length(cast('abc' as string))")
    testFoldConst("SELECT LOCATE(cast('bar' as string), cast('foobarbar' as string))")
    testFoldConst("SELECT LOCATE(cast('xbar' as string), cast('foobar' as string))")
    testFoldConst("SELECT LOCATE(cast('' as string), cast('foobar' as string))")
    testFoldConst("SELECT LOCATE(cast('北京' as string), cast('上海天津北京杭州' as string))")
    testFoldConst("SELECT Lpad(cast('hi' as string), 5, cast('xy' as string))")
    testFoldConst("SELECT Lpad(cast('hi' as string), 1, cast('xy' as string))")
    testFoldConst("SELECT Rpad(cast('hi' as string), 5, cast('xy' as string))")
    testFoldConst("SELECT Rpad(cast('hi' as string), 1, cast('xy' as string))")
    testFoldConst("SELECT Ltrim(cast('   ab d' as string))")
    testFoldConst("SELECT Money_Format(17014116)")
    testFoldConst("SELECT Money_Format(1123.456)")
    testFoldConst("SELECT Money_Format(1123.4)")
    testFoldConst("SELECT Money_Format(Truncate(1000,10))")
    testFoldConst("SELECT Null_Or_Empty(NULL)")
    testFoldConst("SELECT Null_Or_Empty(cast('' as string))")
    testFoldConst("SELECT Null_Or_Empty(cast('a' as string))")
    testFoldConst("SELECT Not_Null_Or_Empty(NULL)")
    testFoldConst("SELECT Not_Null_Or_Empty(cast('' as string))")
    testFoldConst("SELECT Not_Null_Or_Empty(cast('a' as string))")
    testFoldConst("SELECT Repeat(cast('a' as string), 3)")
    testFoldConst("SELECT Repeat(cast('a' as string), -1)")
    testFoldConst("SELECT Repeat(cast('a' as string), 0)")
    testFoldConst("SELECT Repeat(NULL, 1)")
    testFoldConst("SELECT Replace(cast('https://doris.apache.org:9090' as string), cast(':9090' as string), cast('' as string))")
    testFoldConst("SELECT Replace(cast('https://doris.apache.org:9090' as string), cast('' as string), cast('new_str' as string))")
    testFoldConst("SELECT REVERSE(cast('hello' as string))")
    testFoldConst("SELECT Split_Part(cast('hello world' as string), cast(' ' as string), 1)")
    testFoldConst("SELECT Split_Part(cast('hello world' as string), cast(' ' as string), 2)")
    testFoldConst("SELECT Split_Part(cast('hello world' as string), cast(' ' as string), 3)")
    testFoldConst("SELECT Concat(CAST('Hello' AS STRING), CAST(' ' AS STRING), CAST('World' AS STRING))")
    testFoldConst("SELECT Concat(CAST('Hello' AS STRING), CAST(NULL AS STRING))")
    testFoldConst("SELECT Concat(CAST(NULL AS STRING), CAST('World' AS STRING))")

    testFoldConst("SELECT Starts_With(CAST('hello world' AS STRING), CAST('hello' AS STRING))")
    testFoldConst("SELECT Starts_With(CAST('hello world' AS STRING), CAST('world' AS STRING))")
    testFoldConst("SELECT Starts_With(CAST('hello world' AS STRING), CAST(NULL AS STRING))")

    testFoldConst("SELECT StrLeft(CAST(NULL AS STRING), 1)")
    testFoldConst("SELECT StrLeft(CAST('good morning' AS STRING), NULL)")
    testFoldConst("SELECT Left(CAST(NULL AS STRING), 1)")
    testFoldConst("SELECT Left(CAST('good morning' AS STRING), NULL)")
    testFoldConst("SELECT StrLeft(CAST('Hello doris' AS STRING), 5)")
    testFoldConst("SELECT Left(CAST('Hello doris' AS STRING), 5)")
    testFoldConst("SELECT StrRight(CAST(NULL AS STRING), 1)")
    testFoldConst("SELECT StrRight(CAST('good morning' AS STRING), NULL)")
    testFoldConst("SELECT Right(CAST(NULL AS STRING), 1)")
    testFoldConst("SELECT Right(CAST('good morning' AS STRING), NULL)")
    testFoldConst("SELECT StrRight(CAST('Hello doris' AS STRING), 5)")
    testFoldConst("SELECT Right(CAST('Hello doris' AS STRING), 5)")
    testFoldConst("SELECT StrLeft(CAST('good morning' AS STRING), 120)")
    testFoldConst("SELECT StrLeft(CAST('good morning' AS STRING), -5)")
    testFoldConst("SELECT StrRight(CAST('Hello doris' AS STRING), 120)")
    testFoldConst("SELECT StrRight(CAST('Hello doris' AS STRING), -5)")
    testFoldConst("SELECT Left(CAST('good morning' AS STRING), 120)")
    testFoldConst("SELECT Left(CAST('good morning' AS STRING), -5)")
    testFoldConst("SELECT Right(CAST('Hello doris' AS STRING), 120)")
    testFoldConst("SELECT Right(CAST('Hello doris' AS STRING), -6)")

    testFoldConst("SELECT Substring(CAST('abc1' AS STRING), 2)")
    testFoldConst("SELECT Substring(CAST('abc1' AS STRING), -2)")
    testFoldConst("SELECT Substring(CAST('abc1' AS STRING), 5)")
    testFoldConst("SELECT Substring(CAST('abc1def' AS STRING), 2, 2)")
    testFoldConst("SELECT Substring(CAST('abcdef' AS STRING), 3, -1)")
    testFoldConst("SELECT Substring(CAST('abcdef' AS STRING), -3, -1)")
    testFoldConst("SELECT Substring(CAST('abcdef' AS STRING), 10, 1)")

    testFoldConst("SELECT Substr(CAST('a' AS STRING), 3, 1)")
    testFoldConst("SELECT Substr(CAST('a' AS STRING), 2, 1)")
    testFoldConst("SELECT Substr(CAST('a' AS STRING), 1, 1)")
    testFoldConst("SELECT Substr(CAST('a' AS STRING), 0, 1)")
    testFoldConst("SELECT Substr(CAST('a' AS STRING), -1, 1)")
    testFoldConst("SELECT Substr(CAST('a' AS STRING), -2, 1)")
    testFoldConst("SELECT Substr(CAST('a' AS STRING), -3, 1)")
    testFoldConst("SELECT Substr(CAST('abcdef' AS STRING), 3, -1)")
    testFoldConst("SELECT Substr(CAST('abcdef' AS STRING), -3, -1)")

    testFoldConst("SELECT Sub_Replace(CAST('this is origin str' AS STRING), CAST('NEW-STR' AS STRING), 1)")
    testFoldConst("SELECT Sub_Replace(CAST('doris' AS STRING), CAST('***' AS STRING), 1, 2)")

    testFoldConst("SELECT Substring_Index(CAST('hello world' AS STRING), CAST(' ' AS STRING), 1)")
    testFoldConst("SELECT Substring_Index(CAST('hello world' AS STRING), CAST(' ' AS STRING), 2)")
    testFoldConst("SELECT Substring_Index(CAST('hello world' AS STRING), CAST(' ' AS STRING), 3)")
    testFoldConst("SELECT Substring_Index(CAST('hello world' AS STRING), CAST(' ' AS STRING), -1)")
    testFoldConst("SELECT Substring_Index(CAST('hello world' AS STRING), CAST(' ' AS STRING), -2)")
    testFoldConst("SELECT Substring_Index(CAST('hello world' AS STRING), CAST(' ' AS STRING), -3)")
    testFoldConst("SELECT Substring_Index(CAST('prefix__string2' AS STRING), CAST('__' AS STRING), 2)")
    testFoldConst("SELECT Substring_Index(CAST('prefix__string2' AS STRING), CAST('_' AS STRING), 2)")
    testFoldConst("SELECT Substring_Index(CAST('prefix_string2' AS STRING), CAST('__' AS STRING), 1)")
    testFoldConst("SELECT Substring_Index(CAST(NULL AS STRING), CAST('__' AS STRING), 1)")
    testFoldConst("SELECT Substring_Index(CAST('prefix_string' AS STRING), CAST(NULL AS STRING), 1)")
    testFoldConst("SELECT Substring_Index(CAST('prefix_string' AS STRING), CAST('_' AS STRING), NULL)")
    testFoldConst("SELECT Substring_Index(CAST('prefix_string' AS STRING), CAST('__' AS STRING), -1)")

    testFoldConst("SELECT Elt(0, CAST('hello' AS STRING), CAST('doris' AS STRING))")
    testFoldConst("SELECT Elt(1, CAST('hello' AS STRING), CAST('doris' AS STRING))")
    testFoldConst("SELECT Elt(2, CAST('hello' AS STRING), CAST('doris' AS STRING))")
    testFoldConst("SELECT Elt(3, CAST('hello' AS STRING), CAST('doris' AS STRING))")

    testFoldConst("SELECT Sub_Replace(CAST('this is origin str' AS STRING), CAST('NEW-STR' AS STRING), 1)")
    testFoldConst("SELECT Sub_Replace(CAST('doris' AS STRING), CAST('***' AS STRING), 1, 2)")

    testFoldConst("SELECT StrCmp(CAST('a' AS STRING), CAST('abc' AS STRING))")
    testFoldConst("SELECT StrCmp(CAST('abc' AS STRING), CAST('abc' AS STRING))")
    testFoldConst("SELECT StrCmp(CAST('abcd' AS STRING), CAST('abc' AS STRING))")

    // fix problem of cast date and time function exception
    testFoldConst("select ifnull(date_format(CONCAT_WS('', '9999-07', '-00'), '%Y-%m'),3)")

}
