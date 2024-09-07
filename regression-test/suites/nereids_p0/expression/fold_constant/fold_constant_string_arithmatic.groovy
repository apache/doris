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
    testFoldConst("SELECT  Ltrim('  Hello World  ')")
    testFoldConst("SELECT  Rtrim('  Hello World  ')")
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
}
