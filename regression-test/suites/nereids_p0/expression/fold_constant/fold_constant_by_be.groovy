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

suite("fold_constant_by_be") {
    sql 'use nereids_fold_constant_test'

    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_fold_constant_by_be=true'

    test {
        sql '''
            select if(
                date_format(CONCAT_WS('', '9999-07', '-26'), '%Y-%m') = DATE_FORMAT(curdate(), '%Y-%m'),
                curdate(),
                DATE_FORMAT(DATE_SUB(month_ceil(CONCAT_WS('', '9999-07', '-26')), 1), '%Y-%m-%d'))
        '''
        result([['9999-07-31']])
    }

    sql """ 
        CREATE TABLE IF NOT EXISTS str_tb (k1 VARCHAR(10) NULL, v1 STRING NULL) 
        UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
    """

    sql """ INSERT INTO str_tb VALUES (2, repeat("test1111", 10000)); """

    qt_sql_1 """ select length(v1) from str_tb; """

    def res1 = sql " select /*+SET_VAR(enable_fold_constant_by_be=true)*/ ST_CIRCLE(121.510651, 31.234391, 1918.0); "
    def res2 = sql " select /*+SET_VAR(enable_fold_constant_by_be=false)*/ ST_CIRCLE(121.510651, 31.234391, 1918.0); "
    log.info("result: {}, {}", res1, res2)
    assertEquals(res1[0][0], res2[0][0])

    explain {
         sql "select sleep(sign(1)*100);"
         contains "sleep(100)"
    }

    sql 'set query_timeout=12;'
//    qt_sql "select sleep(sign(1)*5);"

    qt_select1 """ explain SELECT  Concat('Hello', ' ', 'World')"""
    qt_select2 """ explain SELECT  Substring('Hello World', 1, 5)"""
    qt_select3 """ explain SELECT  Length('Hello World')"""
    qt_select4 """ explain SELECT  Lower('Hello World')"""
    qt_select5 """ explain SELECT  Upper('Hello World')"""
    qt_select6 """ explain SELECT  Trim('  Hello World  ')"""
    qt_select7 """ explain SELECT  Ltrim('  Hello World  ')"""
    qt_select8 """ explain SELECT  Rtrim('  Hello World  ')"""
    qt_select9 """ explain SELECT  Replace('Hello World', 'World', 'Everyone')"""
    qt_select10 """ explain SELECT  Left('Hello World', 5)"""
    qt_select11 """ explain SELECT  Right('Hello World', 5)"""
    qt_select12 """ explain SELECT  Locate('World', 'Hello World')"""
    qt_select13 """ explain SELECT  Instr('Hello World', 'World')"""
    qt_select14 """ explain SELECT  Ascii('A')"""
    qt_select15 """ explain SELECT  Bin(5)"""
    qt_select16 """ explain SELECT  Hex(255)"""
    qt_select17 """ explain SELECT  Unhex('FF')"""
    qt_select18 """ explain SELECT  Concat_Ws('-', '2024', '09', '02')"""
    qt_select19 """ explain SELECT  Char(65)"""
    qt_select20 """ explain SELECT  Character_Length('Hello World')"""
    qt_select21 """ explain SELECT  Initcap('hello world')"""
    qt_select22 """ explain SELECT  Md5('Hello World')"""
    qt_select23 """ explain SELECT  Md5Sum('Hello World')"""
//    qt_select24 """ explain SELECT  JsonExtract('{"key": "value"}', '$.key')"""
//    qt_select25 """ explain SELECT  JsonbExtractString('{"key": "value"}', '$.key')"""
//    qt_select26 """ explain SELECT  JsonContains('{"key": "value"}', '"key"')"""
//    qt_select27 """ explain SELECT  JsonLength('{"key1": "value1", "key2": "value2"}')"""
//    qt_select28 """ explain SELECT  JsonObject('key', 'value')"""
//    qt_select29 """ explain SELECT  JsonArray('value1', 'value2')"""
//    qt_select30 """ explain SELECT  JsonKeys('{"key1": "value1", "key2": "value2"}')"""
//    qt_select31 """ explain SELECT  JsonInsert('{"key1": "value1"}', '$.key2', 'value2')"""
//    qt_select32 """ explain SELECT  JsonReplace('{"key1": "value1"}', '$.key1', 'new_value')"""
//    qt_select33 """ explain SELECT  JsonSet('{"key1": "value1"}', '$.key2', 'value2')"""
    qt_select34 """ explain SELECT  Json_Quote('Hello World')"""
    qt_select35 """ explain SELECT  Json_UnQuote('"Hello World"')"""
    qt_select36 """ explain SELECT  Field('b', 'a', 'b', 'c')"""
    qt_select37 """ explain SELECT  Find_In_Set('b', 'a,b,c')"""
    qt_select38 """ explain SELECT  Repeat('Hello', 3)"""
    qt_select39 """ explain SELECT  Reverse('Hello')"""
    qt_select40 """ explain SELECT  Space(5)"""
    qt_select41 """ explain SELECT  Split_By_Char(',', 'a,b,c')"""
    qt_select42 """ explain SELECT  Split_By_String('::', 'a::b::c')"""
    qt_select43 """ explain SELECT  Split_Part('a,b,c', ',', 2)"""
    qt_select44 """ explain SELECT  Substring_Index('a,b,c', ',', 2)"""
    qt_select45 """ explain SELECT  Strcmp('abc', 'abd')"""
    qt_select46 """ explain SELECT  StrLeft('Hello World', 5)"""
    qt_select47 """ explain SELECT  StrRight('Hello World', 5)"""
    qt_select48 """ explain SELECT  Overlay('abcdef', '123', 3, 2)"""
    qt_select49 """ explain SELECT  Parse_Url('http://www.example.com/path?query=abc', 'HOST')"""
    qt_select50 """ explain SELECT  Url_Decode('%20Hello%20World%20')"""
    qt_select51 """ explain SELECT  Regexp_Extract('abc123xyz', '\\d+', 0)"""
    qt_select52 """ explain SELECT  Regexp_Extract_All('abc123xyz456', '\\d+')"""
    qt_select53 """ explain SELECT  Regexp_Replace('abc123xyz', '\\d+', '000')"""
    qt_select54 """ explain SELECT  Regexp_Replace_One('abc123xyz123', '\\d+', '000')"""
    qt_select55 """ explain SELECT  Decode_As_Varchar(65)"""
    qt_select56 """ explain SELECT  Encode_As_BigInt('123456789')"""
    qt_select57 """ explain SELECT  Encode_As_Int('12345')"""
    qt_select58 """ explain SELECT  Encode_As_LargeInt('9223372036854775807')"""
    qt_select59 """ explain SELECT  Encode_As_SmallInt('32767')"""

    // Substring with negative start index
    // Expected behavior: Depending on the SQL engine, might return an empty string or error.
    qt_select """SELECT Substring('Hello World', -1, 5)"""  

    // Substring with length exceeding the string length
    // Expected behavior: Return 'Hello' as the length exceeds the string length.
    qt_select """SELECT Substring('Hello', 1, 10)"""  

    // Left with length greater than string length
    // Expected behavior: Return 'Hello'.
    qt_select """SELECT Left('Hello', 10)"""  

    // Right with length greater than string length
    // Expected behavior: Return 'Hello'.
    qt_select """SELECT Right('Hello', 10)"""  

    // SplitPart with part number greater than the number of parts
    // Expected behavior: Return an empty string or error.
    qt_select """SELECT Split_Part('a,b,c', ',', 5)"""

    // SplitPart with negative part number
    // Expected behavior: Return an empty string or error.
    qt_select """SELECT Split_Part('a,b,c', ',', -1)"""

    // Locate with the substring not present
    // Expected behavior: Return 0 as 'World' is not found.
    qt_select """SELECT Locate('World', 'Hello')"""  

    // Instr with the substring not present
    // Expected behavior: Return 0 as 'World' is not found.
    qt_select """SELECT Instr('Hello', 'World')"""  

    // Replace with an empty search string
    // Expected behavior: Some SQL engines may treat this as a no-op, others might throw an error.
    qt_select """SELECT Replace('Hello World', '', 'Everyone')"""  

    // Replace with an empty replacement string
    // Expected behavior: Return 'Hello '.
    qt_select """SELECT Replace('Hello World', 'World', '')"""  

    // Concat with NULL values
    // Expected behavior: Depending on the SQL engine, may return 'HelloWorld' or NULL.
    qt_select """SELECT Concat('Hello', NULL, 'World')"""  

    // Ltrim with a string that has no leading spaces
    // Expected behavior: Return 'Hello'.
    qt_select """SELECT Ltrim('Hello')"""  

    // Rtrim with a string that has no trailing spaces
    // Expected behavior: Return 'Hello'.
    qt_select """SELECT Rtrim('Hello')"""  

    // JsonExtract with an invalid JSON path
    // Expected behavior: Return NULL or an empty string.
//    qt_select """SELECT Json_Extract('{"key": "value"}', '$.invalid')"""

    // JsonLength with a non-JSON string
    // Expected behavior: Return NULL or error.
    qt_select """SELECT Json_Length('Hello World')"""  

    // Field with a value not present in the list
    // Expected behavior: Return 0 as 'd' is not found.
    qt_select """SELECT Field('d', 'a', 'b', 'c')"""  

    // FindInSet with a value not present in the set
    // Expected behavior: Return 0 as 'd' is not found.
    qt_select """SELECT Find_In_Set('d', 'a,b,c')"""  

    // Repeat with a negative repeat count
    // Expected behavior: Return an empty string or error.
    qt_select """SELECT Repeat('Hello', -3)"""  

    // Space with a negative number of spaces
    // Expected behavior: Return an empty string or error.
    qt_select """SELECT Space(-5)"""  

    // SplitByChar with a delimiter not present in the string
    // Expected behavior: Return the original string in a single element array.
    qt_select """explain SELECT Split_By_Char(',', 'abc')"""

    // SplitByString with a delimiter not present in the string
    // Expected behavior: Return the original string in a single element array.
    qt_select """SELECT Split_By_String('::', 'abc')"""  

    // Strcmp with two identical strings
    // Expected behavior: Return 0 as the strings are equal.
    qt_select """SELECT Strcmp('abc', 'abc')"""  

    // Strcmp with a null string
    // Expected behavior: Return NULL or -1 depending on the SQL engine.
    qt_select """SELECT Strcmp('abc', NULL)"""  

    // Hex with a negative number
    // Expected behavior: Return the hexadecimal representation of the two's complement, or an error depending on the SQL engine.
    qt_select """SELECT Hex(-255)"""  

    // Unhex with an invalid hexadecimal string
    // Expected behavior: Return NULL or error as 'GHIJ' is not a valid hex string.
    qt_select """SELECT Unhex('GHIJ')"""  

    // JsonReplace with a path that does not exist
    // Expected behavior: Depending on the engine, might return the original JSON or an error.
//    qt_select """SELECT Json_Replace('{"key": "value"}', '$.nonexistent', 'new_value')"""

    // UrlDecode with an invalid percent-encoded string
    // Expected behavior: Return NULL or error due to invalid encoding.
    qt_select """SELECT Url_Decode('%ZZHello%20World')"""  


}
