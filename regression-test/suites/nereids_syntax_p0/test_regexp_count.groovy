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

suite("test_regexp_count") {
    qt_basic_count1 "SELECT regexp_count('a.b:c;d', '[\\\\.:;]');"
    qt_basic_count2 "SELECT regexp_count('a.b:c;d', '\\\\.');"
    qt_basic_count3 "SELECT regexp_count('a.b:c;d', ':');"
    qt_basic_count4 "SELECT regexp_count('a,b,c', ',');"
    qt_basic_count5 "SELECT regexp_count('a1b2c3d', '\\\\d');"
    qt_basic_count6 "SELECT regexp_count('a1b2346c3d', '\\\\d+');"
    qt_basic_count7 "SELECT regexp_count('abcd', 'x');"
    qt_basic_count8 "SELECT regexp_count('Hello world bye', '\\\\b[a-z]([a-z]*)');"
    qt_basic_count9 "SELECT regexp_count('Baby X', 'by ([A-Z].*)\\\\b[a-z]');"
    qt_basic_count10 "SELECT regexp_count('1a 2b 14m', '\\\\s*[a-z]+\\\\s*');"

    qt_empty_string "SELECT regexp_count('', 'x');"
    qt_empty_pattern "SELECT regexp_count('abcd', '');"
    qt_both_empty "SELECT regexp_count('', '');"

    qt_null_string "SELECT regexp_count(NULL, 'abc');"
    qt_null_pattern "SELECT regexp_count('abc', NULL);"
    qt_both_null "SELECT regexp_count(NULL, NULL);"

    qt_case_sensitive "SELECT regexp_count('Hello HELLO hello', 'hello');"
    qt_case_insensitive "SELECT regexp_count('Hello HELLO hello', '(?i)hello');"
    qt_case_mixed "SELECT regexp_count('AbCdEf', '[a-z]');"
    qt_case_mixed2 "SELECT regexp_count('AbCdEf', '(?i)[a-z]');"

    qt_special_chars "SELECT regexp_count('a+b*c?d', '[+*?]');"
    qt_escape_chars "SELECT regexp_count('a\\\\tb\\\\nc', '\\\\\\\\[tn]');"
    qt_brackets "SELECT regexp_count('a[b]c(d)e{f}', '[\\\\[\\\\]\\\\(\\\\)\\\\{\\\\}]');"

    qt_digits_only "SELECT regexp_count('123abc456def789', '\\\\d+');"
    qt_letters_only "SELECT regexp_count('123abc456def789', '[a-zA-Z]+');"
    qt_alphanumeric "SELECT regexp_count('abc123def456', '[a-zA-Z0-9]+');"

    qt_word_boundary "SELECT regexp_count('hello world hello', '\\\\bhello\\\\b');"
    qt_word_boundary2 "SELECT regexp_count('helloworld hello', '\\\\bhello\\\\b');"

    qt_greedy "SELECT regexp_count('aaabbbccc', 'a+');"
    qt_non_greedy "SELECT regexp_count('aaabbbccc', 'a+?');"
    qt_greedy_any "SELECT regexp_count('abc123def456', '.*\\\\d');"

    qt_chinese_unicode "SELECT regexp_count('这是一个测试字符串', '\\\\p{Han}');"
    qt_chinese_mixed "SELECT regexp_count('Hello世界World世界', '世界');"
    qt_chinese_pattern "SELECT regexp_count('测试123测试456', '测试\\\\d+');"

    qt_unicode_letter "SELECT regexp_count('Hello世界123', '\\\\p{L}');"
    qt_unicode_punct "SELECT regexp_count('Hello, 世界!', '\\\\p{P}');"

    qt_overlapping "SELECT regexp_count('aaaa', 'aa');"

    qt_complex_email "SELECT regexp_count('test@example.com user@domain.org', '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}');"
    qt_complex_phone "SELECT regexp_count('Call 123-456-7890 or 987.654.3210', '\\\\d{3}[-.]\\\\d{3}[-.]\\\\d{4}');"
    qt_complex_url "SELECT regexp_count('Visit https://example.com or http://test.org', 'https?://[a-zA-Z0-9.-]+');"

    qt_long_pattern "SELECT regexp_count('abcdefghijklmnopqrstuvwxyz', '[a-z]');"

    sql """DROP TABLE IF EXISTS `test_table_for_regexp_count`;"""
    sql """CREATE TABLE test_table_for_regexp_count (
        id INT,
        text_data VARCHAR(500),
        pattern VARCHAR(100)
    ) PROPERTIES ("replication_num"="1");"""

    sql """INSERT INTO test_table_for_regexp_count VALUES
        (1, 'Hello World Hello', 'Hello'),
        (2, 'abc123def456ghi789', '\\\\d+'),
        (3, '测试字符串测试', '测试'),
        (4, 'email@test.com and user@domain.org', '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}'),
        (5, '', 'any'),
        (6, 'no match here', 'xyz'),
        (7, NULL, 'pattern'),
        (8, 'text here', NULL),
        (9, 'Case CASE case', '(?i)case'),
        (10, 'line1\\nline2\\nline3', 'line');"""

    qt_table_basic "SELECT id, regexp_count(text_data, pattern) as count_result FROM test_table_for_regexp_count ORDER BY id;"
    qt_table_fixed_pattern "SELECT id, regexp_count(text_data, 'e') as count_e FROM test_table_for_regexp_count WHERE text_data IS NOT NULL ORDER BY id;"
    qt_table_case_insensitive "SELECT id, regexp_count(text_data, '(?i)test') as count_test FROM test_table_for_regexp_count WHERE text_data IS NOT NULL ORDER BY id;"

    check_fold_consistency "regexp_count('abc123', '\\\\d')"
    check_fold_consistency "regexp_count(null, 'abc')"
    check_fold_consistency "regexp_count('abc123', null)"
    check_fold_consistency "regexp_count('Hello HELLO hello', '(?i)hello')"

    test {
        sql "SELECT regexp_count('test', '[invalid');"
        exception "Could not compile regexp pattern"
    }

    // 清理测试表
    sql """DROP TABLE IF EXISTS `test_table_for_regexp_count`;"""
}