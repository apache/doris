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

suite("doc_string_functions_test") {
    qt_auto_partition_name_range_day '''
        SELECT auto_partition_name('range', 'day', '2022-12-12 19:20:30');
    '''

    qt_auto_partition_name_range_month '''
        SELECT auto_partition_name('range', 'month', '2022-12-12 19:20:30');
    '''

    qt_auto_partition_name_list_single '''
        SELECT auto_partition_name('list', 'helloworld');
    '''

    qt_auto_partition_name_list_multi '''
        SELECT auto_partition_name('list', 'hello', 'world');
    '''

    qt_auto_partition_name_utf8 '''
        SELECT auto_partition_name('list', 'ṭṛì', 'ḍḍumai');
    '''

    test {
        sql ''' SELECT auto_partition_name('range', 'years', '2022-12-12'); '''
        exception "range auto_partition_name must accept"
    }

    qt_to_base64_basic '''
        SELECT TO_BASE64('1'), TO_BASE64('A');
    '''

    qt_to_base64_multi '''
        SELECT TO_BASE64('234'), TO_BASE64('Hello');
    '''

    qt_to_base64_null_empty '''
        SELECT TO_BASE64(NULL), TO_BASE64('');
    '''

    qt_to_base64_long '''
        SELECT TO_BASE64('Hello World'), TO_BASE64('The quick brown fox');
    '''

    qt_to_base64_special '''
        SELECT TO_BASE64('123456'), TO_BASE64('!@#$%^&*()');
    '''

    qt_to_base64_utf8 '''
        SELECT TO_BASE64('ṭṛì'), TO_BASE64('ḍḍumai hello');
    '''

    qt_to_base64_email '''
        SELECT TO_BASE64('user@example.com'), TO_BASE64('admin.test@company.org');
    '''

    qt_to_base64_json '''
        SELECT TO_BASE64('{"name":"John","age":30}'), TO_BASE64('[1,2,3,4,5]');
    '''

    qt_to_base64_padding '''
        SELECT TO_BASE64('a'), TO_BASE64('ab'), TO_BASE64('abc');
    '''

    qt_from_base64_basic '''
        SELECT FROM_BASE64('MQ=='), FROM_BASE64('QQ==');
    '''

    qt_from_base64_multi '''
        SELECT FROM_BASE64('MjM0'), FROM_BASE64('SGVsbG8=');
    '''

    qt_from_base64_null '''
        SELECT FROM_BASE64(NULL);
    '''

    qt_from_base64_empty '''
        SELECT FROM_BASE64('');
    '''

    qt_from_base64_invalid '''
        SELECT FROM_BASE64('!!!'), FROM_BASE64('ABC@DEF');
    '''

    qt_from_base64_long '''
        SELECT FROM_BASE64('SGVsbG8gV29ybGQ='), FROM_BASE64('VGhlIHF1aWNrIGJyb3duIGZveA==');
    '''

    qt_from_base64_utf8 '''
        SELECT FROM_BASE64('4bmt4bmbw6w='), FROM_BASE64('4bmN4bmNdW1haSBoZWxsbw==');
    '''

    qt_from_base64_email '''
        SELECT FROM_BASE64('dXNlckBleGFtcGxlLmNvbQ=='), FROM_BASE64('YWRtaW4udGVzdEBjb21wYW55Lm9yZw==');
    '''

    qt_from_base64_json '''
        SELECT FROM_BASE64('eyJuYW1lIjoiSm9obiIsImFnZSI6MzB9'), FROM_BASE64('WzEsMiwzLDQsNV0=');
    '''

    qt_from_base64_round_trip '''
        SELECT FROM_BASE64(TO_BASE64('Hello')), FROM_BASE64(TO_BASE64('测试'));
    '''

    qt_instr_basic '''
        SELECT INSTR('abc', 'b'), INSTR('abc', 'd');
    '''

    qt_instr_substring '''
        SELECT INSTR('hello world', 'world'), INSTR('hello world', 'WORLD');
    '''

    qt_instr_null '''
        SELECT INSTR(NULL, 'test'), INSTR('test', NULL);
    '''

    qt_instr_empty '''
        SELECT INSTR('hello', ''), INSTR('', 'world');
    '''

    qt_instr_repeated '''
        SELECT INSTR('abcabc', 'abc'), INSTR('banana', 'a');
    '''

    qt_instr_special '''
        SELECT INSTR('user@example.com', '@'), INSTR('price: $99.99', '$');
    '''

    qt_instr_utf8 '''
        SELECT INSTR('ṭṛì ḍḍumai hello', 'ḍḍumai'), INSTR('ṭṛì ḍḍumai hello', 'hello');
    '''

    qt_instr_digits '''
        SELECT INSTR('123456789', '456'), INSTR('123-456-789', '-');
    '''

    qt_instr_long '''
        SELECT INSTR('The quick brown fox', 'quick'), INSTR('The quick brown fox', 'slow');
    '''

    qt_instr_url '''
        SELECT INSTR('/home/user/file.txt', '/'), INSTR('https://www.example.com', '://');
    '''

    qt_length_ascii '''
        SELECT LENGTH('abc'), CHAR_LENGTH('abc');
    '''

    qt_length_chinese '''
        SELECT LENGTH('中国'), CHAR_LENGTH('中国');
    '''

    qt_length_null '''
        SELECT LENGTH(NULL);
    '''

    qt_length_empty '''
        SELECT LENGTH('');
    '''

    qt_length_mixed '''
        SELECT LENGTH('Hello世界'), CHAR_LENGTH('Hello世界');
    '''

    qt_length_special '''
        SELECT LENGTH('\t\n\r'), LENGTH('  ');
    '''

    qt_length_utf8 '''
        SELECT LENGTH('ṭṛì'), CHAR_LENGTH('ṭṛì');
    '''

    qt_length_emoji '''
        SELECT LENGTH('😀😁'), CHAR_LENGTH('😀😁');
    '''

    qt_length_digits '''
        SELECT LENGTH('12345'), CHAR_LENGTH('12345');
    '''

    qt_locate_basic '''
        SELECT LOCATE('bar', 'foobarbar'), LOCATE('xbar', 'foobar'), LOCATE('bar', 'foobarbar', 5);
    '''

    qt_locate_first_last '''
        SELECT LOCATE('f', 'foobar'), LOCATE('r', 'foobar');
    '''

    qt_locate_not_found '''
        SELECT LOCATE('xyz', 'foobar'), LOCATE('FOO', 'foobar');
    '''

    qt_locate_null '''
        SELECT LOCATE(NULL, 'foobar'), LOCATE('foo', NULL), LOCATE(NULL, NULL);
    '''

    qt_locate_empty '''
        SELECT LOCATE('', 'foobar'), LOCATE('foo', ''), LOCATE('', '');
    '''

    qt_locate_start_position '''
        SELECT LOCATE('o', 'foobar', 1), LOCATE('o', 'foobar', 2), LOCATE('o', 'foobar', 4);
    '''

    qt_locate_boundaries '''
        SELECT LOCATE('foo', 'foobar', 0), LOCATE('foo', 'foobar', -1), LOCATE('foo', 'foobar', 10);
    '''

    qt_locate_utf8 '''
        SELECT LOCATE('ṛì', 'ṭṛì ḍḍumai'), LOCATE('ḍḍu', 'ṭṛì ḍḍumai');
    '''

    qt_locate_case_sensitive '''
        SELECT LOCATE('BAR', 'foobar'), LOCATE('Bar', 'foobar'), LOCATE('bar', 'fooBAR');
    '''

    qt_locate_empty_with_position '''
        SELECT LOCATE('', 'foobar', 3), LOCATE('', 'foobar', 7), LOCATE('', '', 1);
    '''

    qt_lpad_basic '''
        SELECT LPAD('hi', 5, 'xy'), LPAD('hello', 8, '*');
    '''

    qt_lpad_truncate '''
        SELECT LPAD('hi', 1, 'xy'), LPAD('hello world', 5, 'x');
    '''

    qt_lpad_null '''
        SELECT LPAD(NULL, 5, 'x'), LPAD('hi', NULL, 'x'), LPAD('hi', 5, NULL);
    '''

    qt_lpad_empty_zero '''
        SELECT LPAD('', 0, ''), LPAD('hi', 0, 'x'), LPAD('', 5, '*');
    '''

    qt_lpad_empty_pad '''
        SELECT LPAD('hello', 10, ''), LPAD('hi', 2, '');
    '''

    qt_lpad_long_pad '''
        SELECT LPAD('123', 10, 'abc'), LPAD('X', 7, 'HELLO');
    '''

    qt_lpad_utf8 '''
        SELECT LPAD('hello', 10, 'ṭṛì'), LPAD('ḍḍumai', 3, 'x');
    '''

    qt_lpad_numeric '''
        SELECT LPAD('42', 6, '0'), LPAD('1234', 8, '0');
    '''

    qt_lpad_negative '''
        SELECT LPAD('hello', -1, 'x'), LPAD('test', -5, '*');
    '''
}
