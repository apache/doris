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

suite("string_functions_all") {
    // APPEND_TRAILING_CHAR_IF_ABSENT tests
    qt_append_trailing_char_if_absent_1 "SELECT APPEND_TRAILING_CHAR_IF_ABSENT('a', 'c');"
    testFoldConst("SELECT APPEND_TRAILING_CHAR_IF_ABSENT('a', 'c');")
    qt_append_trailing_char_if_absent_2 "SELECT APPEND_TRAILING_CHAR_IF_ABSENT('ac', 'c');"
    testFoldConst("SELECT APPEND_TRAILING_CHAR_IF_ABSENT('ac', 'c');")
    qt_append_trailing_char_if_absent_3 "SELECT APPEND_TRAILING_CHAR_IF_ABSENT('', '/');"
    testFoldConst("SELECT APPEND_TRAILING_CHAR_IF_ABSENT('', '/');")
    qt_append_trailing_char_if_absent_4 "SELECT APPEND_TRAILING_CHAR_IF_ABSENT(NULL, 'c');"
    testFoldConst("SELECT APPEND_TRAILING_CHAR_IF_ABSENT(NULL, 'c');")
    qt_append_trailing_char_if_absent_5 "SELECT APPEND_TRAILING_CHAR_IF_ABSENT('acf', 'ṛ');"
    testFoldConst("SELECT APPEND_TRAILING_CHAR_IF_ABSENT('acf', 'ṛ');")

    // ASCII tests
    qt_ascii_6 "SELECT ASCII('1'), ASCII('234');"
    testFoldConst("SELECT ASCII('1'), ASCII('234');")
    qt_ascii_7 "SELECT ASCII('A'), ASCII('a'), ASCII('Z');"
    testFoldConst("SELECT ASCII('A'), ASCII('a'), ASCII('Z');")
    qt_ascii_8 "SELECT ASCII('');"
    testFoldConst("SELECT ASCII('');")
    qt_ascii_9 "SELECT ASCII(NULL);"
    testFoldConst("SELECT ASCII(NULL);")
    qt_ascii_10 "SELECT ASCII(' '), ASCII('!'), ASCII('@');"
    testFoldConst("SELECT ASCII(' '), ASCII('!'), ASCII('@');")
    qt_ascii_11 "SELECT ASCII('t'), ASCII('n'), ASCII('r');"
    testFoldConst("SELECT ASCII('t'), ASCII('n'), ASCII('r');")
    qt_ascii_12 "SELECT ASCII('Hello'), ASCII('World123');"
    testFoldConst("SELECT ASCII('Hello'), ASCII('World123');")
    qt_ascii_13 "SELECT ASCII('ṭṛì'), ASCII('ḍḍumai');"
    testFoldConst("SELECT ASCII('ṭṛì'), ASCII('ḍḍumai');")
    qt_ascii_14 "SELECT ASCII('9abc'), ASCII('0xyz');"
    testFoldConst("SELECT ASCII('9abc'), ASCII('0xyz');")

    // AUTO_PARTITION_NAME tests
    qt_auto_partition_name_15 "SELECT auto_partition_name('range', 'day', '2022-12-12 19:20:30');"
    testFoldConst("SELECT auto_partition_name('range', 'day', '2022-12-12 19:20:30');")
    qt_auto_partition_name_16 "SELECT auto_partition_name('range', 'month', '2022-12-12 19:20:30');"
    testFoldConst("SELECT auto_partition_name('range', 'month', '2022-12-12 19:20:30');")
    qt_auto_partition_name_17 "SELECT auto_partition_name('list', 'helloworld');"
    testFoldConst("SELECT auto_partition_name('list', 'helloworld');")
    qt_auto_partition_name_18 "SELECT auto_partition_name('list', 'hello', 'world');"
    testFoldConst("SELECT auto_partition_name('list', 'hello', 'world');")
    qt_auto_partition_name_19 "SELECT auto_partition_name('list', 'ṭṛì', 'ḍḍumai');"
    testFoldConst("SELECT auto_partition_name('list', 'ṭṛì', 'ḍḍumai');")

    // CHAR_LENGTH tests
    qt_char_length_21 "SELECT CHAR_LENGTH('hello');"
    testFoldConst("SELECT CHAR_LENGTH('hello');")
    qt_char_length_22 "SELECT CHAR_LENGTH('中国');"
    testFoldConst("SELECT CHAR_LENGTH('中国');")
    qt_char_length_23 "SELECT CHAR_LENGTH(NULL);"
    testFoldConst("SELECT CHAR_LENGTH(NULL);")
    qt_char_length_24 "SELECT CHAR_LENGTH('中国') AS char_len, LENGTH('中国') AS byte_len;"
    testFoldConst("SELECT CHAR_LENGTH('中国') AS char_len, LENGTH('中国') AS byte_len;")

    // CHAR tests
    qt_char_25 "SELECT CHAR(68, 111, 114, 105, 115);"
    testFoldConst("SELECT CHAR(68, 111, 114, 105, 115);")
    qt_char_26 "SELECT CHAR(15049882, 15179199, 14989469);"
    testFoldConst("SELECT CHAR(15049882, 15179199, 14989469);")
    qt_char_27 "SELECT CHAR(255);"
    testFoldConst("SELECT CHAR(255);")
    qt_char_28 "SELECT CHAR(NULL);"
    testFoldConst("SELECT CHAR(NULL);")

    // UNCOMPRESS tests
    qt_uncompress_29 "SELECT uncompress(compress('hello'));"
    testFoldConst("SELECT uncompress(compress('hello'));")

    // COMPRESS tests
    qt_compress_30 "SELECT compress('');"
    testFoldConst("SELECT compress('');")
    qt_compress_31 "SELECT compress(NULL);"
    testFoldConst("SELECT compress(NULL);")

    // UNCOMPRESS tests
    qt_uncompress_32 "SELECT uncompress(compress('ṭṛì'));"
    testFoldConst("SELECT uncompress(compress('ṭṛì'));")

    // CONCAT_WS tests
    qt_concat_ws_33 "SELECT CONCAT_WS(',', 'apple', 'banana', 'orange'), CONCAT_WS('-', 'hello', 'world');"
    testFoldConst("SELECT CONCAT_WS(',', 'apple', 'banana', 'orange'), CONCAT_WS('-', 'hello', 'world');")
    qt_concat_ws_34 "SELECT CONCAT_WS(NULL, 'd', 'is'), CONCAT_WS('or', 'd', NULL, 'is');"
    testFoldConst("SELECT CONCAT_WS(NULL, 'd', 'is'), CONCAT_WS('or', 'd', NULL, 'is');")
    qt_concat_ws_35 "SELECT CONCAT_WS('|', 'hello', '', 'world', NULL), CONCAT_WS(',', '', 'test', '');"
    testFoldConst("SELECT CONCAT_WS('|', 'hello', '', 'world', NULL), CONCAT_WS(',', '', 'test', '');")
    qt_concat_ws_36 "SELECT CONCAT_WS('x', NULL, NULL), CONCAT_WS('-', NULL, NULL, NULL);"
    testFoldConst("SELECT CONCAT_WS('x', NULL, NULL), CONCAT_WS('-', NULL, NULL, NULL);")
    qt_concat_ws_37 "SELECT CONCAT_WS('or', ['d', 'is']), CONCAT_WS('-', ['apple', 'banana', 'cherry']);"
    testFoldConst("SELECT CONCAT_WS('or', ['d', 'is']), CONCAT_WS('-', ['apple', 'banana', 'cherry']);")
    qt_concat_ws_38 "SELECT CONCAT_WS('or', ['d', NULL, 'is']), CONCAT_WS(',', [NULL, 'a', 'b', NULL, 'c']);"
    testFoldConst("SELECT CONCAT_WS('or', ['d', NULL, 'is']), CONCAT_WS(',', [NULL, 'a', 'b', NULL, 'c']);")
    qt_concat_ws_39 "SELECT CONCAT_WS('-', ['a', 'b'], ['c', NULL], ['d']), CONCAT_WS('|', ['x'], ['y', 'z']);"
    testFoldConst("SELECT CONCAT_WS('-', ['a', 'b'], ['c', NULL], ['d']), CONCAT_WS('|', ['x'], ['y', 'z']);")
    qt_concat_ws_40 "SELECT CONCAT_WS('-', ['a', 'b'], NULL, ['c', NULL], ['d']);"
    testFoldConst("SELECT CONCAT_WS('-', ['a', 'b'], NULL, ['c', NULL], ['d']);")
    qt_concat_ws_41 "SELECT CONCAT_WS('x', 'ṭṛì', 'ḍḍumai'), CONCAT_WS('→', ['ṭṛì', 'ḍḍumai', 'hello']);"
    testFoldConst("SELECT CONCAT_WS('x', 'ṭṛì', 'ḍḍumai'), CONCAT_WS('→', ['ṭṛì', 'ḍḍumai', 'hello']);")
    qt_concat_ws_42 "SELECT CONCAT_WS(',', 'Name', 'Age', 'City'), CONCAT_WS('/', 'home', 'user', 'documents', 'file.txt');"
    testFoldConst("SELECT CONCAT_WS(',', 'Name', 'Age', 'City'), CONCAT_WS('/', 'home', 'user', 'documents', 'file.txt');")

    // CONCAT tests
    qt_concat_43 "SELECT CONCAT('a', 'b'), CONCAT('a', 'b', 'c');"
    testFoldConst("SELECT CONCAT('a', 'b'), CONCAT('a', 'b', 'c');")
    qt_concat_44 "SELECT CONCAT('a', NULL, 'c'), CONCAT('hello', NULL);"
    testFoldConst("SELECT CONCAT('a', NULL, 'c'), CONCAT('hello', NULL);")
    qt_concat_45 "SELECT CONCAT('hello', '', 'world'), CONCAT('', 'test', '');"
    testFoldConst("SELECT CONCAT('hello', '', 'world'), CONCAT('', 'test', '');")
    qt_concat_46 "SELECT CONCAT('User', 123), CONCAT('Price: \$', 99.99);"
    testFoldConst("SELECT CONCAT('User', 123), CONCAT('Price: \$', 99.99);")
    qt_concat_47 "SELECT CONCAT('A', 'B', 'C', 'D', 'E'), CONCAT('1', '2', '3', '4', '5');"
    testFoldConst("SELECT CONCAT('A', 'B', 'C', 'D', 'E'), CONCAT('1', '2', '3', '4', '5');")
    qt_concat_48 "SELECT CONCAT('ṭṛì', ' ', 'ḍḍumai'), CONCAT('Hello', ' ', 'ṭṛì', ' ', 'ḍḍumai');"
    testFoldConst("SELECT CONCAT('ṭṛì', ' ', 'ḍḍumai'), CONCAT('Hello', ' ', 'ṭṛì', ' ', 'ḍḍumai');")
    qt_concat_49 "SELECT CONCAT('/home/', 'user/', 'file.txt'), CONCAT('https://', 'www.example.com', '/api');"
    testFoldConst("SELECT CONCAT('/home/', 'user/', 'file.txt'), CONCAT('https://', 'www.example.com', '/api');")
    qt_concat_50 "SELECT CONCAT('user', '@', 'example.com'), CONCAT('admin.', 'support', '@', 'company.org');"
    testFoldConst("SELECT CONCAT('user', '@', 'example.com'), CONCAT('admin.', 'support', '@', 'company.org');")

    // COUNT_SUBSTRINGS tests
    qt_count_substrings_51 "SELECT count_substrings('a1b1c1d', '1');"
    testFoldConst("SELECT count_substrings('a1b1c1d', '1');")
    qt_count_substrings_52 "SELECT count_substrings(',,a,b,c,', ',');"
    testFoldConst("SELECT count_substrings(',,a,b,c,', ',');")
    qt_count_substrings_53 "SELECT count_substrings('ccc', 'cc');"
    testFoldConst("SELECT count_substrings('ccc', 'cc');")
    qt_count_substrings_54 "SELECT count_substrings(NULL, ',');"
    testFoldConst("SELECT count_substrings(NULL, ',');")
    qt_count_substrings_55 "SELECT count_substrings('a,b,c,abcde', '');"
    testFoldConst("SELECT count_substrings('a,b,c,abcde', '');")
    qt_count_substrings_56 "SELECT count_substrings('ṭṛì ḍḍumai ṭṛì ti ḍḍumannàri', 'ḍḍu', 1);"
    testFoldConst("SELECT count_substrings('ṭṛì ḍḍumai ṭṛì ti ḍḍumannàri', 'ḍḍu', 1);")
    qt_count_substrings_57 "SELECT count_substrings('éèêëìíîïðñòó éèêëìíîïðñòó', 'éèê', 0);"
    testFoldConst("SELECT count_substrings('éèêëìíîïðñòó éèêëìíîïðñòó', 'éèê', 0);")

    // DIGITAL_MASKING tests
    qt_digital_masking_58 "SELECT digital_masking('13812345678');"
    testFoldConst("SELECT digital_masking('13812345678');")
    qt_digital_masking_59 "SELECT digital_masking('1234567890');"
    testFoldConst("SELECT digital_masking('1234567890');")
    qt_digital_masking_60 "SELECT digital_masking('123');"
    testFoldConst("SELECT digital_masking('123');")
    qt_digital_masking_61 "SELECT digital_masking(NULL);"
    testFoldConst("SELECT digital_masking(NULL);")
    qt_digital_masking_62 "SELECT digital_masking('13812ṭṛ34678');"
    testFoldConst("SELECT digital_masking('13812ṭṛ34678');")

    // ELT tests
    qt_elt_63 "SELECT ELT(1, 'aaa', 'bbb', 'ccc');"
    testFoldConst("SELECT ELT(1, 'aaa', 'bbb', 'ccc');")
    qt_elt_64 "SELECT ELT(2, 'aaa', 'bbb', 'ccc');"
    testFoldConst("SELECT ELT(2, 'aaa', 'bbb', 'ccc');")
    qt_elt_65 "SELECT ELT(0, 'aaa', 'bbb'), ELT(5, 'aaa', 'bbb');"
    testFoldConst("SELECT ELT(0, 'aaa', 'bbb'), ELT(5, 'aaa', 'bbb');")
    qt_elt_66 "SELECT ELT(NULL, 'aaa', 'bbb');"
    testFoldConst("SELECT ELT(NULL, 'aaa', 'bbb');")
    qt_elt_67 "SELECT ELT(5, 'aaa', 'bbb', 'ccc');"
    testFoldConst("SELECT ELT(5, 'aaa', 'bbb', 'ccc');")
    qt_elt_68 "SELECT ELT(-1, 'first', 'second');"
    testFoldConst("SELECT ELT(-1, 'first', 'second');")
    qt_elt_70 "SELECT ELT(2, 'first', '', 'third');"
    testFoldConst("SELECT ELT(2, 'first', '', 'third');")

    // ENDS_WITH tests
    qt_ends_with_71 "SELECT ENDS_WITH('Hello doris', 'doris'), ENDS_WITH('Hello doris', 'Hello');"
    testFoldConst("SELECT ENDS_WITH('Hello doris', 'doris'), ENDS_WITH('Hello doris', 'Hello');")
    qt_ends_with_72 "SELECT ENDS_WITH('Hello World', 'world'), ENDS_WITH('Hello World', 'World');"
    testFoldConst("SELECT ENDS_WITH('Hello World', 'world'), ENDS_WITH('Hello World', 'World');")
    qt_ends_with_73 "SELECT ENDS_WITH(NULL, 'test'), ENDS_WITH('test', NULL);"
    testFoldConst("SELECT ENDS_WITH(NULL, 'test'), ENDS_WITH('test', NULL);")
    qt_ends_with_74 "SELECT ENDS_WITH('hello', ''), ENDS_WITH('', 'world');"
    testFoldConst("SELECT ENDS_WITH('hello', ''), ENDS_WITH('', 'world');")
    qt_ends_with_75 "SELECT ENDS_WITH('test', 'test'), ENDS_WITH('testing', 'test');"
    testFoldConst("SELECT ENDS_WITH('test', 'test'), ENDS_WITH('testing', 'test');")
    qt_ends_with_76 "SELECT ENDS_WITH('document.pdf', '.pdf'), ENDS_WITH('image.jpg', '.png');"
    testFoldConst("SELECT ENDS_WITH('document.pdf', '.pdf'), ENDS_WITH('image.jpg', '.png');")
    qt_ends_with_77 "SELECT ENDS_WITH('hello ṭṛì ḍḍumai', 'ḍḍumai'), ENDS_WITH('hello ṭṛì ḍḍumai', 'ṭṛì');"
    testFoldConst("SELECT ENDS_WITH('hello ṭṛì ḍḍumai', 'ḍḍumai'), ENDS_WITH('hello ṭṛì ḍḍumai', 'ṭṛì');")
    qt_ends_with_78 "SELECT ENDS_WITH('https://example.com/api', '/api'), ENDS_WITH('https://example.com/', '.html');"
    testFoldConst("SELECT ENDS_WITH('https://example.com/api', '/api'), ENDS_WITH('https://example.com/', '.html');")
    qt_ends_with_79 "SELECT ENDS_WITH('123456789', '789'), ENDS_WITH('123456789', '456');"
    testFoldConst("SELECT ENDS_WITH('123456789', '789'), ENDS_WITH('123456789', '456');")
    qt_ends_with_80 "SELECT ENDS_WITH('user@gmail.com', '.com'), ENDS_WITH('admin@company.org', '.com');"
    testFoldConst("SELECT ENDS_WITH('user@gmail.com', '.com'), ENDS_WITH('admin@company.org', '.com');")

    // FIND_IN_SET tests
    qt_find_in_set_81 "SELECT FIND_IN_SET('b', 'a,b,c');"
    testFoldConst("SELECT FIND_IN_SET('b', 'a,b,c');")
    qt_find_in_set_82 "SELECT FIND_IN_SET('apple', 'apple,banana,cherry');"
    testFoldConst("SELECT FIND_IN_SET('apple', 'apple,banana,cherry');")
    qt_find_in_set_83 "SELECT FIND_IN_SET('cherry', 'apple,banana,cherry');"
    testFoldConst("SELECT FIND_IN_SET('cherry', 'apple,banana,cherry');")
    qt_find_in_set_84 "SELECT FIND_IN_SET('orange', 'apple,banana,cherry');"
    testFoldConst("SELECT FIND_IN_SET('orange', 'apple,banana,cherry');")
    qt_find_in_set_85 "SELECT FIND_IN_SET(NULL, 'a,b,c'), FIND_IN_SET('b', NULL);"
    testFoldConst("SELECT FIND_IN_SET(NULL, 'a,b,c'), FIND_IN_SET('b', NULL);")
    qt_find_in_set_86 "SELECT FIND_IN_SET('', 'a,b,c'), FIND_IN_SET('a', '');"
    testFoldConst("SELECT FIND_IN_SET('', 'a,b,c'), FIND_IN_SET('a', '');")
    qt_find_in_set_87 "SELECT FIND_IN_SET('a,b', 'a,b,c,d');"
    testFoldConst("SELECT FIND_IN_SET('a,b', 'a,b,c,d');")
    qt_find_in_set_88 "SELECT FIND_IN_SET('B', 'a,b,c'), FIND_IN_SET('b', 'A,B,C');"
    testFoldConst("SELECT FIND_IN_SET('B', 'a,b,c'), FIND_IN_SET('b', 'A,B,C');")
    qt_find_in_set_89 "SELECT FIND_IN_SET('ap', 'apple,banana,cherry');"
    testFoldConst("SELECT FIND_IN_SET('ap', 'apple,banana,cherry');")
    qt_find_in_set_90 "SELECT FIND_IN_SET('2', '1,2,3,10,20');"
    testFoldConst("SELECT FIND_IN_SET('2', '1,2,3,10,20');")

    // FORMAT_NUMBER tests
    qt_format_number_91 "SELECT format_number(1500);"
    testFoldConst("SELECT format_number(1500);")
    qt_format_number_92 "SELECT format_number(5000000);"
    testFoldConst("SELECT format_number(5000000);")
    qt_format_number_93 "SELECT format_number(999);"
    testFoldConst("SELECT format_number(999);")
    qt_format_number_94 "SELECT format_number(NULL);"
    testFoldConst("SELECT format_number(NULL);")

    // FORMAT tests
    qt_format_95 "SELECT format('{:.2}', pi());"
    testFoldConst("SELECT format('{:.2}', pi());")
    qt_format_96 "SELECT format('{0}-{1}', 'hello', 'world');"
    testFoldConst("SELECT format('{0}-{1}', 'hello', 'world');")
    qt_format_97 "SELECT format('{:>10}', 123);"
    testFoldConst("SELECT format('{:>10}', 123);")
    qt_format_98 "SELECT format('{:.2}', NULL);"
    testFoldConst("SELECT format('{:.2}', NULL);")
    qt_format_99 "SELECT format('{0}-{1}', 'ṭṛṭṛ', 'ṭṛ');"
    testFoldConst("SELECT format('{0}-{1}', 'ṭṛṭṛ', 'ṭṛ');")

    // FROM_BASE64 tests
    qt_test_100 "SELECT FROM_BASE64('MQ=='), FROM_BASE64('QQ==');"
    testFoldConst("SELECT FROM_BASE64('MQ=='), FROM_BASE64('QQ==');")
    qt_test_101 "SELECT FROM_BASE64('MjM0'), FROM_BASE64('SGVsbG8=');"
    testFoldConst("SELECT FROM_BASE64('MjM0'), FROM_BASE64('SGVsbG8=');")
    qt_test_102 "SELECT FROM_BASE64(NULL);"
    testFoldConst("SELECT FROM_BASE64(NULL);")
    qt_test_103 "SELECT FROM_BASE64('');"
    testFoldConst("SELECT FROM_BASE64('');")
    qt_test_104 "SELECT FROM_BASE64('!!!'), FROM_BASE64('ABC@DEF');"
    testFoldConst("SELECT FROM_BASE64('!!!'), FROM_BASE64('ABC@DEF');")
    qt_test_105 "SELECT FROM_BASE64('SGVsbG8gV29ybGQ='), FROM_BASE64('VGhlIHF1aWNrIGJyb3duIGZveA==');"
    testFoldConst("SELECT FROM_BASE64('SGVsbG8gV29ybGQ='), FROM_BASE64('VGhlIHF1aWNrIGJyb3duIGZveA==');")
    qt_test_106 "SELECT FROM_BASE64('4bmt4bmb4bmA'), FROM_BASE64('4bmN4bmNdW1haSBoZWxsbw==');"
    testFoldConst("SELECT FROM_BASE64('4bmt4bmb4bmA'), FROM_BASE64('4bmN4bmNdW1haSBoZWxsbw==');")
    qt_test_107 "SELECT FROM_BASE64('dXNlckBleGFtcGxlLmNvbQ=='), FROM_BASE64('YWRtaW4udGVzdEBjb21wYW55Lm9yZw==');"
    testFoldConst("SELECT FROM_BASE64('dXNlckBleGFtcGxlLmNvbQ=='), FROM_BASE64('YWRtaW4udGVzdEBjb21wYW55Lm9yZw==');")
    qt_test_108 "SELECT FROM_BASE64('eyJuYW1lIjoiSm9obiIsImFnZSI6MzB9'), FROM_BASE64('WzEsMiwzLDQsNV0=');"
    testFoldConst("SELECT FROM_BASE64('eyJuYW1lIjoiSm9obiIsImFnZSI6MzB9'), FROM_BASE64('WzEsMiwzLDQsNV0=');")
    qt_test_109 "SELECT FROM_BASE64(TO_BASE64('Hello')), FROM_BASE64(TO_BASE64('测试'));"
    testFoldConst("SELECT FROM_BASE64(TO_BASE64('Hello')), FROM_BASE64(TO_BASE64('测试'));")

    // HEX tests
    qt_hex_110 "SELECT HEX(12), HEX(-1);"
    testFoldConst("SELECT HEX(12), HEX(-1);")
    qt_hex_111 "SELECT HEX('1'), HEX('@'), HEX('12');"
    testFoldConst("SELECT HEX('1'), HEX('@'), HEX('12');")
    qt_hex_112 "SELECT HEX(255), HEX(65535), HEX(16777215);"
    testFoldConst("SELECT HEX(255), HEX(65535), HEX(16777215);")
    qt_hex_113 "SELECT HEX(NULL);"
    testFoldConst("SELECT HEX(NULL);")
    qt_hex_114 "SELECT HEX(0), HEX('');"
    testFoldConst("SELECT HEX(0), HEX('');")
    qt_hex_115 "SELECT HEX(' '), HEX('t'), HEX('n');"
    testFoldConst("SELECT HEX(' '), HEX('t'), HEX('n');")
    qt_hex_116 "SELECT HEX('ṭṛì'), HEX('ḍḍumai');"
    testFoldConst("SELECT HEX('ṭṛì'), HEX('ḍḍumai');")
    qt_hex_117 "SELECT HEX(-128), HEX(-32768);"
    testFoldConst("SELECT HEX(-128), HEX(-32768);")
    qt_hex_118 "SELECT HEX('A1'), HEX('Hello!');"
    testFoldConst("SELECT HEX('A1'), HEX('Hello!');")

    // INITCAP tests
    qt_initcap_119 "SELECT INITCAP('hello world');"
    testFoldConst("SELECT INITCAP('hello world');")
    qt_initcap_120 "SELECT INITCAP('hELLo WoRLD');"
    testFoldConst("SELECT INITCAP('hELLo WoRLD');")
    qt_initcap_121 "SELECT INITCAP(NULL);"
    testFoldConst("SELECT INITCAP(NULL);")
    qt_initcap_122 "SELECT INITCAP('');"
    testFoldConst("SELECT INITCAP('');")
    qt_initcap_123 "SELECT INITCAP('hello hello.,HELLO123HELlo');"
    testFoldConst("SELECT INITCAP('hello hello.,HELLO123HELlo');")
    qt_initcap_124 "SELECT INITCAP('word1@word2#word3\$word4');"
    testFoldConst("SELECT INITCAP('word1@word2#word3\$word4');")
    qt_initcap_125 "SELECT INITCAP('ṭṛì ḍḍumai hello');"
    testFoldConst("SELECT INITCAP('ṭṛì ḍḍumai hello');")
    qt_initcap_126 "SELECT INITCAP('john doe'), INITCAP('MARY JANE');"
    testFoldConst("SELECT INITCAP('john doe'), INITCAP('MARY JANE');")
    qt_initcap_127 "SELECT INITCAP('the quick brown fox'), INITCAP('DATABASE management SYSTEM');"
    testFoldConst("SELECT INITCAP('the quick brown fox'), INITCAP('DATABASE management SYSTEM');")
    qt_initcap_128 "SELECT INITCAP('word1 word2--word3'), INITCAP('hello, world! how are you?');"
    testFoldConst("SELECT INITCAP('word1 word2--word3'), INITCAP('hello, world! how are you?');")

    // INSTR tests
    qt_instr_129 "SELECT INSTR('abc', 'b'), INSTR('abc', 'd');"
    testFoldConst("SELECT INSTR('abc', 'b'), INSTR('abc', 'd');")
    qt_instr_130 "SELECT INSTR('hello world', 'world'), INSTR('hello world', 'WORLD');"
    testFoldConst("SELECT INSTR('hello world', 'world'), INSTR('hello world', 'WORLD');")
    qt_instr_131 "SELECT INSTR(NULL, 'test'), INSTR('test', NULL);"
    testFoldConst("SELECT INSTR(NULL, 'test'), INSTR('test', NULL);")
    qt_instr_132 "SELECT INSTR('hello', ''), INSTR('', 'world');"
    testFoldConst("SELECT INSTR('hello', ''), INSTR('', 'world');")
    qt_instr_133 "SELECT INSTR('abcabc', 'abc'), INSTR('banana', 'a');"
    testFoldConst("SELECT INSTR('abcabc', 'abc'), INSTR('banana', 'a');")
    qt_instr_134 "SELECT INSTR('user@example.com', '@'), INSTR('price: \$99.99', '\$');"
    testFoldConst("SELECT INSTR('user@example.com', '@'), INSTR('price: \$99.99', '\$');")
    qt_instr_135 "SELECT INSTR('ṭṛì ḍḍumai hello', 'ḍḍumai'), INSTR('ṭṛì ḍḍumai hello', 'hello');"
    testFoldConst("SELECT INSTR('ṭṛì ḍḍumai hello', 'ḍḍumai'), INSTR('ṭṛì ḍḍumai hello', 'hello');")
    qt_instr_136 "SELECT INSTR('123456789', '456'), INSTR('123-456-789', '-');"
    testFoldConst("SELECT INSTR('123456789', '456'), INSTR('123-456-789', '-');")
    qt_instr_137 "SELECT INSTR('The quick brown fox', 'quick'), INSTR('The quick brown fox', 'slow');"
    testFoldConst("SELECT INSTR('The quick brown fox', 'quick'), INSTR('The quick brown fox', 'slow');")
    qt_instr_138 "SELECT INSTR('/home/user/file.txt', '/'), INSTR('https://www.example.com', '://');"
    testFoldConst("SELECT INSTR('/home/user/file.txt', '/'), INSTR('https://www.example.com', '://');")

    // INT_TO_UUID tests
    qt_int_to_uuid_139 "SELECT INT_TO_UUID(95721955514869408091759290071393952876);"
    testFoldConst("SELECT INT_TO_UUID(95721955514869408091759290071393952876);")
    qt_int_to_uuid_140 "SELECT INT_TO_UUID(NULL);"
    testFoldConst("SELECT INT_TO_UUID(NULL);")

    // LOWER tests
    qt_lower_141 "SELECT LOWER('AbC123'), LCASE('AbC123');"
    testFoldConst("SELECT LOWER('AbC123'), LCASE('AbC123');")
    qt_lower_142 "SELECT LOWER('Hello World!'), LCASE('TEST@123');"
    testFoldConst("SELECT LOWER('Hello World!'), LCASE('TEST@123');")
    qt_lower_143 "SELECT LOWER(NULL), LCASE(NULL);"
    testFoldConst("SELECT LOWER(NULL), LCASE(NULL);")
    qt_lower_144 "SELECT LOWER(''), LCASE('');"
    testFoldConst("SELECT LOWER(''), LCASE('');")
    qt_lower_145 "SELECT LOWER('already lowercase'), LCASE('abc123');"
    testFoldConst("SELECT LOWER('already lowercase'), LCASE('abc123');")
    qt_lower_146 "SELECT LOWER('123!@#\$%'), LCASE('PRICE: \$99.99');"
    testFoldConst("SELECT LOWER('123!@#\$%'), LCASE('PRICE: \$99.99');")
    qt_lower_147 "SELECT LOWER('ṬṚÌ TEST'), LCASE('ḌḌUMAI HELLO');"
    testFoldConst("SELECT LOWER('ṬṚÌ TEST'), LCASE('ḌḌUMAI HELLO');")

    // LENGTH tests
    qt_length_148 "SELECT LENGTH('abc'), CHAR_LENGTH('abc');"
    testFoldConst("SELECT LENGTH('abc'), CHAR_LENGTH('abc');")
    qt_length_149 "SELECT LENGTH('中国'), CHAR_LENGTH('中国');"
    testFoldConst("SELECT LENGTH('中国'), CHAR_LENGTH('中国');")
    qt_length_150 "SELECT LENGTH(NULL);"
    testFoldConst("SELECT LENGTH(NULL);")
    qt_length_151 "SELECT LENGTH('');"
    testFoldConst("SELECT LENGTH('');")
    qt_length_152 "SELECT LENGTH('Hello世界'), CHAR_LENGTH('Hello世界');"
    testFoldConst("SELECT LENGTH('Hello世界'), CHAR_LENGTH('Hello世界');")
    qt_length_153 "SELECT LENGTH('tnr'), LENGTH(' ');"
    testFoldConst("SELECT LENGTH('tnr'), LENGTH(' ');")
    qt_length_154 "SELECT LENGTH('ṭṛì'), CHAR_LENGTH('ṭṛì');"
    testFoldConst("SELECT LENGTH('ṭṛì'), CHAR_LENGTH('ṭṛì');")
    qt_length_155 "SELECT LENGTH('😀😁'), CHAR_LENGTH('😀😁');"
    testFoldConst("SELECT LENGTH('😀😁'), CHAR_LENGTH('😀😁');")
    qt_length_156 "SELECT LENGTH('12345'), CHAR_LENGTH('12345');"
    testFoldConst("SELECT LENGTH('12345'), CHAR_LENGTH('12345');")

    // LOCATE tests
    qt_locate_157 "SELECT LOCATE('bar', 'foobarbar'), LOCATE('xbar', 'foobar'), LOCATE('bar', 'foobarbar', 5);"
    testFoldConst("SELECT LOCATE('bar', 'foobarbar'), LOCATE('xbar', 'foobar'), LOCATE('bar', 'foobarbar', 5);")
    qt_locate_158 "SELECT LOCATE('f', 'foobar'), LOCATE('r', 'foobar');"
    testFoldConst("SELECT LOCATE('f', 'foobar'), LOCATE('r', 'foobar');")
    qt_locate_159 "SELECT LOCATE('xyz', 'foobar'), LOCATE('FOO', 'foobar');"
    testFoldConst("SELECT LOCATE('xyz', 'foobar'), LOCATE('FOO', 'foobar');")
    qt_locate_160 "SELECT LOCATE(NULL, 'foobar'), LOCATE('foo', NULL), LOCATE(NULL, NULL);"
    testFoldConst("SELECT LOCATE(NULL, 'foobar'), LOCATE('foo', NULL), LOCATE(NULL, NULL);")
    qt_locate_161 "SELECT LOCATE('', 'foobar'), LOCATE('foo', ''), LOCATE('', '');"
    testFoldConst("SELECT LOCATE('', 'foobar'), LOCATE('foo', ''), LOCATE('', '');")
    qt_locate_162 "SELECT LOCATE('o', 'foobar', 1), LOCATE('o', 'foobar', 2), LOCATE('o', 'foobar', 4);"
    testFoldConst("SELECT LOCATE('o', 'foobar', 1), LOCATE('o', 'foobar', 2), LOCATE('o', 'foobar', 4);")
    qt_locate_163 "SELECT LOCATE('foo', 'foobar', 0), LOCATE('foo', 'foobar', -1), LOCATE('foo', 'foobar', 10);"
    testFoldConst("SELECT LOCATE('foo', 'foobar', 0), LOCATE('foo', 'foobar', -1), LOCATE('foo', 'foobar', 10);")
    qt_locate_164 "SELECT LOCATE('ṛì', 'ṭṛì ḍḍumai'), LOCATE('ḍḍu', 'ṭṛì ḍḍumai');"
    testFoldConst("SELECT LOCATE('ṛì', 'ṭṛì ḍḍumai'), LOCATE('ḍḍu', 'ṭṛì ḍḍumai');")
    qt_locate_165 "SELECT LOCATE('BAR', 'foobar'), LOCATE('Bar', 'foobar'), LOCATE('bar', 'fooBAR');"
    testFoldConst("SELECT LOCATE('BAR', 'foobar'), LOCATE('Bar', 'foobar'), LOCATE('bar', 'fooBAR');")
    qt_locate_166 "SELECT LOCATE('', 'foobar', 3), LOCATE('', 'foobar', 7), LOCATE('', '', 1);"
    testFoldConst("SELECT LOCATE('', 'foobar', 3), LOCATE('', 'foobar', 7), LOCATE('', '', 1);")

    // LPAD tests
    qt_lpad_167 "SELECT LPAD('hi', 5, 'xy'), LPAD('hello', 8, '*');"
    testFoldConst("SELECT LPAD('hi', 5, 'xy'), LPAD('hello', 8, '*');")
    qt_lpad_168 "SELECT LPAD('hi', 1, 'xy'), LPAD('hello world', 5, 'x');"
    testFoldConst("SELECT LPAD('hi', 1, 'xy'), LPAD('hello world', 5, 'x');")
    qt_lpad_169 "SELECT LPAD(NULL, 5, 'x'), LPAD('hi', NULL, 'x'), LPAD('hi', 5, NULL);"
    testFoldConst("SELECT LPAD(NULL, 5, 'x'), LPAD('hi', NULL, 'x'), LPAD('hi', 5, NULL);")
    qt_lpad_170 "SELECT LPAD('', 0, ''), LPAD('hi', 0, 'x'), LPAD('', 5, '*');"
    testFoldConst("SELECT LPAD('', 0, ''), LPAD('hi', 0, 'x'), LPAD('', 5, '*');")
    qt_lpad_171 "SELECT LPAD('hello', 10, ''), LPAD('hi', 2, '');"
    testFoldConst("SELECT LPAD('hello', 10, ''), LPAD('hi', 2, '');")
    qt_lpad_172 "SELECT LPAD('123', 10, 'abc'), LPAD('X', 7, 'HELLO');"
    testFoldConst("SELECT LPAD('123', 10, 'abc'), LPAD('X', 7, 'HELLO');")
    qt_lpad_173 "SELECT LPAD('hello', 10, 'ṭṛì'), LPAD('ḍḍumai', 3, 'x');"
    testFoldConst("SELECT LPAD('hello', 10, 'ṭṛì'), LPAD('ḍḍumai', 3, 'x');")
    qt_lpad_174 "SELECT LPAD('42', 6, '0'), LPAD('1234', 8, '0');"
    testFoldConst("SELECT LPAD('42', 6, '0'), LPAD('1234', 8, '0');")
    qt_lpad_175 "SELECT LPAD('hello', -1, 'x'), LPAD('test', -5, '*');"
    testFoldConst("SELECT LPAD('hello', -1, 'x'), LPAD('test', -5, '*');")

    // LTRIM tests
    qt_ltrim_176 "SELECT LTRIM(' ab d');"
    testFoldConst("SELECT LTRIM(' ab d');")
    qt_ltrim_177 "SELECT LTRIM('ababccaab', 'ab');"
    testFoldConst("SELECT LTRIM('ababccaab', 'ab');")
    qt_ltrim_178 "SELECT LTRIM(' tn hello world');"
    testFoldConst("SELECT LTRIM(' tn hello world');")
    qt_ltrim_179 "SELECT LTRIM(NULL), LTRIM('test', NULL);"
    testFoldConst("SELECT LTRIM(NULL), LTRIM('test', NULL);")
    qt_ltrim_180 "SELECT LTRIM(''), LTRIM('test', '');"
    testFoldConst("SELECT LTRIM(''), LTRIM('test', '');")
    qt_ltrim_181 "SELECT LTRIM('abcdefg', 'abc'), LTRIM('123456', '12');"
    testFoldConst("SELECT LTRIM('abcdefg', 'abc'), LTRIM('123456', '12');")
    qt_ltrim_182 "SELECT LTRIM('aaaaa', 'a'), LTRIM(' ', ' ');"
    testFoldConst("SELECT LTRIM('aaaaa', 'a'), LTRIM(' ', ' ');")
    qt_ltrim_183 "SELECT LTRIM('ṭṛìṭṛì test', 'ṭṛì'), LTRIM('ḍḍuḍḍu hello', 'ḍu');"
    testFoldConst("SELECT LTRIM('ṭṛìṭṛì test', 'ṭṛì'), LTRIM('ḍḍuḍḍu hello', 'ḍu');")
    qt_ltrim_184 "SELECT LTRIM('000123', '0'), LTRIM('123abc123', '123');"
    testFoldConst("SELECT LTRIM('000123', '0'), LTRIM('123abc123', '123');")
    qt_ltrim_185 "SELECT LTRIM('---text---', '-'), LTRIM('@@hello@@', '@');"
    testFoldConst("SELECT LTRIM('---text---', '-'), LTRIM('@@hello@@', '@');")

    // MAKE_SET tests
    qt_make_set_186 "SELECT make_set(3, 'dog', 'cat', 'bird');"
    testFoldConst("SELECT make_set(3, 'dog', 'cat', 'bird');")
    qt_make_set_187 "SELECT make_set(5, NULL, 'warm', 'hot');"
    testFoldConst("SELECT make_set(5, NULL, 'warm', 'hot');")
    qt_make_set_188 "SELECT make_set(0, 'hello', 'world');"
    testFoldConst("SELECT make_set(0, 'hello', 'world');")
    qt_make_set_189 "SELECT make_set(NULL, 'a', 'b', 'c');"
    testFoldConst("SELECT make_set(NULL, 'a', 'b', 'c');")
    qt_make_set_190 "SELECT make_set(15, 'first', 'second');"
    testFoldConst("SELECT make_set(15, 'first', 'second');")
    qt_make_set_191 "SELECT make_set(7, 'ṭṛì', 'ḍḍumai', 'test');"
    testFoldConst("SELECT make_set(7, 'ṭṛì', 'ḍḍumai', 'test');")

    // MASK_FIRST_N tests
    qt_mask_first_n_192 "SELECT mask_first_n('1234-5678', 4);"
    testFoldConst("SELECT mask_first_n('1234-5678', 4);")
    qt_mask_first_n_193 "SELECT mask_first_n('abc123');"
    testFoldConst("SELECT mask_first_n('abc123');")
    qt_mask_first_n_194 "SELECT mask_first_n('Hello', 100);"
    testFoldConst("SELECT mask_first_n('Hello', 100);")
    qt_mask_first_n_195 "SELECT mask_first_n(NULL, 5);"
    testFoldConst("SELECT mask_first_n(NULL, 5);")
    qt_mask_first_n_196 "SELECT mask_first_n('Hello123', 0);"
    testFoldConst("SELECT mask_first_n('Hello123', 0);")
    qt_mask_first_n_197 "SELECT mask_first_n('Test', 100);"
    testFoldConst("SELECT mask_first_n('Test', 100);")
    qt_mask_first_n_198 "SELECT mask_first_n('user@example.com', 6);"
    testFoldConst("SELECT mask_first_n('user@example.com', 6);")
    qt_mask_first_n_199 "SELECT mask_first_n('13812345678', 3);"
    testFoldConst("SELECT mask_first_n('13812345678', 3);")
    qt_mask_first_n_200 "SELECT mask_first_n('Abc-123-XYZ', 7);"
    testFoldConst("SELECT mask_first_n('Abc-123-XYZ', 7);")
    qt_mask_first_n_201 "SELECT mask_first_n('ṭṛWorld123', 7);"
    testFoldConst("SELECT mask_first_n('ṭṛWorld123', 7);")

    // MASK_LAST_N tests
    qt_mask_last_n_202 "SELECT mask_last_n('1234-5678', 4);"
    testFoldConst("SELECT mask_last_n('1234-5678', 4);")
    qt_mask_last_n_203 "SELECT mask_last_n('abc123');"
    testFoldConst("SELECT mask_last_n('abc123');")
    qt_mask_last_n_204 "SELECT mask_last_n('Hello', 100);"
    testFoldConst("SELECT mask_last_n('Hello', 100);")
    qt_mask_last_n_205 "SELECT mask_last_n(NULL, 5);"
    testFoldConst("SELECT mask_last_n(NULL, 5);")
    qt_mask_last_n_206 "SELECT mask_last_n('Hello123', 0);"
    testFoldConst("SELECT mask_last_n('Hello123', 0);")
    qt_mask_last_n_207 "SELECT mask_last_n('Test', 100);"
    testFoldConst("SELECT mask_last_n('Test', 100);")
    qt_mask_last_n_208 "SELECT mask_last_n('user@example.com', 11);"
    testFoldConst("SELECT mask_last_n('user@example.com', 11);")
    qt_mask_last_n_209 "SELECT mask_last_n('13812345678', 4);"
    testFoldConst("SELECT mask_last_n('13812345678', 4);")
    qt_mask_last_n_210 "SELECT mask_last_n('ABC-123-xyz', 7);"
    testFoldConst("SELECT mask_last_n('ABC-123-xyz', 7);")
    qt_mask_last_n_211 "SELECT mask_last_n('Helloṭṛ123', 9);"
    testFoldConst("SELECT mask_last_n('Helloṭṛ123', 9);")

    // MASK tests
    qt_mask_212 "SELECT mask('abc123XYZ');"
    testFoldConst("SELECT mask('abc123XYZ');")
    qt_mask_213 "SELECT mask('abc123XYZ', '*', '#', '\$');"
    testFoldConst("SELECT mask('abc123XYZ', '*', '#', '\$');")
    qt_mask_214 "SELECT mask('Hello-123!');"
    testFoldConst("SELECT mask('Hello-123!');")
    qt_mask_215 "SELECT mask(NULL);"
    testFoldConst("SELECT mask(NULL);")
    qt_mask_216 "SELECT mask('1234567890');"
    testFoldConst("SELECT mask('1234567890');")
    qt_mask_217 "SELECT mask('AbCdEfGh');"
    testFoldConst("SELECT mask('AbCdEfGh');")
    qt_mask_218 "SELECT mask('');"
    testFoldConst("SELECT mask('');")
    qt_mask_219 "SELECT mask('Test123', 'ABC', 'xyz', '999');"
    testFoldConst("SELECT mask('Test123', 'ABC', 'xyz', '999');")
    qt_mask_220 "SELECT mask('1234-5678-9012-3456');"
    testFoldConst("SELECT mask('1234-5678-9012-3456');")
    qt_mask_221 "SELECT mask('user@example.com');"
    testFoldConst("SELECT mask('user@example.com');")

    // MULTI_SEARCH_ALL_POSITIONS tests
    qt_multi_search_all_positions_222 "SELECT multi_search_all_positions('Hello, World!', ['Hello', 'World']);"
    testFoldConst("SELECT multi_search_all_positions('Hello, World!', ['Hello', 'World']);")
    qt_multi_search_all_positions_223 "SELECT multi_search_all_positions('Hello, World!', ['hello', '!', 'world']);"
    testFoldConst("SELECT multi_search_all_positions('Hello, World!', ['hello', '!', 'world']);")
    qt_multi_search_all_positions_224 "SELECT multi_search_all_positions('Hello, World!', ['Hello', '!', 'xyz']);"
    testFoldConst("SELECT multi_search_all_positions('Hello, World!', ['Hello', '!', 'xyz']);")
    qt_multi_search_all_positions_225 "SELECT multi_search_all_positions('Hello', []);"
    testFoldConst("SELECT multi_search_all_positions('Hello', []);")
    qt_multi_search_all_positions_226 "SELECT multi_search_all_positions('ṭṛì ḍḍumai Hello', ['ṭṛì', 'Hello', 'test']);"
    testFoldConst("SELECT multi_search_all_positions('ṭṛì ḍḍumai Hello', ['ṭṛì', 'Hello', 'test']);")

    // NGRAM_SEARCH tests
    qt_ngram_search_227 "SELECT ngram_search('123456789', '12345', 3);"
    testFoldConst("SELECT ngram_search('123456789', '12345', 3);")
    qt_ngram_search_228 "SELECT ngram_search('abababab', 'babababa', 2);"
    testFoldConst("SELECT ngram_search('abababab', 'babababa', 2);")
    qt_ngram_search_229 "SELECT ngram_search('ab', 'abc', 3);"
    testFoldConst("SELECT ngram_search('ab', 'abc', 3);")
    qt_ngram_search_230 "SELECT ngram_search(NULL, 'test', 2);"
    testFoldConst("SELECT ngram_search(NULL, 'test', 2);")

    // OVERLAY tests
    qt_overlay_231 "SELECT overlay('Quadratic', 3, 4, 'What');"
    testFoldConst("SELECT overlay('Quadratic', 3, 4, 'What');")
    qt_overlay_232 "SELECT overlay('Quadratic', 2, -1, 'Hi');"
    testFoldConst("SELECT overlay('Quadratic', 2, -1, 'Hi');")
    qt_overlay_233 "SELECT overlay('Hello', 10, 2, 'X');"
    testFoldConst("SELECT overlay('Hello', 10, 2, 'X');")
    qt_overlay_234 "SELECT overlay('Hello', NULL, 2, 'X');"
    testFoldConst("SELECT overlay('Hello', NULL, 2, 'X');")

    // PARSE_DATA_SIZE tests
    qt_parse_data_size_235 "SELECT parse_data_size('1024B');"
    testFoldConst("SELECT parse_data_size('1024B');")
    qt_parse_data_size_236 "SELECT parse_data_size('1kB');"
    testFoldConst("SELECT parse_data_size('1kB');")
    qt_parse_data_size_237 "SELECT parse_data_size('2.5MB');"
    testFoldConst("SELECT parse_data_size('2.5MB');")
    qt_parse_data_size_238 "SELECT parse_data_size('1GB');"
    testFoldConst("SELECT parse_data_size('1GB');")
    qt_parse_data_size_239 "SELECT parse_data_size('1TB');"
    testFoldConst("SELECT parse_data_size('1TB');")
    qt_parse_data_size_241 "SELECT parse_data_size(NUll);"
    testFoldConst("SELECT parse_data_size(NUll);")

    // POSITION tests
    qt_position_242 "SELECT POSITION('bar' IN 'foobarbar'), POSITION('bar', 'foobarbar');"
    testFoldConst("SELECT POSITION('bar' IN 'foobarbar'), POSITION('bar', 'foobarbar');")
    qt_position_243 "SELECT POSITION('bar', 'foobarbar', 5), POSITION('xbar', 'foobar');"
    testFoldConst("SELECT POSITION('bar', 'foobarbar', 5), POSITION('xbar', 'foobar');")
    qt_position_244 "SELECT POSITION('test' IN NULL), POSITION(NULL, 'test');"
    testFoldConst("SELECT POSITION('test' IN NULL), POSITION(NULL, 'test');")
    qt_position_245 "SELECT POSITION('' IN 'hello'), POSITION('world' IN '');"
    testFoldConst("SELECT POSITION('' IN 'hello'), POSITION('world' IN '');")
    qt_position_246 "SELECT POSITION('World' IN 'Hello World'), POSITION('world' IN 'Hello World');"
    testFoldConst("SELECT POSITION('World' IN 'Hello World'), POSITION('world' IN 'Hello World');")
    qt_position_247 "SELECT POSITION('a', 'banana', 1), POSITION('a', 'banana', 3);"
    testFoldConst("SELECT POSITION('a', 'banana', 1), POSITION('a', 'banana', 3);")
    qt_position_248 "SELECT POSITION('ḍḍumai' IN 'ṭṛì ḍḍumai hello'), POSITION('hello', 'ṭṛì ḍḍumai hello', 8);"
    testFoldConst("SELECT POSITION('ḍḍumai' IN 'ṭṛì ḍḍumai hello'), POSITION('hello', 'ṭṛì ḍḍumai hello', 8);")
    qt_position_249 "SELECT POSITION('@' IN 'user@domain.com'), POSITION('.', 'user@domain.com', 10);"
    testFoldConst("SELECT POSITION('@' IN 'user@domain.com'), POSITION('.', 'user@domain.com', 10);")
    qt_position_250 "SELECT POSITION('test', 'hello world', 20), POSITION('test', 'hello world', 0);"
    testFoldConst("SELECT POSITION('test', 'hello world', 20), POSITION('test', 'hello world', 0);")
    qt_position_251 "SELECT POSITION('123' IN '456123789'), POSITION('-', 'phone: 123-456-7890', 11);"
    testFoldConst("SELECT POSITION('123' IN '456123789'), POSITION('-', 'phone: 123-456-7890', 11);")

    // QUOTE tests
    qt_quote_252 "SELECT quote('hello');"
    testFoldConst("SELECT quote('hello');")
    qt_quote_253 "SELECT quote('It is a test');"
    testFoldConst("SELECT quote('It is a test');")
    qt_quote_254 "SELECT quote(NULL);"
    testFoldConst("SELECT quote(NULL);")
    qt_quote_255 "SELECT quote('');"
    testFoldConst("SELECT quote('');")
    qt_quote_256 "SELECT quote('aaa\\\\');"
    testFoldConst("SELECT quote('aaa\\\\');")
    qt_quote_257 "SELECT quote('aaacccb');"
    testFoldConst("SELECT quote('aaacccb');")

    // RANDOM_BYTES tests - Skipped (non-deterministic results)

    // REPEAT tests
    qt_repeat_263 "SELECT REPEAT('a', 3);"
    testFoldConst("SELECT REPEAT('a', 3);")
    qt_repeat_264 "SELECT REPEAT('hello', 2);"
    testFoldConst("SELECT REPEAT('hello', 2);")
    qt_repeat_265 "SELECT REPEAT('test', 0);"
    testFoldConst("SELECT REPEAT('test', 0);")
    qt_repeat_266 "SELECT REPEAT('a', -1);"
    testFoldConst("SELECT REPEAT('a', -1);")
    qt_repeat_267 "SELECT REPEAT(NULL, 3), REPEAT('a', NULL);"
    testFoldConst("SELECT REPEAT(NULL, 3), REPEAT('a', NULL);")
    qt_repeat_268 "SELECT REPEAT('', 5);"
    testFoldConst("SELECT REPEAT('', 5);")
    qt_repeat_269 "SELECT REPEAT('-', 10), REPEAT('*', 5);"
    testFoldConst("SELECT REPEAT('-', 10), REPEAT('*', 5);")
    qt_repeat_270 "SELECT REPEAT('ṭṛì', 3), REPEAT('ḍḍu', 2);"
    testFoldConst("SELECT REPEAT('ṭṛì', 3), REPEAT('ḍḍu', 2);")
    qt_repeat_271 "SELECT REPEAT('123', 3), REPEAT('@#', 4);"
    testFoldConst("SELECT REPEAT('123', 3), REPEAT('@#', 4);")
    qt_repeat_272 "SELECT REPEAT('ṭṛìṭṛì', 3);"
    testFoldConst("SELECT REPEAT('ṭṛìṭṛì', 3);")

    // REPLACE_EMPTY tests
    qt_replace_empty_273 "SELECT replace_empty('abc', '', 'x');"
    testFoldConst("SELECT replace_empty('abc', '', 'x');")
    qt_replace_empty_274 "SELECT replace_empty('hello', 'l', 'L');"
    testFoldConst("SELECT replace_empty('hello', 'l', 'L');")
    qt_replace_empty_275 "SELECT replace_empty('', '', 'x');"
    testFoldConst("SELECT replace_empty('', '', 'x');")
    qt_replace_empty_276 "SELECT replace_empty(NULL, 'old', 'new');"
    testFoldConst("SELECT replace_empty(NULL, 'old', 'new');")
    qt_replace_empty_277 "SELECT replace_empty('hello', 'l', 'ṭṛìṭ');"
    testFoldConst("SELECT replace_empty('hello', 'l', 'ṭṛìṭ');")

    // REPLACE tests
    qt_replace_278 "SELECT REPLACE('hello world', 'world', 'universe');"
    testFoldConst("SELECT REPLACE('hello world', 'world', 'universe');")
    qt_replace_279 "SELECT REPLACE('apple apple apple', 'apple', 'orange');"
    testFoldConst("SELECT REPLACE('apple apple apple', 'apple', 'orange');")
    qt_replace_280 "SELECT REPLACE('banana', 'a', '');"
    testFoldConst("SELECT REPLACE('banana', 'a', '');")
    qt_replace_281 "SELECT REPLACE(NULL, 'old', 'new'), REPLACE('test', NULL, 'new'), REPLACE('test', 'old', NULL);"
    testFoldConst("SELECT REPLACE(NULL, 'old', 'new'), REPLACE('test', NULL, 'new'), REPLACE('test', 'old', NULL);")
    qt_replace_282 "SELECT REPLACE('', 'old', 'new'), REPLACE('test', '', 'new'), REPLACE('test', 'old', '');"
    testFoldConst("SELECT REPLACE('', 'old', 'new'), REPLACE('test', '', 'new'), REPLACE('test', 'old', '');")
    qt_replace_283 "SELECT REPLACE('Hello HELLO hello', 'hello', 'hi');"
    testFoldConst("SELECT REPLACE('Hello HELLO hello', 'hello', 'hi');")
    qt_replace_284 "SELECT REPLACE('hello world', 'xyz', 'abc');"
    testFoldConst("SELECT REPLACE('hello world', 'xyz', 'abc');")
    qt_replace_285 "SELECT REPLACE('ṭṛì ḍḍumai test ṭṛì ḍḍumannàri', 'ṭṛì', 'replaced');"
    testFoldConst("SELECT REPLACE('ṭṛì ḍḍumai test ṭṛì ḍḍumannàri', 'ṭṛì', 'replaced');")
    qt_replace_286 "SELECT REPLACE('123123123', '123', 'ABC');"
    testFoldConst("SELECT REPLACE('123123123', '123', 'ABC');")

    // REVERSE tests
    qt_reverse_287 "SELECT REVERSE('hello');"
    testFoldConst("SELECT REVERSE('hello');")
    qt_reverse_288 "SELECT REVERSE(['hello', 'world']);"
    testFoldConst("SELECT REVERSE(['hello', 'world']);")
    qt_reverse_289 "SELECT REVERSE(NULL);"
    testFoldConst("SELECT REVERSE(NULL);")
    qt_reverse_290 "SELECT REVERSE(''), REVERSE([]);"
    testFoldConst("SELECT REVERSE(''), REVERSE([]);")
    qt_reverse_291 "SELECT REVERSE('A'), REVERSE(['single']);"
    testFoldConst("SELECT REVERSE('A'), REVERSE(['single']);")
    qt_reverse_292 "SELECT REVERSE('12345'), REVERSE('!@#\$%');"
    testFoldConst("SELECT REVERSE('12345'), REVERSE('!@#\$%');")
    qt_reverse_293 "SELECT REVERSE('ṭṛì ḍḍumai'), REVERSE('ḍḍumannàri');"
    testFoldConst("SELECT REVERSE('ṭṛì ḍḍumai'), REVERSE('ḍḍumannàri');")
    qt_reverse_294 "SELECT REVERSE('Hello123'), REVERSE('test@email.com');"
    testFoldConst("SELECT REVERSE('Hello123'), REVERSE('test@email.com');")
    qt_reverse_295 "SELECT REVERSE([1, 2, 3, 4, 5]), REVERSE(['a', 'b', 'c']);"
    testFoldConst("SELECT REVERSE([1, 2, 3, 4, 5]), REVERSE(['a', 'b', 'c']);")
    qt_reverse_296 "SELECT REVERSE('level'), REVERSE('12321');"
    testFoldConst("SELECT REVERSE('level'), REVERSE('12321');")

    // RPAD tests
    qt_rpad_297 "SELECT RPAD('hi', 5, 'xy'), RPAD('hello', 8, '*');"
    testFoldConst("SELECT RPAD('hi', 5, 'xy'), RPAD('hello', 8, '*');")
    qt_rpad_298 "SELECT RPAD('hello', 1, ''), RPAD('hello world', 5, 'x');"
    testFoldConst("SELECT RPAD('hello', 1, ''), RPAD('hello world', 5, 'x');")
    qt_rpad_299 "SELECT RPAD(NULL, 5, 'x'), RPAD('hi', NULL, 'x'), RPAD('hi', 5, NULL);"
    testFoldConst("SELECT RPAD(NULL, 5, 'x'), RPAD('hi', NULL, 'x'), RPAD('hi', 5, NULL);")
    qt_rpad_300 "SELECT RPAD('', 0, ''), RPAD('hi', 0, 'x'), RPAD('', 5, '*');"
    testFoldConst("SELECT RPAD('', 0, ''), RPAD('hi', 0, 'x'), RPAD('', 5, '*');")
    qt_rpad_301 "SELECT RPAD('hello', 10, ''), RPAD('hi', 2, '');"
    testFoldConst("SELECT RPAD('hello', 10, ''), RPAD('hi', 2, '');")
    qt_rpad_302 "SELECT RPAD('hello', 10, 'world'), RPAD('X', 7, 'ABC');"
    testFoldConst("SELECT RPAD('hello', 10, 'world'), RPAD('X', 7, 'ABC');")
    qt_rpad_303 "SELECT RPAD('hello', 10, 'ṭṛì'), RPAD('ḍḍumai', 3, 'x');"
    testFoldConst("SELECT RPAD('hello', 10, 'ṭṛì'), RPAD('ḍḍumai', 3, 'x');")
    qt_rpad_304 "SELECT RPAD('\$99', 8, '.'), RPAD('Item1', 10, ' ');"
    testFoldConst("SELECT RPAD('\$99', 8, '.'), RPAD('Item1', 10, ' ');")
    qt_rpad_305 "SELECT RPAD('Name', 15, ' '), RPAD('Price', 10, ' ');"
    testFoldConst("SELECT RPAD('Name', 15, ' '), RPAD('Price', 10, ' ');")
    qt_rpad_306 "SELECT RPAD('hello', -1, 'x'), RPAD('test', -5, '*');"
    testFoldConst("SELECT RPAD('hello', -1, 'x'), RPAD('test', -5, '*');")

    // RTRIM_IN tests
    qt_rtrim_in_307 "SELECT rtrim_in('ab d ') str;"
    testFoldConst("SELECT rtrim_in('ab d ') str;")
    qt_rtrim_in_308 "SELECT rtrim_in('ababccaab', 'ab') str;"
    testFoldConst("SELECT rtrim_in('ababccaab', 'ab') str;")
    qt_rtrim_in_309 "SELECT rtrim_in('ababccaab', 'ab'), rtrim('ababccaab', 'ab');"
    testFoldConst("SELECT rtrim_in('ababccaab', 'ab'), rtrim('ababccaab', 'ab');")
    qt_rtrim_in_310 "SELECT rtrim_in('Helloabc', 'cba');"
    testFoldConst("SELECT rtrim_in('Helloabc', 'cba');")
    qt_rtrim_in_311 "SELECT rtrim_in('ṭṛì ḍḍumai+++', '+');"
    testFoldConst("SELECT rtrim_in('ṭṛì ḍḍumai+++', '+');")
    qt_rtrim_in_312 "SELECT rtrim_in(NULL, 'abc');"
    testFoldConst("SELECT rtrim_in(NULL, 'abc');")
    qt_rtrim_in_313 "SELECT rtrim_in('', 'abc'),rtrim_in('abc', '');"
    testFoldConst("SELECT rtrim_in('', 'abc'),rtrim_in('abc', '');")

    // RTRIM tests
    qt_rtrim_314 "SELECT RTRIM('ab d ');"
    testFoldConst("SELECT RTRIM('ab d ');")
    qt_rtrim_315 "SELECT RTRIM('ababccaab', 'ab');"
    testFoldConst("SELECT RTRIM('ababccaab', 'ab');")
    qt_rtrim_316 "SELECT RTRIM('hello world tn ');"
    testFoldConst("SELECT RTRIM('hello world tn ');")
    qt_rtrim_317 "SELECT RTRIM(NULL), RTRIM('test', NULL);"
    testFoldConst("SELECT RTRIM(NULL), RTRIM('test', NULL);")
    qt_rtrim_318 "SELECT RTRIM(''), RTRIM('test', '');"
    testFoldConst("SELECT RTRIM(''), RTRIM('test', '');")
    qt_rtrim_319 "SELECT RTRIM('abcdefg', 'efg'), RTRIM('123456', '56');"
    testFoldConst("SELECT RTRIM('abcdefg', 'efg'), RTRIM('123456', '56');")
    qt_rtrim_320 "SELECT RTRIM('aaaaa', 'a'), RTRIM(' ', ' ');"
    testFoldConst("SELECT RTRIM('aaaaa', 'a'), RTRIM(' ', ' ');")
    qt_rtrim_321 "SELECT RTRIM('test ṭṛìṭṛì', 'ṭṛì'), RTRIM('hello ḍḍuḍḍ', 'ḍ');"
    testFoldConst("SELECT RTRIM('test ṭṛìṭṛì', 'ṭṛì'), RTRIM('hello ḍḍuḍḍ', 'ḍ');")
    qt_rtrim_322 "SELECT RTRIM('123000', '0'), RTRIM('123abc123', '123');"
    testFoldConst("SELECT RTRIM('123000', '0'), RTRIM('123abc123', '123');")
    qt_rtrim_323 "SELECT RTRIM('---text---', '-'), RTRIM('@@hello@@', '@');"
    testFoldConst("SELECT RTRIM('---text---', '-'), RTRIM('@@hello@@', '@');")

    // SOUNDEX tests
    qt_soundex_324 "SELECT soundex('Doris');"
    testFoldConst("SELECT soundex('Doris');")
    qt_soundex_325 "SELECT soundex('Smith'), soundex('Smyth');"
    testFoldConst("SELECT soundex('Smith'), soundex('Smyth');")
    qt_soundex_326 "SELECT soundex('');"
    testFoldConst("SELECT soundex('');")
    qt_soundex_327 "SELECT soundex(NULL);"
    testFoldConst("SELECT soundex(NULL);")
    qt_soundex_328 "SELECT soundex('');"
    testFoldConst("SELECT soundex('');")
    qt_soundex_329 "SELECT soundex('123@*%');"
    testFoldConst("SELECT soundex('123@*%');")
    qt_soundex_330 "SELECT soundex('R@b-e123rt'), soundex('Robert');"
    testFoldConst("SELECT soundex('R@b-e123rt'), soundex('Robert');")
    // SOUNDEX tests with non-ASCII characters - Skipped (not supported)

    // SPACE tests
    qt_space_333 "SELECT space(5);"
    testFoldConst("SELECT space(5);")
    qt_space_334 "SELECT space(0), space(-5);"
    testFoldConst("SELECT space(0), space(-5);")
    qt_space_335 "SELECT space(NULL);"
    testFoldConst("SELECT space(NULL);")
    qt_concat_336 "SELECT CONCAT('Hello', space(3), 'World');"
    testFoldConst("SELECT CONCAT('Hello', space(3), 'World');")

    // SPLIT_BY_REGEXP tests
    qt_split_by_regexp_337 "SELECT SPLIT_BY_REGEXP('abcde', '');"
    testFoldConst("SELECT SPLIT_BY_REGEXP('abcde', '');")
    qt_split_by_regexp_338 "SELECT SPLIT_BY_REGEXP('a12bc23de345f', '\\\\d+');"
    testFoldConst("SELECT SPLIT_BY_REGEXP('a12bc23de345f', '\\\\d+');")
    qt_split_by_regexp_339 "SELECT SPLIT_BY_REGEXP(NULL, '\\\\d+'), SPLIT_BY_REGEXP('test', NULL);"
    testFoldConst("SELECT SPLIT_BY_REGEXP(NULL, '\\\\d+'), SPLIT_BY_REGEXP('test', NULL);")
    qt_split_by_regexp_340 "SELECT SPLIT_BY_REGEXP('', ','), SPLIT_BY_REGEXP('hello', 'xyz');"
    testFoldConst("SELECT SPLIT_BY_REGEXP('', ','), SPLIT_BY_REGEXP('hello', 'xyz');")
    qt_split_by_regexp_341 "SELECT SPLIT_BY_REGEXP('a,b,c,d,e', ',', 3), SPLIT_BY_REGEXP('1-2-3-4-5', '-', 2);"
    testFoldConst("SELECT SPLIT_BY_REGEXP('a,b,c,d,e', ',', 3), SPLIT_BY_REGEXP('1-2-3-4-5', '-', 2);")
    qt_split_by_regexp_342 "SELECT SPLIT_BY_REGEXP('hello world test', '\\\\s+'), SPLIT_BY_REGEXP('atbncrd', '\\\\s');"
    testFoldConst("SELECT SPLIT_BY_REGEXP('hello world test', '\\\\s+'), SPLIT_BY_REGEXP('atbncrd', '\\\\s');")
    qt_split_by_regexp_343 "SELECT SPLIT_BY_REGEXP('a.b.c.d', '\\\\.'), SPLIT_BY_REGEXP('x(y)z[w]', '[\\\\(\\\\)\\\\[\\\\]]');"
    testFoldConst("SELECT SPLIT_BY_REGEXP('a.b.c.d', '\\\\.'), SPLIT_BY_REGEXP('x(y)z[w]', '[\\\\(\\\\)\\\\[\\\\]]');")
    qt_split_by_regexp_344 "SELECT SPLIT_BY_REGEXP('TheQuickBrownFox', '[A-Z]'), SPLIT_BY_REGEXP('user@example.com', '@|\\\\.');"
    testFoldConst("SELECT SPLIT_BY_REGEXP('TheQuickBrownFox', '[A-Z]'), SPLIT_BY_REGEXP('user@example.com', '@|\\\\.');")
    qt_split_by_regexp_345 "SELECT SPLIT_BY_REGEXP('ṭṛì→ḍḍumai→hello', '→'), SPLIT_BY_REGEXP('αβγδε', '[βδ]');"
    testFoldConst("SELECT SPLIT_BY_REGEXP('ṭṛì→ḍḍumai→hello', '→'), SPLIT_BY_REGEXP('αβγδε', '[βδ]');")
    qt_split_by_regexp_346 "SELECT SPLIT_BY_REGEXP('a,,b,c', ','), SPLIT_BY_REGEXP('123abc456def', '[a-z]+');"
    testFoldConst("SELECT SPLIT_BY_REGEXP('a,,b,c', ','), SPLIT_BY_REGEXP('123abc456def', '[a-z]+');")

    // SPLIT_BY_STRING tests
    qt_split_by_string_347 "SELECT SPLIT_BY_STRING('hello', 'l');"
    testFoldConst("SELECT SPLIT_BY_STRING('hello', 'l');")
    qt_split_by_string_348 "SELECT SPLIT_BY_STRING('hello', '');"
    testFoldConst("SELECT SPLIT_BY_STRING('hello', '');")
    qt_split_by_string_349 "SELECT SPLIT_BY_STRING('apple::banana::cherry', '::');"
    testFoldConst("SELECT SPLIT_BY_STRING('apple::banana::cherry', '::');")
    qt_split_by_string_350 "SELECT SPLIT_BY_STRING(NULL, ','), SPLIT_BY_STRING('hello', NULL);"
    testFoldConst("SELECT SPLIT_BY_STRING(NULL, ','), SPLIT_BY_STRING('hello', NULL);")
    qt_split_by_string_351 "SELECT SPLIT_BY_STRING('', ','), SPLIT_BY_STRING('hello', 'xyz');"
    testFoldConst("SELECT SPLIT_BY_STRING('', ','), SPLIT_BY_STRING('hello', 'xyz');")
    qt_split_by_string_352 "SELECT SPLIT_BY_STRING('a,,b,c', ',');"
    testFoldConst("SELECT SPLIT_BY_STRING('a,,b,c', ',');")
    qt_split_by_string_353 "SELECT SPLIT_BY_STRING(',a,b,', ',');"
    testFoldConst("SELECT SPLIT_BY_STRING(',a,b,', ',');")
    qt_split_by_string_354 "SELECT SPLIT_BY_STRING('|||', '|');"
    testFoldConst("SELECT SPLIT_BY_STRING('|||', '|');")
    qt_split_by_string_355 "SELECT SPLIT_BY_STRING('ṭṛì ḍḍumai ṭṛì', ' ');"
    testFoldConst("SELECT SPLIT_BY_STRING('ṭṛì ḍḍumai ṭṛì', ' ');")
    qt_split_by_string_356 "SELECT SPLIT_BY_STRING('hello world', 'xyz');"
    testFoldConst("SELECT SPLIT_BY_STRING('hello world', 'xyz');")

    // SPLIT_PART tests
    qt_split_part_357 "SELECT SPLIT_PART('hello world', ' ', 1);"
    testFoldConst("SELECT SPLIT_PART('hello world', ' ', 1);")
    qt_split_part_358 "SELECT SPLIT_PART('apple,banana,cherry', ',', 2);"
    testFoldConst("SELECT SPLIT_PART('apple,banana,cherry', ',', 2);")
    qt_split_part_359 "SELECT SPLIT_PART('apple,banana,cherry', ',', 0);"
    testFoldConst("SELECT SPLIT_PART('apple,banana,cherry', ',', 0);")
    qt_split_part_360 "SELECT SPLIT_PART('apple,banana,cherry', ',', -1), SPLIT_PART('apple,banana,cherry', ',', -2);"
    testFoldConst("SELECT SPLIT_PART('apple,banana,cherry', ',', -1), SPLIT_PART('apple,banana,cherry', ',', -2);")
    qt_split_part_361 "SELECT SPLIT_PART('apple,banana', ',', 5), SPLIT_PART('apple,banana', ',', -5);"
    testFoldConst("SELECT SPLIT_PART('apple,banana', ',', 5), SPLIT_PART('apple,banana', ',', -5);")
    qt_split_part_362 "SELECT SPLIT_PART(NULL, ',', 1), SPLIT_PART('test', NULL, 1), SPLIT_PART('test', ',', NULL);"
    testFoldConst("SELECT SPLIT_PART(NULL, ',', 1), SPLIT_PART('test', NULL, 1), SPLIT_PART('test', ',', NULL);")
    qt_split_part_363 "SELECT SPLIT_PART('', ',', 1), SPLIT_PART('test', '', 2);"
    testFoldConst("SELECT SPLIT_PART('', ',', 1), SPLIT_PART('test', '', 2);")
    qt_split_part_364 "SELECT SPLIT_PART('hello world', '|', 1), SPLIT_PART('hello world', '|', 2);"
    testFoldConst("SELECT SPLIT_PART('hello world', '|', 1), SPLIT_PART('hello world', '|', 2);")
    qt_split_part_365 "SELECT SPLIT_PART('a,,c', ',', 1), SPLIT_PART('a,,c', ',', 2), SPLIT_PART('a,,c', ',', 3);"
    testFoldConst("SELECT SPLIT_PART('a,,c', ',', 1), SPLIT_PART('a,,c', ',', 2), SPLIT_PART('a,,c', ',', 3);")
    qt_split_part_366 "SELECT SPLIT_PART('ṭṛì ḍḍumai ṭṛì', ' ', 2);"
    testFoldConst("SELECT SPLIT_PART('ṭṛì ḍḍumai ṭṛì', ' ', 2);")

    // STARTS_WITH tests
    qt_starts_with_367 "SELECT STARTS_WITH('hello world', 'hello'), STARTS_WITH('hello world', 'world');"
    testFoldConst("SELECT STARTS_WITH('hello world', 'hello'), STARTS_WITH('hello world', 'world');")
    qt_starts_with_368 "SELECT STARTS_WITH('Hello World', 'hello'), STARTS_WITH('Hello World', 'Hello');"
    testFoldConst("SELECT STARTS_WITH('Hello World', 'hello'), STARTS_WITH('Hello World', 'Hello');")
    qt_starts_with_369 "SELECT STARTS_WITH(NULL, 'test'), STARTS_WITH('test', NULL);"
    testFoldConst("SELECT STARTS_WITH(NULL, 'test'), STARTS_WITH('test', NULL);")
    qt_starts_with_370 "SELECT STARTS_WITH('hello', ''), STARTS_WITH('', 'world');"
    testFoldConst("SELECT STARTS_WITH('hello', ''), STARTS_WITH('', 'world');")
    qt_starts_with_371 "SELECT STARTS_WITH('test', 'test'), STARTS_WITH('test', 'testing');"
    testFoldConst("SELECT STARTS_WITH('test', 'test'), STARTS_WITH('test', 'testing');")
    qt_starts_with_372 "SELECT STARTS_WITH('/home/user/file.txt', '/home'), STARTS_WITH('C:\\\\Windows\\\\file.txt', 'C:\\\\');"
    testFoldConst("SELECT STARTS_WITH('/home/user/file.txt', '/home'), STARTS_WITH('C:\\\\Windows\\\\file.txt', 'C:\\\\');")
    qt_starts_with_373 "SELECT STARTS_WITH('ṭṛì ḍḍumai hello', 'ṭṛì'), STARTS_WITH('ṭṛì ḍḍumai hello', 'ḍḍumai');"
    testFoldConst("SELECT STARTS_WITH('ṭṛì ḍḍumai hello', 'ṭṛì'), STARTS_WITH('ṭṛì ḍḍumai hello', 'ḍḍumai');")
    qt_starts_with_374 "SELECT STARTS_WITH('https://example.com', 'https://'), STARTS_WITH('ftp://server.com', 'http://');"
    testFoldConst("SELECT STARTS_WITH('https://example.com', 'https://'), STARTS_WITH('ftp://server.com', 'http://');")
    qt_starts_with_375 "SELECT STARTS_WITH('123456789', '123'), STARTS_WITH('987654321', '123');"
    testFoldConst("SELECT STARTS_WITH('123456789', '123'), STARTS_WITH('987654321', '123');")
    qt_starts_with_376 "SELECT STARTS_WITH('@username', '@'), STARTS_WITH('#hashtag', '#');"
    testFoldConst("SELECT STARTS_WITH('@username', '@'), STARTS_WITH('#hashtag', '#');")

    // STRLEFT tests
    qt_strleft_377 "SELECT STRLEFT('Hello doris', 5), LEFT('Hello doris', 5);"
    testFoldConst("SELECT STRLEFT('Hello doris', 5), LEFT('Hello doris', 5);")
    qt_strleft_378 "SELECT STRLEFT('Hello World', 3), STRLEFT('Hello World', 8);"
    testFoldConst("SELECT STRLEFT('Hello World', 3), STRLEFT('Hello World', 8);")
    qt_strleft_379 "SELECT STRLEFT(NULL, 5), STRLEFT('Hello doris', NULL);"
    testFoldConst("SELECT STRLEFT(NULL, 5), STRLEFT('Hello doris', NULL);")
    qt_strleft_380 "SELECT STRLEFT('', 5), STRLEFT('Hello World', 0);"
    testFoldConst("SELECT STRLEFT('', 5), STRLEFT('Hello World', 0);")
    qt_strleft_381 "SELECT STRLEFT('Hello doris', -5), STRLEFT('Hello doris', -1);"
    testFoldConst("SELECT STRLEFT('Hello doris', -5), STRLEFT('Hello doris', -1);")
    qt_strleft_382 "SELECT STRLEFT('ABC', 10), STRLEFT('short', 20);"
    testFoldConst("SELECT STRLEFT('ABC', 10), STRLEFT('short', 20);")
    qt_strleft_383 "SELECT STRLEFT('ṭṛì ḍḍumai hello', 3), STRLEFT('ṭṛì ḍḍumai hello', 7);"
    testFoldConst("SELECT STRLEFT('ṭṛì ḍḍumai hello', 3), STRLEFT('ṭṛì ḍḍumai hello', 7);")
    qt_strleft_384 "SELECT STRLEFT('ID123456789', 5), STRLEFT('USER_987654321', 5);"
    testFoldConst("SELECT STRLEFT('ID123456789', 5), STRLEFT('USER_987654321', 5);")
    qt_strleft_385 "SELECT STRLEFT('ṭṛì ḍḍu', 5);"
    testFoldConst("SELECT STRLEFT('ṭṛì ḍḍu', 5);")

    // STRRIGHT tests
    qt_strright_386 "SELECT STRRIGHT('Hello doris', 5), RIGHT('Hello doris', 5);"
    testFoldConst("SELECT STRRIGHT('Hello doris', 5), RIGHT('Hello doris', 5);")
    qt_strright_387 "SELECT STRRIGHT('Hello World', 3), STRRIGHT('Hello World', 8);"
    testFoldConst("SELECT STRRIGHT('Hello World', 3), STRRIGHT('Hello World', 8);")
    qt_strright_388 "SELECT STRRIGHT(NULL, 5), STRRIGHT('Hello doris', NULL);"
    testFoldConst("SELECT STRRIGHT(NULL, 5), STRRIGHT('Hello doris', NULL);")
    qt_strright_389 "SELECT STRRIGHT('', 5), STRRIGHT('Hello World', 0);"
    testFoldConst("SELECT STRRIGHT('', 5), STRRIGHT('Hello World', 0);")
    qt_strright_390 "SELECT STRRIGHT('Hello doris', -7), STRRIGHT('Hello doris', -5);"
    testFoldConst("SELECT STRRIGHT('Hello doris', -7), STRRIGHT('Hello doris', -5);")
    qt_strright_391 "SELECT STRRIGHT('ABC', 10), STRRIGHT('short', 20);"
    testFoldConst("SELECT STRRIGHT('ABC', 10), STRRIGHT('short', 20);")
    qt_strright_392 "SELECT STRRIGHT('ṭṛì ḍḍumai hello', 5), STRRIGHT('ṭṛì ḍḍumai hello', 11);"
    testFoldConst("SELECT STRRIGHT('ṭṛì ḍḍumai hello', 5), STRRIGHT('ṭṛì ḍḍumai hello', 11);")
    qt_strright_393 "SELECT STRRIGHT('123456789', 3), STRRIGHT('ID_987654321', 6);"
    testFoldConst("SELECT STRRIGHT('123456789', 3), STRRIGHT('ID_987654321', 6);")
    qt_strright_394 "SELECT STRRIGHT('user@example.com', 11), STRRIGHT('admin@company.org.cn', 14);"
    testFoldConst("SELECT STRRIGHT('user@example.com', 11), STRRIGHT('admin@company.org.cn', 14);")

    // SUB_REPLACE tests
    qt_sub_replace_395 "SELECT sub_replace('doris', '***', 1, 2);"
    testFoldConst("SELECT sub_replace('doris', '***', 1, 2);")
    qt_sub_replace_396 "SELECT sub_replace('hello', 'Hi', 0);"
    testFoldConst("SELECT sub_replace('hello', 'Hi', 0);")
    qt_sub_replace_397 "SELECT sub_replace('hello', 'Hi', -1, 2);"
    testFoldConst("SELECT sub_replace('hello', 'Hi', -1, 2);")
    qt_sub_replace_398 "SELECT sub_replace(NULL, 'new', 0, 3);"
    testFoldConst("SELECT sub_replace(NULL, 'new', 0, 3);")
    qt_sub_replace_399 "SELECT sub_replace('doris', 'ṛìḍḍ', 1, 2);"
    testFoldConst("SELECT sub_replace('doris', 'ṛìḍḍ', 1, 2);")
    qt_sub_replace_400 "SELECT sub_replace('hello', 'Hi', 1, 9);"
    testFoldConst("SELECT sub_replace('hello', 'Hi', 1, 9);")

    // SUBSTRING_INDEX tests
    qt_substring_index_401 "SELECT SUBSTRING_INDEX('hello world', ' ', 1), SUBSTRING_INDEX('one,two,three', ',', 2);"
    testFoldConst("SELECT SUBSTRING_INDEX('hello world', ' ', 1), SUBSTRING_INDEX('one,two,three', ',', 2);")
    qt_substring_index_402 "SELECT SUBSTRING_INDEX('hello world', ' ', -1), SUBSTRING_INDEX('one,two,three', ',', -1);"
    testFoldConst("SELECT SUBSTRING_INDEX('hello world', ' ', -1), SUBSTRING_INDEX('one,two,three', ',', -1);")
    qt_substring_index_403 "SELECT SUBSTRING_INDEX(NULL, ',', 1), SUBSTRING_INDEX('test', NULL, 1);"
    testFoldConst("SELECT SUBSTRING_INDEX(NULL, ',', 1), SUBSTRING_INDEX('test', NULL, 1);")
    qt_substring_index_404 "SELECT SUBSTRING_INDEX('hello world', ' ', 0), SUBSTRING_INDEX('a,b,c', ',', 0);"
    testFoldConst("SELECT SUBSTRING_INDEX('hello world', ' ', 0), SUBSTRING_INDEX('a,b,c', ',', 0);")
    qt_substring_index_405 "SELECT SUBSTRING_INDEX('hello world', ',', 1), SUBSTRING_INDEX('no-delimiter', '|', -1);"
    testFoldConst("SELECT SUBSTRING_INDEX('hello world', ',', 1), SUBSTRING_INDEX('no-delimiter', '|', -1);")
    qt_substring_index_406 "SELECT SUBSTRING_INDEX('a,b,c', ',', 5), SUBSTRING_INDEX('a,b,c', ',', -5);"
    testFoldConst("SELECT SUBSTRING_INDEX('a,b,c', ',', 5), SUBSTRING_INDEX('a,b,c', ',', -5);")
    qt_substring_index_407 "SELECT SUBSTRING_INDEX('ṭṛì→ḍḍumai→hello', '→', 1), SUBSTRING_INDEX('ṭṛì→ḍḍumai→hello', '→', -1);"
    testFoldConst("SELECT SUBSTRING_INDEX('ṭṛì→ḍḍumai→hello', '→', 1), SUBSTRING_INDEX('ṭṛì→ḍḍumai→hello', '→', -1);")
    qt_substring_index_408 "SELECT SUBSTRING_INDEX('data::field::value', '::', 2), SUBSTRING_INDEX('data::field::value', '::', -1);"
    testFoldConst("SELECT SUBSTRING_INDEX('data::field::value', '::', 2), SUBSTRING_INDEX('data::field::value', '::', -1);")
    qt_substring_index_409 "SELECT SUBSTRING_INDEX('', ' ', 1);"
    testFoldConst("SELECT SUBSTRING_INDEX('', ' ', 1);")

    // SUBSTRING tests
    qt_substring_410 "SELECT substring('abc1', 2);"
    testFoldConst("SELECT substring('abc1', 2);")
    qt_substring_411 "SELECT substring('abc1', -2);"
    testFoldConst("SELECT substring('abc1', -2);")
    qt_substring_412 "SELECT substring('abc1', 0);"
    testFoldConst("SELECT substring('abc1', 0);")
    qt_substring_413 "SELECT substring('abc1', 5);"
    testFoldConst("SELECT substring('abc1', 5);")
    qt_substring_414 "SELECT substring('abc1def', 2, 2);"
    testFoldConst("SELECT substring('abc1def', 2, 2);")
    qt_substring_415 "SELECT substring('foobarbar' FROM 4 FOR 3);"
    testFoldConst("SELECT substring('foobarbar' FROM 4 FOR 3);")
    qt_substring_416 "SELECT substring('foobarbar' FROM 4);"
    testFoldConst("SELECT substring('foobarbar' FROM 4);")

    // MID tests
    qt_mid_417 "SELECT MID(NULL, 2);"
    testFoldConst("SELECT MID(NULL, 2);")

    // SUBSTRING tests
    qt_substring_418 "SELECT substring('ṭṛì ḍḍumai test', 5, 7);"
    testFoldConst("SELECT substring('ṭṛì ḍḍumai test', 5, 7);")
    qt_substring_419 "SELECT substring(NULL, 1, 3);"
    testFoldConst("SELECT substring(NULL, 1, 3);")
    qt_substring_420 "SELECT substring('', 1, 3);"
    testFoldConst("SELECT substring('', 1, 3);")

    // TO_BASE64 tests
    qt_test_421 "SELECT TO_BASE64('1'), TO_BASE64('A');"
    testFoldConst("SELECT TO_BASE64('1'), TO_BASE64('A');")
    qt_test_422 "SELECT TO_BASE64('234'), TO_BASE64('Hello');"
    testFoldConst("SELECT TO_BASE64('234'), TO_BASE64('Hello');")
    qt_test_423 "SELECT TO_BASE64(NULL), TO_BASE64('');"
    testFoldConst("SELECT TO_BASE64(NULL), TO_BASE64('');")
    qt_test_424 "SELECT TO_BASE64('Hello World'), TO_BASE64('The quick brown fox');"
    testFoldConst("SELECT TO_BASE64('Hello World'), TO_BASE64('The quick brown fox');")
    qt_test_425 "SELECT TO_BASE64('123456'), TO_BASE64('!@#\$%^&*()');"
    testFoldConst("SELECT TO_BASE64('123456'), TO_BASE64('!@#\$%^&*()');")
    qt_test_426 "SELECT TO_BASE64('ṭṛì'), TO_BASE64('ḍḍumai hello');"
    testFoldConst("SELECT TO_BASE64('ṭṛì'), TO_BASE64('ḍḍumai hello');")
    qt_test_427 "SELECT TO_BASE64('user@example.com'), TO_BASE64('admin.test@company.org');"
    testFoldConst("SELECT TO_BASE64('user@example.com'), TO_BASE64('admin.test@company.org');")
    qt_test_428 "SELECT TO_BASE64('{\"name\":\"John\",\"age\":30}'), TO_BASE64('[1,2,3,4,5]');"
    testFoldConst("SELECT TO_BASE64('{\"name\":\"John\",\"age\":30}'), TO_BASE64('[1,2,3,4,5]');")
    qt_test_429 "SELECT TO_BASE64('a'), TO_BASE64('ab'), TO_BASE64('abc');"
    testFoldConst("SELECT TO_BASE64('a'), TO_BASE64('ab'), TO_BASE64('abc');")

    // TRANSLATE tests
    qt_translate_430 "SELECT TRANSLATE('abcd', 'a', 'z');"
    testFoldConst("SELECT TRANSLATE('abcd', 'a', 'z');")
    qt_translate_431 "SELECT TRANSLATE('abcd', 'ac', 'zx');"
    testFoldConst("SELECT TRANSLATE('abcd', 'ac', 'zx');")
    qt_translate_432 "SELECT TRANSLATE('abacad', 'aac', 'zxy');"
    testFoldConst("SELECT TRANSLATE('abacad', 'aac', 'zxy');")
    qt_translate_433 "SELECT TRANSLATE(NULL, 'a', 'z'), TRANSLATE('abc', NULL, 'z'), TRANSLATE('abc', 'a', NULL);"
    testFoldConst("SELECT TRANSLATE(NULL, 'a', 'z'), TRANSLATE('abc', NULL, 'z'), TRANSLATE('abc', 'a', NULL);")
    qt_translate_434 "SELECT TRANSLATE('', 'a', 'z'), TRANSLATE('abc', '', 'z'), TRANSLATE('abc', 'a', '');"
    testFoldConst("SELECT TRANSLATE('', 'a', 'z'), TRANSLATE('abc', '', 'z'), TRANSLATE('abc', 'a', '');")
    qt_translate_435 "SELECT TRANSLATE('abcde', 'ace', 'xy');"
    testFoldConst("SELECT TRANSLATE('abcde', 'ace', 'xy');")
    qt_translate_436 "SELECT TRANSLATE('ṭṛì ḍḍumai', 'ṭṛ', 'ab');"
    testFoldConst("SELECT TRANSLATE('ṭṛì ḍḍumai', 'ṭṛ', 'ab');")
    qt_translate_437 "SELECT TRANSLATE('a1b2c3', '123', 'xyz');"
    testFoldConst("SELECT TRANSLATE('a1b2c3', '123', 'xyz');")
    qt_translate_438 "SELECT TRANSLATE('aabbccaa', 'abab', 'xyuv');"
    testFoldConst("SELECT TRANSLATE('aabbccaa', 'abab', 'xyuv');")
    qt_translate_439 "SELECT TRANSLATE('hello@world.com', '@.', '-_');"
    testFoldConst("SELECT TRANSLATE('hello@world.com', '@.', '-_');")

    // TRIM_IN tests
    qt_trim_in_440 "SELECT trim_in(' ab d ');"
    testFoldConst("SELECT trim_in(' ab d ');")
    qt_trim_in_441 "SELECT trim_in('ababccaab', 'ab');"
    testFoldConst("SELECT trim_in('ababccaab', 'ab');")
    qt_trim_in_442 "SELECT trim_in('abcHelloabc', 'cba');"
    testFoldConst("SELECT trim_in('abcHelloabc', 'cba');")
    qt_trim_in_443 "SELECT trim_in('+++ṭṛì ḍḍumai+++', '+');"
    testFoldConst("SELECT trim_in('+++ṭṛì ḍḍumai+++', '+');")
    qt_trim_in_444 "SELECT trim_in(NULL, 'abc');"
    testFoldConst("SELECT trim_in(NULL, 'abc');")
    qt_trim_in_445 "SELECT trim_in('', 'abc'),trim_in('abc', '');"
    testFoldConst("SELECT trim_in('', 'abc'),trim_in('abc', '');")

    // TRIM tests
    qt_trim_446 "SELECT trim(' hello ');"
    testFoldConst("SELECT trim(' hello ');")
    qt_trim_447 "SELECT trim('xxxhelloxxx', 'x');"
    testFoldConst("SELECT trim('xxxhelloxxx', 'x');")
    qt_trim_448 "SELECT trim(' ab d ');"
    testFoldConst("SELECT trim(' ab d ');")
    qt_trim_449 "SELECT trim('ababccaab', 'ab');"
    testFoldConst("SELECT trim('ababccaab', 'ab');")
    qt_trim_450 "SELECT trim('ṭṛì ḍḍumai+++', 'ṭṛì');"
    testFoldConst("SELECT trim('ṭṛì ḍḍumai+++', 'ṭṛì');")
    qt_trim_451 "SELECT trim(NULL);"
    testFoldConst("SELECT trim(NULL);")
    qt_trim_452 "SELECT trim('xxxhelloxxx', ''),trim('', 'x');"
    testFoldConst("SELECT trim('xxxhelloxxx', ''),trim('', 'x');")

    // UCASE tests
    qt_ucase_453 "SELECT UCASE('aBc123'), UPPER('aBc123');"
    testFoldConst("SELECT UCASE('aBc123'), UPPER('aBc123');")
    qt_ucase_454 "SELECT UCASE('Hello World!'), UPPER('test@123');"
    testFoldConst("SELECT UCASE('Hello World!'), UPPER('test@123');")
    qt_ucase_455 "SELECT UCASE(NULL), UPPER(NULL);"
    testFoldConst("SELECT UCASE(NULL), UPPER(NULL);")
    qt_ucase_456 "SELECT UCASE(''), UPPER('');"
    testFoldConst("SELECT UCASE(''), UPPER('');")
    qt_ucase_457 "SELECT UCASE('ALREADY UPPERCASE'), UPPER('ABC123');"
    testFoldConst("SELECT UCASE('ALREADY UPPERCASE'), UPPER('ABC123');")
    qt_ucase_458 "SELECT UCASE('123!@#\$%'), UPPER('price: \$99.99');"
    testFoldConst("SELECT UCASE('123!@#\$%'), UPPER('price: \$99.99');")
    qt_ucase_459 "SELECT UCASE('ṭṛì test'), UPPER('ḍḍumai hello');"
    testFoldConst("SELECT UCASE('ṭṛì test'), UPPER('ḍḍumai hello');")
    qt_ucase_460 "SELECT UCASE('Кириллица'), UPPER('Бәйтерек');"
    testFoldConst("SELECT UCASE('Кириллица'), UPPER('Бәйтерек');")

    // UNHEX tests
    qt_unhex_461 "SELECT UNHEX('41'), UNHEX('61');"
    testFoldConst("SELECT UNHEX('41'), UNHEX('61');")
    qt_unhex_462 "SELECT UNHEX('4142'), UNHEX('48656C6C6F');"
    testFoldConst("SELECT UNHEX('4142'), UNHEX('48656C6C6F');")
    qt_unhex_463 "SELECT UNHEX(NULL), UNHEX_NULL(NULL);"
    testFoldConst("SELECT UNHEX(NULL), UNHEX_NULL(NULL);")
    qt_unhex_464 "SELECT UNHEX(''), UNHEX_NULL('');"
    testFoldConst("SELECT UNHEX(''), UNHEX_NULL('');")
    qt_unhex_465 "SELECT UNHEX('@'), UNHEX_NULL('@');"
    testFoldConst("SELECT UNHEX('@'), UNHEX_NULL('@');")
    qt_unhex_466 "SELECT UNHEX('123'), UNHEX_NULL('123');"
    testFoldConst("SELECT UNHEX('123'), UNHEX_NULL('123');")
    qt_unhex_467 "SELECT UNHEX('E4B8AD'), UNHEX('E69687');"
    testFoldConst("SELECT UNHEX('E4B8AD'), UNHEX('E69687');")
    qt_unhex_468 "SELECT UNHEX('313233'), UNHEX('393837');"
    testFoldConst("SELECT UNHEX('313233'), UNHEX('393837');")
    qt_unhex_469 "SELECT UNHEX(HEX('Hello')), UNHEX(HEX('Test123'));"
    testFoldConst("SELECT UNHEX(HEX('Hello')), UNHEX(HEX('Test123'));")

    // UUID_TO_INT tests
    qt_uuid_to_int_470 "SELECT uuid_to_int('6ce4766f-6783-4b30-b357-bba1c7600348');"
    testFoldConst("SELECT uuid_to_int('6ce4766f-6783-4b30-b357-bba1c7600348');")
    qt_uuid_to_int_471 "SELECT uuid_to_int(NULL);"
    testFoldConst("SELECT uuid_to_int(NULL);")
    qt_uuid_to_int_474 "SELECT uuid_to_int('6CE4766F-6783-4B30-B357-BBA1C7600348');"
    testFoldConst("SELECT uuid_to_int('6CE4766F-6783-4B30-B357-BBA1C7600348');")

    // UUID tests - Skipped (non-deterministic results)

    // XPATH_STRING tests
    qt_xpath_string_478 "SELECT xpath_string('<a>123</a>', '/a');"
    testFoldConst("SELECT xpath_string('<a>123</a>', '/a');")
    qt_xpath_string_479 "SELECT xpath_string('<a><b>123</b></a>', '/a/b');"
    testFoldConst("SELECT xpath_string('<a><b>123</b></a>', '/a/b');")
    qt_xpath_string_480 "SELECT xpath_string('<a><b id=\"1\">123</b></a>', '//b[@id=\"1\"]');"
    testFoldConst("SELECT xpath_string('<a><b id=\"1\">123</b></a>', '//b[@id=\"1\"]');")
    qt_xpath_string_481 "SELECT xpath_string('<a><b>1</b><b>2</b></a>', '/a/b[2]');"
    testFoldConst("SELECT xpath_string('<a><b>1</b><b>2</b></a>', '/a/b[2]');")
    qt_xpath_string_482 "SELECT xpath_string('<a><![CDATA[123]]></a>', '/a');"
    testFoldConst("SELECT xpath_string('<a><![CDATA[123]]></a>', '/a');")
    qt_xpath_string_483 "SELECT xpath_string('<a><!-- comment -->123</a>', '/a');"
    testFoldConst("SELECT xpath_string('<a><!-- comment -->123</a>', '/a');")
    qt_xpath_string_484 "SELECT xpath_string('<a>123</a>', '/b');"
    testFoldConst("SELECT xpath_string('<a>123</a>', '/b');")
    qt_xpath_string_485 "SELECT xpath_string(NULL, '/a');"
    testFoldConst("SELECT xpath_string(NULL, '/a');")
    qt_xpath_string_486 "SELECT xpath_string('<a><!-- comment -->123</a>', '/a');"
    testFoldConst("SELECT xpath_string('<a><!-- comment -->123</a>', '/a');")
}