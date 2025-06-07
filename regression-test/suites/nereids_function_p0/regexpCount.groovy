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

suite("test_regexp_count_new") {
    // Test multi-line string matching (\n)
    qt_multiline "SELECT regexp_count('line1\nline2\nline3', 'line\\d');"
    
    // Test non-greedy matching (?)
    qt_non_greedy_pattern "SELECT regexp_count('aabbaabbaabb', 'a+?b');"
    
    // Test backreferences (\\1)
    qt_backreference "SELECT regexp_count('ababa', '(a)(b)\\1');"
    
    // Test Unicode characters (emojis)
    qt_emoji "SELECT regexp_count('Hello😊World😀', '\\\\p{So}');"
    
    // Test word boundaries with special characters
    qt_word_special "SELECT regexp_count('apple_banana_orange', '\\b\\w+_\\w+\\b');"
    
    // Test positive lookahead assertion
    qt_lookahead "SELECT regexp_count('abcd123efg456', '\\d+(?=efg)');"
    
    // Test escaped characters (\\)
    qt_escape_character "SELECT regexp_count('a\\b\\c\\d', '\\\\\\w');"
    
    // Test repeated range ({n,m})
    qt_repeat_range "SELECT regexp_count('aaaaa', 'a{2,4}');"
    
    // Test Chinese punctuation marks
    qt_chinese_punct "SELECT regexp_count('你好，世界！', '[，！]');"
    
    // Test IPv4 address matching
    qt_ipv4 "SELECT regexp_count('IP: 192.168.1.1 and 10.0.0.1', '\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b');"
    
    // Test HTML tag extraction
    qt_html_tag "SELECT regexp_count('<div>Hello</div><p>World</p>', '<\\w+>.*?</\\w+>');"
    
    // Test patterns containing spaces
    qt_space_pattern "SELECT regexp_count('test pattern', '\\s+pattern');"
    
    // Test negative number matching
    qt_negative_number "SELECT regexp_count('-123 +456 -789', '-\\d+');"
    
    // Test multi-line mode (^ and $ match line start/end)
    qt_multiline_mode "SELECT regexp_count('Line1\nLine2\nLine3', '^Line\\d$');"
    
    // Test URL parameters
    qt_url_params "SELECT regexp_count('https://example.com?param=1&param=2', 'param=\\d');"
    
    // Test binary data (0s and 1s)
    qt_binary_data "SELECT regexp_count('101010', '101');"
    
    // Test ZIP code format (US)
    qt_zip_code "SELECT regexp_count('Zip: 90210-1234 and 12345', '\\d{5}(?:-\\d{4})?');"
    
    // Test scientific notation
    qt_scientific_notation "SELECT regexp_count('1e3 2.5e-2 3E4', '\\d+\\.?\\d*[eE][+-]?\\d+');"
    
    // Test credit card number masking
    qt_credit_card_mask "SELECT regexp_count('Card: 1234-5678-9012-3456', '\\d{4}-');"
    
    // Test date format (YYYY-MM-DD)
    qt_date_format "SELECT regexp_count('Date: 2023-10-01 and 2024-12-31', '\\d{4}-\\d{2}-\\d{2}');"
    
    // Test email aliases (+ symbol)
    qt_email_alias "SELECT regexp_count('user+label@domain.com', '\\w+\\+\\w+@\\w+\\.\\w+');"
    
    // Test path separators (/ and \)
    qt_path_separator "SELECT regexp_count('/user/data\\file.txt', '[\\\\/]');"
    
    // Test leading/trailing whitespace handling
    qt_trim_space "SELECT regexp_count('   hello   ', '\\s+hello\\s+');"
    
    // Test Roman numerals
    qt_roman_numerals "SELECT regexp_count('IV IX XII', 'M{0,3}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})');"
    
    // Test consecutive punctuation marks
    qt_consecutive_punct "SELECT regexp_count('!!!???', '[!?]{2,}');"
    
    // Test timezone format (+HH:MM)
    qt_timezone "SELECT regexp_count('UTC+08:00 UTC-05:00', '\\+\\d{2}:\\d{2}');"
    
    // Test IPv6 address (simplified)
    qt_ipv6 "SELECT regexp_count('IP6: 2001:0db8:85a3::8a2e:0370:7334', '::');"
    
    // Test mathematical symbols in formulas
    qt_math_symbols "SELECT regexp_count('a+b^2=c*3', '[+^=*]');"
    
    // Test international phone numbers
    qt_international_phone "SELECT regexp_count('+86-10-12345678 +1-202-555-1234', '\\+\\d{1,3}-\\d{3}-\\d{4}');"
    
    // Test file extensions
    qt_file_extension "SELECT regexp_count('file.pdf image.jpg script.py', '\\.[a-z]{3}$');"
    
    // Test delimiters in tabular data
    qt_table_delimiter "SELECT regexp_count('col1,col2\tcol3;col4', '[,\\t;]');"

    // Test table data insertion and query
    sql """DROP TABLE IF EXISTS `test_table_new`;"""
    sql """CREATE TABLE test_table_new (
        id INT,
        content VARCHAR(500)
    ) PROPERTIES ("replication_num"="1");"""
    
    sql """INSERT INTO test_table_new VALUES
        (1, 'apple,banana,cherry'),
        (2, '123-456-789'),
        (3, 'Hello\nWorld'),
        (4, 'aabbccddee'),
        (5, 'https://doris.apache.org/docs');"""
    
    qt_table_query "SELECT id, regexp_count(content, ',') as comma_count FROM test_table_new ORDER BY id;"
    
    test {
        sql "SELECT regexp_count('invalid][pattern', '[]');"
        exception "Could not compile regexp pattern"
    }

    sql """DROP TABLE IF EXISTS `test_table_new`;"""
}