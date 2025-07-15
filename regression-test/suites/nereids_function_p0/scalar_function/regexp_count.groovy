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
    qt_basic_count1 "SELECT regexp_count('a.b:c;d', '[.:;]');"
    qt_basic_count2 "SELECT regexp_count('a.b:c;d', '.');"
    qt_basic_count3 "SELECT regexp_count('a.b:c;d', ':');"
    qt_basic_count4 "SELECT regexp_count('Hello123World!', '[a-zA-Z]');"
    qt_basic_count5 "SELECT regexp_count('a1b2c3d', '[^0-9]');"
    qt_basic_count6 "SELECT regexp_count('Hello World\tJava\nSQL', 's');"
    qt_basic_count7 "SELECT regexp_count('Hello, World!', '[[:punct:]]');"
    qt_basic_count8 "SELECT regexp_count('abc123def456', 'd+');"
    qt_basic_count9 """SELECT regexp_count('Contact us at user@example.com or info@domain.com', 
                    '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+.[A-Za-z]{2,}');"""
    qt_basic_count10 "SELECT regexp_count('An apple a day keeps the doctor away', '[ae][a-z]*');"

    qt_empty_string "SELECT regexp_count('', 'x');"
    qt_empty_pattern "SELECT regexp_count('abcd', '');"
    qt_both_empty "SELECT regexp_count('', '');"

    sql """DROP TABLE IF EXISTS `test_table_for_regexp_count`;"""
    sql """CREATE TABLE test_table_for_regexp_count (
        id INT,
        text_data VARCHAR(500),
        pattern VARCHAR(100)
    ) PROPERTIES ("replication_num"="1");"""

    sql  """ INSERT INTO test_table_for_regexp_count VALUES
    (1, 'HelloWorld', '[A-Z][a-z]+'),    
    (2, 'apple123', '[a-z]{5}[0-9]'),    
    (3, 'aabbcc', '(aa|bb|cc)'),         
    (4, '123-456-7890', '[0-9][0-9][0-9]'), 
    (5, 'test,data', ','),              
    (6, 'a1b2c3', '[a-z][0-9]'),         
    (7, 'book keeper', 'oo|ee'),        
    (8, 'ababab', '(ab)(ab)(ab)'),       
    (9, 'aabbcc', '(aa|bb|cc)'),         
    (10, 'apple,banana', '[aeiou][a-z]+');
"""

    qt_table_basic "SELECT id, regexp_count(text_data, pattern) as count_result FROM test_table_for_regexp_count ORDER BY id;"
    qt_table_fixed_pattern "SELECT id, regexp_count(text_data, 'e') as count_e FROM test_table_for_regexp_count WHERE text_data IS NOT NULL ORDER BY id;"

    sql """DROP TABLE IF EXISTS `test_table_for_regexp_count`;"""
}
