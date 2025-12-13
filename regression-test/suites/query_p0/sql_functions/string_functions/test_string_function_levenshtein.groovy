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

suite("test_string_function_levenshtein") {
    // 1. Sanity check for constant values
    qt_const_1 "SELECT levenshtein('kitten', 'sitting')"
    qt_const_2 "SELECT levenshtein('hello', 'hello')"
    qt_const_3 "SELECT levenshtein('abc', '')"
    qt_const_4 "SELECT levenshtein('', 'def')"
    
    // Check UTF-8 characters (distance should be character-based, not byte-based)
    // distance 1
    qt_const_utf8_1 "SELECT levenshtein('中国', '中')"
    // distance 1
    qt_const_utf8_2 "SELECT levenshtein('测试', '测验')"
    
    qt_const_null "SELECT levenshtein(NULL, 'abc')"

    // 2. Prepare table data
    def tableName = "test_levenshtein_tbl"
    sql "DROP TABLE IF EXISTS ${tableName}"
    
    // Use multi-line string for SQL definition
    sql """
        CREATE TABLE ${tableName} (
            `id` int,
            `s1` string,
            `s2` string
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    // Insert test data
    sql """
        INSERT INTO ${tableName} VALUES 
        (1, 'kitten', 'sitting'),
        (2, 'rosettacode', 'raisethysword'),
        (3, 'abc', 'abc'),
        (4, '', 'abc'),
        (5, 'abc', ''),
        (6, '中国', '中'),
        (7, '测试', '测验'),
        (8, NULL, 'abc'),
        (9, 'abc', NULL)
    """

    // 3. Test case: Column vs Column
    qt_select_col_col "SELECT id, levenshtein(s1, s2) FROM ${tableName} ORDER BY id"

    // 4. Test case: Column vs Constant
    qt_select_col_const "SELECT id, levenshtein(s1, 'abc') FROM ${tableName} ORDER BY id"

    // 5. Test case: Constant vs Column
    qt_select_const_col "SELECT id, levenshtein('kitten', s2) FROM ${tableName} ORDER BY id"

    // Clean up
    sql "DROP TABLE ${tableName}"
}