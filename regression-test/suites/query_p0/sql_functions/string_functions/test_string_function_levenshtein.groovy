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
    // 1. Constant Value Tests (Sanity Check)
    qt_select_const "SELECT levenshtein('kitten', 'sitting')"
    qt_select_const "SELECT levenshtein('hello', 'hello')"
    qt_select_const "SELECT levenshtein('abc', '')"
    qt_select_const "SELECT levenshtein('', 'def')"
    // UTF-8 fix check: '中国' (2 chars) vs '中' (1 char) -> Distance 1
    qt_select_const "SELECT levenshtein('中国', '中')"
    // UTF-8 fix check: '测试' (2 chars) vs '测验' (2 chars) -> Distance 1
    qt_select_const "SELECT levenshtein('测试', '测验')"
    qt_select_const "SELECT levenshtein(NULL, 'abc')"

    // 2. Prepare Table Data for Column Tests
    def tableName = "test_levenshtein_tbl"
    sql "DROP TABLE IF EXISTS ${tableName}"
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

    // Insert data covering boundary and UTF-8 cases
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

    // 3. Column vs Column Test
    qt_select_col_col "SELECT id, levenshtein(s1, s2) FROM ${tableName} ORDER BY id"

    // 4. Partial Constant Test: Column vs Constant (Reviewer Request)
    qt_select_col_const "SELECT id, levenshtein(s1, 'abc') FROM ${tableName} ORDER BY id"

    // 5. Partial Constant Test: Constant vs Column (Reviewer Request)
    qt_select_const_col "SELECT id, levenshtein('kitten', s2) FROM ${tableName} ORDER BY id"

    sql "DROP TABLE ${tableName}"
}