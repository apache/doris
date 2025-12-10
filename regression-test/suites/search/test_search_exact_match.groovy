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

suite("test_search_exact_match") {
    def tableName = "search_exact_test_table"

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create test table with different index configurations
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(255),
            content TEXT,
            keyword VARCHAR(200),
            mixed_index VARCHAR(200),
            INDEX idx_title (title) USING INVERTED,
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_keyword (keyword) USING INVERTED,
            INDEX idx_mixed_tokenized (mixed_index) USING INVERTED PROPERTIES("parser" = "standard"),
            INDEX idx_mixed_untokenized (mixed_index) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    // Insert test data for EXACT matching
    sql """INSERT INTO ${tableName} VALUES
        (1, 'Hello World', 'This is a test document', 'test keyword', 'tokenized value'),
        (2, 'hello world', 'Another test document', 'hello world', 'another value'),
        (3, 'HELLO WORLD', 'Third test document', 'HELLO WORLD', 'UPPER VALUE'),
        (4, 'Hello', 'Just hello', 'hello', 'hello'),
        (5, 'World', 'Just world', 'world', 'world'),
        (6, 'a b c', 'Space separated', 'a b c', 'a b c'),
        (7, 'machine learning', 'About ML', 'ML', 'machine learning'),
        (8, 'deep learning', 'About DL', 'DL', 'deep learning'),
        (9, 'Special!@#Chars', 'Special characters test', 'special!@#chars', 'special chars')
    """

    // Wait for index building
    Thread.sleep(5000)

    // Test 1: EXACT match without tokenization - exact case matching
    qt_exact_basic "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:EXACT(Hello World)') ORDER BY id"

    // Test 2: EXACT match should not find partial matches (unlike ANY)
    qt_exact_no_partial "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:EXACT(Hello)') ORDER BY id"

    // Test 3: EXACT match on field without tokenization - case sensitive
    qt_exact_case_sensitive "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, keyword FROM ${tableName} WHERE search('keyword:EXACT(hello world)') ORDER BY id"

    // Test 4: EXACT match with different case
    qt_exact_different_case "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, keyword FROM ${tableName} WHERE search('keyword:EXACT(HELLO WORLD)') ORDER BY id"

    // Test 5: EXACT match with spaces
    qt_exact_spaces "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, keyword FROM ${tableName} WHERE search('keyword:EXACT(a b c)') ORDER BY id"

    // Test 6: Compare EXACT vs ANY on tokenized field
    qt_exact_vs_any_exact "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content FROM ${tableName} WHERE search('content:EXACT(test document)') ORDER BY id"
    qt_exact_vs_any_any "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content FROM ${tableName} WHERE search('content:ALL(test document)') ORDER BY id"

    // Test 7: EXACT on field with mixed index (both tokenized and untokenized)
    // EXACT should use the untokenized index
    qt_exact_mixed_index "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, mixed_index FROM ${tableName} WHERE search('mixed_index:EXACT(machine learning)') ORDER BY id"

    // Test 8: EXACT with special characters
    qt_exact_special_chars "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:EXACT(Special!@#Chars)') ORDER BY id"

    // Test 9: EXACT in boolean queries
    qt_exact_and "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:EXACT(Hello World) AND content:test') ORDER BY id"
    qt_exact_or "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:EXACT(Hello World) OR title:EXACT(hello world)') ORDER BY id"
    qt_exact_not "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('NOT title:EXACT(HELLO WORLD)') ORDER BY id LIMIT 10"

    // Test 10: EXACT with NULL values
    sql """INSERT INTO ${tableName} VALUES (10, NULL, 'null title test', 'null', 'null')"""
    Thread.sleep(2000)
    qt_exact_null "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:EXACT(NULL)') ORDER BY id"

    // Test 11: Multiple EXACT conditions
    qt_exact_multiple "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, keyword FROM ${tableName} WHERE search('title:EXACT(Hello World) AND keyword:EXACT(test keyword)') ORDER BY id"

    // Test 12: EXACT vs regular term query comparison
    qt_term_query "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:Hello') ORDER BY id"
    qt_exact_query "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:EXACT(Hello)') ORDER BY id"

    // Test 13: EXACT with ALL - combining different operators
    qt_exact_and_all "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName} WHERE search('title:EXACT(Hello World) AND content:ALL(test document)') ORDER BY id"

    // Test 14: Case sensitivity without lowercase config
    qt_exact_case_hello "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, keyword FROM ${tableName} WHERE search('keyword:EXACT(hello)') ORDER BY id"
    qt_exact_case_HELLO "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, keyword FROM ${tableName} WHERE search('keyword:EXACT(HELLO)') ORDER BY id"

    // Test 15: EXACT on mixed_index should prefer untokenized index
    // So it should match exact string, not tokens
    qt_exact_mixed_exact_match "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, mixed_index FROM ${tableName} WHERE search('mixed_index:EXACT(tokenized value)') ORDER BY id"
    qt_any_mixed_token_match "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, mixed_index FROM ${tableName} WHERE search('mixed_index:ALL(tokenized value)') ORDER BY id"

}
