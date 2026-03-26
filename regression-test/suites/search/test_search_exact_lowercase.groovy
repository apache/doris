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

suite("test_search_exact_lowercase", "p0") {
    def tableName = "exact_lowercase_test"

    // Pin enable_common_expr_pushdown to prevent CI flakiness from fuzzy testing.
    sql """ set enable_common_expr_pushdown = true """

    sql "DROP TABLE IF EXISTS ${tableName}"

    // EXACT on mixed indexes: prefers untokenized, but untokenized index doesn't support lowercase
    // So EXACT is always case-sensitive regardless of lowercase config
    // This test verifies that EXACT behavior is consistent
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            text_normal VARCHAR(200),
            text_mixed VARCHAR(200),
            INDEX idx_normal (text_normal) USING INVERTED,
            INDEX idx_mixed_tokenized (text_mixed) USING INVERTED PROPERTIES("parser" = "unicode", "lower_case" = "true"),
            INDEX idx_mixed_untokenized (text_mixed) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // Insert test data with various cases
    sql """INSERT INTO ${tableName} VALUES
        (1, 'Hello World', 'Hello World'),
        (2, 'hello world', 'hello world'),
        (3, 'HELLO WORLD', 'HELLO WORLD'),
        (4, 'HeLLo WoRLd', 'HeLLo WoRLd'),
        (5, 'Test Case', 'Test Case'),
        (6, 'TEST CASE', 'TEST CASE')
    """

    Thread.sleep(3000)

    // Test 1: EXACT on normal field (untokenized) - case sensitive
    qt_normal_exact_lower "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, text_normal FROM ${tableName} WHERE search('text_normal:EXACT(hello world)') ORDER BY id"
    qt_normal_exact_upper "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, text_normal FROM ${tableName} WHERE search('text_normal:EXACT(HELLO WORLD)') ORDER BY id"
    qt_normal_exact_mixed "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, text_normal FROM ${tableName} WHERE search('text_normal:EXACT(Hello World)') ORDER BY id"

    // Test 2: EXACT on mixed index field
    // EXACT prefers untokenized index, so it's case sensitive (lowercase config is ignored)
    qt_mixed_exact_lower "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, text_mixed FROM ${tableName} WHERE search('text_mixed:EXACT(hello world)') ORDER BY id"
    qt_mixed_exact_upper "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, text_mixed FROM ${tableName} WHERE search('text_mixed:EXACT(HELLO WORLD)') ORDER BY id"
    qt_mixed_exact_mixed "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, text_mixed FROM ${tableName} WHERE search('text_mixed:EXACT(Hello World)') ORDER BY id"

    // Test 3: Verify that ANY on mixed index uses tokenized index with lowercase
    qt_mixed_any_lowercase "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, text_mixed FROM ${tableName} WHERE search('text_mixed:ANY(hello world)') ORDER BY id"

    // Test 4: Compare EXACT (case-sensitive) vs ANY (with lowercase)
    qt_exact_case_sensitive "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('text_mixed:EXACT(Test Case)') ORDER BY id"
    qt_any_case_insensitive "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('text_mixed:ANY(test case)') ORDER BY id"
}
