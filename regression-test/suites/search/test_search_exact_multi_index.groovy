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

suite("test_search_exact_multi_index") {
    def tableName = "exact_multi_index_test"

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Table with multiple indexes on the same column
    // This tests that EXACT uses the untokenized index (STRING_TYPE)
    // while ANY/ALL use the tokenized index (FULLTEXT)
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            content VARCHAR(200),
            INDEX idx_untokenized (content) USING INVERTED,
            INDEX idx_tokenized (content) USING INVERTED PROPERTIES("parser" = "standard")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // Insert test data
    sql """INSERT INTO ${tableName} VALUES
        (1, 'machine learning'),
        (2, 'deep learning'),
        (3, 'machine'),
        (4, 'learning'),
        (5, 'artificial intelligence'),
        (6, 'natural language processing')
    """

    Thread.sleep(3000)

    // Test 1: EXACT should use untokenized index - exact match only
    qt_exact_full_match "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content FROM ${tableName} WHERE search('content:EXACT(machine learning)') ORDER BY id"

    // Test 2: EXACT should not match partial tokens
    qt_exact_no_partial "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content FROM ${tableName} WHERE search('content:EXACT(machine)') ORDER BY id"

    // Test 3: ANY should use tokenized index - matches any token
    qt_any_token_match "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content FROM ${tableName} WHERE search('content:ANY(machine learning)') ORDER BY id"

    // Test 4: ALL should use tokenized index - matches all tokens
    qt_all_token_match "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content FROM ${tableName} WHERE search('content:ALL(machine learning)') ORDER BY id"

    // Test 5: Verify EXACT vs ANY behavior difference
    // EXACT: only exact string match
    // ANY: any token matches
    qt_exact_strict "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content FROM ${tableName} WHERE search('content:EXACT(deep learning)') ORDER BY id"
    qt_any_loose "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content FROM ${tableName} WHERE search('content:ANY(deep learning)') ORDER BY id"

    // Test 6: Multiple conditions with EXACT and ANY
    qt_mixed_exact_any "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, content FROM ${tableName} WHERE search('content:EXACT(machine learning) OR content:ANY(intelligence)') ORDER BY id"
}
