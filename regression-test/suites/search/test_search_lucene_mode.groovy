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

/**
 * Tests for Lucene mode parsing in search() function.
 *
 * Lucene mode mimics Elasticsearch/Lucene query_string behavior where boolean
 * operators work as left-to-right modifiers, not traditional boolean algebra.
 *
 * Key differences from standard mode:
 * - AND/OR/NOT are modifiers that affect adjacent terms
 * - Operator precedence is left-to-right
 * - Uses MUST/SHOULD/MUST_NOT internally (like Lucene's Occur enum)
 * - minimum_should_match controls SHOULD clause behavior
 *
 * Enable Lucene mode with options parameter (JSON format):
 *   search(dsl, '{"default_field":"title","default_operator":"and","mode":"lucene"}')
 */
suite("test_search_lucene_mode", "p0") {
    def tableName = "search_lucene_mode_test"

    // Pin enable_common_expr_pushdown to prevent CI flakiness from fuzzy testing.
    sql """ set enable_common_expr_pushdown = true """

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create table with inverted indexes
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(100),
            content VARCHAR(200),
            category VARCHAR(50),
            INDEX idx_title(title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content(content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_category(category) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // Insert test data
    // Test data designed to verify Lucene-style boolean logic
    sql """INSERT INTO ${tableName} VALUES
        (1, 'apple banana cherry', 'red green blue', 'fruit'),
        (2, 'apple banana', 'red green', 'fruit'),
        (3, 'apple', 'red', 'fruit'),
        (4, 'banana cherry', 'green blue', 'fruit'),
        (5, 'cherry date', 'blue yellow', 'fruit'),
        (6, 'date elderberry', 'yellow purple', 'berry'),
        (7, 'fig grape', 'orange pink', 'mixed'),
        (8, 'apple fig', 'red orange', 'mixed')
    """

    // Wait for index building
    Thread.sleep(3000)

    // ============ Test 1: Standard mode AND behavior ============
    // In standard mode, 'apple AND banana' behaves like boolean AND
    qt_standard_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:apple AND title:banana')
        ORDER BY id
    """

    // ============ Test 2: Lucene mode AND behavior ============
    // In Lucene mode, 'a AND b' marks both as MUST (+a +b)
    // Expected same result as standard mode for simple AND
    qt_lucene_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('apple AND banana', '{"default_field":"title","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 3: Standard mode OR behavior ============
    // In standard mode, 'apple OR date' returns any row matching either
    qt_standard_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:apple OR title:date')
        ORDER BY id
    """

    // ============ Test 4: Lucene mode OR behavior ============
    // In Lucene mode, 'a OR b' marks both as SHOULD with minimum_should_match=1
    qt_lucene_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('apple OR date', '{"default_field":"title","default_operator":"or","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 5: Lucene mode complex expression ============
    // 'a AND b OR c' in Lucene mode (left-to-right parsing):
    // - 'apple' starts as MUST (default_operator=AND)
    // - 'AND banana' makes 'banana' MUST
    // - 'OR cherry' makes 'cherry' SHOULD, AND changes 'banana' from MUST to SHOULD!
    // Final state: +apple banana cherry (only 'apple' is MUST, 'banana' and 'cherry' are SHOULD)
    // With minimum_should_match=0 (default when MUST exists), SHOULD clauses are discarded.
    // So effectively: +apple only
    // Expected: rows containing 'apple' -> 1, 2, 3, 8
    qt_lucene_complex_and_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('apple AND banana OR cherry', '{"default_field":"title","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 6: Lucene mode with explicit minimum_should_match=1 ============
    // 'a AND b OR c' with minimum_should_match=1 (same Lucene left-to-right parsing):
    // - 'apple': MUST
    // - 'AND banana': banana becomes MUST
    // - 'OR cherry': cherry becomes SHOULD, banana changes from MUST to SHOULD
    // Final state: +apple banana cherry (apple is MUST, banana and cherry are SHOULD)
    // With minimum_should_match=1, at least 1 SHOULD must match.
    // So effectively: apple AND (banana OR cherry)
    // Expected: rows with 'apple' AND ('banana' OR 'cherry') -> 1, 2
    qt_lucene_min_should_match_1 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('apple AND banana OR cherry', '{"default_field":"title","default_operator":"and","mode":"lucene","minimum_should_match":1}')
        ORDER BY id
    """

    // ============ Test 7: Lucene mode NOT operator (pure negative query) ============
    // 'NOT a' in Lucene mode is rewritten to: SHOULD(MATCH_ALL_DOCS) + MUST_NOT(a)
    // This matches all documents EXCEPT those containing the negated term.
    // Expected: all docs without "apple" in title (4, 5, 6, 7)
    qt_lucene_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('NOT apple', '{"default_field":"title","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 8: Lucene mode AND NOT ============
    // 'a AND NOT b' in Lucene mode:
    // - 'a' is MUST
    // - 'NOT b' makes 'b' MUST_NOT
    // Expected: rows with 'apple' but NOT 'banana'
    qt_lucene_and_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('apple AND NOT banana', '{"default_field":"title","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 9: Lucene mode OR NOT ============
    // 'a OR NOT b' in Lucene mode:
    // - 'a' is SHOULD
    // - 'NOT b' makes 'b' MUST_NOT
    // Expected: rows with 'apple' OR (NOT 'banana')
    qt_lucene_or_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('apple OR NOT banana', '{"default_field":"title","default_operator":"or","mode":"lucene","minimum_should_match":1}')
        ORDER BY id
    """

    // ============ Test 10: Lucene mode only OR (SHOULD only) ============
    // 'a OR b OR c' with only SHOULD clauses
    // minimum_should_match defaults to 1 (at least one must match)
    qt_lucene_or_only """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('apple OR date OR fig', '{"default_field":"title","default_operator":"or","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 11: Lucene mode cross-field query ============
    // Multi-field query with Lucene mode
    qt_lucene_cross_field """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, category
        FROM ${tableName}
        WHERE search('title:apple AND category:fruit', '{"default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 12: Standard mode for comparison ============
    // Same query in standard mode for comparison
    qt_standard_cross_field """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, category
        FROM ${tableName}
        WHERE search('title:apple AND category:fruit')
        ORDER BY id
    """

    // ============ Test 13: Lucene mode with phrase query ============
    qt_lucene_phrase """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('"apple banana"', '{"default_field":"title","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 14: Lucene mode with wildcard ============
    qt_lucene_wildcard """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('app* AND ban*', '{"default_field":"title","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 15: Verify standard mode unchanged ============
    // Ensure standard mode is not affected by the Lucene mode addition
    qt_standard_unchanged """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:apple AND title:banana')
        ORDER BY id
    """

    // ============ Test 16: Lucene mode with empty options (should use standard mode) ============
    qt_empty_options """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('apple AND banana', '{"default_field":"title","default_operator":"and"}')
        ORDER BY id
    """

    // ============ Test 17: Lucene mode minimum_should_match=0 default behavior ============
    // With minimum_should_match=0 (default in filter context), SHOULD clauses are discarded
    // when MUST clauses exist
    qt_lucene_min_should_match_0 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('apple AND banana OR date', '{"default_field":"title","default_operator":"and","mode":"lucene","minimum_should_match":0}')
        ORDER BY id
    """

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"
}
