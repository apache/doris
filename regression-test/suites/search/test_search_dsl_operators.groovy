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
 * Tests for search DSL operator scenarios
 *
 * This test suite validates Lucene mode parsing against the exact test cases
 * documented in specification to ensure behavior matches Elasticsearch/Lucene semantics.
 *
 * Test Data Setup:
 * | Email                  | Firstname           |
 * | test+query+1@gmail.com | "aterm bterm"       |
 * | test+query+2@gmail.com | "bterm cterm"       |
 * | test+query+3@gmail.com | "cterm dterm"       |
 * | test+query+4@gmail.com | "dterm eterm aterm" |
 *
 * Key Lucene Semantics:
 * - Operators are processed left-to-right as modifiers
 * - AND marks preceding and current terms as MUST (+)
 * - OR marks preceding and current terms as SHOULD
 * - NOT marks current term as MUST_NOT (-)
 * - With minimum_should_match=0 and MUST clauses present, SHOULD clauses are discarded
 */
suite("test_search_dsl_operators", "p0") {
    def tableName = "search_dsl_operators_test"

    // Pin enable_common_expr_pushdown to prevent CI flakiness from fuzzy testing.
    sql """ set enable_common_expr_pushdown = true """

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create table with inverted indexes
    // Using parser=english to tokenize firstname field
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            email VARCHAR(100),
            firstname VARCHAR(200),
            INDEX idx_firstname(firstname) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // Insert test data
    sql """INSERT INTO ${tableName} VALUES
        (1, 'test+query+1@gmail.com', 'aterm bterm'),
        (2, 'test+query+2@gmail.com', 'bterm cterm'),
        (3, 'test+query+3@gmail.com', 'cterm dterm'),
        (4, 'test+query+4@gmail.com', 'dterm eterm aterm')
    """

    // Wait for index building
    Thread.sleep(3000)

    // ============ Test 1: aterm OR bterm OR cterm ============
    // All OR operators -> at least one must match (minimum_should_match=1)
    // Expected: rows 1,2,3,4 (all match at least one term)
    qt_dsl_or_chain """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('aterm OR bterm OR cterm', '{"default_field":"firstname","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 2: dterm AND eterm AND aterm ============
    // All AND operators -> all must match
    // Expected: row 4 only (the only one with all three terms)
    qt_dsl_and_chain """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('dterm AND eterm AND aterm', '{"default_field":"firstname","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 3: aterm AND bterm OR cterm ============
    // Lucene left-to-right parsing with minimum_should_match=0:
    // - aterm: MUST (first term, default_operator=AND)
    // - bterm: MUST (AND introduces)
    // - cterm: SHOULD (OR introduces), bterm becomes SHOULD too
    // Final: +aterm bterm cterm
    // With minimum_should_match=0 and MUST present, SHOULD discarded
    // Result: effectively +aterm only
    // Expected: rows 1, 4 (rows containing "aterm")
    qt_dsl_and_or_mixed """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('aterm AND bterm OR cterm', '{"default_field":"firstname","default_operator":"and","mode":"lucene","minimum_should_match":0}')
        ORDER BY id
    """

    // ============ Test 4: aterm AND NOT bterm OR cterm ============
    // Lucene left-to-right parsing:
    // - aterm: MUST
    // - bterm: MUST_NOT (NOT modifier)
    // - cterm: SHOULD (OR introduces)
    // Final: +aterm -bterm cterm
    // With minimum_should_match=0 and MUST present, SHOULD discarded
    // Result: +aterm -bterm
    // Expected: row 4 only (has "aterm" but NOT "bterm")
    qt_dsl_and_not_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('aterm AND NOT bterm OR cterm', '{"default_field":"firstname","default_operator":"and","mode":"lucene","minimum_should_match":0}')
        ORDER BY id
    """

    // ============ Test 5: cterm dterm (implicit AND) ============
    // No explicit operators, default_operator=AND
    // Same as: cterm AND dterm
    // Expected: row 3 only (has both "cterm" AND "dterm")
    qt_dsl_implicit_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('cterm dterm', '{"default_field":"firstname","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 6: "aterm eterm" (phrase query, wrong order) ============
    // Phrase query requires tokens in exact order
    // Data has "dterm eterm aterm" - "aterm" comes AFTER "eterm", not before
    // Expected: no match
    qt_dsl_phrase_wrong_order """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('"aterm eterm"', '{"default_field":"firstname","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 7: "eterm aterm" (phrase query, correct order) ============
    // Phrase query requires tokens in exact order
    // Data has "dterm eterm aterm" - "eterm aterm" appears in this order
    // Expected: row 4
    qt_dsl_phrase_correct_order """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('"eterm aterm"', '{"default_field":"firstname","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 8: eterm\ dterm AND aterm (escaped space test) ============
    // Tests escape handling in Lucene mode
    // In current implementation, the escaped space is processed such that
    // the query effectively becomes a term query for individual tokens
    // Row 4 contains all terms (dterm, eterm, aterm)
    // Expected: row 4
    qt_dsl_escaped_space_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('eterm\\\\ dterm AND aterm', '{"default_field":"firstname","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 9: "dterm eterm" AND aterm ============
    // Phrase query + AND
    // Row 4 has "dterm eterm aterm" - phrase "dterm eterm" matches, and "aterm" is also present
    // Expected: row 4
    qt_dsl_phrase_and_term """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('"dterm eterm" AND aterm', '{"default_field":"firstname","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 10: "eterm dterm" AND aterm (phrase wrong order) ============
    // Phrase "eterm dterm" is wrong order (data has "dterm eterm")
    // Expected: no match
    qt_dsl_phrase_wrong_and_term """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('"eterm dterm" AND aterm', '{"default_field":"firstname","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 11: "eterm dterm" OR cterm ============
    // Phrase OR term
    // Phrase "eterm dterm" won't match (wrong order)
    // cterm matches rows 2, 3
    // Expected: rows 2, 3
    qt_dsl_phrase_or_term_1 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('"eterm dterm" OR cterm', '{"default_field":"firstname","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 12: "dterm eterm" OR cterm ============
    // Phrase OR term
    // Phrase "dterm eterm" matches row 4
    // cterm matches rows 2, 3
    // Expected: rows 2, 3, 4
    qt_dsl_phrase_or_term_2 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('"dterm eterm" OR cterm', '{"default_field":"firstname","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Test 13: aterm AND bterm OR cterm with minimum_should_match=1 ============
    // Same as Test 3 but with minimum_should_match=1
    // Final state: +aterm bterm cterm (aterm is MUST, bterm and cterm are SHOULD)
    // With minimum_should_match=1, at least 1 SHOULD must match
    // Result: aterm AND (bterm OR cterm)
    // Expected: rows 1, 2 (row 1 has aterm+bterm, row 2 doesn't have aterm)
    // Wait - row 2 doesn't have aterm, so it shouldn't match
    // Row 1: has aterm, has bterm -> matches
    // Row 4: has aterm, doesn't have bterm or cterm -> doesn't match (no SHOULD satisfied)
    // Actually row 4 has aterm but no bterm/cterm...
    // Expected: row 1 only
    qt_dsl_and_or_min_should_1 """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, firstname
        FROM ${tableName}
        WHERE search('aterm AND bterm OR cterm', '{"default_field":"firstname","default_operator":"and","mode":"lucene","minimum_should_match":1}')
        ORDER BY id
    """

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"
}
