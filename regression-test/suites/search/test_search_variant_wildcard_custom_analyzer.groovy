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
 * Test wildcard search on variant subcolumns with multi-index (none + analyzer)
 * in Lucene mode.
 *
 * Wildcard queries (*, ?) on variant subcolumns with multiple inverted indexes
 * (one without analyzer, one with analyzer) previously returned empty results
 * even when regular TERM search works correctly.
 *
 * Scenario: HubSpot-like contacts table with firstname/lastname stored in
 * variant subcolumns. The table has dual indexes per field_pattern:
 * - idx_string: no analyzer (for exact/keyword matching)
 * - idx_string_analyzer: standard parser + lowercase (for full-text search)
 *
 * Root cause: In FieldReaderResolver::resolve(), only EQUAL_QUERY was upgraded
 * to MATCH_ANY_QUERY for variant subcolumns with analyzers. WILDCARD_QUERY was
 * not upgraded, so it selected the wrong index reader (STRING_TYPE instead of
 * FULLTEXT), causing term enumeration to fail.
 */
suite("test_search_variant_wildcard_custom_analyzer", "p0") {
    def tableName = "test_search_variant_wildcard_custom_analyzer"

    sql """ set enable_match_without_inverted_index = false """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_enable_doc_mode = false """

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create table with variant column using dual indexes (none + analyzer)
    // matching the HubSpot objects_small schema from the reported bug.
    // Uses built-in parser properties instead of custom analyzers for stability.
    sql """
        CREATE TABLE ${tableName} (
            `portalId` int NOT NULL,
            `objectId` bigint NOT NULL,
            `overflowProperties` variant<
                MATCH_NAME_GLOB 'string_*' : text
            > NULL,
            INDEX idx_string (overflowProperties) USING INVERTED PROPERTIES(
                'field_pattern' = 'string_*'
            ),
            INDEX idx_string_analyzer (overflowProperties) USING INVERTED PROPERTIES(
                'support_phrase' = 'true',
                'field_pattern' = 'string_*',
                'parser' = 'standard',
                'lower_case' = 'true'
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(`portalId`, `objectId`)
        DISTRIBUTED BY HASH(`portalId`) BUCKETS 1
        PROPERTIES (
            'replication_allocation' = 'tag.location.default: 1',
            'disable_auto_compaction' = 'true'
        )
    """

    // Insert test data matching the reported scenario:
    // c1: John Smith (firstname = John, lastname = Smith) — 73095521135
    // c2: Jane Smithson (firstname = Jane, lastname = Smithson) — 73095446198
    // c3: Michael David Johnson (firstname = Michael David, lastname = Johnson) — 73095754047
    // string_8 = firstname, string_17 = lastname
    sql """INSERT INTO ${tableName} VALUES
        (1, 73095521135, '{"string_8": "John", "string_17": "Smith"}'),
        (1, 73095446198, '{"string_8": "Jane", "string_17": "Smithson"}'),
        (1, 73095754047, '{"string_8": "Michael David", "string_17": "Johnson"}')
    """

    sql "sync"
    Thread.sleep(5000)

    // ============ Baseline: TERM search works ============

    // Test 1: TERM search for 'smith' on lastname - should return John Smith
    qt_term_smith """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ objectId FROM ${tableName}
        WHERE search('smith', '{"default_field":"overflowProperties.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY objectId
    """

    // Test 2: TERM search for 'smithson' on lastname - should return Jane Smithson
    qt_term_smithson """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ objectId FROM ${tableName}
        WHERE search('smithson', '{"default_field":"overflowProperties.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY objectId
    """

    // Test 3: TERM search for 'johnson' on lastname - should return Michael David Johnson
    qt_term_johnson """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ objectId FROM ${tableName}
        WHERE search('johnson', '{"default_field":"overflowProperties.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY objectId
    """

    // ============ the reported bug: Wildcard searches on lastname ============

    // Test 4: Leading wildcard '*ith' - should match "smith" (ends with "ith")
    qt_wildcard_star_ith """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ objectId FROM ${tableName}
        WHERE search('*ith', '{"default_field":"overflowProperties.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY objectId
    """

    // Test 5: Middle wildcard 'sm*th' - should match "smith"
    qt_wildcard_sm_star_th """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ objectId FROM ${tableName}
        WHERE search('sm*th', '{"default_field":"overflowProperties.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY objectId
    """

    // Test 6: Single char wildcard 'sm?th' - should match "smith"
    qt_wildcard_sm_q_th """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ objectId FROM ${tableName}
        WHERE search('sm?th', '{"default_field":"overflowProperties.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY objectId
    """

    // Test 7: Trailing wildcard 'smith*' - should match "smith" and "smithson"
    qt_wildcard_smith_star """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ objectId FROM ${tableName}
        WHERE search('smith*', '{"default_field":"overflowProperties.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY objectId
    """

    // Test 8: Trailing wildcard 'sm*' - should match "smith" and "smithson"
    qt_wildcard_sm_star """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ objectId FROM ${tableName}
        WHERE search('sm*', '{"default_field":"overflowProperties.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY objectId
    """

    // Test 9: Leading wildcard '*son' - should match "smithson" and "johnson"
    qt_wildcard_star_son """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ objectId FROM ${tableName}
        WHERE search('*son', '{"default_field":"overflowProperties.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY objectId
    """

    // ============ Wildcard on firstname field ============

    // Test 10: Wildcard 'jo?n' on firstname - should match "john"
    qt_wildcard_firstname """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ objectId FROM ${tableName}
        WHERE search('jo?n', '{"default_field":"overflowProperties.string_8","default_operator":"and","mode":"lucene"}')
        ORDER BY objectId
    """

    // Test 11: Wildcard 'mi*' on firstname - should match "michael david"
    qt_wildcard_firstname_michael """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ objectId FROM ${tableName}
        WHERE search('mi*', '{"default_field":"overflowProperties.string_8","default_operator":"and","mode":"lucene"}')
        ORDER BY objectId
    """

    // ============ Cross-field wildcard + AND ============

    // Test 12: Wildcard combined with AND across fields
    qt_wildcard_cross_field """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ objectId FROM ${tableName}
        WHERE search('overflowProperties.string_17:sm*th AND overflowProperties.string_8:john', '{"default_operator":"and","mode":"lucene"}')
        ORDER BY objectId
    """

    // ============ Wildcard '*' matches all ============

    // Test 13: Standalone wildcard '*' matches all non-null values
    qt_wildcard_star_all """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ objectId FROM ${tableName}
        WHERE search('*', '{"default_field":"overflowProperties.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY objectId
    """

    // Clean up
    sql "DROP TABLE IF EXISTS ${tableName}"
}
