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
 * Test wildcard search on variant subcolumns in Lucene mode.
 *
 * Bug: Wildcard queries (*, ?) on variant subcolumns return empty results
 * even when the data exists and regular TERM search works correctly.
 *
 * Root cause: In FieldReaderResolver::resolve(), only EQUAL_QUERY is upgraded
 * to MATCH_ANY_QUERY for variant subcolumns with analyzers. WILDCARD_QUERY is
 * not upgraded, so it may select the wrong index reader (STRING_TYPE instead of
 * FULLTEXT), causing term enumeration to fail.
 *
 * Scenario: Contacts with firstname/lastname stored in variant subcolumns.
 * - TERM search for 'smith' correctly returns John Smith
 * - WILDCARD searches '*ith', 'sm*th', 'sm?th' should also match but returned empty
 */
suite("test_search_variant_wildcard", "p0") {
    def tableName = "test_search_variant_wildcard"

    sql """ set enable_match_without_inverted_index = false """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_enable_doc_mode = false """

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create table with variant column using field_pattern index
    sql """
        CREATE TABLE ${tableName} (
            `id` BIGINT NOT NULL,
            `props` variant<
                MATCH_NAME_GLOB 'string_*' : string,
                properties("variant_max_subcolumns_count" = "100")
            > NULL,
            INDEX idx_props (props) USING INVERTED PROPERTIES(
                "parser" = "unicode",
                "field_pattern" = "string_*",
                "lower_case" = "true"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """

    // Insert test data matching the reported scenario
    // string_8 = firstname, string_17 = lastname
    sql """INSERT INTO ${tableName} VALUES
        (73095521135, '{"string_8": "John", "string_17": "Smith"}'),
        (73095446198, '{"string_8": "Jane", "string_17": "Smithson"}'),
        (73095754047, '{"string_8": "Michael David", "string_17": "Johnson"}')
    """

    sql "sync"
    Thread.sleep(5000)

    // ============ Baseline: TERM search works ============

    // Test 1: TERM search for 'smith' on lastname - should return John Smith
    qt_term_smith """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('smith', '{"default_field":"props.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // Test 2: TERM search for 'smithson' on lastname - should return Jane Smithson
    qt_term_smithson """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('smithson', '{"default_field":"props.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // Test 3: TERM search for 'johnson' on lastname - should return Michael David Johnson
    qt_term_johnson """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('johnson', '{"default_field":"props.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // ============ Bug: Wildcard searches return empty ============

    // Test 4: Leading wildcard '*ith' - should match "Smith" (ends with "ith")
    qt_wildcard_star_ith """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('*ith', '{"default_field":"props.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // Test 5: Middle wildcard 'sm*th' - should match "Smith"
    qt_wildcard_sm_star_th """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('sm*th', '{"default_field":"props.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // Test 6: Single char wildcard 'sm?th' - should match "Smith"
    qt_wildcard_sm_q_th """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('sm?th', '{"default_field":"props.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // Test 7: Trailing wildcard 'smith*' - should match "Smith" and "Smithson"
    qt_wildcard_smith_star """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('smith*', '{"default_field":"props.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // Test 8: Wildcard 'sm*' - should match "Smith" and "Smithson"
    qt_wildcard_sm_star """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('sm*', '{"default_field":"props.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // Test 9: Wildcard '*son' - should match "Smithson" and "Johnson"
    qt_wildcard_star_son """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('*son', '{"default_field":"props.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // Test 10: Wildcard on firstname field 'jo?n' - should match "John"
    qt_wildcard_firstname """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('jo?n', '{"default_field":"props.string_8","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // Test 11: Wildcard combined with AND - 'sm*th AND props.string_8:john'
    qt_wildcard_and_term """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('props.string_17:sm*th AND props.string_8:john', '{"default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // Test 12: Standalone wildcard '*' matches all non-null values
    qt_wildcard_star_all """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('*', '{"default_field":"props.string_17","default_operator":"and","mode":"lucene"}')
        ORDER BY id
    """

    // Clean up
    sql "DROP TABLE IF EXISTS ${tableName}"
}
