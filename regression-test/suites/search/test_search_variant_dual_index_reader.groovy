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
 * Regression test for variant subcolumn with dual inverted indexes on the same field pattern.
 *
 * Bug scenario: When a variant column has two indexes on the same field_pattern (e.g. "string_*"):
 *   - idx_no_analyzer:   no parser   -> STRING_TYPE reader (untokenized)
 *   - idx_with_analyzer: parser=xxx  -> FULLTEXT reader (tokenized)
 *
 * FE correctly resolves the field to the analyzer-based index and sends its index_properties
 * via TSearchFieldBinding. However, BE's FieldReaderResolver::resolve() called
 * select_best_reader(column_type, EQUAL_QUERY, "") which preferred STRING_TYPE for EQUAL_QUERY.
 * This opened the untokenized index directory, so tokenized search terms never matched.
 *
 * Fix: For variant subcolumns, when FE provides index_properties indicating an analyzer,
 * upgrade EQUAL_QUERY to MATCH_ANY_QUERY before reader selection so the FULLTEXT reader is chosen.
 *
 * Before fix: search() returns empty (wrong reader selected)
 * After fix:  search() returns matching rows (correct FULLTEXT reader selected)
 */
suite("test_search_variant_dual_index_reader", "p0") {
    def tableName = "test_variant_dual_index_reader"

    sql """ set enable_match_without_inverted_index = false """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    // Pin doc_mode to false to prevent CI flakiness from fuzzy testing.
    sql """ set default_variant_enable_doc_mode = false """

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create table with variant column and TWO indexes on the same field_pattern:
    // one without analyzer (STRING_TYPE) and one with analyzer (FULLTEXT).
    // This is the exact scenario that triggers the bug.
    sql """
        CREATE TABLE ${tableName} (
            `id` INT NOT NULL,
            `props` variant<
                MATCH_NAME_GLOB 'string_*' : string,
                properties("variant_max_subcolumns_count" = "100")
            > NULL,
            INDEX idx_no_analyzer (props) USING INVERTED PROPERTIES(
                "field_pattern" = "string_*"
            ),
            INDEX idx_with_analyzer (props) USING INVERTED PROPERTIES(
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

    sql """INSERT INTO ${tableName} VALUES
        (1, '{"string_8": "admin user"}'),
        (2, '{"string_8": "readonly access"}'),
        (3, '{"string_8": "admin access granted"}'),
        (4, '{"string_1": "hello world"}'),
        (5, '{"string_8": "guest only"}')
    """

    sql "sync"
    Thread.sleep(5000)

    // Test 1: Basic tokenized search on variant subcolumn with dual indexes.
    // "admin" should match rows 1 and 3 via the FULLTEXT reader (tokenized).
    // Before fix: returns empty because EQUAL_QUERY selects STRING_TYPE reader.
    // After fix: returns rows 1, 3 because MATCH_ANY_QUERY selects FULLTEXT reader.
    qt_dual_index_basic """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('admin', '{"default_field":"props.string_8","mode":"lucene"}')
        ORDER BY id
    """

    // Test 2: Multi-term AND search. Both "admin" and "access" must match.
    // Before fix: empty. After fix: row 3.
    qt_dual_index_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('admin access', '{"default_field":"props.string_8","mode":"lucene","default_operator":"AND"}')
        ORDER BY id
    """

    // Test 3: Search on a different subcolumn matching the same field_pattern.
    // Ensures the fix works across different subcolumns under the same pattern.
    qt_dual_index_other_field """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('hello', '{"default_field":"props.string_1","mode":"lucene"}')
        ORDER BY id
    """

    // Test 4: Field-qualified syntax with dual indexes.
    qt_dual_index_field_syntax """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('props.string_8:access', '{"mode":"lucene"}')
        ORDER BY id
    """

    // Test 5: Case-insensitive search (lowercase index).
    // "ADMIN" should match "admin user" and "admin access granted".
    qt_dual_index_case_insensitive """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('ADMIN', '{"default_field":"props.string_8","mode":"lucene"}')
        ORDER BY id
    """

    // Test 6: Verify MATCH_ANY also works as baseline (uses MATCH query type directly,
    // so it always picks FULLTEXT reader — this should work both before and after the fix).
    qt_dual_index_match_baseline """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE props['string_8'] MATCH_ANY 'admin'
        ORDER BY id
    """

    sql "DROP TABLE IF EXISTS ${tableName}"
}
