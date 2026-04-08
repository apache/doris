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
 * Test search() function with variant subcolumn and field_pattern index.
 *
 * This test verifies that the analyzer (parser) from field_pattern matched indexes
 * is correctly applied when using search() on variant subcolumns.
 *
 * Bug: When using search() on variant subcolumns with field_pattern indexes,
 * the analyzer was not applied because FE did not pass index properties to BE.
 * This caused exact-match-only behavior instead of tokenized matching.
 *
 * Fix: FE now looks up the Index for each field in SearchExpression and passes
 * the index_properties via TSearchFieldBinding to BE.
 */
suite("test_search_variant_subcolumn_analyzer", "p0") {
    def tableName = "test_variant_subcolumn_analyzer"

    sql """ set enable_match_without_inverted_index = false """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    // Pin doc_mode to false to prevent CI flakiness from fuzzy testing.
    // When default_variant_enable_doc_mode=true (randomly set by fuzzy testing),
    // variant subcolumns are stored in document mode, causing inverted index
    // iterators to be unavailable in BE for search() evaluation.
    sql """ set default_variant_enable_doc_mode = false """

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create table with variant column using predefined field pattern and field_pattern index
    sql """
        CREATE TABLE ${tableName} (
            `id` INT NOT NULL,
            `data` variant<
                MATCH_NAME_GLOB 'string_*' : string,
                properties("variant_max_subcolumns_count" = "100")
            > NULL,
            INDEX idx_text (data) USING INVERTED PROPERTIES(
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

    // Insert test data
    sql """INSERT INTO ${tableName} VALUES
        (1, '{"string_8": "admin only"}'),
        (2, '{"string_8": "user access"}'),
        (3, '{"string_8": "admin access granted"}'),
        (4, '{"string_1": "hello world"}'),
        (5, '{"string_8": "readonly user"}'),
        (6, '{"number_1": 42}')
    """

    // Wait for data to be flushed and indexes built
    sql "sync"
    Thread.sleep(5000)

    // Test 1: search() with default_field on variant subcolumn matching field_pattern
    // "admin" should match "admin only" and "admin access granted" because the unicode
    // parser tokenizes them into ["admin", "only"] and ["admin", "access", "granted"]
    qt_search_variant_analyzer_basic """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('admin', '{"default_field":"data.string_8","mode":"lucene"}')
        ORDER BY id
    """

    // Test 2: Verify MATCH also works (as a baseline)
    qt_match_variant_baseline """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE data['string_8'] MATCH_ANY 'admin'
        ORDER BY id
    """

    // Test 3: Multi-term search should also work with tokenization
    qt_search_variant_analyzer_multi """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('admin access', '{"default_field":"data.string_8","mode":"lucene","default_operator":"AND"}')
        ORDER BY id
    """

    // Test 4: Search on a different subcolumn matching the same field_pattern
    qt_search_variant_analyzer_other_field """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('hello', '{"default_field":"data.string_1","mode":"lucene"}')
        ORDER BY id
    """

    // Test 5: Search with field-qualified syntax on variant subcolumn
    qt_search_variant_analyzer_field_syntax """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('data.string_8:user', '{"mode":"lucene"}')
        ORDER BY id
    """

    // Test 6: Verify lowercase is applied (search for "ADMIN" should match "admin only")
    qt_search_variant_analyzer_lowercase """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('ADMIN', '{"default_field":"data.string_8","mode":"lucene"}')
        ORDER BY id
    """

    // Test 7: Phrase search on variant subcolumn with analyzer
    qt_search_variant_analyzer_phrase """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName}
        WHERE search('"admin only"', '{"default_field":"data.string_8","mode":"lucene"}')
        ORDER BY id
    """

    // Clean up
    sql "DROP TABLE IF EXISTS ${tableName}"

    // Test Case 2: Variant with direct named field and field_pattern index for comparison
    def tableName2 = "test_variant_direct_index"

    sql "DROP TABLE IF EXISTS ${tableName2}"

    sql """
        CREATE TABLE ${tableName2} (
            `id` INT NOT NULL,
            `data` variant<
                'name' : string,
                properties("variant_max_subcolumns_count" = "10")
            > NULL,
            INDEX idx_text (data) USING INVERTED PROPERTIES(
                "parser" = "unicode",
                "field_pattern" = "name",
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

    sql """INSERT INTO ${tableName2} VALUES
        (1, '{"name": "admin only"}'),
        (2, '{"name": "user access"}'),
        (3, '{"name": "admin access granted"}')
    """

    sql "sync"
    Thread.sleep(5000)

    // Test 8: search() on variant subcolumn with named field_pattern (direct match)
    qt_search_variant_direct_index """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id FROM ${tableName2}
        WHERE search('admin', '{"default_field":"data.name","mode":"lucene"}')
        ORDER BY id
    """

    sql "DROP TABLE IF EXISTS ${tableName2}"

    logger.info("All variant subcolumn analyzer tests completed!")
}
