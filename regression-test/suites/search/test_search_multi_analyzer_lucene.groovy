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
 * Regression test for DORIS-24542:
 * search() in Lucene/scalar mode returns empty results when a column has
 * multiple inverted indexes with different analyzers (e.g., one default
 * untokenized index and one with an English parser).
 *
 * Root cause: BE FieldReaderResolver::resolve() did not use FE-provided
 * index_properties to select the correct reader, causing it to pick the
 * wrong (untokenized) index for tokenized queries.
 */
suite("test_search_multi_analyzer_lucene") {
    def tableName = "search_multi_analyzer_lucene_test"

    // Pin enable_common_expr_pushdown to prevent CI flakiness from fuzzy testing.
    sql """ set enable_common_expr_pushdown = true """

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Reproduce DORIS-24542: two inverted indexes on same column with different analyzers
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(255) NOT NULL,
            INDEX idx_title0 (title) USING INVERTED,
            INDEX idx_title3 (title) USING INVERTED PROPERTIES("lower_case" = "true", "parser" = "english", "support_phrase" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "inverted_index_storage_format" = "V2"
        )
    """

    sql """INSERT INTO ${tableName} VALUES
        (1, 'hello world'),
        (2, 'hello doris'),
        (3, 'world peace'),
        (4, 'foo bar baz')
    """

    Thread.sleep(3000)

    // Test 1: Lucene mode TERM query - should use tokenized index
    qt_lucene_term """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('hello', '{"default_field":"title", "default_operator":"AND", "mode":"lucene", "minimum_should_match": 0}')
        ORDER BY id
    """

    // Test 2: Lucene mode AND query
    qt_lucene_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('hello AND world', '{"default_field":"title", "default_operator":"AND", "mode":"lucene"}')
        ORDER BY id
    """

    // Test 3: Lucene mode OR query
    qt_lucene_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('hello OR peace', '{"default_field":"title", "default_operator":"OR", "mode":"lucene"}')
        ORDER BY id
    """

    // Test 4: Lucene mode wildcard query
    qt_lucene_wildcard """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('h*llo', '{"default_field":"title", "default_operator":"AND", "mode":"lucene", "minimum_should_match": 0}')
        ORDER BY id
    """

    // Test 5: Standard mode TERM query (non-lucene) with multi-index
    qt_standard_term """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:hello')
        ORDER BY id
    """

    // Test 6: Standard mode phrase query with multi-index
    qt_standard_phrase """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:"hello world"')
        ORDER BY id
    """

    // Test 7: Lucene mode phrase query with multi-index
    qt_lucene_phrase """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('"hello world"', '{"default_field":"title", "default_operator":"AND", "mode":"lucene"}')
        ORDER BY id
    """

    // Test 8: ANY clause with multi-index
    qt_any_multi_index """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:ANY(hello peace)')
        ORDER BY id
    """

    // Test 9: ALL clause with multi-index
    qt_all_multi_index """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${tableName}
        WHERE search('title:ALL(hello world)')
        ORDER BY id
    """

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"

    // =========================================================================
    // Untokenized-only index: verify WILDCARD/PREFIX/REGEXP still work correctly
    // when only a STRING_TYPE (no parser) index exists on the column.
    // select_best_reader() single-reader fast path must return the only reader
    // regardless of the MATCH_ANY_QUERY override.
    // =========================================================================
    def untokTable = "search_untokenized_only_test"
    sql "DROP TABLE IF EXISTS ${untokTable}"

    sql """
        CREATE TABLE ${untokTable} (
            id INT,
            title VARCHAR(255) NOT NULL,
            INDEX idx_title (title) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "inverted_index_storage_format" = "V2"
        )
    """

    sql """INSERT INTO ${untokTable} VALUES
        (1, 'hello world'),
        (2, 'hello doris'),
        (3, 'world peace'),
        (4, 'foo bar baz')
    """

    Thread.sleep(3000)

    // Test 10: Untokenized wildcard h*llo - no match because full string "hello world" != h*llo
    qt_untok_wildcard_no_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${untokTable}
        WHERE search('h*llo', '{"default_field":"title", "mode":"lucene"}')
        ORDER BY id
    """

    // Test 11: Untokenized wildcard hello* - matches full strings starting with "hello"
    qt_untok_wildcard_prefix """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${untokTable}
        WHERE search('hello*', '{"default_field":"title", "mode":"lucene"}')
        ORDER BY id
    """

    // Test 12: Untokenized wildcard *world - matches full strings ending with "world"
    qt_untok_wildcard_suffix """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${untokTable}
        WHERE search('*world', '{"default_field":"title", "mode":"lucene"}')
        ORDER BY id
    """

    // Test 13: Untokenized PREFIX hel* - matches full strings starting with "hel"
    qt_untok_prefix """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${untokTable}
        WHERE search('title:hel*', '{"mode":"lucene"}')
        ORDER BY id
    """

    // Test 14: Untokenized REGEXP hel.* - matches full strings matching regex
    qt_untok_regexp """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${untokTable}
        WHERE search('title:/hel.*/', '{"mode":"lucene"}')
        ORDER BY id
    """

    // Test 15: Untokenized exact phrase match
    qt_untok_exact """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title
        FROM ${untokTable}
        WHERE search('title:"hello world"', '{"mode":"lucene"}')
        ORDER BY id
    """

    // Cleanup
    sql "DROP TABLE IF EXISTS ${untokTable}"
}
