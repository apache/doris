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

// DORIS-24464: search() REGEXP with lower_case=true should be consistent with match_regexp
// Regex patterns are NOT lowercased (matching ES query_string behavior).
// Wildcard patterns ARE lowercased (matching ES query_string normalizer behavior).

suite("test_search_regexp_lowercase", "p0") {
    def tableName = "search_regexp_lowercase_test"

    // Pin enable_common_expr_pushdown to prevent CI flakiness from fuzzy testing.
    sql """ set enable_common_expr_pushdown = true """

    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            a INT,
            title VARCHAR(512) NOT NULL,
            INDEX idx_title (title) USING INVERTED PROPERTIES("lower_case" = "true", "parser" = "english", "support_phrase" = "true")
        ) ENGINE=OLAP
        DUPLICATE KEY(a)
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql "INSERT INTO ${tableName} VALUES(1, 'ABC DEF')"
    sql "INSERT INTO ${tableName} VALUES(2, 'abc def')"
    sql "INSERT INTO ${tableName} VALUES(3, 'Apple Banana Cherry')"
    sql "INSERT INTO ${tableName} VALUES(4, 'apple banana cherry')"

    // Wait for data to be ready
    Thread.sleep(5000)

    // =========================================================================
    // Test 1: REGEXP with uppercase pattern should NOT match lowercased terms
    // (ES-compatible behavior: regex patterns are not analyzed/lowercased)
    // =========================================================================

    // search() REGEXP with uppercase pattern - should return 0 rows
    // because indexed terms are lowercased (abc, def) but pattern AB.* is case-sensitive
    qt_regexp_uppercase_no_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ * FROM ${tableName}
        WHERE search('/AB.*/', '{"default_field":"title","default_operator":"AND","mode":"lucene", "minimum_should_match": 0}')
        ORDER BY a
    """

    // match_regexp with uppercase pattern - should also return 0 rows
    qt_match_regexp_uppercase_no_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ * FROM ${tableName}
        WHERE title match_regexp 'AB.*'
        ORDER BY a
    """

    // =========================================================================
    // Test 2: REGEXP with lowercase pattern SHOULD match lowercased terms
    // =========================================================================

    // search() REGEXP with lowercase pattern - should match both rows with "abc"
    qt_regexp_lowercase_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ * FROM ${tableName}
        WHERE search('/ab.*/', '{"default_field":"title","default_operator":"AND","mode":"lucene", "minimum_should_match": 0}')
        ORDER BY a
    """

    // match_regexp with lowercase pattern - should also match
    qt_match_regexp_lowercase_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ * FROM ${tableName}
        WHERE title match_regexp 'ab.*'
        ORDER BY a
    """

    // =========================================================================
    // Test 3: WILDCARD with uppercase pattern should match (wildcards ARE lowercased)
    // =========================================================================

    // search() WILDCARD with uppercase - should match because wildcard patterns are lowercased
    qt_wildcard_uppercase_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ * FROM ${tableName}
        WHERE search('AB*', '{"default_field":"title","default_operator":"AND","mode":"lucene", "minimum_should_match": 0}')
        ORDER BY a
    """

    // search() WILDCARD with lowercase - should also match
    qt_wildcard_lowercase_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ * FROM ${tableName}
        WHERE search('ab*', '{"default_field":"title","default_operator":"AND","mode":"lucene", "minimum_should_match": 0}')
        ORDER BY a
    """

    // =========================================================================
    // Test 4: More complex REGEXP patterns
    // =========================================================================

    // Lowercase regex that matches "apple" - should match rows 3 and 4
    qt_regexp_apple_lowercase """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ * FROM ${tableName}
        WHERE search('/app.*/', '{"default_field":"title","default_operator":"AND","mode":"lucene", "minimum_should_match": 0}')
        ORDER BY a
    """

    // Uppercase regex "App.*" should NOT match (terms are lowercased as "apple")
    qt_regexp_apple_uppercase_no_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ * FROM ${tableName}
        WHERE search('/App.*/', '{"default_field":"title","default_operator":"AND","mode":"lucene", "minimum_should_match": 0}')
        ORDER BY a
    """

    // =========================================================================
    // Test 5: REGEXP consistency with match_regexp for various patterns
    // =========================================================================

    // Both should return same results for lowercase pattern
    qt_consistency_regexp_cherry """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ a FROM ${tableName}
        WHERE search('/cher.*/', '{"default_field":"title","default_operator":"AND","mode":"lucene", "minimum_should_match": 0}')
        ORDER BY a
    """

    qt_consistency_match_regexp_cherry """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ a FROM ${tableName}
        WHERE title match_regexp 'cher.*'
        ORDER BY a
    """

    // Both should return 0 rows for uppercase pattern
    qt_consistency_regexp_cherry_upper """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ a FROM ${tableName}
        WHERE search('/CHER.*/', '{"default_field":"title","default_operator":"AND","mode":"lucene", "minimum_should_match": 0}')
        ORDER BY a
    """

    qt_consistency_match_regexp_cherry_upper """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ a FROM ${tableName}
        WHERE title match_regexp 'CHER.*'
        ORDER BY a
    """

    sql "DROP TABLE IF EXISTS ${tableName}"
}
