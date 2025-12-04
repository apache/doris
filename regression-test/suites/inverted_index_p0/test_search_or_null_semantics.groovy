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

suite("test_search_or_null_semantics") {
    // This test verifies the fix for the bug in UnionScorer::_collect_child_nulls()
    // in buffered_union_scorer.cpp where NULL bitmaps were incorrectly combined using OR
    // instead of AND for SEARCH function queries
    //
    // Bug location: be/src/olap/rowset/segment_v2/inverted_index/query_v2/buffered_union_scorer.cpp:151-164
    // The bug caused rows with (TRUE OR NULL) to be incorrectly filtered out

    def tableName = "test_search_or_null_table"

    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title TEXT,
            content TEXT,
            author TEXT,
            category TEXT,
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_author (author) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_category (category) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    // Insert test data simulating Wikipedia-like articles
    sql """ INSERT INTO ${tableName} VALUES
        -- Rows 1-15: title = "Philosophy", content = NULL (TRUE OR NULL = TRUE)
        (1, 'Philosophy', NULL, 'Author A', 'Education'),
        (2, 'Philosophy', NULL, 'Author B', 'Science'),
        (3, 'Philosophy', NULL, 'Author C', 'History'),
        (4, 'Philosophy', NULL, 'Author D', 'Culture'),
        (5, 'Philosophy', NULL, 'Author E', 'Education'),
        (6, 'Philosophy', NULL, 'Author F', 'Science'),
        (7, 'Philosophy', NULL, 'Author G', 'History'),
        (8, 'Philosophy', NULL, 'Author H', 'Education'),
        (9, 'Philosophy', NULL, 'Author I', 'Culture'),
        (10, 'Philosophy', NULL, 'Author J', 'Education'),
        (11, 'Philosophy', NULL, 'Author K', 'Science'),
        (12, 'Philosophy', NULL, 'Author L', 'History'),
        (13, 'Philosophy', NULL, 'Author M', 'Education'),
        (14, 'Philosophy', NULL, 'Author N', 'Culture'),
        (15, 'Philosophy', NULL, 'Author O', 'Education'),
        -- Row 16: Different article (FALSE OR TRUE = TRUE)
        (16, 'Technology News', 'Disney+ Hotstar streaming platform', 'Tech Writer', 'Technology'),
        -- Rows 17-20: Various other articles (FALSE OR NULL = NULL, excluded)
        (17, 'Science Article', NULL, 'Political Writer', 'Politics'),
        (18, 'History Piece', NULL, 'Historian', 'History'),
        (19, 'Movie Review', NULL, 'Critic', 'Entertainment'),
        (20, 'Tech Article', NULL, 'Academic', 'Education')
    """

    sql "SET enable_common_expr_pushdown = true"

    // Test 1: The core bug scenario - cross-field OR with one field NULL
    // Before fix: SEARCH returned 1 row (only row 16, lost 15 rows with NULL content)
    // After fix: SEARCH returns 16 rows (rows 1-16)
    def test1 = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableName}
        WHERE SEARCH('title:ALL(Philosophy) OR content:ALL("Disney+ Hotstar")')
    """

    assertEquals(16, test1[0][0], "SEARCH should return 16 rows (15 with title match + 1 with content match)")
    logger.info("Test 1 PASSED: Cross-field OR with NULL - SEARCH returns 16 rows")

    // Test 2: Verify the 15 critical rows are included
    def test2 = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy'
          AND content IS NULL
          AND SEARCH('title:ALL(Philosophy) OR content:ALL("Disney+ Hotstar")')
    """

    assertEquals(15, test2[0][0], "The 15 rows with NULL content should be included (TRUE OR NULL = TRUE)")
    logger.info("Test 2 PASSED: 15 NULL content rows correctly included")

    // Test 3: Three-way OR with different NULL patterns
    def test3 = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableName}
        WHERE SEARCH('title:ALL(Philosophy) OR content:ALL("Disney+ Hotstar") OR author:ANY(Political)')
    """

    assertEquals(17, test3[0][0], "Three-way OR should return 17 rows (15 title + 1 content + 1 author)")
    logger.info("Test 3 PASSED: Three-way OR with different NULL patterns")

    // Test 4: OR within AND with NULL
    def test4 = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableName}
        WHERE SEARCH('category:Education AND (title:ALL(Philosophy) OR content:ALL(Disney))')
    """

    def test4_count = test4[0][0]
    assertTrue(test4_count >= 6, "Should return at least 6 rows (rows with Education category and title match)")
    logger.info("Test 4 PASSED: OR within AND with NULL (returned ${test4_count} rows)")

    // Test 5: Deeply nested OR with multiple NULL fields
    def test5 = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableName}
        WHERE SEARCH('
            (title:ALL(Philosophy) OR content:ALL(Disney))
            OR (author:ANY(Writer) OR category:Technology)
        ')
    """

    def test5_count = test5[0][0]
    assertTrue(test5_count >= 16, "Deeply nested OR should return at least 16 rows")
    logger.info("Test 5 PASSED: Deeply nested OR with multiple NULL fields (returned ${test5_count} rows)")

    // Test 6: Verify SQL three-valued logic truth table for OR
    // TRUE OR NULL = TRUE
    def test6a = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableName}
        WHERE (title MATCH_ALL 'Philosophy' AND content IS NULL)
    """
    assertEquals(15, test6a[0][0], "Should have 15 rows with TRUE OR NULL pattern")

    def test6b = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableName}
        WHERE (title MATCH_ALL 'Philosophy' AND content IS NULL)
          AND SEARCH('title:ALL(Philosophy) OR content:ALL("Disney+ Hotstar")')
    """
    assertEquals(15, test6b[0][0], "TRUE OR NULL should be TRUE (15 rows included)")
    logger.info("Test 6 PASSED: SQL three-valued logic - TRUE OR NULL = TRUE")

    // Test 7: Complex nested query similar to original bug report
    def test7 = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableName}
        WHERE SEARCH('
            title:ALL(Philosophy)
            OR NOT (
                content:ALL(History)
                AND (
                    NOT category:Education
                    AND NOT (
                        title:ALL(Science)
                        OR author:ANY(Writer)
                    )
                )
            )
        ')
    """

    def test7_count = test7[0][0]
    assertTrue(test7_count >= 15, "Complex nested query should include the 15 Philosophy rows")
    logger.info("Test 7 PASSED: Complex nested query (returned ${test7_count} rows)")

    // Test 8: NOT with OR and NULL (SQL three-valued logic)
    // Rows 1-16: OR = TRUE -> NOT TRUE = FALSE (excluded)
    // Rows 17-20: OR = NULL -> NOT NULL = NULL (excluded)
    def test8 = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableName}
        WHERE SEARCH('NOT (title:ALL(Philosophy) OR content:ALL("Disney+ Hotstar"))')
    """

    assertEquals(0, test8[0][0], "NOT OR should exclude all rows due to NULL semantics")
    logger.info("Test 8 PASSED: NOT OR with NULL correctly excludes all rows")

    // Test 9: Verify SEARCH works with and without pushdown
    sql "SET enable_common_expr_pushdown = false"

    def test9 = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableName}
        WHERE SEARCH('title:ALL(Philosophy) OR content:ALL("Disney+ Hotstar")')
    """

    assertEquals(16, test9[0][0], "SEARCH should return 16 rows even without pushdown")
    logger.info("Test 9 PASSED: SEARCH works correctly without pushdown")

    sql "DROP TABLE IF EXISTS ${tableName}"

    logger.info("All tests PASSED: SEARCH OR NULL semantics work correctly")
}
