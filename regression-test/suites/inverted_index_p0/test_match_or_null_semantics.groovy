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

suite("test_match_or_null_semantics") {
    // This test verifies the fix for the bug in InvertedIndexResultBitmap::operator|=()
    // in inverted_index_reader.h where NULL bitmaps were incorrectly combined using OR
    // instead of AND for MATCH syntax queries with enable_common_expr_pushdown=true
    //
    // Bug location: be/src/olap/rowset/segment_v2/inverted_index_reader.h:138
    // The bug caused rows with (TRUE OR NULL) to be incorrectly filtered out

    def tableName = "test_match_or_null_table"

    sql "DROP TABLE IF EXISTS ${tableName}"

    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title TEXT,
            content TEXT,
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    // Insert test data
    // Rows 1-15: title matches "Philosophy", content is NULL (TRUE OR NULL = TRUE)
    // Row 16: title doesn't match, content matches "Disney+ Hotstar" (FALSE OR TRUE = TRUE)
    // Rows 17-20: title doesn't match, content is NULL (FALSE OR NULL = NULL, excluded)
    sql """ INSERT INTO ${tableName} VALUES
        (1, 'Philosophy 101', NULL),
        (2, 'Ancient Philosophy', NULL),
        (3, 'Modern Philosophy', NULL),
        (4, 'Eastern Philosophy', NULL),
        (5, 'Western Philosophy', NULL),
        (6, 'Philosophy of Mind', NULL),
        (7, 'Philosophy of Science', NULL),
        (8, 'Philosophy Basics', NULL),
        (9, 'Greek Philosophy', NULL),
        (10, 'Medieval Philosophy', NULL),
        (11, 'Renaissance Philosophy', NULL),
        (12, 'Contemporary Philosophy', NULL),
        (13, 'Philosophy and Logic', NULL),
        (14, 'Philosophy Fundamentals', NULL),
        (15, 'Introduction to Philosophy', NULL),
        (16, 'Science Today', 'Disney+ Hotstar streaming service'),
        (17, 'Random Article', NULL),
        (18, 'Another Topic', NULL),
        (19, 'Sample Entry', NULL),
        (20, 'Test Data', NULL)
    """

    // Enable pushdown to trigger the bug in InvertedIndexResultBitmap::operator|=
    sql "SET enable_common_expr_pushdown = true"

    // Test 1: Core bug scenario - cross-field OR with NULL
    // Before fix: returned 1 row (only row 16, lost 15 rows with NULL content)
    // After fix: returns 16 rows (rows 1-16)
    def test1 = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney+ Hotstar'
    """

    assertEquals(16, test1[0][0], "MATCH should return 16 rows (15 with title match + 1 with content match)")
    logger.info("Test 1 PASSED: Cross-field OR with NULL - 16 rows returned")

    // Test 2: Verify the 15 critical rows with NULL content are included
    def test2 = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy'
          AND content IS NULL
    """

    assertEquals(15, test2[0][0], "Should have 15 rows with title match and NULL content")
    logger.info("Test 2 PASSED: 15 rows with NULL content correctly exist")

    // Test 3: Verify these 15 rows are included in the OR query
    def test3 = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy'
          AND content IS NULL
          AND (title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney+ Hotstar')
    """

    assertEquals(15, test3[0][0], "The 15 NULL content rows should be included (TRUE OR NULL = TRUE)")
    logger.info("Test 3 PASSED: TRUE OR NULL correctly returns TRUE")

    // Test 4: Three-way OR with NULL
    def test4 = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy'
           OR content MATCH_ALL 'Disney+ Hotstar'
           OR content MATCH_ALL 'streaming'
    """

    assertEquals(16, test4[0][0], "Three-way OR should also return 16 rows")
    logger.info("Test 4 PASSED: Three-way OR with NULL")

    // Test 5: Nested OR with NULL
    def test5 = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy'
           OR (content MATCH_ALL 'Disney+ Hotstar' OR content MATCH_ALL 'streaming')
    """

    assertEquals(16, test5[0][0], "Nested OR should return 16 rows")
    logger.info("Test 5 PASSED: Nested OR with NULL")

    // Test 6: OR within AND with NULL
    def test6 = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE id <= 10
          AND (title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney')
    """

    assertEquals(10, test6[0][0], "OR within AND should return 10 rows (rows 1-10 all have title match)")
    logger.info("Test 6 PASSED: OR within AND with NULL")

    // Test 7: NOT with OR and NULL (SQL three-valued logic)
    // Rows 1-16: OR = TRUE -> NOT TRUE = FALSE (excluded)
    // Rows 17-20: OR = NULL -> NOT NULL = NULL (excluded)
    def test7 = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE NOT (title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney+ Hotstar')
    """

    assertEquals(0, test7[0][0], "NOT OR should exclude all rows due to NULL semantics")
    logger.info("Test 7 PASSED: NOT OR with NULL correctly excludes all rows")

    // Test 8: Verify behavior without pushdown (should still work correctly)
    sql "SET enable_common_expr_pushdown = false"

    def test8 = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney+ Hotstar'
    """

    assertEquals(16, test8[0][0], "Should return 16 rows even without pushdown")
    logger.info("Test 8 PASSED: Query works correctly without pushdown")
    logger.info("All tests PASSED: MATCH OR NULL semantics work correctly")
}
