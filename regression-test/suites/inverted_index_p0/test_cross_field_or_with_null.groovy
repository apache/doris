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

suite("test_cross_field_or_with_null") {
    // This test verifies the fix for the bug where OR queries with NULL values
    // incorrectly filtered out rows where "TRUE OR NULL" should evaluate to TRUE

    def tableName = "test_cross_field_or_null_table"

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create table with inverted indexes on multiple fields
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(200),
            content TEXT,
            category VARCHAR(100),
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_category (category) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    // Insert test data with specific NULL patterns
    // Rows 1-15: title matches "Philosophy", content is NULL
    // Row 16: both title and content have values
    // Rows 17-20: content is NULL, title does not match
    sql """ INSERT INTO ${tableName} VALUES
        (1, 'Philosophy 101', NULL, 'Education'),
        (2, 'Ancient Philosophy', NULL, 'History'),
        (3, 'Modern Philosophy', NULL, 'Education'),
        (4, 'Eastern Philosophy', NULL, 'Culture'),
        (5, 'Western Philosophy', NULL, 'Culture'),
        (6, 'Philosophy of Mind', NULL, 'Science'),
        (7, 'Philosophy of Science', NULL, 'Science'),
        (8, 'Philosophy Basics', NULL, 'Education'),
        (9, 'Greek Philosophy', NULL, 'History'),
        (10, 'Medieval Philosophy', NULL, 'History'),
        (11, 'Renaissance Philosophy', NULL, 'History'),
        (12, 'Contemporary Philosophy', NULL, 'Education'),
        (13, 'Philosophy and Logic', NULL, 'Science'),
        (14, 'Philosophy Fundamentals', NULL, 'Education'),
        (15, 'Introduction to Philosophy', NULL, 'Education'),
        (16, 'Science Today', 'Disney+ Hotstar streaming service', 'Technology'),
        (17, 'Random Article', NULL, 'News'),
        (18, 'Another Topic', NULL, 'General'),
        (19, 'Sample Entry', NULL, 'Misc'),
        (20, 'Test Data', NULL, 'Test')
    """

    // Test 1: Simple cross-field OR with NULL
    // title MATCH "Philosophy" OR content MATCH "Disney+ Hotstar"
    // Expected: Rows 1-16 (rows 1-15 have title match, row 16 has content match)
    sql "SET enable_common_expr_pushdown = true"

    def result1_match = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney+ Hotstar'
    """

    def result1_search = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE SEARCH('title:ALL(Philosophy) OR content:ALL("Disney+ Hotstar")')
    """

    // Both should return 16 rows
    assertEquals(16, result1_match[0][0])
    assertEquals(16, result1_search[0][0])

    logger.info("Test 1 passed: Simple cross-field OR with NULL")

    // Test 2: Verify that the 15 NULL content rows with matching title are included
    def result2 = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy'
          AND content IS NULL
          AND (title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney+ Hotstar')
    """

    assertEquals(15, result2[0][0])
    logger.info("Test 2 passed: Verified 15 rows with NULL content are included")

    // Test 3: Complex nested OR query
    // (title MATCH "Philosophy" OR (content MATCH "Disney" OR content MATCH "streaming"))
    def result3_match = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy'
           OR (content MATCH_ALL 'Disney' OR content MATCH_ALL 'streaming')
    """

    def result3_search = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE SEARCH('title:ALL(Philosophy) OR (content:ALL(Disney) OR content:ALL(streaming))')
    """

    assertEquals(result3_match[0][0], result3_search[0][0])
    logger.info("Test 3 passed: Complex nested OR query")

    // Test 4: NOT with cross-field OR
    // NOT (title MATCH "Philosophy" OR content MATCH "Disney+ Hotstar")
    // Rows 1-16: title matches or content matches -> OR = TRUE -> NOT TRUE = FALSE (excluded)
    // Rows 17-20: title doesn't match (FALSE) and content is NULL -> FALSE OR NULL = NULL -> NOT NULL = NULL (excluded)
    // Expected: 0 rows (all rows are either TRUE or NULL in the OR, none are FALSE)
    def result4_match = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE NOT (title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney+ Hotstar')
    """

    def result4_search = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE SEARCH('NOT (title:ALL(Philosophy) OR content:ALL("Disney+ Hotstar"))')
    """

    assertEquals(0, result4_match[0][0])  // All rows excluded due to NULL semantics
    assertEquals(result4_match[0][0], result4_search[0][0])
    logger.info("Test 4 passed: NOT with cross-field OR (NULL semantics correctly exclude rows)")

    // Test 5: AND with cross-field OR containing NULL
    // category = "Education" AND (title MATCH "Philosophy" OR content MATCH "Disney")
    def result5_match = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE category MATCH 'Education'
          AND (title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney')
    """

    def result5_search = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE SEARCH('category:Education AND (title:ALL(Philosophy) OR content:ALL(Disney))')
    """

    assertEquals(result5_match[0][0], result5_search[0][0])
    logger.info("Test 5 passed: AND with cross-field OR containing NULL")

    // Test 6: Test with pushdown disabled (should also work correctly)
    sql "SET enable_common_expr_pushdown = false"

    def result6_nopush = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney+ Hotstar'
    """

    assertEquals(16, result6_nopush[0][0])
    logger.info("Test 6 passed: Query works correctly without pushdown")

    // Test 7: Multiple OR conditions with different NULL patterns
    sql "SET enable_common_expr_pushdown = true"

    def result7_match = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy'
           OR content MATCH_ALL 'Disney+ Hotstar'
           OR category MATCH 'Technology'
    """

    def result7_search = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE SEARCH('title:ALL(Philosophy) OR content:ALL("Disney+ Hotstar") OR category:Technology')
    """

    assertEquals(result7_match[0][0], result7_search[0][0])
    logger.info("Test 7 passed: Multiple OR conditions with different NULL patterns")

    // Test 8: Verify individual field queries work correctly
    def result8_title = sql "SELECT COUNT(*) FROM ${tableName} WHERE title MATCH_ALL 'Philosophy'"
    def result8_content_null = sql "SELECT COUNT(*) FROM ${tableName} WHERE content IS NULL"

    assertEquals(15, result8_title[0][0])
    assertEquals(19, result8_content_null[0][0])  // Rows 1-15, 17-20
    logger.info("Test 8 passed: Individual field queries return expected counts")

    sql "DROP TABLE IF EXISTS ${tableName}"
}
