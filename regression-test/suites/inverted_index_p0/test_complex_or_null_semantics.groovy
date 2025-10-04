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

suite("test_complex_or_null_semantics") {
    // This regression test verifies the fix for complex nested OR queries with NULL values
    // Based on the real-world bug where MATCH and SEARCH returned different results

    def tableName = "test_complex_or_null_table"

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
        -- Rows 1-15: title = "Philosophy", content = NULL
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
        -- Row 16: Different article
        (16, 'Technology News', 'Disney+ Hotstar streaming platform', 'Tech Writer', 'Technology'),
        -- Rows 17-30: Various other articles with content
        (17, 'Science Article', 'President of the United States speech', 'Political Writer', 'Politics'),
        (18, 'History Piece', 'List of presidents of India', 'Historian', 'History'),
        (19, 'Movie Review', 'Braveheart film analysis', 'Critic', 'Entertainment'),
        (20, 'Tech Article', 'University of Southern California research', 'Academic', 'Education'),
        (21, 'Biography', 'Elliot Page interview', 'Entertainment Writer', 'Celebrity'),
        (22, 'History Document', 'Archduke Franz Ferdinand of Austria', 'Historian', 'History'),
        (23, 'Film Review', 'Renée Zellweger performance', 'Critic', 'Movies'),
        (24, 'Sports', 'Randy Savage wrestling career', 'Sports Writer', 'Sports'),
        (25, 'Random', 'Cindy story', 'Random Writer', 'General'),
        (26, 'Wildlife', 'Bindi Irwin conservation work', 'Nature Writer', 'Environment'),
        (27, 'Job Portal', 'Indeed job listings', 'HR Writer', 'Business'),
        (28, 'Short Story', 'Boy adventure tale', 'Fiction Writer', 'Literature'),
        (29, 'Generic', 'Article about of preposition', 'Grammar Writer', 'Language'),
        (30, 'Misc', 'Random content', 'Anonymous', 'Misc')
    """

    sql "SET enable_common_expr_pushdown = true"

    // Test 1: The core bug scenario - cross-field OR with one field NULL
    // title:ALL("Philosophy") OR content:ALL("Disney+ Hotstar")
    // Should match rows 1-16 (15 with title match despite NULL content, 1 with content match)

    def test1_match = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney+ Hotstar'
    """

    def test1_search = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE SEARCH('title:ALL(Philosophy) OR content:ALL("Disney+ Hotstar")')
    """

    assertEquals(16, test1_match[0][0], "MATCH should return 16 rows")
    assertEquals(16, test1_search[0][0], "SEARCH should return 16 rows")
    assertEquals(test1_match[0][0], test1_search[0][0], "MATCH and SEARCH should return same count")

    logger.info("Test 1 PASSED: Cross-field OR with NULL - MATCH and SEARCH consistent")

    // Test 2: Complex nested query similar to original bug report
    def test2_match = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE content MATCH_ANY 'President of the United States'
           OR NOT (
                content MATCH 'Braveheart'
                AND (
                    NOT content MATCH_ALL 'List of presidents of India'
                    AND NOT (
                        title MATCH_ALL 'Philosophy'
                        OR content MATCH_ALL 'Disney+ Hotstar'
                    )
                )
            )
    """

    def test2_search = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE SEARCH('
            content:ANY("President of the United States")
            OR NOT (
                content:Braveheart
                AND (
                    NOT content:ALL("List of presidents of India")
                    AND NOT (
                        title:ALL(Philosophy)
                        OR content:ALL("Disney+ Hotstar")
                    )
                )
            )
        ')
    """

    assertEquals(test2_match[0][0], test2_search[0][0],
        "Complex nested query: MATCH and SEARCH should return same count")

    logger.info("Test 2 PASSED: Complex nested query - MATCH and SEARCH consistent (count: ${test2_match[0][0]})")

    // Test 3: Verify the 15 critical rows are included
    def test3 = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy'
          AND content IS NULL
          AND SEARCH('title:ALL(Philosophy) OR content:ALL("Disney+ Hotstar")')
    """

    assertEquals(15, test3[0][0], "The 15 rows with NULL content should be included")

    logger.info("Test 3 PASSED: 15 NULL content rows correctly included")

    // Test 4: Three-way OR with different NULL patterns
    def test4_match = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE title MATCH_ALL 'Philosophy'
           OR content MATCH_ALL 'Disney+ Hotstar'
           OR author MATCH_ANY 'Political'
    """

    def test4_search = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE SEARCH('title:ALL(Philosophy) OR content:ALL("Disney+ Hotstar") OR author:ANY(Political)')
    """

    assertEquals(test4_match[0][0], test4_search[0][0], "Three-way OR should be consistent")

    logger.info("Test 4 PASSED: Three-way OR with different NULL patterns")

    // Test 5: OR within AND with NULL
    def test5_match = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE category MATCH 'Education'
          AND (title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney')
    """

    def test5_search = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE SEARCH('category:Education AND (title:ALL(Philosophy) OR content:ALL(Disney))')
    """

    assertEquals(test5_match[0][0], test5_search[0][0], "OR within AND should be consistent")

    logger.info("Test 5 PASSED: OR within AND with NULL")

    // Test 6: Deeply nested OR with multiple NULL fields
    def test6_match = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE (
            (title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney')
            OR (author MATCH_ANY 'Writer' OR category MATCH 'Technology')
        )
    """

    def test6_search = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE SEARCH('
            (title:ALL(Philosophy) OR content:ALL(Disney))
            OR (author:ANY(Writer) OR category:Technology)
        ')
    """

    assertEquals(test6_match[0][0], test6_search[0][0], "Deeply nested OR should be consistent")

    logger.info("Test 6 PASSED: Deeply nested OR with multiple NULL fields (count: ${test6_match[0][0]})")

    // Test 7: Verify SQL three-valued logic truth table for OR
    // TRUE OR NULL = TRUE
    def test7a = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE (title MATCH_ALL 'Philosophy' AND content IS NULL)
          AND (title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney+ Hotstar')
    """
    assertEquals(15, test7a[0][0], "TRUE OR NULL should be TRUE (15 rows)")

    // FALSE OR NULL = NULL (excluded from WHERE clause)
    def test7b = sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE (title NOT MATCH_ALL 'Philosophy' AND content IS NULL)
          AND (title MATCH_ALL 'Philosophy' OR content MATCH_ALL 'Disney+ Hotstar')
    """
    assertEquals(0, test7b[0][0], "FALSE OR NULL should be NULL (0 rows)")

    logger.info("Test 7 PASSED: SQL three-valued logic truth table verified")

    // Test 8: Performance - ensure both use inverted index
    sql "SET enable_profile = true"

    sql """
        SELECT COUNT(*) FROM ${tableName}
        WHERE SEARCH('title:ALL(Philosophy) OR content:ALL("Disney+ Hotstar")')
    """

    // Just ensure query completes without error (actual profile checking would be more complex)
    logger.info("Test 8 PASSED: Query execution successful with profiling enabled")

    sql "SET enable_profile = false"
    logger.info("All tests PASSED: Complex OR NULL semantics work correctly")
}
