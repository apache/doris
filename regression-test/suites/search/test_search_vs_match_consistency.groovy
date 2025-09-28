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

suite("test_search_vs_match_consistency") {
    def tableName = "search_match_consistency_test"

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create test table similar to wikipedia structure
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(500),
            content TEXT,
            author VARCHAR(200),
            tags VARCHAR(300),
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_author (author) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_tags (tags) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    // Insert comprehensive test data
    sql """
        INSERT INTO ${tableName} VALUES
        -- Basic matching cases
        (1, 'Ronald Reagan Biography', 'Detailed biography of Ronald Reagan', 'John Smith', 'biography,president'),
        (2, 'Selma Blair Movies', 'Complete filmography of Selma Blair', 'Jane Doe', 'actress,movies'),
        (3, 'Round Table History', 'History of the Round Table knights', 'Bob Wilson', 'history,medieval'),

        -- NULL in title field
        (4, NULL, 'Biography about Ronald McDonald and his Round adventures', 'Alice Brown', 'food,mascot'),
        (5, NULL, 'Selma Blair interview about Round performances', 'Carol White', 'interview,entertainment'),

        -- NULL in content field
        (6, 'Ronald McDonald Story', NULL, 'David Black', 'mascot,food'),
        (7, 'Selma Montgomery March', NULL, 'Eva Green', 'history,civil-rights'),

        -- NULL in multiple fields
        (8, NULL, NULL, 'Frank Blue', 'unknown'),
        (9, 'Mystery Title', 'No keywords matching our tests', NULL, 'mystery'),

        -- Complex matching scenarios
        (10, 'Ronald and Selma Story', 'A story about both Ronald Reagan and Selma Blair meeting at a Round table', 'Grace Red', 'fiction,crossover'),
        (11, 'Not Round at All', 'This content specifically avoids the Round keyword', 'Henry Yellow', 'geometry,shapes'),
        (12, 'Partial Match Test', 'Contains Selma but not Blair, and Ronald but not Reagan', 'Ivy Purple', 'partial'),

        -- Edge cases
        (13, '', 'Empty title test with Round content', 'Jack Orange', 'test'),
        (14, 'Non-empty title', '', 'Kate Pink', 'test'),
        (15, 'All Fields Present', 'All fields have content including Round', 'Leo Cyan', 'complete')
    """

    // Wait for data to be ready
    Thread.sleep(5000)

    // Test Suite 1: Basic OR query consistency
    qt_test_1_1_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('title:Ronald or title:Selma')
    """

    qt_test_1_1_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE title match "Ronald" or title match "Selma"
    """

    // Test 1.2: OR across different fields
    qt_test_1_2_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('title:Ronald or content:Selma')
    """

    qt_test_1_2_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE title match "Ronald" or content match "Selma"
    """

    // Test 1.3: Complex OR with ALL operation
    qt_test_1_3_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('title:Ronald or (content:ALL(Selma Blair))')
    """

    qt_test_1_3_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE title match "Ronald" or (content match_all "Selma Blair")
    """

    // Test Suite 2: NOT query consistency
    qt_test_2_1_internal_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('not content:Round')
    """

    qt_test_2_1_external_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE not search('content:Round')
    """

    // Test 2.2: NOT with different fields
    qt_test_2_2_internal_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('not title:Ronald')
    """

    qt_test_2_2_external_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE not search('title:Ronald')
    """

    // Test 2.3: NOT with complex expression
    qt_test_2_3_internal_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('not (title:Ronald and content:biography)')
    """

    qt_test_2_3_external_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE not search('title:Ronald and content:biography')
    """

    // Test Suite 3: NULL value behavior in OR queries
    qt_test_3_1_or_with_null """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName}
        WHERE search('title:NonExistent or content:Ronald')
        ORDER BY id
    """

    qt_test_3_2_or_multiple_null """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName}
        WHERE search('title:Mystery or content:Round')
        ORDER BY id
    """

    // Test Suite 4: AND query behavior with NULLs
    qt_test_4_1_and_with_null """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName}
        WHERE search('title:Ronald and content:biography')
        ORDER BY id
    """

    // Test Suite 5: Edge cases and complex scenarios
    qt_test_5_1_empty_string """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('title:"" or content:Round')
    """

    qt_test_5_2_complex_nested """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('(title:Ronald or title:Selma) and not (content:Round and author:NonExistent)')
    """

    // Test Suite 6: Performance and consistency verification
    qt_test_6_1_large_or_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('title:Ronald or title:Selma or content:Round or content:biography or author:Smith or tags:history')
    """

    qt_test_6_1_large_or_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE title match "Ronald" or title match "Selma" or content match "Round" or content match "biography" or author match "Smith" or tags match "history"
    """
}