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

suite("test_search_null_regression", "p0") {
    def tableName = "search_null_regression_test"

    // Pin enable_common_expr_pushdown to prevent CI flakiness from fuzzy testing.
    sql """ set enable_common_expr_pushdown = true """

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create test table that reproduces the original bug scenarios
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(500),
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

    // Insert data that reproduces the original issue scenarios
    sql """
        INSERT INTO ${tableName} VALUES
        -- Scenario 1: Records that should match in OR queries
        (1, 'Ronald Reagan', 'Biography of the 40th president'),
        (2, 'Movie Review', 'Selma Blair gives an outstanding performance'),
        (3, NULL, 'A story about Ronald McDonald at the Round table'),
        (4, 'Selma Montgomery', NULL),
        (5, 'Biography', 'The life of Selma Blair, a talented actress'),

        -- Scenario 2: Records for NOT query testing
        (6, 'News Article', 'Latest Round of negotiations failed'),
        (7, 'Sports Report', 'The team went Round the track'),
        (8, 'Weather Update', 'Temperature will Round to 25 degrees'),
        (9, NULL, 'This content contains Round keyword'),
        (10, 'Clean Article', NULL),
        (11, 'Simple Article', 'No special keywords in this content'),

        -- Additional edge cases
        (12, '', 'Empty title with Round content'),
        (13, 'Non-empty title', ''),
        (14, NULL, NULL),
        (15, 'Mixed Case Round', 'Testing ROUND and round variations')
    """

    // Wait for data to be ready
    Thread.sleep(5000)

    // Regression Test 1: Original Issue - OR Query Inconsistency
    // This reproduces the original bug: search('title:"Ronald" or (content:ALL("Selma Blair"))')
    // vs title match "Ronald" or (content match_all "Selma Blair")
    qt_regression_1_search_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('title:Ronald OR (content:ALL(Selma Blair))')
    """

    qt_regression_1_match_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE title match "Ronald" or (content match_all "Selma Blair")
    """

    // Detailed verification - get actual matching rows for OR query
    qt_regression_1_search_or_rows """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName}
        WHERE search('title:Ronald OR (content:ALL(Selma Blair))')
        ORDER BY id
    """

    qt_regression_1_match_or_rows """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName}
        WHERE title match "Ronald" or (content match_all "Selma Blair")
        ORDER BY id
    """

    // Regression Test 2: Original Issue - NOT Query Inconsistency
    // This reproduces: search('not content:"Round"') vs not search('content:"Round"')
    qt_regression_2_internal_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('NOT content:Round')
    """

    qt_regression_2_external_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE not search('content:Round')
    """

    // Detailed verification for NOT query
    qt_regression_2_internal_not_rows """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName}
        WHERE search('NOT content:Round')
        ORDER BY id
    """

    qt_regression_2_external_not_rows """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName}
        WHERE not search('content:Round')
        ORDER BY id
    """

    // Regression Test 3: NULL Handling in OR Queries
    // Verify that OR queries properly handle NULL values according to SQL semantics
    qt_regression_3_null_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName}
        WHERE search('title:NonExistent OR content:Ronald')
        ORDER BY id
    """

    // Test with match function for comparison
    qt_regression_3_match_null_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName}
        WHERE title match "NonExistent" or content match "Ronald"
        ORDER BY id
    """

    // Regression Test 4: NULL Handling in AND Queries
    qt_regression_4_null_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName}
        WHERE search('title:Ronald AND content:biography')
        ORDER BY id
    """

    // Regression Test 5: Complex Boolean Operations
    qt_regression_5_complex_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('(title:Ronald OR content:Selma) AND NOT content:Round')
    """

    qt_regression_5_complex_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE (title match "Ronald" or content match "Selma") and not search('content:"Round"')
    """

    // Regression Test 6: Edge Case - All NULL Query
    qt_regression_6_all_null """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('title:NonExistent AND content:NonExistent')
    """

    // Regression Test 7: Case Sensitivity and Variations
    qt_regression_7_case_lower """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('content:round')
    """

    qt_regression_7_case_upper """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('content:Round')
    """

    // Regression Test 8: Multiple NOT operations
    qt_regression_8_multiple_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('NOT (title:nonexistent OR content:nonexistent)')
    """

    qt_regression_8_external_multiple_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE not search('title:nonexistent OR content:nonexistent')
    """

    // Regression Test 9: Empty string handling
    qt_regression_9_empty_string """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('title:"" OR content:Round')
    """

    // Regression Test 10: Performance test with complex query
    qt_regression_10_performance """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('(title:Ronald OR title:Selma OR content:Round) AND NOT (title:NonExistent AND content:NonExistent)')
    """
}