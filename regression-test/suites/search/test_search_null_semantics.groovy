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

suite("test_search_null_semantics") {
    def tableName = "search_null_test"

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create test table with inverted index and NULL values
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(255),
            content TEXT,
            category VARCHAR(100),
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_category (category) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    // Insert test data with NULL values
    sql """
        INSERT INTO ${tableName} VALUES
        (1, 'Ronald Reagan', 'President of the United States', 'Politics'),
        (2, 'Selma Blair', 'American actress known for Round performances', 'Entertainment'),
        (3, NULL, 'Biography of Ronald McDonald', 'Biography'),
        (4, 'Movie Review', NULL, 'Entertainment'),
        (5, 'Selma Montgomery', 'Civil rights history about Round Table', 'History'),
        (6, NULL, NULL, 'Unknown'),
        (7, 'Ronald McDonald', 'Fast food mascot', NULL),
        (8, 'Biography', 'Selma Blair biography with Round details', 'Biography'),
        (9, 'Round Table', 'Knights of the Round Table story', 'History'),
        (10, 'Test', 'No special keywords here', 'Test')
    """

    // Wait for data to be ready
    Thread.sleep(5000)

    // Test Case 1: OR query consistency - Original Issue Reproduction
    // search('title:"Ronald" or (content:ALL("Selma Blair"))') should match
    // title match "Ronald" or (content match_all "Selma Blair")
    qt_test_case_1_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('title:Ronald or (content:ALL(Selma Blair))')
    """

    qt_test_case_1_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE title match "Ronald" or (content match_all "Selma Blair")
    """

    // Test Case 2: NOT query consistency - Original Issue Reproduction
    // search('not content:"Round"') should match not search('content:"Round"')
    qt_test_case_2_internal_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('not content:Round')
    """

    qt_test_case_2_external_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE not search('content:Round')
    """

    // Test Case 3: NULL handling in OR queries
    // Verify that NULL OR TRUE = TRUE logic works
    qt_test_case_3_or_with_null """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName}
        WHERE search('title:Ronald or content:biography')
        ORDER BY id
    """

    // Test Case 4: NULL handling in AND queries
    // Verify that NULL AND TRUE = NULL logic works
    qt_test_case_4_and_with_null """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName}
        WHERE search('title:Ronald and content:biography')
        ORDER BY id
    """

    // Test Case 5: Complex OR query with multiple NULL scenarios
    qt_test_case_5_complex_or_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('title:Unknown or content:mascot or category:Test')
    """

    qt_test_case_5_complex_or_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE title match "Unknown" or content match "mascot" or category match "Test"
    """

    // Test Case 6: NOT query with different field types
    qt_test_case_6_not_title_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('not title:Ronald')
    """

    qt_test_case_6_not_title_external """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE not search('title:Ronald')
    """

    // Test Case 7: Mixed boolean operations
    qt_test_case_7_mixed """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('(title:Ronald or content:Selma) and not category:Unknown')
    """

    // Test Case 8: Edge case - all NULL fields
    qt_test_case_8_all_null """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('title:NonExistent or content:NonExistent or category:NonExistent')
    """
}