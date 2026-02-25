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

suite("test_search_null_semantics", "p0") {
    def tableName = "search_null_test"

    // Pin enable_common_expr_pushdown to prevent CI flakiness from fuzzy testing.
    sql """ set enable_common_expr_pushdown = true """

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
        WHERE search('title:Ronald OR (content:ALL(Selma Blair))')
    """

    qt_test_case_1_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE title match "Ronald" or (content match_all "Selma Blair")
    """

    // Test Case 2: NOT query consistency - Original Issue Reproduction
    // search('not content:"Round"') should match not search('content:"Round"')
    qt_test_case_2_internal_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('NOT content:Round')
    """

    qt_test_case_2_external_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE not search('content:Round')
    """

    // Test Case 2b: Phrase NOT queries must treat NULL rows as UNKNOWN
    qt_test_case_2_phrase_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName}
        WHERE NOT search('content:"Selma Blair"')
        ORDER BY id
    """

    // Test Case 3: NULL handling in OR queries
    // Verify that NULL OR TRUE = TRUE logic works
    qt_test_case_3_or_with_null """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName}
        WHERE search('title:Ronald OR content:biography')
        ORDER BY id
    """

    // Test Case 4: NULL handling in AND queries
    // Verify that NULL AND TRUE = NULL logic works
    qt_test_case_4_and_with_null """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName}
        WHERE search('title:Ronald AND content:biography')
        ORDER BY id
    """

    // Test Case 5: Complex OR query with multiple NULL scenarios
    qt_test_case_5_complex_or_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('title:Unknown OR content:mascot OR category:Test')
    """

    qt_test_case_5_complex_or_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE title match "Unknown" or content match "mascot" or category match "Test"
    """

    // Test Case 6: NOT query with different field types
    qt_test_case_6_not_title_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('NOT title:Ronald')
    """

    qt_test_case_6_not_title_external """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE not search('title:Ronald')
    """

    // Test Case 7: Mixed boolean operations
    qt_test_case_7_mixed """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('(title:Ronald OR content:Selma) AND NOT category:Unknown')
    """

    // Test Case 8: Edge case - all NULL fields
    qt_test_case_8_all_null """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName}
        WHERE search('title:NonExistent OR content:NonExistent OR category:NonExistent')
    """

    // ------------------------------------------------------------------
    // Additional coverage: nested boolean logic with explicit NULL checks
    // ------------------------------------------------------------------

    def nestedTable = "search_nested_null_logic"

    sql "DROP TABLE IF EXISTS ${nestedTable}"

    sql """
        CREATE TABLE ${nestedTable} (
            id INT,
            field_a VARCHAR(100),
            field_b VARCHAR(100),
            field_c VARCHAR(100),
            field_d VARCHAR(100),
            INDEX idx_a (field_a) USING INVERTED PROPERTIES("parser" = "standard"),
            INDEX idx_b (field_b) USING INVERTED PROPERTIES("parser" = "standard"),
            INDEX idx_c (field_c) USING INVERTED PROPERTIES("parser" = "standard"),
            INDEX idx_d (field_d) USING INVERTED PROPERTIES("parser" = "standard")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO ${nestedTable} VALUES
        (1,  'alpha', 'beta',  'gamma', 'delta'),
        (2,  'alpha', NULL,    'gamma', 'delta'),
        (3,  NULL,    'beta',  'gamma', 'delta'),
        (4,  'alpha', 'beta',  NULL,    'delta'),
        (5,  'alpha', 'beta',  'gamma', NULL),
        (6,  NULL,    NULL,    'gamma', 'delta'),
        (7,  'alpha', 'beta',  NULL,    NULL),
        (8,  NULL,    'beta',  NULL,    'delta'),
        (9,  'alpha', NULL,    'gamma', NULL),
        (10, NULL,    NULL,    NULL,    NULL),
        (11, 'exclude', 'beta',   'gamma', 'delta'),
        (12, 'alpha',   'exclude','gamma', 'delta'),
        (13, NULL,      'beta',   'gamma', 'delta'),
        (14, 'alpha',   NULL,     'gamma', 'delta'),
        (15, 'target',  'any',    'any',   'any'),
        (16, 'other',   'beta',   'gamma', 'any'),
        (17, 'other',   'beta',   'any',   'delta'),
        (18, NULL,      'beta',   'gamma', 'any'),
        (19, NULL,      'beta',   NULL,    'delta'),
        (20, NULL,      NULL,     'gamma', 'delta'),
        (21, 'forbidden', 'safe',      'any', 'any'),
        (22, 'safe',      'forbidden', 'any', 'any'),
        (23, 'safe',      'safe',      'any', 'any'),
        (24, NULL,        'safe',      'any', 'any'),
        (25, 'safe',      NULL,        'any', 'any'),
        (26, NULL,        NULL,        'any', 'any'),
        (27, '',          'beta',   'gamma', 'delta'),
        (28, 'alpha',     '',       'gamma', 'delta'),
        (29, 'alpha',     'beta',   '',      'delta'),
        (30, 'alpha',     'beta',   'gamma', '')
    """

    Thread.sleep(10000)

    qt_nested_1_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${nestedTable}
        WHERE search('(field_a:alpha OR field_b:beta) AND (field_c:gamma OR field_d:delta)')
    """

    qt_nested_1_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${nestedTable}
        WHERE (field_a match "alpha" OR field_b match "beta") AND (field_c match "gamma" OR field_d match "delta")
    """

    qt_nested_1_rows """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, field_a, field_b, field_c, field_d
        FROM ${nestedTable}
        WHERE search('(field_a:alpha OR field_b:beta) AND (field_c:gamma OR field_d:delta)')
        ORDER BY id
    """

    qt_nested_2_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${nestedTable}
        WHERE search('field_a:target OR (field_b:beta AND (field_c:gamma OR field_d:delta))')
    """

    qt_nested_2_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${nestedTable}
        WHERE field_a match "target" OR (field_b match "beta" AND (field_c match "gamma" OR field_d match "delta"))
    """

    qt_nested_3_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${nestedTable}
        WHERE search('NOT (field_a:forbidden OR field_b:forbidden)')
    """

    qt_nested_3_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${nestedTable}
        WHERE NOT (field_a match "forbidden" OR field_b match "forbidden")
    """

    qt_nested_4_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${nestedTable}
        WHERE search('(field_a:alpha OR field_b:beta) AND NOT (field_c:exclude OR field_d:exclude)')
    """

    qt_nested_4_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${nestedTable}
        WHERE (field_a match "alpha" OR field_b match "beta") AND NOT (field_c match "exclude" OR field_d match "exclude")
    """

    qt_nested_5_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${nestedTable}
        WHERE search('field_a:alpha OR field_b:beta OR field_c:gamma OR field_d:delta')
    """

    qt_nested_5_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${nestedTable}
        WHERE field_a match "alpha" OR field_b match "beta" OR field_c match "gamma" OR field_d match "delta"
    """

    qt_nested_6_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${nestedTable}
        WHERE search('field_a:alpha AND (field_b:beta OR (field_c:gamma AND field_d:delta))')
    """

    qt_nested_6_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${nestedTable}
        WHERE field_a match "alpha" AND (field_b match "beta" OR (field_c match "gamma" AND field_d match "delta"))
    """

    sql "DROP TABLE IF EXISTS ${nestedTable}"

    // ------------------------------------------------------------------
    // Additional coverage: general ternary logic scenarios with articles
    // ------------------------------------------------------------------

    def ternaryTable = "search_three_value_logic"

    sql "DROP TABLE IF EXISTS ${ternaryTable}"

    sql """
        CREATE TABLE ${ternaryTable} (
            id INT,
            title VARCHAR(500),
            content TEXT,
            category VARCHAR(100),
            author VARCHAR(100),
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_category (category) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_author (author) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO ${ternaryTable} VALUES
        (1,  'Ronald Reagan Biography', 'A detailed biography of the 40th president', 'biography', 'John Smith'),
        (2,  'Movie Reviews', 'Selma Blair gives outstanding performance in this film', 'entertainment', 'Jane Doe'),
        (3,  NULL, 'A story about Ronald McDonald and his adventures', 'fiction', 'Bob Wilson'),
        (4,  'Selma Montgomery Story', NULL, 'history', 'Alice Brown'),
        (5,  'Biography Collection', 'The life story of Selma Blair, talented actress', 'biography', 'Carol White'),
        (6,  'Ronald Reagan', 'The president who changed America', 'politics', 'David Green'),
        (7,  'Reagan Era', 'Ronald Reagan policies shaped the decade', NULL, 'Eva Black'),
        (8,  NULL, 'Political analysis of Reagan administration', 'politics', 'Frank Blue'),
        (9,  'Random Title', 'Unrelated content about technology', 'tech', NULL),
        (10, 'Clean Article', 'Technology advances in modern times', 'tech', 'Grace Red'),
        (11, NULL, 'Article without spam content', 'news', 'Henry Yellow'),
        (12, 'News Report', NULL, 'news', 'Iris Purple'),
        (13, 'Spam Article', 'This contains spam keywords', 'spam', 'Jack Orange'),
        (14, 'Mixed Content', 'Ronald Reagan met with Selma representatives', 'politics', 'Kate Pink'),
        (15, NULL, NULL, 'unknown', 'Leo Cyan'),
        (16, '', 'Empty title but valid content', 'misc', ''),
        (17, 'Valid Title', '', 'misc', 'Mary Gray'),
        (18, 'Ronald Article', 'Content about Selma Blair movie', 'entertainment', 'Nick Violet'),
        (19, 'Selma Documentary', 'Ronald Reagan historical documentary', 'history', NULL),
        (20, NULL, NULL, NULL, NULL)
    """

    Thread.sleep(10000)

    qt_ternary_1_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${ternaryTable}
        WHERE search('title:Ronald OR content:Selma')
    """

    qt_ternary_1_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${ternaryTable}
        WHERE title match "Ronald" OR content match "Selma"
    """

    qt_ternary_1_search_rows """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content, category FROM ${ternaryTable}
        WHERE search('title:Ronald OR content:Selma')
        ORDER BY id
    """

    qt_ternary_1_match_rows """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content, category FROM ${ternaryTable}
        WHERE title match "Ronald" OR content match "Selma"
        ORDER BY id
    """

    qt_ternary_2_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${ternaryTable}
        WHERE search('title:Ronald AND category:politics')
    """

    qt_ternary_2_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${ternaryTable}
        WHERE title match "Ronald" AND category match "politics"
    """

    qt_ternary_3_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${ternaryTable}
        WHERE search('NOT content:spam')
    """

    qt_ternary_3_external """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${ternaryTable}
        WHERE NOT search('content:spam')
    """

    qt_ternary_4_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${ternaryTable}
        WHERE search('title:Ronald OR content:Selma OR category:biography')
    """

    qt_ternary_4_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${ternaryTable}
        WHERE title match "Ronald" OR content match "Selma" OR category match "biography"
    """

    qt_ternary_5_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${ternaryTable}
        WHERE search('(title:Ronald OR content:Selma) AND category:politics')
    """

    qt_ternary_5_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${ternaryTable}
        WHERE (title match "Ronald" OR content match "Selma") AND category match "politics"
    """

    qt_ternary_6_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${ternaryTable}
        WHERE search('title:Ronald OR NOT content:spam')
    """

    qt_ternary_6_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${ternaryTable}
        WHERE title match "Ronald" OR NOT content match "spam"
    """

    qt_ternary_7_all_null """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${ternaryTable}
        WHERE search('title:NonExistent OR content:NonExistent OR category:NonExistent')
    """

    sql "DROP TABLE IF EXISTS ${ternaryTable}"
}
