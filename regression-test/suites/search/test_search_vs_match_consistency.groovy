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

    // Regression coverage: untokenized keyword indexes should keep search() and match_* aligned
    def keywordTable = "search_keyword_exact_case"

    sql "DROP TABLE IF EXISTS ${keywordTable}"

    sql """
        CREATE TABLE ${keywordTable} (
            title VARCHAR(256),
            redirect VARCHAR(512),
            INDEX idx_redirect (redirect) USING INVERTED PROPERTIES("ignore_above" = "1024")
        ) ENGINE=OLAP
        DUPLICATE KEY(title)
        DISTRIBUTED BY HASH(title) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO ${keywordTable} VALUES
        ('Rainbowman Page', 'Rainbowman'),
        ('rainbowman lowercase', 'rainbowman'),
        ('Other Entry', 'Rainbow Man'),
        ('Null Redirect', NULL)
    """

    Thread.sleep(5000)

    qt_keyword_case_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ title FROM ${keywordTable}
        WHERE redirect match_all "Rainbowman"
        ORDER BY title
    """

    qt_keyword_case_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ title FROM ${keywordTable}
        WHERE search('redirect:All("Rainbowman")')
        ORDER BY title
    """

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

    // ------------------------------------------------------------------
    // Additional regression coverage: Mandy Patinkin / Kesha consistency
    // ------------------------------------------------------------------

    def mandyTable = "search_match_issue_mandy_kesha"

    sql "DROP TABLE IF EXISTS ${mandyTable}"

    sql """
        CREATE TABLE ${mandyTable} (
            id INT,
            title VARCHAR(500),
            content TEXT,
            category VARCHAR(100),
            tags VARCHAR(200),
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_category (category) USING INVERTED PROPERTIES("parser" = "standard"),
            INDEX idx_tags (tags) USING INVERTED PROPERTIES("parser" = "standard")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO ${mandyTable} VALUES
        (1,  'Mandy Patinkin Career', 'Biography of the talented actor Mandy Patinkin', 'entertainment', 'actor,biography'),
        (2,  'Movie Reviews', 'Kesha music video review and analysis', 'music', 'review,kesha'),
        (3,  NULL, 'Article about Mandy Patinkin Broadway performances', 'theater', 'broadway,performance'),
        (4,  'Music Industry', NULL, 'music', 'industry,analysis'),
        (5,  'Entertainment Weekly', 'Kesha new album release covered by major publications', 'music', 'album,release'),
        (6,  'Mandy Biography', 'Complete story of Mandy Patinkin life and career', 'biography', NULL),
        (7,  NULL, 'Kesha concert review from last summer tour', 'music', 'concert,tour'),
        (8,  'Theater Arts', 'Mandy Patinkin theatrical performances analysis', NULL, 'theater,analysis'),
        (9,  'Pop Music', 'Kesha influence on modern pop music industry', 'music', 'pop,influence'),
        (10, NULL, NULL, 'unknown', 'misc'),
        (11, 'Random Title', 'Neither Mandy nor Kesha mentioned here', 'misc', 'random'),
        (12, '', 'Empty title with Mandy Patinkin content', 'biography', ''),
        (13, 'Valid Title', '', 'misc', 'empty_content'),
        (14, 'Mandy Kesha Collaboration', 'Fictional collaboration between Mandy Patinkin and Kesha', 'fantasy', 'collaboration'),
        (15, NULL, 'Content with both Mandy Patinkin and Kesha references', NULL, NULL),
        (16, 'Comprehensive Bio', 'Mandy Patinkin full biography with career details', 'biography', 'complete,detailed'),
        (17, 'Music Analysis', 'Kesha musical style and artistic evolution', 'music', 'style,evolution'),
        (18, NULL, 'Theater review mentioning Mandy Patinkin outstanding performance', 'theater', 'review,outstanding'),
        (19, 'Pop Culture', NULL, 'culture', 'pop,trends'),
        (20, 'Celebrity News', 'Latest news about Kesha tour and Mandy Patinkin Broadway show', 'news', 'celebrity,latest')
    """

    Thread.sleep(10000)

    // Mandy/Kesha consistency checks
    qt_man_pat_1_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${mandyTable}
        WHERE search('content:ALL("Mandy Patinkin") or not (content:ANY("Kesha"))')
    """

    qt_man_pat_1_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${mandyTable}
        WHERE content match_all "Mandy Patinkin" or not (content match_any "Kesha")
    """

    qt_man_pat_1_search_rows """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content,
               CASE WHEN title IS NULL THEN 'NULL' ELSE 'NOT_NULL' END AS title_status,
               CASE WHEN content IS NULL THEN 'NULL' ELSE 'NOT_NULL' END AS content_status
        FROM ${mandyTable}
        WHERE search('content:ALL("Mandy Patinkin") or not (content:ANY("Kesha"))')
        ORDER BY id
    """

    qt_man_pat_1_match_rows """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content,
               CASE WHEN title IS NULL THEN 'NULL' ELSE 'NOT_NULL' END AS title_status,
               CASE WHEN content IS NULL THEN 'NULL' ELSE 'NOT_NULL' END AS content_status
        FROM ${mandyTable}
        WHERE content match_all "Mandy Patinkin" or not (content match_any "Kesha")
        ORDER BY id
    """

    qt_man_pat_2_search_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${mandyTable}
        WHERE search('title:Mandy OR content:Kesha')
    """

    qt_man_pat_2_match_or """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${mandyTable}
        WHERE title match "Mandy" OR content match "Kesha"
    """

    qt_man_pat_2_search_or_ids """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${mandyTable}
        WHERE search('title:Mandy OR content:Kesha')
        ORDER BY id
    """

    qt_man_pat_2_match_or_ids """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${mandyTable}
        WHERE title match "Mandy" OR content match "Kesha"
        ORDER BY id
    """

    qt_man_pat_3_search_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${mandyTable}
        WHERE search('title:Mandy AND category:biography')
    """

    qt_man_pat_3_match_and """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${mandyTable}
        WHERE title match "Mandy" AND category match "biography"
    """

    qt_man_pat_4_search_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${mandyTable}
        WHERE search('NOT content:Kesha')
    """

    qt_man_pat_4_match_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${mandyTable}
        WHERE NOT content match "Kesha"
    """

    qt_man_pat_5_search_nested """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${mandyTable}
        WHERE search('(title:Mandy OR content:Kesha) AND category:music')
    """

    qt_man_pat_5_match_nested """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${mandyTable}
        WHERE (title match "Mandy" OR content match "Kesha") AND category match "music"
    """

    // ------------------------------------------------------------------
    // Regression coverage: title:Fred OR NOT content:ANY(Rahul Gandhi)
    // ------------------------------------------------------------------

    def fredTable = "search_not_or_consistency"

    sql "DROP TABLE IF EXISTS ${fredTable}"

    sql """
        CREATE TABLE ${fredTable} (
            id INT,
            title VARCHAR(500),
            content TEXT,
            remark VARCHAR(100),
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        INSERT INTO ${fredTable} VALUES
        (1,  'Fred Smith', 'Article about Rahul Gandhi politics', 'news'),
        (2,  'John Doe', 'Technology article about software', 'tech'),
        (3,  NULL, 'Article mentioning Rahul Gandhi campaign', 'politics'),
        (4,  'Fred Wilson', NULL, 'biography'),
        (5,  'Mary Johnson', NULL, 'lifestyle'),
        (6,  NULL, NULL, 'unknown'),
        (7,  'Another Fred', 'Some other content without the target name', 'misc'),
        (8,  'Random Person', 'Discussion about Rahul Gandhi leadership', 'politics'),
        (9,  'Fred Thompson', 'Mixed article about Rahul Gandhi and others', 'mixed'),
        (10, '', 'Clean article without target names', 'clean'),
        (11, 'Different Name', 'Article with neither Fred nor Rahul Gandhi', 'general'),
        (12, NULL, 'Technology review without political content', 'tech'),
        (13, 'Fred Frederick', 'Biography of Rahul Gandhi family', 'biography'),
        (14, 'News Reporter', NULL, 'news'),
        (15, '', '', 'empty')
    """

    Thread.sleep(10000)

    qt_fred_1_search """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${fredTable}
        WHERE search('title:Fred OR NOT content:ANY("Rahul Gandhi")')
    """

    qt_fred_1_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${fredTable}
        WHERE title match "Fred" OR NOT (content match_any "Rahul Gandhi")
    """

    qt_fred_1_search_rows """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${fredTable}
        WHERE search('title:Fred OR NOT content:ANY("Rahul Gandhi")')
        ORDER BY id
    """

    qt_fred_1_match_rows """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${fredTable}
        WHERE title match "Fred" OR NOT (content match_any "Rahul Gandhi")
        ORDER BY id
    """

    qt_fred_2_title_only """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${fredTable}
        WHERE search('title:Fred')
    """

    qt_fred_2_content_any """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${fredTable}
        WHERE search('content:ANY("Rahul Gandhi")')
    """

    qt_fred_2_not_content """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${fredTable}
        WHERE search('NOT content:ANY("Rahul Gandhi")')
    """

    qt_fred_3_or_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${fredTable}
        WHERE search('title:Fred OR NOT title:Random')
    """

    qt_fred_3_or_not_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${fredTable}
        WHERE title match "Fred" OR NOT title match "Random"
    """

    qt_fred_4_and_not """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${fredTable}
        WHERE search('title:Fred AND NOT content:ANY("Rahul Gandhi")')
    """

    qt_fred_4_and_not_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${fredTable}
        WHERE title match "Fred" AND NOT (content match_any "Rahul Gandhi")
    """

    qt_fred_5_nested """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${fredTable}
        WHERE search('(title:Fred OR title:John) OR NOT (content:ANY("Rahul Gandhi") OR content:ANY("politics"))')
    """

    qt_fred_5_nested_match """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${fredTable}
        WHERE (title match "Fred" OR title match "John") OR NOT (content match_any "Rahul Gandhi" OR content match_any "politics")
    """

    // Clean up auxiliary tables created in this suite
    sql "DROP TABLE IF EXISTS ${mandyTable}"
    sql "DROP TABLE IF EXISTS ${fredTable}"
}
