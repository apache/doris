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

suite("test_search_inverted_index", "p0") {
    def tableWithIndex = "search_index_test_table"
    def tableWithoutIndex = "search_no_index_test_table"

    sql "DROP TABLE IF EXISTS ${tableWithIndex}"
    sql "DROP TABLE IF EXISTS ${tableWithoutIndex}"

    // Create table WITH inverted indexes
    sql """
        CREATE TABLE ${tableWithIndex} (
            id BIGINT,
            title VARCHAR(500),
            content TEXT,
            description TEXT,
            category VARCHAR(100),
            tags TEXT,
            author VARCHAR(200),
            publish_date DATE,
            view_count BIGINT,
            rating DECIMAL(3,2),
            INDEX idx_title (title) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_description (description) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_category (category) USING INVERTED,
            INDEX idx_tags (tags) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_author (author) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    // Create table WITHOUT inverted indexes for comparison
    sql """
        CREATE TABLE ${tableWithoutIndex} (
            id BIGINT,
            title VARCHAR(500),
            content TEXT,
            description TEXT,
            category VARCHAR(100),
            tags TEXT,
            author VARCHAR(200),
            publish_date DATE,
            view_count BIGINT,
            rating DECIMAL(3,2)
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    // Generate larger dataset for performance testing
    def generateTestData = { int count ->
        def data = []
        def categories = ["Technology", "Science", "Business", "Health", "Education", "Entertainment", "Sports", "Travel", "Food", "Fashion"]
        def titlePrefixes = ["Advanced", "Introduction to", "Complete Guide", "Mastering", "Understanding", "Learning", "Exploring", "Discovering", "Building", "Creating"]
        def titleTopics = ["Machine Learning", "Data Science", "Web Development", "Mobile Apps", "Cloud Computing", "Artificial Intelligence", "Database Systems", "Software Engineering", "Cybersecurity", "DevOps"]
        def authors = ["John Smith", "Jane Doe", "Bob Johnson", "Alice Wilson", "Charlie Brown", "Diana Prince", "Eve Adams", "Frank Miller", "Grace Lee", "Henry Taylor"]
        
        for (int i = 1; i <= count; i++) {
            def title = "${titlePrefixes[i % titlePrefixes.size()]} ${titleTopics[i % titleTopics.size()]}"
            def content = "This is comprehensive content about ${titleTopics[i % titleTopics.size()]} with detailed explanations and practical examples for beginners and advanced users alike."
            def description = "Detailed description covering all aspects of ${titleTopics[i % titleTopics.size()]} including best practices and real-world applications."
            def category = categories[i % categories.size()]
            def tags = "${titleTopics[i % titleTopics.size()].toLowerCase().replace(' ', '-')}, tutorial, guide, ${category.toLowerCase()}"
            def author = authors[i % authors.size()]
            def publishDate = "2023-" + String.format("%02d", (i % 12) + 1) + "-" + String.format("%02d", (i % 28) + 1)
            def viewCount = 1000 + (i * 137) % 5000  // Pseudo-random view counts
            def rating = 3.0 + ((i * 73) % 20) / 10.0  // Pseudo-random ratings between 3.0-4.9
            
            data.add([i, title, content, description, category, tags, author, publishDate, viewCount, rating])
        }
        return data
    }

    // Insert test data (100 records for performance testing)
    def testData = generateTestData(100)

    logger.info("Inserting ${testData.size()} records into tables...")

    for (def row : testData) {
        def insertSql = """INSERT INTO ${tableWithIndex} VALUES (${row[0]}, '${row[1]}', '${row[2]}', '${row[3]}', '${row[4]}', '${row[5]}', '${row[6]}', '${row[7]}', ${row[8]}, ${row[9]})"""
        sql insertSql
        
        insertSql = """INSERT INTO ${tableWithoutIndex} VALUES (${row[0]}, '${row[1]}', '${row[2]}', '${row[3]}', '${row[4]}', '${row[5]}', '${row[6]}', '${row[7]}', ${row[8]}, ${row[9]})"""
        sql insertSql
    }

    // Wait for index building and data settling
    Thread.sleep(10000)

    // Verify data insertion
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex}"
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithoutIndex}"

    // Test 1: Basic search functionality on indexed table
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex} WHERE search('title:Machine')"

    // Test 2: Compare results between indexed and non-indexed tables
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableWithIndex} WHERE search('title:Learning') ORDER BY id"
    //qt_sql "SELECT id FROM ${tableWithoutIndex} WHERE search('title:Learning') ORDER BY id"

    // Test 3: Complex search with multiple fields
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex} WHERE search('title:Machine AND category:Technology')"

    // Test 4: Search with phrase queries
    //qt_sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('content:\"comprehensive content\"')"

    // Test 5: Search with boolean operators
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex} WHERE search('(title:Learning OR title:Guide) AND category:Technology')"

    // Test 6: Search with NOT operator
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex} WHERE search('NOT title:Machine')"

    // Test 7: Wildcard searches
    //qt_sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('title:Learn*')"

    // Test 8: Search in tags field (test tokenized field)
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex} WHERE search('tags:tutorial')"
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex} WHERE search('tags:devops')"
    // Test 9: Search by author
    //qt_sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('author:\"John Smith\"')"

    // Test 10: Search combined with regular WHERE clauses
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex} WHERE search('category:Technology') AND view_count > 3000"

    // Test 11: Search with GROUP BY
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ category, COUNT(*) as cnt FROM ${tableWithIndex} WHERE search('title:Learning') GROUP BY category ORDER BY cnt DESC, category ASC"

    // Test 12: Search with ORDER BY and LIMIT
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableWithIndex} WHERE search('title:Advanced') ORDER BY view_count DESC LIMIT 10"

    // Test 13: Search with aggregation functions
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ AVG(rating), MAX(view_count), MIN(view_count) FROM ${tableWithIndex} WHERE search('category:Technology')"

    // Test 14: Search with HAVING clause
    qt_sql """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ category, COUNT(*) as cnt
            FROM ${tableWithIndex}
            WHERE search('tags:guide OR tags:tutorial')
            GROUP BY category
            HAVING cnt > 10
            ORDER BY cnt DESC
    """

    // Test 15: Nested search queries
    qt_sql """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableWithIndex}
            WHERE search('title:Complete OR title:Advanced')
            AND id IN (
                SELECT id FROM ${tableWithIndex}
                WHERE search('category:Technology OR category:Science')
            )
            ORDER BY id LIMIT 5
    """
    // Edge case tests

    // Edge Case 1: Search with special characters
    //qt_sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('title:\"Complete Guide\"')"

    // Edge Case 2: Search with very common terms
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex} WHERE search('content:the')"

    // Edge Case 3: Search with numeric-like terms
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex} WHERE search('content:2023')"

    // Edge Case 4: Case sensitivity test
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex} WHERE search('title:MACHINE')"
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex} WHERE search('title:machine')"
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex} WHERE search('title:Machine')"

    // Index effectiveness test
    //qt_sql "SELECT id, title FROM ${tableWithIndex} WHERE search('title:Introduction AND content:\"practical examples\" AND tags:tutorial') ORDER BY view_count DESC LIMIT 100"

    // Test ANY query performance
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex} WHERE search('tags:ANY(tutorial guide example)')"

    // Test ALL query performance  
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableWithIndex} WHERE search('tags:ALL(machine learning)')"

    // Test complex ANY/ALL combinations
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableWithIndex} WHERE search('(tags:ANY(java python) OR tags:ALL(tutorial guide)) AND category:Technology') order by id LIMIT 10"
}
