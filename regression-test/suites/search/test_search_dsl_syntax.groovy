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

suite("test_search_dsl_syntax") {
    def tableName = "search_dsl_test_table"
    
    sql "DROP TABLE IF EXISTS ${tableName}"
    
    // Create test table with comprehensive inverted indexes
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(255),
            content TEXT,
            category VARCHAR(100),
            tags VARCHAR(200),
            author VARCHAR(100),
            status VARCHAR(50),
            priority INT,
            created_date DATE,
            modified_date DATETIME,
            score FLOAT,
            INDEX idx_title (title) USING INVERTED,
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_category (category) USING INVERTED,
            INDEX idx_tags (tags) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_author (author) USING INVERTED,
            INDEX idx_status (status) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """
    
    // Insert comprehensive test data for DSL testing
    def testData = [
        [1, "Machine Learning Introduction", "Basic concepts of machine learning and artificial intelligence", "Technology", "machine-learning, AI, tutorial", "John Doe", "published", 1, "2023-01-15", "2023-01-15 10:30:00", 4.5],
        [2, "Advanced Deep Learning", "Neural networks, CNNs, RNNs and transformer architectures", "Technology", "deep-learning, neural-networks, AI", "Jane Smith", "published", 2, "2023-02-20", "2023-02-20 14:15:00", 4.8],
        [3, "Python for Beginners", "Complete guide to Python programming from basics to advanced", "Programming", "python, programming, beginners", "Bob Johnson", "draft", 1, "2023-03-10", "2023-03-10 09:45:00", 4.2],
        [4, "Data Science with R", "Statistical analysis and data visualization using R programming", "Science", "data-science, R, statistics", "Alice Wilson", "published", 3, "2023-04-05", "2023-04-05 16:20:00", 4.0],
        [5, "Web Development 2023", "Modern web technologies: React, Vue.js, and Node.js", "Web", "web-development, javascript, react", "Charlie Brown", "published", 1, "2023-05-12", "2023-05-12 11:10:00", 4.3],
        [6, "Database Design Patterns", "Relational and NoSQL database optimization techniques", "Database", "database, SQL, NoSQL, optimization", "Diana Prince", "published", 2, "2023-06-18", "2023-06-18 13:30:00", 4.6],
        [7, "Natural Language Processing", "NLP algorithms, sentiment analysis, and text mining", "AI", "NLP, text-mining, sentiment-analysis", "Eve Adams", "review", 3, "2023-07-22", "2023-07-22 15:45:00", 4.4],
        [8, "Cloud Computing AWS", "Amazon Web Services deployment and scaling strategies", "Cloud", "cloud-computing, AWS, deployment", "Frank Miller", "published", 1, "2023-08-14", "2023-08-14 08:20:00", 4.7],
        [9, "Kubernetes Orchestration", "Container orchestration with Kubernetes and Docker", "DevOps", "kubernetes, docker, containers", "Grace Lee", "published", 2, "2023-09-09", "2023-09-09 12:50:00", 4.1],
        [10, "Microservices Architecture", "Design patterns for scalable microservices systems", "Architecture", "microservices, scalability, patterns", "Henry Taylor", "published", 1, "2023-10-01", "2023-10-01 17:25:00", 4.5],
        // Test data with null values for null handling tests
        [11, "Test with null content", null, "Test", "test, null", "Test Author", "draft", 1, "2023-11-01", "2023-11-01 10:00:00", 3.0],
        [12, "Test with null title", "This content has a title but title field is null", "Test", "test, null-title", "Test Author", "draft", 1, "2023-11-02", "2023-11-02 10:00:00", 3.0],
        [13, "Test with null category", "Content with null category field", null, "test, null-category", "Test Author", "draft", 1, "2023-11-03", "2023-11-03 10:00:00", 3.0],
        [14, "Test with null tags", "Content with null tags field", "Test", null, "Test Author", "draft", 1, "2023-11-04", "2023-11-04 10:00:00", 3.0],
        [15, "Test with null author", "Content with null author field", "Test", "test, null-author", null, "draft", 1, "2023-11-05", "2023-11-05 10:00:00", 3.0],
        [16, "Test with null status", "Content with null status field", "Test", "test, null-status", "Test Author", null, 1, "2023-11-06", "2023-11-06 10:00:00", 3.0],
        // Test data for NOT search functionality
        [17, "Message about success", "This is a success message for testing", "Message", "success, message", "System", "published", 1, "2023-11-07", "2023-11-07 10:00:00", 3.5],
        [18, "Error message details", "This is an error message for testing", "Message", "error, message", "System", "published", 1, "2023-11-08", "2023-11-08 10:00:00", 3.5],
        [19, "Warning message content", "This is a warning message for testing", "Message", "warning, message", "System", "published", 1, "2023-11-09", "2023-11-09 10:00:00", 3.5],
        [20, "Regular article without msg", "This is a regular article without any message content", "Article", "article, regular", "Author", "published", 1, "2023-11-10", "2023-11-10 10:00:00", 4.0]
    ]
    
    for (def row : testData) {
        def title = row[1] == null ? "NULL" : "'${row[1]}'"
        def content = row[2] == null ? "NULL" : "'${row[2]}'"
        def category = row[3] == null ? "NULL" : "'${row[3]}'"
        def tags = row[4] == null ? "NULL" : "'${row[4]}'"
        def author = row[5] == null ? "NULL" : "'${row[5]}'"
        def status = row[6] == null ? "NULL" : "'${row[6]}'"
        sql """INSERT INTO ${tableName} VALUES (${row[0]}, ${title}, ${content}, ${category}, ${tags}, ${author}, ${status}, ${row[7]}, '${row[8]}', '${row[9]}', ${row[10]})"""
    }
    
    // Wait for index building
    Thread.sleep(5000)
    
    // DSL Syntax Test 1: Simple term queries
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:Machine') ORDER BY id"
    
    // DSL Syntax Test 2: Phrase queries with quotes
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:\"Machine Learning\"')"
    
    // DSL Syntax Test 3: Wildcard queries
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:*Learning') ORDER BY id"
    
    // DSL Syntax Test 4: Prefix queries
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:Data*')"
    
    // DSL Syntax Test 5: Regular expression queries
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:/.*[Dd]ata.*/')"
    
    // DSL Syntax Test 6: Boolean AND operator
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('category:Technology AND tags:AI') ORDER BY id"
    
    // DSL Syntax Test 7: Boolean OR operator
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('category:Programming OR category:Science') ORDER BY id"
    
    // DSL Syntax Test 8: Boolean NOT operator
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ COUNT(*) FROM ${tableName} WHERE search('status:published AND NOT category:Technology')"
    
    // DSL Syntax Test 9: Parentheses for grouping
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('(category:Technology OR category:AI) AND status:published') ORDER BY id"
    
    // DSL Syntax Test 10: Complex nested queries
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('(title:Learning OR title:Development) AND (status:published OR status:review)') ORDER BY id"
    
    // DSL Syntax Test 11: Range queries (if supported)
    try {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('priority:[1 TO 2]') ORDER BY id"
        // This may or may not work depending on implementation
    } catch (Exception e) {
        // Range queries might not be implemented yet
        logger.info("Range query not supported: " + e.getMessage())
    }

    // DSL Syntax Test 12: List queries with IN operator (if supported)
    try {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('status:IN(published draft)') "
               "ORDER BY id"
        // This may or may not work depending on implementation
    } catch (Exception e) {
        // List queries might not be implemented yet
        logger.info("List query not supported: " + e.getMessage())
    }

    // DSL Syntax Test 13: ANY queries
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('tags:ANY(AI python)') ORDER BY id"

    // DSL Syntax Test 14: ALL queries
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('tags:ALL(machine learning)') ORDER BY id"

    // DSL Syntax Test 15: Field names with quotes
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('\"content\":programming') ORDER BY id"

    // DSL Syntax Test 16: Case insensitive field names
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('CONTENT:programming') ORDER BY id"

    // DSL Syntax Test 17: Multiple terms in same field
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('content:machine AND content:learning') ORDER BY id"

    // DSL Syntax Test 18: Hyphenated terms in tags
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('tags:machine-learning') ORDER BY id"

    // DSL Syntax Test 19: Special characters in search terms
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('author:\"John Doe\"') ORDER BY id"

    // DSL Syntax Test 20: Multiple field search with complex boolean logic
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('(title:Learning AND category:Technology) OR (title:Development AND category:Web)') ORDER BY id"

    // Error handling tests for invalid DSL syntax

    // Error Test 1: Unclosed parentheses
    try {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('(title:Machine') ORDER BY id"
        assertTrue(false, "Expected exception for unclosed parentheses")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Invalid") || e.getMessage().contains("syntax"))
    }

    // Error Test 2: Missing field name
    try {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search(':value') ORDER BY id"
        assertTrue(false, "Expected exception for missing field name")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Invalid") || e.getMessage().contains("syntax"))
    }

    // Error Test 3: Missing value
    try {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('field:') ORDER BY id"
        assertTrue(false, "Expected exception for missing value")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Invalid") || e.getMessage().contains("syntax"))
    }

    // Error Test 4: Invalid boolean operator
    try {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('title:Machine XOR category:Technology') ORDER BY id"
        assertTrue(false, "Expected exception for invalid boolean operator")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Invalid") || e.getMessage().contains("syntax"))
    }

    // Error Test 5: Unclosed quotes
    try {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('title:\"Machine Learning') ORDER BY id"
        assertTrue(false, "Expected exception for unclosed quotes")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Invalid") || e.getMessage().contains("syntax"))
    }

    // Error Test 6: Invalid regular expression
    //try {
    //    sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('title:/[invalid/') ORDER BY id"
    //    assertTrue(false, "Expected exception for invalid regex")
    //} catch (Exception e) {
    //    assertTrue(e.getMessage().contains("Invalid") || e.getMessage().contains("syntax"))
    //}

    // Performance and edge case tests

    // Edge Case Test 1: Empty search string
    try {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('') ORDER BY id"
        // Should handle empty string gracefully
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("only inverted index queries are supported") || e.getMessage().contains("Invalid"))
    }

    // Edge Case Test 2: Very long search string
    def longTerm = "a" * 1000
    try {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('title:${longTerm}') ORDER BY id"
        // Should handle long strings gracefully
    } catch (Exception e) {
        // Acceptable if implementation has length limits
        logger.info("Long search string error: " + e.getMessage())
    }

    // Edge Case Test 3: Unicode characters
    try {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('title:测试') ORDER BY id"
        // Should handle Unicode gracefully
    } catch (Exception e) {
        logger.info("Unicode search error: " + e.getMessage())
    }

    // NOT search functionality tests

    // NOT Search Test 1: Basic NOT search for content field
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName} WHERE NOT search('content:msg') ORDER BY id"

    // NOT Search Test 2: NOT search with specific field
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE NOT search('title:Message') ORDER BY id"

    // NOT Search Test 3: NOT search with multiple terms
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE NOT search('content:message AND category:Message') ORDER BY id"

    // NOT Search Test 4: NOT search with complex boolean logic
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE NOT search('(content:msg OR title:Message) AND status:published') ORDER BY id"

    // NOT Search Test 5: NOT search with wildcard
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE NOT search('content:*msg*') ORDER BY id"

    // NOT Search Test 6: NOT search with phrase
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE NOT search('content:\"success message\"') ORDER BY id"

    // Null value handling tests

    // Null Test 1: Search on field with null values
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName} WHERE search('content:null') ORDER BY id"

    // Null Test 2: Search on null field should return no results
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:null') ORDER BY id"

    // Null Test 3: NOT search on field with null values
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName} WHERE NOT search('content:null') ORDER BY id"

    // Null Test 4: Search on category field with null values
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, category FROM ${tableName} WHERE search('category:Test') ORDER BY id"

    // Null Test 5: NOT search on category field with null values
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, category FROM ${tableName} WHERE NOT search('category:Test') ORDER BY id"

    // Null Test 6: Search on tags field with null values
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, tags FROM ${tableName} WHERE search('tags:test') ORDER BY id"

    // Null Test 7: NOT search on tags field with null values
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, tags FROM ${tableName} WHERE NOT search('tags:test') ORDER BY id"

    // Null Test 8: Search on author field with null values
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, author FROM ${tableName} WHERE search('author:Test') ORDER BY id"

    // Null Test 9: NOT search on author field with null values
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, author FROM ${tableName} WHERE NOT search('author:Test') ORDER BY id"

    // Null Test 10: Search on status field with null values
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, status FROM ${tableName} WHERE search('status:draft') ORDER BY id"

    // Null Test 11: NOT search on status field with null values
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, status FROM ${tableName} WHERE NOT search('status:draft') ORDER BY id"

    // Combined NOT search and null handling tests

    // Combined Test 1: NOT search with null content handling
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, content FROM ${tableName} WHERE NOT search('content:msg') AND content IS NOT NULL ORDER BY id"

    // Combined Test 2: NOT search with null title handling
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE NOT search('title:Test') AND title IS NOT NULL ORDER BY id"

    // Combined Test 3: NOT search with null category handling
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, category FROM ${tableName} WHERE NOT search('category:Test') AND category IS NOT NULL ORDER BY id"

    // Combined Test 4: NOT search with null tags handling
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, tags FROM ${tableName} WHERE NOT search('tags:test') AND tags IS NOT NULL ORDER BY id"

    // Combined Test 5: NOT search with null author handling
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, author FROM ${tableName} WHERE NOT search('author:Test') AND author IS NOT NULL ORDER BY id"

    // Combined Test 6: NOT search with null status handling
    qt_sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title, status FROM ${tableName} WHERE NOT search('status:draft') AND status IS NOT NULL ORDER BY id"
}
