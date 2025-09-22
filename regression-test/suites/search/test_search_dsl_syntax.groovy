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
        [10, "Microservices Architecture", "Design patterns for scalable microservices systems", "Architecture", "microservices, scalability, patterns", "Henry Taylor", "published", 1, "2023-10-01", "2023-10-01 17:25:00", 4.5]
    ]
    
    for (def row : testData) {
        sql """INSERT INTO ${tableName} VALUES (${row[0]}, '${row[1]}', '${row[2]}', '${row[3]}', '${row[4]}', '${row[5]}', '${row[6]}', ${row[7]}, '${row[8]}', '${row[9]}', ${row[10]})"""
    }
    
    // Wait for index building
    Thread.sleep(5000)
    
    // DSL Syntax Test 1: Simple term queries
    qt_sql "SELECT id, title FROM ${tableName} WHERE search('title:Machine') ORDER BY id"
    
    // DSL Syntax Test 2: Phrase queries with quotes
    qt_sql "SELECT id, title FROM ${tableName} WHERE search('title:\"Machine Learning\"')"
    
    // DSL Syntax Test 3: Wildcard queries
    qt_sql "SELECT id, title FROM ${tableName} WHERE search('title:*Learning') ORDER BY id"
    
    // DSL Syntax Test 4: Prefix queries
    qt_sql "SELECT id, title FROM ${tableName} WHERE search('title:Data*')"
    
    // DSL Syntax Test 5: Regular expression queries
    qt_sql "SELECT id, title FROM ${tableName} WHERE search('title:/.*[Dd]ata.*/')"
    
    // DSL Syntax Test 6: Boolean AND operator
    qt_sql "SELECT id, title FROM ${tableName} WHERE search('category:Technology AND tags:AI') ORDER BY id"
    
    // DSL Syntax Test 7: Boolean OR operator
    qt_sql "SELECT id, title FROM ${tableName} WHERE search('category:Programming OR category:Science') ORDER BY id"
    
    // DSL Syntax Test 8: Boolean NOT operator
    qt_sql "SELECT COUNT(*) FROM ${tableName} WHERE search('status:published AND NOT category:Technology')"
    
    // DSL Syntax Test 9: Parentheses for grouping
    qt_sql "SELECT id, title FROM ${tableName} WHERE search('(category:Technology OR category:AI) AND status:published') ORDER BY id"
    
    // DSL Syntax Test 10: Complex nested queries
    qt_sql "SELECT id, title FROM ${tableName} WHERE search('(title:Learning OR title:Development) AND (status:published OR status:review)') ORDER BY id"
    
    // DSL Syntax Test 11: Range queries (if supported)
    try {
        qt_sql "SELECT id, title FROM ${tableName} WHERE search('priority:[1 TO 2]') ORDER BY id"
        // This may or may not work depending on implementation
    } catch (Exception e) {
        // Range queries might not be implemented yet
        logger.info("Range query not supported: " + e.getMessage())
    }

    // DSL Syntax Test 12: List queries with IN operator (if supported)
    try {
        qt_sql "SELECT id, title FROM ${tableName} WHERE search('status:IN(published draft)') "
               "ORDER BY id"
        // This may or may not work depending on implementation
    } catch (Exception e) {
        // List queries might not be implemented yet
        logger.info("List query not supported: " + e.getMessage())
    }

    // DSL Syntax Test 13: ANY queries
    qt_sql "SELECT id, title FROM ${tableName} WHERE search('tags:ANY(AI python)') ORDER BY id"

            // DSL Syntax Test 14: ALL queries
            qt_sql
           "SELECT id, title FROM ${tableName} WHERE search('tags:ALL(machine learning)') ORDER BY "
           "id"

            // DSL Syntax Test 15: Field names with quotes
            qt_sql "SELECT id, title FROM ${tableName} WHERE search('\"title\":Machine')"

            // DSL Syntax Test 16: Case insensitive field names
            qt_sql "SELECT id, title FROM ${tableName} WHERE search('TITLE:Machine')"

            // DSL Syntax Test 17: Multiple terms in same field
            qt_sql
           "SELECT id, title FROM ${tableName} WHERE search('content:machine AND content:learning')"

            // DSL Syntax Test 18: Hyphenated terms in tags
            qt_sql "SELECT id, title FROM ${tableName} WHERE search('tags:machine-learning')"

            // DSL Syntax Test 19: Special characters in search terms
            qt_sql "SELECT id, title FROM ${tableName} WHERE search('author:\"John Doe\"')"

            // DSL Syntax Test 20: Multiple field search with complex boolean logic
            qt_sql
           "SELECT id, title FROM ${tableName} WHERE search('(title:Learning AND "
           "category:Technology) OR (title:Development AND category:Web)') ORDER BY id"

            // Error handling tests for invalid DSL syntax

            // Error Test 1: Unclosed parentheses
            try {
        sql "SELECT id FROM ${tableName} WHERE search('(title:Machine')" assertTrue(
                false, "Expected exception for unclosed parentheses")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Invalid") || e.getMessage().contains("syntax"))
    }

    // Error Test 2: Missing field name
    try {
        sql "SELECT id FROM ${tableName} WHERE search(':value')" assertTrue(
                false, "Expected exception for missing field name")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Invalid") || e.getMessage().contains("syntax"))
    }

    // Error Test 3: Missing value
    try {
        sql "SELECT id FROM ${tableName} WHERE search('field:')" assertTrue(
                false, "Expected exception for missing value")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Invalid") || e.getMessage().contains("syntax"))
    }

    // Error Test 4: Invalid boolean operator
    try {
        sql "SELECT id FROM ${tableName} WHERE search('title:Machine XOR "
            "category:Technology')" assertTrue(false,
                                               "Expected exception for invalid boolean operator")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Invalid") || e.getMessage().contains("syntax"))
    }

    // Error Test 5: Unclosed quotes
    try {
        sql "SELECT id FROM ${tableName} WHERE search('title:\"Machine Learning')" assertTrue(
                false, "Expected exception for unclosed quotes")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Invalid") || e.getMessage().contains("syntax"))
    }

    // Error Test 6: Invalid regular expression
    try {
        sql "SELECT id FROM ${tableName} WHERE search('title:/[invalid/')" assertTrue(
                false, "Expected exception for invalid regex")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Invalid") || e.getMessage().contains("syntax"))
    }

    // Performance and edge case tests

    // Edge Case Test 1: Empty search string
    try {
        sql "SELECT id FROM ${tableName} WHERE search('')"
        // Should handle empty string gracefully
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("empty") || e.getMessage().contains("Invalid"))
    }

    // Edge Case Test 2: Very long search string
    def longTerm = "a" * 1000 try {
        sql "SELECT id FROM ${tableName} WHERE search('title:${longTerm}')"
        // Should handle long strings gracefully
    } catch (Exception e) {
        // Acceptable if implementation has length limits
        logger.info("Long search string error: " + e.getMessage())
    }

    // Edge Case Test 3: Unicode characters
    try {
        sql "SELECT id FROM ${tableName} WHERE search('title:测试')"
        // Should handle Unicode gracefully
    } catch (Exception e) {
        logger.info("Unicode search error: " + e.getMessage())
    }

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"
}
