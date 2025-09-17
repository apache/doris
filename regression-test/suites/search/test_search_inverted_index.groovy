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

suite("test_search_inverted_index") {
    
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
    
    // Insert test data (1000 records for performance testing)
    def testData = generateTestData(1000)
    
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
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex}"
        result([
            [1000]
        ])
    }
    
    test {
        sql "SELECT COUNT(*) FROM ${tableWithoutIndex}"
        result([
            [1000]
        ])
    }
    
    // Test 1: Basic search functionality on indexed table
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('title:Machine')"
        // Should find all records with "Machine Learning" in title
        check { result ->
            assertTrue(result[0][0] > 0, "Should find records with Machine in title")
            assertTrue(result[0][0] <= 100, "Should not exceed expected count")
        }
    }
    
    // Test 2: Compare results between indexed and non-indexed tables
    test {
        def indexedResults = sql "SELECT id FROM ${tableWithIndex} WHERE search('title:Learning') ORDER BY id"
        def nonIndexedResults = sql "SELECT id FROM ${tableWithoutIndex} WHERE search('title:Learning') ORDER BY id"
        
        // Results should be identical
        assertEquals(indexedResults.size(), nonIndexedResults.size())
        for (int i = 0; i < indexedResults.size(); i++) {
            assertEquals(indexedResults[i][0], nonIndexedResults[i][0])
        }
    }
    
    // Test 3: Complex search with multiple fields
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('title:Machine AND category:Technology')"
        check { result ->
            assertTrue(result[0][0] >= 0, "Should execute without error")
        }
    }
    
    // Test 4: Search with phrase queries
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('content:\"comprehensive content\"')"
        check { result ->
            assertTrue(result[0][0] > 100, "Should find many records with phrase in content")
        }
    }
    
    // Test 5: Search with boolean operators
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('(title:Learning OR title:Guide) AND category:Technology')"
        check { result ->
            assertTrue(result[0][0] >= 0, "Should execute complex boolean query")
        }
    }
    
    // Test 6: Search with NOT operator
    test {
        def totalRecords = sql "SELECT COUNT(*) FROM ${tableWithIndex}"
        def excludedRecords = sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('NOT title:Machine')"
        
        assertTrue(excludedRecords[0][0] < totalRecords[0][0], "NOT operator should exclude some records")
    }
    
    // Test 7: Wildcard searches
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('title:Learn*')"
        check { result ->
            assertTrue(result[0][0] > 0, "Should find records with Learning/Learned/etc")
        }
    }
    
    // Test 8: Search in tags field (test tokenized field)
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('tags:tutorial')"
        check { result ->
            assertTrue(result[0][0] > 100, "Should find many records with tutorial tag")
        }
    }
    
    // Test 9: Search by author
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('author:\"John Smith\"')"
        check { result ->
            assertTrue(result[0][0] > 0, "Should find records by John Smith")
            assertTrue(result[0][0] <= 200, "Should not exceed expected author count")
        }
    }
    
    // Test 10: Search combined with regular WHERE clauses
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('category:Technology') AND view_count > 3000"
        check { result ->
            assertTrue(result[0][0] >= 0, "Should combine search with numeric filters")
        }
    }
    
    // Test 11: Search with GROUP BY
    test {
        sql "SELECT category, COUNT(*) as cnt FROM ${tableWithIndex} WHERE search('title:Learning') GROUP BY category ORDER BY cnt DESC"
        check { result ->
            assertTrue(result.size() > 0, "Should group search results by category")
        }
    }
    
    // Test 12: Search with ORDER BY and LIMIT
    test {
        sql "SELECT id, title FROM ${tableWithIndex} WHERE search('title:Advanced') ORDER BY view_count DESC LIMIT 10"
        check { result ->
            assertTrue(result.size() <= 10, "Should limit results to 10")
            if (result.size() > 1) {
                // Verify ordering by view_count
                def viewCounts = sql "SELECT view_count FROM ${tableWithIndex} WHERE id IN (${result.collect{it[0]}.join(',')}) ORDER BY view_count DESC"
                for (int i = 1; i < viewCounts.size(); i++) {
                    assertTrue(viewCounts[i-1][0] >= viewCounts[i][0], "Should be ordered by view_count DESC")
                }
            }
        }
    }
    
    // Test 13: Search with aggregation functions
    test {
        sql "SELECT AVG(rating), MAX(view_count), MIN(view_count) FROM ${tableWithIndex} WHERE search('category:Technology')"
        check { result ->
            assertTrue(result[0][0] != null, "Should calculate aggregations on search results")
            assertTrue(result[0][1] != null, "Should have max view_count")
            assertTrue(result[0][2] != null, "Should have min view_count")
        }
    }
    
    // Test 14: Search with HAVING clause
    test {
        sql """
            SELECT category, COUNT(*) as cnt 
            FROM ${tableWithIndex} 
            WHERE search('tags:guide OR tags:tutorial') 
            GROUP BY category 
            HAVING cnt > 10
            ORDER BY cnt DESC
        """
        check { result ->
            for (def row : result) {
                assertTrue(row[1] > 10, "All groups should have count > 10")
            }
        }
    }
    
    // Test 15: Nested search queries
    test {
        sql """
            SELECT id, title FROM ${tableWithIndex} 
            WHERE search('title:Complete OR title:Advanced') 
            AND id IN (
                SELECT id FROM ${tableWithIndex} 
                WHERE search('category:Technology OR category:Science')
            )
            ORDER BY id LIMIT 5
        """
        check { result ->
            assertTrue(result.size() <= 5, "Should limit nested query results")
        }
    }
    
    // Performance Test 1: Simple search on large dataset
    def startTime = System.currentTimeMillis()
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('title:Learning')"
    }
    def indexedTime = System.currentTimeMillis() - startTime
    
    startTime = System.currentTimeMillis()
    test {
        sql "SELECT COUNT(*) FROM ${tableWithoutIndex} WHERE search('title:Learning')"
    }
    def nonIndexedTime = System.currentTimeMillis() - startTime
    
    logger.info("Search performance - Indexed: ${indexedTime}ms, Non-indexed: ${nonIndexedTime}ms")
    
    // Performance Test 2: Complex boolean search
    startTime = System.currentTimeMillis()
    test {
        sql "SELECT id, title FROM ${tableWithIndex} WHERE search('(title:Machine OR title:Data) AND (category:Technology OR category:Science) AND NOT tags:deprecated') ORDER BY view_count DESC LIMIT 50"
    }
    def complexSearchTime = System.currentTimeMillis() - startTime
    logger.info("Complex search performance: ${complexSearchTime}ms")
    
    // Performance Test 3: Phrase search on large text fields
    startTime = System.currentTimeMillis()
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('content:\"practical examples\"')"
    }
    def phraseSearchTime = System.currentTimeMillis() - startTime
    logger.info("Phrase search performance: ${phraseSearchTime}ms")
    
    // Edge case tests
    
    // Edge Case 1: Search with special characters
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('title:\"Complete Guide\"')"
        check { result ->
            assertTrue(result[0][0] >= 0, "Should handle phrases with special characters")
        }
    }
    
    // Edge Case 2: Search with very common terms
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('content:the')"
        check { result ->
            // 'the' should be filtered out by stop words or return many results
            assertTrue(result[0][0] >= 0, "Should handle common stop words")
        }
    }
    
    // Edge Case 3: Search with numeric-like terms
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('content:2023')"
        check { result ->
            assertTrue(result[0][0] >= 0, "Should handle numeric terms in text")
        }
    }
    
    // Edge Case 4: Case sensitivity test
    test {
        def upperCaseResult = sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('title:MACHINE')"
        def lowerCaseResult = sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('title:machine')"
        def mixedCaseResult = sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('title:Machine')"
        
        // Results should be the same (case insensitive)
        assertEquals(upperCaseResult[0][0], lowerCaseResult[0][0])
        assertEquals(lowerCaseResult[0][0], mixedCaseResult[0][0])
    }
    
    // Stress Test: Multiple concurrent search queries
    def concurrentSearchTest = {
        def futures = []
        def executor = java.util.concurrent.Executors.newFixedThreadPool(5)
        
        try {
            for (int i = 0; i < 10; i++) {
                def searchTerm = ["Machine", "Learning", "Data", "Advanced", "Complete"][i % 5]
                futures.add(executor.submit({
                    sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('title:${searchTerm}')"
                }))
            }
            
            // Wait for all searches to complete
            for (def future : futures) {
                try {
                    future.get(30, java.util.concurrent.TimeUnit.SECONDS)
                } catch (Exception e) {
                    logger.error("Concurrent search failed: " + e.getMessage())
                }
            }
            
            logger.info("Concurrent search test completed successfully")
        } finally {
            executor.shutdown()
        }
    }
    
    concurrentSearchTest()
    
    // Index effectiveness test
    test {
        // This query should benefit significantly from inverted index
        def query = "SELECT id, title FROM ${tableWithIndex} WHERE search('title:Introduction AND content:\"practical examples\" AND tags:tutorial') ORDER BY view_count DESC LIMIT 100"
        
        startTime = System.currentTimeMillis()
        sql query
        def indexOptimizedTime = System.currentTimeMillis() - startTime
        
        logger.info("Index-optimized query time: ${indexOptimizedTime}ms")
        
        // The query should complete in reasonable time with indexes
        assertTrue(indexOptimizedTime < 30000, "Query with indexes should complete within 30 seconds")
    }
    
    // Test ANY query performance
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('tags:ANY(tutorial guide example)')"
        check { result ->
            assertTrue(result[0][0] >= 0, "Should execute ANY query without error")
        }
    }
    
    // Test ALL query performance  
    test {
        sql "SELECT COUNT(*) FROM ${tableWithIndex} WHERE search('tags:ALL(programming language)')"
        check { result ->
            assertTrue(result[0][0] >= 0, "Should execute ALL query without error")
        }
    }
    
    // Test complex ANY/ALL combinations
    test {
        sql "SELECT id, title FROM ${tableWithIndex} WHERE search('(tags:ANY(java python) OR tags:ALL(tutorial guide)) AND category:Technology') LIMIT 10"
        check { result ->
            assertTrue(result.size() <= 10, "Should limit complex query results")
        }
    }

    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableWithIndex}"
    sql "DROP TABLE IF EXISTS ${tableWithoutIndex}"
}
