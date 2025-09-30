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

suite("test_search_function") {
    
    def tableName = "search_test_table"
    def indexTableName = "search_test_index_table"
    
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${indexTableName}"
    
    // Create test table without inverted index
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(255),
            content TEXT,
            category VARCHAR(100),
            tags VARCHAR(200),
            publish_date DATE,
            view_count INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """
    
    // Create test table with inverted index
    sql """
        CREATE TABLE ${indexTableName} (
            id INT,
            title VARCHAR(255),
            content TEXT,
            category VARCHAR(100),
            tags VARCHAR(200),
            publish_date DATE,
            view_count INT,
            INDEX idx_title (title) USING INVERTED,
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_category (category) USING INVERTED,
            INDEX idx_tags (tags) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """
    
    // Insert test data
    def testData = [
        [1, "Machine Learning Basics", "Introduction to machine learning algorithms and concepts", "Technology", "machine learning, AI, algorithms", "2023-01-15", 1500],
        [2, "Deep Learning Tutorial", "Advanced deep learning techniques and neural networks", "Technology", "deep learning, neural networks, AI", "2023-02-20", 2300],
        [3, "Python Programming Guide", "Complete guide to Python programming language", "Programming", "python, programming, tutorial", "2023-03-10", 1800],
        [4, "Data Science Methods", "Statistical methods for data science and analytics", "Science", "data science, statistics, analytics", "2023-04-05", 1200],
        [5, "Web Development Tips", "Modern web development best practices", "Technology", "web development, javascript, HTML", "2023-05-12", 950],
        [6, "Algorithm Design", "Fundamental algorithms and data structures", "Computer Science", "algorithms, data structures, programming", "2023-06-18", 1650],
        [7, "Natural Language Processing", "NLP techniques and applications", "Technology", "NLP, natural language, processing", "2023-07-22", 1100],
        [8, "Cloud Computing Overview", "Introduction to cloud computing platforms", "Technology", "cloud computing, AWS, Azure", "2023-08-14", 1350],
        [9, "Database Systems", "Relational and NoSQL database concepts", "Technology", "database, SQL, NoSQL", "2023-09-09", 1450],
        [10, "Software Engineering", "Best practices in software development", "Programming", "software engineering, development, practices", "2023-10-01", 1750]
    ]
    
    for (def row : testData) {
        sql """INSERT INTO ${tableName} VALUES (${row[0]}, '${row[1]}', '${row[2]}', '${row[3]}', '${row[4]}', '${row[5]}', ${row[6]})"""
        sql """INSERT INTO ${indexTableName} VALUES (${row[0]}, '${row[1]}', '${row[2]}', '${row[3]}', '${row[4]}', '${row[5]}', ${row[6]})"""
    }
    
    // Wait for index building
    Thread.sleep(5000)
    
    test {
        sql "SELECT COUNT(*) FROM ${tableName}"
        result([
            [10]
        ])
    }
    
    test {
        sql "SELECT COUNT(*) FROM ${indexTableName}"
        result([
            [10]
        ])
    }
    
    // Test 1: Basic term search
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('title:Machine')"
        result([
            [1, "Machine Learning Basics"]
        ])
    }
    
    // Test 2: Phrase search
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('title:\"Machine Learning\"')"
        result([
            [1, "Machine Learning Basics"]
        ])
    }
    
    // Test 3: Multiple field search with AND
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('title:Learning AND category:Technology') ORDER BY id"
        result([
            [1, "Machine Learning Basics"],
            [2, "Deep Learning Tutorial"]
        ])
    }
    
    // Test 4: Multiple field search with OR
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('title:Python OR title:Algorithm') ORDER BY id"
        result([
            [3, "Python Programming Guide"],
            [6, "Algorithm Design"]
        ])
    }
    
    // Test 5: NOT search
    test {
        sql "SELECT COUNT(*) FROM ${indexTableName} WHERE search('category:Technology AND NOT title:Machine')"
        result([
            [5]
        ])
    }
    
    // Test 6: Complex nested search
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('(title:Learning OR content:algorithms) AND category:Technology') ORDER BY id"
        result([
            [1, "Machine Learning Basics"],
            [2, "Deep Learning Tutorial"]
        ])
    }
    
    // Test 7: Wildcard search
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('title:Learn*') ORDER BY id"
        result([
            [1, "Machine Learning Basics"],
            [2, "Deep Learning Tutorial"]
        ])
    }
    
    // Test 8: Prefix search
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('title:Data*')"
        result([
            [4, "Data Science Methods"],
            [6, "Algorithm Design"]
        ])
    }
    
    // Test 9: Search in content field
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('content:neural')"
        result([
            [2, "Deep Learning Tutorial"]
        ])
    }
    
    // Test 10: Search in tags field
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('tags:programming') ORDER BY id"
        result([
            [3, "Python Programming Guide"],
            [6, "Algorithm Design"]
        ])
    }
    
    // Test 11: Case insensitive search
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('title:MACHINE')"
        result([
            [1, "Machine Learning Basics"]
        ])
    }
    
    // Test 12: Search with spaces in field values
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('content:\"machine learning\"')"
        result([
            [1, "Machine Learning Basics"]
        ])
    }
    
    // Test 13: Empty search result
    test {
        sql "SELECT COUNT(*) FROM ${indexTableName} WHERE search('title:nonexistent')"
        result([
            [0]
        ])
    }
    
    // Test 14: Search combined with other WHERE conditions
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('category:Technology') AND view_count > 1400 ORDER BY id"
        result([
            [1, "Machine Learning Basics"],
            [2, "Deep Learning Tutorial"]
        ])
    }
    
    // Test 15: Search with GROUP BY
    test {
        sql "SELECT category, COUNT(*) as cnt FROM ${indexTableName} WHERE search('title:Learning OR title:Programming') GROUP BY category ORDER BY category"
        result([
            ["Programming", 1],
            ["Technology", 2]
        ])
    }
    
    // Test 16: Search with ORDER BY
    test {
        sql "SELECT id, title, view_count FROM ${indexTableName} WHERE search('tags:AI OR tags:programming') ORDER BY view_count DESC"
        result([
            [2, "Deep Learning Tutorial", 2300],
            [3, "Python Programming Guide", 1800],
            [6, "Algorithm Design", 1650],
            [1, "Machine Learning Basics", 1500]
        ])
    }
    
    // Test 17: Search with LIMIT
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('category:Technology') ORDER BY id LIMIT 3"
        result([
            [1, "Machine Learning Basics"],
            [2, "Deep Learning Tutorial"],
            [5, "Web Development Tips"]
        ])
    }
    
    // Test 18: Search function in SELECT clause (should not be allowed - search is a predicate)
    test {
        try {
            sql "SELECT id, search('title:Machine') FROM ${indexTableName}"
            assertTrue(false, "Expected exception for search in SELECT clause")
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("search") || e.getMessage().contains("not found"))
        }
    }
    
    // Test 19: Invalid DSL syntax
    test {
        try {
            sql "SELECT id FROM ${indexTableName} WHERE search('title:')"
            assertTrue(false, "Expected exception for invalid DSL")
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Invalid") || e.getMessage().contains("syntax"))
        }
    }
    
    // Test 20: ANY query test
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('tags:ANY(AI programming)') ORDER BY id"
        check { result ->
            assertTrue(result.size() > 0, "Should find records with AI or programming in tags")
        }
    }
    
    // Test 21: ALL query test
    test {
        sql "SELECT id, title FROM ${indexTableName} WHERE search('tags:ALL(machine learning)') ORDER BY id"
        result([
            [1, "Machine Learning Basics"]
        ])
    }
    
    // Test 22: Search on non-indexed table (should still work but may be slower)
    test {
        sql "SELECT id, title FROM ${tableName} WHERE search('title:Machine')"
        result([
            [1, "Machine Learning Basics"]
        ])
    }
    
    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${indexTableName}"
}
