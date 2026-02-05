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

suite("test_search_usage_restrictions") {
    def tableName = "search_usage_test_table"
    def tableName2 = "search_usage_test_table2"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP TABLE IF EXISTS ${tableName2}"

    // Create test table with inverted index
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            title VARCHAR(255),
            content TEXT,
            category VARCHAR(100),
            INDEX idx_title (title) USING INVERTED,
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX idx_category (category) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    // Create second table for join tests
    sql """
        CREATE TABLE ${tableName2} (
            id INT,
            name VARCHAR(255)
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    // Insert test data
    sql """INSERT INTO ${tableName} VALUES
        (1, 'Machine Learning', 'AI and ML tutorial', 'Technology'),
        (2, 'Deep Learning', 'Neural networks guide', 'Technology'),
        (3, 'Python Guide', 'Python programming', 'Programming'),
        (4, 'Data Science', 'Data analysis methods', 'Science'),
        (5, 'Web Development', 'Web dev tips', 'Technology')
    """

    sql """INSERT INTO ${tableName2} VALUES (1, 'Test'), (2, 'Example')"""

    // Wait for data
    Thread.sleep(5000)

    // ============ Valid Usage Tests ============

    // Test 1: search() in WHERE clause is allowed
    qt_valid_where "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} WHERE search('title:Learning') ORDER BY id"

    // Test 2: search() with AND/OR in WHERE is allowed
    qt_valid_where_complex "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('title:Learning') AND id > 1 ORDER BY id"

    // Test 3: Multiple search() in WHERE is allowed
    qt_valid_multiple_search "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('title:Learning') OR search('content:tutorial') ORDER BY id"

    // Test 4: search() with LIMIT is allowed
    qt_valid_with_limit "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('title:Learning') LIMIT 2"

    // ============ Invalid Usage Tests - Should Fail ============

    // Test 5: search() in GROUP BY should fail
    test {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(*) FROM ${tableName} GROUP BY search('title:Learning')"
        exception "predicates are only supported inside WHERE filters on single-table scans"
    }

    // Test 6: search() in SELECT then GROUP BY alias should fail
    test {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ search('title:Learning') as s, count(*) FROM ${tableName} GROUP BY s"
        exception "search()"
    }

    // Test 7: search() in SELECT projection (without WHERE) should fail
    test {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ search('title:Learning'), title FROM ${tableName}"
        exception "search()"
    }

    // Test 8: search() in aggregate output should fail
    test {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ count(search('title:Learning')) FROM ${tableName}"
        exception "search()"
    }

    // Test 9: search() in HAVING clause should fail
    test {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ category, count(*) FROM ${tableName} GROUP BY category HAVING search('title:Learning')"
        exception "predicates are only supported inside WHERE filters on single-table scans"
    }

    // Test 10: search() with JOIN should fail (not single table)
    test {
        sql """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ t1.id FROM ${tableName} t1
            JOIN ${tableName2} t2 ON t1.id = t2.id
            WHERE search('title:Learning')
        """
        exception "single"
    }

    // Test 11: search() in ORDER BY should fail
    test {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, title FROM ${tableName} ORDER BY search('title:Learning')"
        exception "search()"
    }

    // Test 12: search() in CASE WHEN (outside WHERE) should fail
    test {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ CASE WHEN search('title:Learning') THEN 1 ELSE 0 END FROM ${tableName}"
        exception "search()"
    }

    // Test 13: search() in aggregate function context should fail
    test {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ max(search('title:Learning')) FROM ${tableName}"
        exception "search()"
    }

    // Test 14: search() in window function should fail
    test {
        sql "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, row_number() OVER (ORDER BY search('title:Learning')) FROM ${tableName}"
        exception "search()"
    }

    // ============ Edge Cases ============

    // Test 15: search() in subquery WHERE is allowed (subquery is single table)
    qt_valid_subquery """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ * FROM (
            SELECT id, title FROM ${tableName} WHERE search('title:Learning')
        ) t WHERE id > 1 ORDER BY id
    """

    // Test 16: Multiple fields in search() DSL is allowed
    qt_valid_multi_field "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('title:Learning AND content:tutorial') ORDER BY id"

    // Test 17: search() with complex boolean logic in WHERE is allowed
    qt_valid_complex_where """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName}
        WHERE (search('title:Learning') OR id = 1) AND category = 'Technology'
        ORDER BY id
    """

    // Test 18: search() in UNION queries (each part valid) should work
    qt_valid_union """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id FROM ${tableName} WHERE search('title:Learning')
        UNION ALL
        SELECT id FROM ${tableName} WHERE search('content:tutorial')
        ORDER BY id
    """
}
