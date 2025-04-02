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

suite("test_expression_tree_reconstruction") {

    def testTable = "test_expression_tree_reconstruction"
    // Clean up any existing table with the same name
    sql "DROP TABLE IF EXISTS ${testTable}"

    // Create a test table
    sql """
    CREATE TABLE ${testTable}
    (
        id int,
        name string,
        price decimal(10, 2),
        created_date datetime
    )
    COMMENT "test table for expression tree reconstruction"
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    )
    """

    // Insert data into the table
    sql """
    INSERT INTO ${testTable} VALUES
    (1, 'item1', 10.50, '2025-01-01 12:00:00'),
    (2, 'item2', 20.75, '2025-01-02 13:30:00'),
    (3, 'item3', 15.25, '2025-01-03 09:45:00'),
    (4, 'item4', 30.00, '2025-01-04 16:15:00'),
    (5, 'item5', 25.50, '2025-01-05 11:20:00')
    """

    // Synchronize to ensure data is visible
    sql "SYNC"

    // Test complex expressions that would trigger the tree reconstruction logic
    // These queries will create expression trees that need to be reconstructed from Thrift

    // Test 1: Simple expression with multiple operands
    qt_test1 """
    SELECT id, name, price * 1.1 + 5 AS adjusted_price
    FROM ${testTable}
    ORDER BY id
    """

    // Test 2: Nested expressions
    qt_test2 """
    SELECT id,
           CASE
               WHEN price > 20.0 THEN 'expensive'
               WHEN price > 15.0 THEN 'moderate'
               ELSE 'cheap'
           END AS price_category
    FROM ${testTable}
    ORDER BY id
    """

    // Test 3: Complex expression with multiple functions and nested conditions
    qt_test3 """
    SELECT
        id,
        name,
        IF(price > 20.0,
           CONCAT(name, ' - premium'),
           CONCAT(name, ' - standard')) AS product_tier,
        DATEDIFF(NOW(), created_date) AS days_since_created
    FROM ${testTable}
    ORDER BY id
    """

    // Test 4: Aggregation with expressions
    qt_test4 """
    SELECT
        SUBSTR(name, 1, 4) AS name_prefix,
        AVG(price) AS avg_price,
        SUM(IF(price > 20.0, price, 0)) AS premium_price_sum,
        COUNT(*) AS count
    FROM ${testTable}
    GROUP BY SUBSTR(name, 1, 4)
    """

    // Test 5: Multiple joins with expressions
    // First create a companion table
    def joinTable = "test_expression_tree_join"
    sql "DROP TABLE IF EXISTS ${joinTable}"
    sql """
    CREATE TABLE ${joinTable}
    (
        id int,
        category string,
        discount decimal(5, 2)
    )
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1"
    )
    """

    sql """
    INSERT INTO ${joinTable} VALUES
    (1, 'electronics', 0.10),
    (2, 'books', 0.05),
    (3, 'clothing', 0.15),
    (4, 'food', 0.02),
    (5, 'toys', 0.12)
    """

    sql "SYNC"

    qt_test5 """
    SELECT
        t1.id,
        t1.name,
        t2.category,
        t1.price * (1 - t2.discount) AS discounted_price,
        CASE
            WHEN t2.discount > 0.10 THEN 'High Discount'
            WHEN t2.discount > 0.05 THEN 'Medium Discount'
            ELSE 'Low Discount'
        END AS discount_tier
    FROM ${testTable} t1
    JOIN ${joinTable} t2 ON t1.id = t2.id
    ORDER BY t1.id
    """

    // Clean up
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql "DROP TABLE IF EXISTS ${joinTable}"
}