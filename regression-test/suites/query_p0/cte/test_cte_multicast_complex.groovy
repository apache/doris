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

suite("test_cte_multicast_complex") {
    // Force CTE materialization to trigger MultiCast
    sql "SET inline_cte_referenced_threshold=0"

    sql "DROP TABLE IF EXISTS test_cte_multicast_tbl"
    sql """
        CREATE TABLE test_cte_multicast_tbl (
            id INT,
            category VARCHAR(50),
            amount DECIMAL(10, 2),
            create_date DATE,
            is_valid BOOLEAN,
            description VARCHAR(200)
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    // Insert test data
    sql """
        INSERT INTO test_cte_multicast_tbl VALUES
        (1, 'Electronics', 1299.99, '2024-01-15', true, 'Laptop computer'),
        (2, 'Electronics', 799.99, '2024-01-16', true, 'Smartphone'),
        (3, 'Books', 29.99, '2024-01-17', true, 'Programming book'),
        (4, 'Books', 19.99, '2024-01-18', false, 'Novel'),
        (5, 'Furniture', 499.99, '2024-02-01', true, 'Office chair'),
        (6, 'Furniture', 899.99, '2024-02-02', true, 'Desk'),
        (7, 'Electronics', 149.99, '2024-02-03', true, 'Headphones'),
        (8, 'Books', 39.99, '2024-02-04', true, 'Technical manual'),
        (9, 'Furniture', 199.99, '2024-02-05', false, 'Lamp'),
        (10, 'Electronics', 2499.99, '2024-02-06', true, 'Gaming PC'),
        (11, 'Books', 15.99, '2024-02-07', true, 'Magazine'),
        (12, 'Furniture', 1299.99, '2024-02-08', true, 'Sofa')
    """

    // Test 1: Basic CTE with multiple references and different filters
    // This triggers MultiCast with filter operations on shared columns
    order_qt_cte_multi_filter """
        WITH base_data AS (
            SELECT id, category, amount, is_valid
            FROM test_cte_multicast_tbl
            WHERE amount > 0
        )
        SELECT 'high_value' as tier, id, category, amount FROM base_data WHERE amount > 500
        UNION ALL
        SELECT 'medium_value' as tier, id, category, amount FROM base_data WHERE amount BETWEEN 100 AND 500
        UNION ALL
        SELECT 'low_value' as tier, id, category, amount FROM base_data WHERE amount < 100
        """

    // Test 2: CTE with aggregation and multiple consumers with boolean filters
    // Tests COW with boolean column operations
    order_qt_cte_agg_boolean """
        WITH category_stats AS (
            SELECT 
                category,
                COUNT(*) as item_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount,
                MAX(is_valid) as has_valid
            FROM test_cte_multicast_tbl
            GROUP BY category
        )
        SELECT 'high_count' as label, category, item_count, total_amount FROM category_stats WHERE item_count > 3
        UNION ALL
        SELECT 'low_count' as label, category, item_count, total_amount FROM category_stats WHERE item_count <= 3
        UNION ALL
        SELECT 'has_valid' as label, category, item_count, total_amount FROM category_stats WHERE has_valid = true
        """

    // Test 3: Nested CTE with multiple levels of references
    order_qt_cte_nested_multi_ref """
        WITH level1 AS (
            SELECT id, category, amount, is_valid FROM test_cte_multicast_tbl WHERE is_valid = true
        ),
        level2 AS (
            SELECT category, SUM(amount) as cat_total FROM level1 GROUP BY category
        ),
        level3 AS (
            SELECT l1.*, l2.cat_total 
            FROM level1 l1 
            JOIN level2 l2 ON l1.category = l2.category
        )
        SELECT 'above_avg' as comparison, id, category, amount, cat_total 
        FROM level3 WHERE amount > cat_total / 3
        UNION ALL
        SELECT 'below_avg' as comparison, id, category, amount, cat_total 
        FROM level3 WHERE amount <= cat_total / 3
        """

    // Test 4: CTE with window functions and multiple consumers
    // Window functions create complex column dependencies
    order_qt_cte_window_multi_ref """
        WITH ranked_items AS (
            SELECT 
                id, category, amount,
                ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) as rn,
                SUM(amount) OVER (PARTITION BY category) as category_total,
                amount > AVG(amount) OVER (PARTITION BY category) as above_avg
            FROM test_cte_multicast_tbl
            WHERE is_valid = true
        )
        SELECT 'top3' as selection, id, category, amount, rn, category_total 
        FROM ranked_items WHERE rn <= 3
        UNION ALL
        SELECT 'above_avg' as selection, id, category, amount, rn, category_total 
        FROM ranked_items WHERE above_avg = true
        UNION ALL
        SELECT 'bottom' as selection, id, category, amount, rn, category_total 
        FROM ranked_items WHERE rn > 3 AND above_avg = false
        """

    // Test 5: CTE with CASE expressions and multiple boolean conditions
    order_qt_cte_case_boolean """
        WITH categorized AS (
            SELECT 
                id, category, amount, is_valid,
                CASE 
                    WHEN amount > 1000 THEN 'premium'
                    WHEN amount > 100 THEN 'standard'
                    ELSE 'budget'
                END as price_tier,
                amount > 500 AND is_valid as high_value_valid
            FROM test_cte_multicast_tbl
        )
        SELECT 'premium' as filter, id, category, amount, price_tier 
        FROM categorized WHERE price_tier = 'premium'
        UNION ALL
        SELECT 'high_valid' as filter, id, category, amount, price_tier 
        FROM categorized WHERE high_value_valid = true
        UNION ALL
        SELECT 'budget_invalid' as filter, id, category, amount, price_tier 
        FROM categorized WHERE price_tier = 'budget' AND is_valid = false
        """

    // Test 6: CTE with self-join creating multiple references
    order_qt_cte_self_join """
        WITH items AS (
            SELECT id, category, amount FROM test_cte_multicast_tbl WHERE is_valid = true
        )
        SELECT 
            'self_compare' as op,
            a.id as id1, a.category, a.amount as amount1,
            b.id as id2, b.amount as amount2
        FROM items a
        JOIN items b ON a.category = b.category AND a.id < b.id
        WHERE a.amount > b.amount
        UNION ALL
        SELECT 
            'category_match' as op,
            a.id as id1, a.category, a.amount as amount1,
            b.id as id2, b.amount as amount2
        FROM items a
        JOIN items b ON a.category = b.category AND a.id != b.id
        WHERE ABS(a.amount - b.amount) < 200
        LIMIT 20
    """

    // Test 7: CTE with empty result in some branches (tests count=0 filter case)
    order_qt_cte_empty_branches """
        WITH base AS (
            SELECT id, category, amount FROM test_cte_multicast_tbl
        )
        SELECT 'exists' as status, id, category, amount FROM base WHERE category = 'Electronics'
        UNION ALL
        SELECT 'not_exists' as status, id, category, amount FROM base WHERE category = 'NonExistent'
        UNION ALL
        SELECT 'filtered_out' as status, id, category, amount FROM base WHERE amount > 10000
        """

    // Test 8: Multiple CTEs with cross-references
    order_qt_cte_cross_reference """
        WITH 
        electronics AS (
            SELECT id, amount, 'Electronics' as cat FROM test_cte_multicast_tbl WHERE category = 'Electronics'
        ),
        books AS (
            SELECT id, amount, 'Books' as cat FROM test_cte_multicast_tbl WHERE category = 'Books'
        ),
        furniture AS (
            SELECT id, amount, 'Furniture' as cat FROM test_cte_multicast_tbl WHERE category = 'Furniture'
        )
        SELECT 'e_high' as label, id, amount, cat FROM electronics WHERE amount > 500
        UNION ALL
        SELECT 'b_all' as label, id, amount, cat FROM books
        UNION ALL
        SELECT 'f_valid' as label, f.id, f.amount, f.cat 
        FROM furniture f 
        WHERE f.amount > (SELECT AVG(amount) FROM electronics)
        UNION ALL
        SELECT 'e_low' as label, id, amount, cat FROM electronics WHERE amount <= 500
        """

    // Test 9: CTE with complex expressions in filter
    order_qt_cte_complex_filter_expr """
        WITH calculated AS (
            SELECT 
                id, category, amount,
                amount * 1.1 as with_tax,
                COALESCE(NULLIF(amount, 0), 1) as safe_amount,
                LENGTH(category) as cat_len
            FROM test_cte_multicast_tbl
        )
        SELECT 'tax_high' as filter, id, category, amount, with_tax 
        FROM calculated WHERE with_tax > 1000 AND cat_len > 5
        UNION ALL
        SELECT 'safe_positive' as filter, id, category, amount, with_tax 
        FROM calculated WHERE safe_amount > 100 AND safe_amount < 500
        UNION ALL
        SELECT 'short_cat' as filter, id, category, amount, with_tax 
        FROM calculated WHERE cat_len <= 5
        """

    // Test 10: CTE with nullable columns and null handling
    order_qt_cte_null_handling """
        WITH nullable_data AS (
            SELECT 
                id, category, amount,
                CASE WHEN is_valid THEN amount ELSE NULL END as valid_amount,
                CASE WHEN amount > 500 THEN category ELSE NULL END as high_category
            FROM test_cte_multicast_tbl
        )
        SELECT 'has_valid_amount' as check_type, id, category, amount, valid_amount 
        FROM nullable_data WHERE valid_amount IS NOT NULL
        UNION ALL
        SELECT 'null_valid_amount' as check_type, id, category, amount, valid_amount 
        FROM nullable_data WHERE valid_amount IS NULL
        UNION ALL
        SELECT 'has_high_category' as check_type, id, category, amount, valid_amount 
        FROM nullable_data WHERE high_category IS NOT NULL
        """

    // Test 11: Stress test with many consumers (5 branches)
    order_qt_cte_many_consumers """
        WITH source AS (
            SELECT id, category, amount, is_valid FROM test_cte_multicast_tbl
        )
        SELECT 'branch1' as b, COUNT(*) as cnt, SUM(amount) as total FROM source WHERE id <= 2
        UNION ALL
        SELECT 'branch2' as b, COUNT(*) as cnt, SUM(amount) as total FROM source WHERE id BETWEEN 3 AND 5
        UNION ALL
        SELECT 'branch3' as b, COUNT(*) as cnt, SUM(amount) as total FROM source WHERE id BETWEEN 6 AND 8
        UNION ALL
        SELECT 'branch4' as b, COUNT(*) as cnt, SUM(amount) as total FROM source WHERE id BETWEEN 9 AND 10
        UNION ALL
        SELECT 'branch5' as b, COUNT(*) as cnt, SUM(amount) as total FROM source WHERE id > 10
        """

    // Test 12: CTE with string operations
    order_qt_cte_string_ops """
        WITH string_data AS (
            SELECT 
                id, category, amount, description,
                UPPER(category) as upper_cat,
                CONCAT(category, '_', CAST(id AS VARCHAR)) as cat_id
            FROM test_cte_multicast_tbl
        )
        SELECT 'upper_e' as op, id, upper_cat, amount FROM string_data WHERE upper_cat LIKE 'E%'
        UNION ALL
        SELECT 'concat_check' as op, id, cat_id, amount FROM string_data WHERE cat_id LIKE '%_1%'
        UNION ALL
        SELECT 'desc_long' as op, id, description, amount FROM string_data WHERE LENGTH(description) > 10
        """
}
