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

suite("test_pythonudtf_sql_integration_module") {
    // Test Python UDTF Integration with SQL Operations
    // Coverage: WHERE, JOIN, GROUP BY, ORDER BY, LIMIT, Subqueries, CTEs
    
    def pyPath = """${context.file.parent}/udtf_scripts/pyudtf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${pyPath}".toString())
    
    try {
        // ========================================
        // Prepare Common UDTF Functions
        // ========================================
        
        // Helper UDTF: Split string into multiple records
        sql """ DROP FUNCTION IF EXISTS udtf_split_module(STRING, STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_split_module(STRING, STRING)
        RETURNS ARRAY<STRUCT<position:INT, value:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.sql_integration_udtf.split_with_position",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        // Helper UDTF: Generate number sequence
        sql """ DROP FUNCTION IF EXISTS udtf_range_module(INT, INT); """
        sql """
        CREATE TABLES FUNCTION udtf_range_module(INT, INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.sql_integration_udtf.generate_range",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        // Helper UDTF: Expand array elements
        sql """ DROP FUNCTION IF EXISTS udtf_explode_array_module(ARRAY<INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_explode_array_module(ARRAY<INT>)
        RETURNS ARRAY<STRUCT<element:INT, element_index:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.sql_integration_udtf.explode_with_index",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        // ========================================
        // Section 1: UDTF with WHERE Clause
        // ========================================
        
        // Test 1.1: Filter BEFORE UDTF (reduce input)
        sql """ DROP TABLE IF EXISTS test_where_before_module; """
        sql """
        CREATE TABLE test_where_before_module (
            id INT,
            category STRING,
            data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_where_before_module VALUES 
        (1, 'A', 'apple,banana'),
        (2, 'B', 'cat,dog'),
        (3, 'A', 'red,green,blue'),
        (4, 'C', 'one,two');
        """
        
        qt_where_before """
            SELECT id, category, tmp.position, tmp.value
            FROM test_where_before_module
            LATERAL VIEW udtf_split_module(data, ',') tmp AS position, value
            WHERE category = 'A'
            ORDER BY id, tmp.position;
        """
        
        // Test 1.2: Filter AFTER UDTF (filter expanded results)
        qt_where_after """
            SELECT id, tmp.position, tmp.value
            FROM test_where_before_module
            LATERAL VIEW udtf_split_module(data, ',') tmp AS position, value
            WHERE tmp.value LIKE '%e%'
            ORDER BY id, tmp.position;
        """
        
        // Test 1.3: Combined Filter (before and after UDTF)
        qt_where_combined """
            SELECT id, category, tmp.value
            FROM test_where_before_module
            LATERAL VIEW udtf_split_module(data, ',') tmp AS position, value
            WHERE category IN ('A', 'B') AND tmp.position = 0
            ORDER BY id;
        """
        
        // ========================================
        // Section 2: UDTF with JOIN Operations
        // ========================================
        
        // Prepare dimension table
        sql """ DROP TABLE IF EXISTS dim_numbers_module; """
        sql """
        CREATE TABLE dim_numbers_module (
            num INT,
            num_name STRING,
            is_even BOOLEAN
        ) ENGINE=OLAP
        DUPLICATE KEY(num)
        DISTRIBUTED BY HASH(num) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO dim_numbers_module VALUES 
        (1, 'one', false),
        (2, 'two', true),
        (3, 'three', false),
        (4, 'four', true),
        (5, 'five', false);
        """
        
        // Prepare fact table
        sql """ DROP TABLE IF EXISTS fact_ranges_module; """
        sql """
        CREATE TABLE fact_ranges_module (
            id INT,
            start_num INT,
            end_num INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO fact_ranges_module VALUES 
        (1, 1, 3),
        (2, 2, 4);
        """
        
        // Test 2.1: INNER JOIN with UDTF
        qt_join_inner """
            SELECT 
                f.id,
                tmp.num,
                d.num_name,
                d.is_even
            FROM fact_ranges_module f
            LATERAL VIEW udtf_range_module(f.start_num, f.end_num) tmp AS num
            INNER JOIN dim_numbers_module d ON tmp.num = d.num
            ORDER BY f.id, tmp.num;
        """
        
        // Test 2.2: LEFT JOIN with UDTF (some generated values may not match)
        sql """ DROP TABLE IF EXISTS fact_ranges_extended_module; """
        sql """
        CREATE TABLE fact_ranges_extended_module (
            id INT,
            start_num INT,
            end_num INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO fact_ranges_extended_module VALUES 
        (1, 1, 2),
        (2, 5, 7);
        """
        
        qt_join_left """
            SELECT 
                f.id,
                tmp.num,
                d.num_name
            FROM fact_ranges_extended_module f
            LATERAL VIEW udtf_range_module(f.start_num, f.end_num) tmp AS num
            LEFT JOIN dim_numbers_module d ON tmp.num = d.num
            ORDER BY f.id, tmp.num;
        """
        
        // Test 2.3: Self-JOIN through UDTF
        sql """ DROP TABLE IF EXISTS test_self_join_module; """
        sql """
        CREATE TABLE test_self_join_module (
            id INT,
            value_list STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_self_join_module VALUES 
        (1, '10,20,30'),
        (2, '20,30,40');
        """
        
        qt_join_self """
            SELECT 
                t1.id AS id1,
                value1,
                t2.id AS id2,
                value2
            FROM test_self_join_module t1
            LATERAL VIEW udtf_split_module(t1.value_list, ',') tmp1 AS pos1, value1
            INNER JOIN test_self_join_module t2
            LATERAL VIEW udtf_split_module(t2.value_list, ',') tmp2 AS pos2, value2
            ON value1 = value2 AND t1.id < t2.id
            ORDER BY t1.id, value1, t2.id;
        """
        
        // ========================================
        // Section 3: UDTF with GROUP BY and Aggregation
        // ========================================
        
        sql """ DROP TABLE IF EXISTS test_group_by_module; """
        sql """
        CREATE TABLE test_group_by_module (
            id INT,
            category STRING,
            tags STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_group_by_module VALUES 
        (1, 'fruit', 'apple,banana,apple'),
        (2, 'fruit', 'banana,cherry'),
        (3, 'animal', 'cat,dog,cat');
        """
        
        // Test 3.1: GROUP BY after UDTF expansion
        qt_group_by_udtf """
            SELECT 
                tmp.value AS tag,
                COUNT(*) AS occurrence_count
            FROM test_group_by_module
            LATERAL VIEW udtf_split_module(tags, ',') tmp AS position, value
            GROUP BY tmp.value
            ORDER BY occurrence_count DESC, tag;
        """
        
        // Test 3.2: GROUP BY with original table columns
        qt_group_by_mixed """
            SELECT 
                category,
                tmp.value AS tag,
                COUNT(*) AS tag_count
            FROM test_group_by_module
            LATERAL VIEW udtf_split_module(tags, ',') tmp AS position, value
            GROUP BY category, tmp.value
            ORDER BY category, tag_count DESC, tag;
        """
        
        // Test 3.3: Aggregation with HAVING clause
        qt_group_by_having """
            SELECT 
                tmp.value AS tag,
                COUNT(*) AS cnt
            FROM test_group_by_module
            LATERAL VIEW udtf_split_module(tags, ',') tmp AS position, value
            GROUP BY tmp.value
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC, tag;
        """
        
        // Test 3.4: Multiple aggregation functions
        sql """ DROP TABLE IF EXISTS test_agg_numbers_module; """
        sql """
        CREATE TABLE test_agg_numbers_module (
            id INT,
            start_val INT,
            end_val INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_agg_numbers_module VALUES 
        (1, 1, 5),
        (2, 3, 7),
        (3, 10, 12);
        """
        
        qt_group_by_multi_agg """
            SELECT 
                id,
                COUNT(*) AS total_count,
                MIN(tmp.num) AS min_num,
                MAX(tmp.num) AS max_num,
                SUM(tmp.num) AS sum_num,
                AVG(tmp.num) AS avg_num
            FROM test_agg_numbers_module
            LATERAL VIEW udtf_range_module(start_val, end_val) tmp AS num
            GROUP BY id
            ORDER BY id;
        """
        
        // ========================================
        // Section 4: UDTF with ORDER BY and LIMIT
        // ========================================
        
        sql """ DROP TABLE IF EXISTS test_order_limit_module; """
        sql """
        CREATE TABLE test_order_limit_module (
            id INT,
            name STRING,
            scores STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_order_limit_module VALUES 
        (1, 'Alice', '85,92,78'),
        (2, 'Bob', '90,88,95'),
        (3, 'Charlie', '70,82,88');
        """
        
        // Test 4.1: ORDER BY UDTF output
        qt_order_by_udtf """
            SELECT 
                id,
                name,
                tmp.value AS score
            FROM test_order_limit_module
            LATERAL VIEW udtf_split_module(scores, ',') tmp AS position, value
            ORDER BY CAST(tmp.value AS INT) DESC, name
            LIMIT 5;
        """
        
        // Test 4.2: ORDER BY original and UDTF columns
        qt_order_by_mixed """
            SELECT 
                id,
                name,
                tmp.position,
                tmp.value AS score
            FROM test_order_limit_module
            LATERAL VIEW udtf_split_module(scores, ',') tmp AS position, value
            ORDER BY id ASC, tmp.position DESC;
        """
        
        // Test 4.3: LIMIT without ORDER BY
        qt_limit_only """
            SELECT 
                id,
                tmp.value
            FROM test_order_limit_module
            LATERAL VIEW udtf_split_module(scores, ',') tmp AS position, value
            LIMIT 3;
        """
        
        // Test 4.4: TOP-N pattern (ORDER BY + LIMIT per group)
        qt_top_n_pattern """
            SELECT id, name, score
            FROM (
                SELECT 
                    id,
                    name,
                    CAST(tmp.value AS INT) AS score,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY CAST(tmp.value AS INT) DESC) AS rn
                FROM test_order_limit_module
                LATERAL VIEW udtf_split_module(scores, ',') tmp AS position, value
            ) ranked
            WHERE rn <= 2
            ORDER BY id, score DESC;
        """
        
        // ========================================
        // Section 5: UDTF in Subqueries
        // ========================================
        
        sql """ DROP TABLE IF EXISTS test_subquery_module; """
        sql """
        CREATE TABLE test_subquery_module (
            id INT,
            item_list STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_subquery_module VALUES 
        (1, 'A,B,C'),
        (2, 'B,C,D'),
        (3, 'A,C,E');
        """
        
        // Test 5.1: UDTF in WHERE IN subquery
        qt_subquery_in """
            SELECT id, item_list
            FROM test_subquery_module
            WHERE id IN (
                SELECT DISTINCT id
                FROM test_subquery_module
                LATERAL VIEW udtf_split_module(item_list, ',') tmp AS position, value
                WHERE tmp.value = 'A'
            )
            ORDER BY id;
        """
        
        // Test 5.2: UDTF in FROM subquery
        qt_subquery_from """
            SELECT 
                item,
                COUNT(DISTINCT source_id) AS source_count
            FROM (
                SELECT id AS source_id, tmp.value AS item
                FROM test_subquery_module
                LATERAL VIEW udtf_split_module(item_list, ',') tmp AS position, value
            ) expanded
            GROUP BY item
            ORDER BY source_count DESC, item;
        """
        
        // Test 5.3: Nested subqueries with UDTF
        qt_subquery_nested """
            SELECT item, total_occurrences
            FROM (
                SELECT item, COUNT(*) AS total_occurrences
                FROM (
                    SELECT id, tmp.value AS item
                    FROM test_subquery_module
                    LATERAL VIEW udtf_split_module(item_list, ',') tmp AS position, value
                ) level1
                GROUP BY item
            ) level2
            WHERE total_occurrences >= 2
            ORDER BY total_occurrences DESC, item;
        """
        
        // ========================================
        // Section 6: UDTF with DISTINCT
        // ========================================
        
        sql """ DROP TABLE IF EXISTS test_distinct_module; """
        sql """
        CREATE TABLE test_distinct_module (
            id INT,
            tags STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_distinct_module VALUES 
        (1, 'red,blue,red'),
        (2, 'blue,green'),
        (3, 'red,yellow');
        """
        
        // Test 6.1: DISTINCT on UDTF output
        qt_distinct_udtf """
            SELECT DISTINCT tmp.value AS tag
            FROM test_distinct_module
            LATERAL VIEW udtf_split_module(tags, ',') tmp AS position, value
            ORDER BY tag;
        """
        
        // Test 6.2: COUNT DISTINCT
        qt_count_distinct """
            SELECT COUNT(DISTINCT tmp.value) AS unique_tag_count
            FROM test_distinct_module
            LATERAL VIEW udtf_split_module(tags, ',') tmp AS position, value;
        """
        
        // ========================================
        // Section 7: UDTF with UNION
        // ========================================
        
        sql """ DROP TABLE IF EXISTS test_union_a_module; """
        sql """
        CREATE TABLE test_union_a_module (
            id INT,
            items STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_union_a_module VALUES (1, 'X,Y');
        """
        
        sql """ DROP TABLE IF EXISTS test_union_b_module; """
        sql """
        CREATE TABLE test_union_b_module (
            id INT,
            items STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_union_b_module VALUES (2, 'Y,Z');
        """
        
        // Test 7.1: UNION ALL with UDTF
        qt_union_all """
            SELECT id, tmp.value AS item
            FROM test_union_a_module
            LATERAL VIEW udtf_split_module(items, ',') tmp AS position, value
            UNION ALL
            SELECT id, tmp.value AS item
            FROM test_union_b_module
            LATERAL VIEW udtf_split_module(items, ',') tmp AS position, value
            ORDER BY id, item;
        """
        
        // Test 7.2: UNION (removes duplicates)
        qt_union_distinct """
            SELECT tmp.value AS item
            FROM test_union_a_module
            LATERAL VIEW udtf_split_module(items, ',') tmp AS position, value
            UNION
            SELECT tmp.value AS item
            FROM test_union_b_module
            LATERAL VIEW udtf_split_module(items, ',') tmp AS position, value
            ORDER BY item;
        """
        
        // ========================================
        // Section 8: UDTF with Complex Array Operations
        // ========================================
        
        sql """ DROP TABLE IF EXISTS test_array_ops_module; """
        sql """
        CREATE TABLE test_array_ops_module (
            id INT,
            numbers ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_array_ops_module VALUES 
        (1, [1, 2, 3]),
        (2, [2, 3, 4, 5]),
        (3, [3, 4]);
        """
        
        // Test 8.1: Filter array elements through UDTF
        qt_array_filter """
            SELECT 
                id,
                tmp.element
            FROM test_array_ops_module
            LATERAL VIEW udtf_explode_array_module(numbers) tmp AS element, element_index
            WHERE tmp.element > 2
            ORDER BY id, tmp.element;
        """
        
        // Test 8.2: Aggregate array elements
        qt_array_aggregate """
            SELECT 
                id,
                COUNT(*) AS element_count,
                SUM(tmp.element) AS element_sum,
                AVG(tmp.element) AS element_avg
            FROM test_array_ops_module
            LATERAL VIEW udtf_explode_array_module(numbers) tmp AS element, element_index
            GROUP BY id
            ORDER BY id;
        """
        
        // ========================================
        // Section 9: UDTF with Window Functions
        // ========================================
        
        sql """ DROP TABLE IF EXISTS test_window_module; """
        sql """
        CREATE TABLE test_window_module (
            id INT,
            category STRING,
            value_list STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_window_module VALUES 
        (1, 'A', '10,20,30'),
        (2, 'A', '15,25'),
        (3, 'B', '5,10,15');
        """
        
        // Test 9.1: Window function over UDTF results
        qt_window_function """
            SELECT 
                id,
                category,
                CAST(tmp.value AS INT) AS val,
                ROW_NUMBER() OVER (PARTITION BY category ORDER BY CAST(tmp.value AS INT)) AS rn,
                SUM(CAST(tmp.value AS INT)) OVER (PARTITION BY category) AS category_total
            FROM test_window_module
            LATERAL VIEW udtf_split_module(value_list, ',') tmp AS position, value
            ORDER BY category, val;
        """
        
        // ========================================
        // Section 10: UDTF with CASE WHEN
        // ========================================
        
        sql """ DROP TABLE IF EXISTS test_case_when_module; """
        sql """
        CREATE TABLE test_case_when_module (
            id INT,
            data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_case_when_module VALUES 
        (1, '5,15,25'),
        (2, '10,20,30');
        """
        
        // Test 10.1: CASE WHEN on UDTF results
        qt_case_when """
            SELECT 
                id,
                tmp.value,
                CASE 
                    WHEN CAST(tmp.value AS INT) < 10 THEN 'small'
                    WHEN CAST(tmp.value AS INT) < 20 THEN 'medium'
                    ELSE 'large'
                END AS size_category
            FROM test_case_when_module
            LATERAL VIEW udtf_split_module(data, ',') tmp AS position, value
            ORDER BY id, CAST(tmp.value AS INT);
        """
        
        // ========================================
        // Section 11: - Multiple LATERAL VIEW Nesting
        // ========================================
        
        // Test 11.1: Two-level LATERAL VIEW nesting (sequential)
        sql """ DROP TABLE IF EXISTS test_nested_2level_module; """
        sql """
        CREATE TABLE test_nested_2level_module (
            id INT,
            categories STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_nested_2level_module VALUES 
        (1, 'A:1,2|B:3'),
        (2, 'C:4,5');
        """
        
        qt_nested_2level """
            SELECT 
                id,
                cat,
                CAST(num AS INT) as num
            FROM test_nested_2level_module
            LATERAL VIEW udtf_split_module(categories, '|') t1 AS p1, cat_nums
            LATERAL VIEW udtf_split_module(cat_nums, ':') t2 AS p2, cat
            LATERAL VIEW udtf_split_module(cat, ',') t3 AS p3, num
            WHERE p2 = 1
            ORDER BY id, cat, num;
        """
        
        // Test 11.2: Parallel LATERAL VIEWs (cartesian product)
        sql """ DROP TABLE IF EXISTS test_parallel_lateral_module; """
        sql """
        CREATE TABLE test_parallel_lateral_module (
            id INT,
            list1 STRING,
            list2 STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_parallel_lateral_module VALUES 
        (1, 'A,B', 'X,Y'),
        (2, 'C', 'Z');
        """
        
        qt_parallel_lateral """
            SELECT 
                id,
                item1,
                item2
            FROM test_parallel_lateral_module
            LATERAL VIEW udtf_split_module(list1, ',') t1 AS p1, item1
            LATERAL VIEW udtf_split_module(list2, ',') t2 AS p2, item2
            ORDER BY id, item1, item2;
        """
        
        // Test 11.3: Nested LATERAL VIEW with JOIN
        sql """ DROP TABLE IF EXISTS test_nested_join_base_module; """
        sql """
        CREATE TABLE test_nested_join_base_module (
            user_id INT,
            tags STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_nested_join_base_module VALUES 
        (1, 'sports:soccer,tennis|food:pizza'),
        (2, 'music:rock');
        """
        
        sql """ DROP TABLE IF EXISTS dim_tag_info_module; """
        sql """
        CREATE TABLE dim_tag_info_module (
            tag VARCHAR(50),
            score INT
        ) ENGINE=OLAP
        DUPLICATE KEY(tag)
        DISTRIBUTED BY HASH(tag) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO dim_tag_info_module VALUES 
        ('soccer', 10),
        ('tennis', 8),
        ('pizza', 5),
        ('rock', 9);
        """
        
        qt_nested_join """
            SELECT 
                u.user_id,
                tag_name,
                d.score
            FROM test_nested_join_base_module u
            LATERAL VIEW udtf_split_module(u.tags, '|') t1 AS p1, cat_tags
            LATERAL VIEW udtf_split_module(cat_tags, ':') t2 AS p2, part
            LATERAL VIEW udtf_split_module(part, ',') t3 AS p3, tag_name
            INNER JOIN dim_tag_info_module d ON d.tag = tag_name
            WHERE p2 = 1
            ORDER BY u.user_id, d.score DESC;
        """
        
        // Test 11.4: Nested LATERAL VIEW with GROUP BY aggregation
        sql """ DROP TABLE IF EXISTS test_nested_groupby_module; """
        sql """
        CREATE TABLE test_nested_groupby_module (
            store_id INT,
            sales_data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(store_id)
        DISTRIBUTED BY HASH(store_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_nested_groupby_module VALUES 
        (1, 'day1:100,200|day2:150'),
        (2, 'day1:300|day2:250,100');
        """
        
        qt_nested_groupby """
            SELECT 
                store_id,
                COUNT(*) as sale_count,
                SUM(CAST(amount AS INT)) as total_amount
            FROM test_nested_groupby_module
            LATERAL VIEW udtf_split_module(sales_data, '|') t1 AS p1, day_amounts
            LATERAL VIEW udtf_split_module(day_amounts, ':') t2 AS p2, part
            LATERAL VIEW udtf_split_module(part, ',') t3 AS p3, amount
            WHERE p2 = 1
            GROUP BY store_id
            ORDER BY store_id;
        """
        
        // Test 11.5: Three-level deep nesting
        sql """ DROP TABLE IF EXISTS test_nested_3level_module; """
        sql """
        CREATE TABLE test_nested_3level_module (
            id INT,
            data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_nested_3level_module VALUES 
        (1, 'A,B,C|D,E|F');
        """
        
        qt_nested_3level """
            SELECT 
                id,
                grp_pos,
                item
            FROM test_nested_3level_module
            LATERAL VIEW udtf_split_module(data, '|') t1 AS grp_pos, group_items
            LATERAL VIEW udtf_split_module(group_items, ',') t2 AS item_pos, item
            ORDER BY id, grp_pos, item_pos;
        """
        
        // Test 11.6: Nested with array expansion
        sql """ DROP TABLE IF EXISTS test_nested_array_expansion_module; """
        sql """
        CREATE TABLE test_nested_array_expansion_module (
            id INT,
            group_id INT,
            numbers ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_nested_array_expansion_module VALUES 
        (1, 1, [10, 20]),
        (1, 2, [30]),
        (2, 1, [40, 50]);
        """
        
        qt_nested_array_expansion """
            SELECT 
                id,
                group_id,
                element
            FROM test_nested_array_expansion_module
            LATERAL VIEW udtf_explode_array_module(numbers) t1 AS element, idx
            ORDER BY id, group_id, element;
        """
        
        // Test 11.7: Nested with WHERE filtering at multiple levels
        sql """ DROP TABLE IF EXISTS test_nested_multifilter_module; """
        sql """
        CREATE TABLE test_nested_multifilter_module (
            id INT,
            data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_nested_multifilter_module VALUES 
        (1, 'A:10,20,30|B:40'),
        (2, 'C:50,60');
        """
        
        qt_nested_multifilter """
            SELECT 
                id,
                cat_name,
                CAST(num AS INT) as num
            FROM (
                SELECT 
                    id,
                    p1,
                    CASE WHEN p2 = 0 THEN part END AS cat_name,
                    CASE WHEN p2 = 1 THEN part END AS nums
                FROM test_nested_multifilter_module
                LATERAL VIEW udtf_split_module(data, '|') t1 AS p1, cat_nums
                LATERAL VIEW udtf_split_module(cat_nums, ':') t2 AS p2, part
            ) t
            LATERAL VIEW udtf_split_module(nums, ',') t3 AS p3, num
            WHERE nums IS NOT NULL AND CAST(num AS INT) >= 20
            ORDER BY id, p1, num;
        """
        
        // Test 11.8: Nested with DISTINCT across levels
        sql """ DROP TABLE IF EXISTS test_nested_distinct_module; """
        sql """
        CREATE TABLE test_nested_distinct_module (
            id INT,
            tags STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_nested_distinct_module VALUES 
        (1, 'red,blue|red,green'),
        (2, 'blue,yellow');
        """
        
        qt_nested_distinct """
            SELECT DISTINCT color
            FROM test_nested_distinct_module
            LATERAL VIEW udtf_split_module(tags, '|') t1 AS p1, color_list
            LATERAL VIEW udtf_split_module(color_list, ',') t2 AS p2, color
            ORDER BY color;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS udtf_split_module(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_range_module(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_explode_array_module(ARRAY<INT>);")
        try_sql("DROP TABLE IF EXISTS test_where_before_module;")
        try_sql("DROP TABLE IF EXISTS dim_numbers_module;")
        try_sql("DROP TABLE IF EXISTS fact_ranges_module;")
        try_sql("DROP TABLE IF EXISTS fact_ranges_extended_module;")
        try_sql("DROP TABLE IF EXISTS test_self_join_module;")
        try_sql("DROP TABLE IF EXISTS test_group_by_module;")
        try_sql("DROP TABLE IF EXISTS test_agg_numbers_module;")
        try_sql("DROP TABLE IF EXISTS test_order_limit_module;")
        try_sql("DROP TABLE IF EXISTS test_subquery_module;")
        try_sql("DROP TABLE IF EXISTS test_distinct_module;")
        try_sql("DROP TABLE IF EXISTS test_union_a_module;")
        try_sql("DROP TABLE IF EXISTS test_union_b_module;")
        try_sql("DROP TABLE IF EXISTS test_array_ops_module;")
        try_sql("DROP TABLE IF EXISTS test_window_module;")
        try_sql("DROP TABLE IF EXISTS test_case_when_module;")
        try_sql("DROP TABLE IF EXISTS test_nested_2level_module;")
        try_sql("DROP TABLE IF EXISTS test_parallel_lateral_module;")
        try_sql("DROP TABLE IF EXISTS test_nested_join_base_module;")
        try_sql("DROP TABLE IF EXISTS dim_tag_info_module;")
        try_sql("DROP TABLE IF EXISTS test_nested_groupby_module;")
        try_sql("DROP TABLE IF EXISTS test_nested_3level_module;")
        try_sql("DROP TABLE IF EXISTS test_nested_array_expansion_module;")
        try_sql("DROP TABLE IF EXISTS test_nested_multifilter_module;")
        try_sql("DROP TABLE IF EXISTS test_nested_distinct_module;")
    }
}
