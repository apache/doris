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

suite("test_pythonudaf_complex_state_objects_module") {
    // Comprehensive test for complex Python objects as aggregate states using MODULE mode
    // All UDAFs are loaded from pyudaf.zip (complex_state_udaf.py module)
    // Tests various pickle-serializable data structures
    
    def pyPath = """${context.file.parent}/udaf_scripts/pyudaf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${pyPath}".toString())
    
    try {
        // ========================================
        // Setup: Create test tables
        // ========================================
        
        // Table 1: Transaction data for complex aggregations
        sql """ DROP TABLE IF EXISTS complex_transactions_mod; """
        sql """
        CREATE TABLE complex_transactions_mod (
            transaction_id INT,
            user_id INT,
            product_id INT,
            product_name VARCHAR(100),
            category VARCHAR(50),
            price DECIMAL(10,2),
            quantity INT,
            timestamp DATETIME,
            region VARCHAR(50),
            payment_method VARCHAR(50)
        ) ENGINE=OLAP 
        DUPLICATE KEY(transaction_id)
        DISTRIBUTED BY HASH(transaction_id) BUCKETS 3
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO complex_transactions_mod VALUES
        (1, 101, 1001, 'Laptop Pro', 'Electronics', 1299.99, 1, '2024-01-01 10:00:00', 'North', 'Credit'),
        (2, 101, 1002, 'Mouse', 'Electronics', 29.99, 2, '2024-01-01 10:05:00', 'North', 'Credit'),
        (3, 102, 1003, 'Keyboard', 'Electronics', 79.99, 1, '2024-01-02 11:00:00', 'South', 'Debit'),
        (4, 101, 1004, 'Monitor', 'Electronics', 399.99, 1, '2024-01-03 09:30:00', 'North', 'Credit'),
        (5, 103, 1001, 'Laptop Pro', 'Electronics', 1299.99, 1, '2024-01-03 14:00:00', 'East', 'PayPal'),
        (6, 102, 1005, 'USB Cable', 'Accessories', 9.99, 3, '2024-01-04 10:00:00', 'South', 'Cash'),
        (7, 104, 1002, 'Mouse', 'Electronics', 29.99, 1, '2024-01-04 15:00:00', 'West', 'Credit'),
        (8, 103, 1006, 'Webcam', 'Electronics', 89.99, 1, '2024-01-05 11:00:00', 'East', 'PayPal'),
        (9, 105, 1003, 'Keyboard', 'Electronics', 79.99, 2, '2024-01-05 16:00:00', 'North', 'Debit'),
        (10, 104, 1007, 'HDMI Cable', 'Accessories', 15.99, 2, '2024-01-06 10:00:00', 'West', 'Cash'),
        (11, 101, 1008, 'Headphones', 'Electronics', 149.99, 1, '2024-01-06 14:00:00', 'North', 'Credit'),
        (12, 106, 1004, 'Monitor', 'Electronics', 399.99, 2, '2024-01-07 09:00:00', 'South', 'Credit'),
        (13, 102, 1009, 'Desk Lamp', 'Home', 45.99, 1, '2024-01-07 15:00:00', 'South', 'Debit'),
        (14, 107, 1010, 'Office Chair', 'Furniture', 299.99, 1, '2024-01-08 10:00:00', 'East', 'Credit'),
        (15, 103, 1002, 'Mouse', 'Electronics', 29.99, 3, '2024-01-08 11:00:00', 'East', 'PayPal');
        """
        
        // ========================================
        // UDAF 1: Nested Dictionary State - User Purchase Profile
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_user_profile_mod(INT, VARCHAR, VARCHAR, DECIMAL, INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_user_profile_mod(INT, VARCHAR, VARCHAR, DECIMAL, INT)
        RETURNS VARCHAR
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_state_udaf.UserProfileUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        // ========================================
        // UDAF 2: Custom Class State - Product Statistics
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_product_stats_mod(VARCHAR, DECIMAL, INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_product_stats_mod(VARCHAR, DECIMAL, INT)
        RETURNS VARCHAR
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_state_udaf.ProductStatsUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        // ========================================
        // UDAF 3: List of Tuples State - Transaction Timeline
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_transaction_timeline_mod(DATETIME, DECIMAL); """
        sql """
        CREATE AGGREGATE FUNCTION py_transaction_timeline_mod(DATETIME, DECIMAL)
        RETURNS VARCHAR
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_state_udaf.TransactionTimelineUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        // ========================================
        // UDAF 4: Set-based State - Unique Value Tracker
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_unique_tracker_mod(INT, INT, VARCHAR); """
        sql """
        CREATE AGGREGATE FUNCTION py_unique_tracker_mod(INT, INT, VARCHAR)
        RETURNS VARCHAR
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_state_udaf.UniqueTrackerUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        // ========================================
        // UDAF 5: Named Tuple State - Category Summary
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_category_summary_mod(VARCHAR, DECIMAL, INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_category_summary_mod(VARCHAR, DECIMAL, INT)
        RETURNS VARCHAR
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_state_udaf.CategorySummaryUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        // ========================================
        // UDAF 6: Complex Nested State - Hierarchical Aggregation
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_hierarchical_agg_mod(VARCHAR, VARCHAR, VARCHAR, DECIMAL, INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_hierarchical_agg_mod(VARCHAR, VARCHAR, VARCHAR, DECIMAL, INT)
        RETURNS VARCHAR
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_state_udaf.HierarchicalAggUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        // ========================================
        // Test Cases
        // ========================================
        
        // Test 1: User Profile Aggregation (Nested Dict)
        qt_test_user_profile """
            SELECT 
                py_user_profile_mod(user_id, product_name, category, price, quantity) as user_profiles
            FROM complex_transactions_mod;
        """
        
        // Test 2: Product Statistics (Custom Class)
        qt_test_product_stats """
            SELECT 
                py_product_stats_mod(product_name, price, quantity) as product_statistics
            FROM complex_transactions_mod;
        """
        
        // Test 3: Transaction Timeline (List of Tuples)
        qt_test_transaction_timeline """
            SELECT 
                region,
                py_transaction_timeline_mod(timestamp, price * quantity) as timeline
            FROM complex_transactions_mod
            GROUP BY region
            ORDER BY region;
        """
        
        // Test 4: Unique Tracker (Sets)
        qt_test_unique_tracker """
            SELECT 
                category,
                py_unique_tracker_mod(user_id, product_id, payment_method) as unique_stats
            FROM complex_transactions_mod
            GROUP BY category
            ORDER BY category;
        """
        
        // Test 5: Category Summary (Named Tuples)
        qt_test_category_summary """
            SELECT 
                py_category_summary_mod(category, price, quantity) as category_summary
            FROM complex_transactions_mod;
        """
        
        // Test 6: Hierarchical Aggregation (Deep Nesting)
        qt_test_hierarchical_agg """
            SELECT 
                py_hierarchical_agg_mod(region, category, product_name, price, quantity) as hierarchy
            FROM complex_transactions_mod;
        """
        
        // Test 7: Complex State with Window Function
        qt_test_complex_window """
            SELECT 
                user_id,
                product_name,
                price,
                py_user_profile_mod(user_id, product_name, category, price, quantity) 
                    OVER (PARTITION BY user_id ORDER BY transaction_id) as running_profile
            FROM complex_transactions_mod
            ORDER BY user_id, transaction_id;
        """
        
        // Test 8: Multiple Complex UDAFs in Single Query
        qt_test_multi_complex """
            SELECT 
                region,
                py_unique_tracker_mod(user_id, product_id, payment_method) as uniques,
                py_category_summary_mod(category, price, quantity) as summary
            FROM complex_transactions_mod
            GROUP BY region
            ORDER BY region;
        """
        
        // Test 9: Nested Query with Complex State
        qt_test_nested_complex """
            SELECT 
                region,
                product_stats
            FROM (
                SELECT 
                    region,
                    py_product_stats_mod(product_name, price, quantity) as product_stats
                FROM complex_transactions_mod
                WHERE price > 50
                GROUP BY region
            ) t
            ORDER BY region;
        """
        
        // Test 10: Complex State Serialization in Shuffle (GROUP BY multiple columns)
        qt_test_complex_shuffle """
            SELECT 
                region,
                category,
                py_hierarchical_agg_mod(region, category, product_name, price, quantity) as hier_stats
            FROM complex_transactions_mod
            GROUP BY region, category
            ORDER BY region, category;
        """
        
        // Test 11: Edge Case - Empty Groups
        qt_test_empty_groups """
            SELECT 
                region,
                py_user_profile_mod(user_id, product_name, category, price, quantity) as profile
            FROM complex_transactions_mod
            WHERE 1 = 0
            GROUP BY region;
        """
        
        // Test 12: Edge Case - NULL Values
        sql """ DROP TABLE IF EXISTS complex_nulls_mod; """
        sql """
        CREATE TABLE complex_nulls_mod (
            id INT,
            user_id INT,
            product VARCHAR(50),
            category VARCHAR(50),
            price DECIMAL(10,2)
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO complex_nulls_mod VALUES
        (1, 101, 'ItemA', 'Cat1', 100.0),
        (2, NULL, 'ItemB', 'Cat2', 200.0),
        (3, 102, NULL, 'Cat3', 300.0),
        (4, 103, 'ItemC', NULL, 400.0),
        (5, 104, 'ItemD', 'Cat4', NULL);
        """
        
        qt_test_null_handling """
            SELECT 
                py_user_profile_mod(user_id, product, category, price, 1) as profile_with_nulls
            FROM complex_nulls_mod;
        """
        
        // Test 13: Performance - Large Complex State
        qt_test_large_state """
            SELECT 
                COUNT(*) as total_transactions,
                py_hierarchical_agg_mod(region, category, product_name, price, quantity) as full_hierarchy,
                py_user_profile_mod(user_id, product_name, category, price, quantity) as all_profiles
            FROM complex_transactions_mod;
        """
        
        // Test 14: Module Reusability - Create another function from same module
        sql """ DROP FUNCTION IF EXISTS py_user_profile_mod2(INT, VARCHAR, VARCHAR, DECIMAL, INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_user_profile_mod2(INT, VARCHAR, VARCHAR, DECIMAL, INT)
        RETURNS VARCHAR
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_state_udaf.UserProfileUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        qt_module_reuse """
            SELECT 
                py_user_profile_mod(user_id, product_name, category, price, quantity) as profile1,
                py_user_profile_mod2(user_id, product_name, category, price, quantity) as profile2
            FROM complex_transactions_mod;
        """
        
        // Test 15: Global Functions
        sql """ DROP GLOBAL FUNCTION IF EXISTS py_user_profile_global(INT, VARCHAR, VARCHAR, DECIMAL, INT); """
        sql """
        CREATE GLOBAL AGGREGATE FUNCTION py_user_profile_global(INT, VARCHAR, VARCHAR, DECIMAL, INT)
        RETURNS VARCHAR
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_state_udaf.UserProfileUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        qt_global_test """
            SELECT 
                py_user_profile_global(user_id, product_name, category, price, quantity) as user_profiles
            FROM complex_transactions_mod;
        """
        
    } finally {
        // Cleanup
        try_sql("DROP GLOBAL FUNCTION IF EXISTS py_user_profile_global(INT, VARCHAR, VARCHAR, DECIMAL, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_user_profile_mod(INT, VARCHAR, VARCHAR, DECIMAL, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_product_stats_mod(VARCHAR, DECIMAL, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_transaction_timeline_mod(DATETIME, DECIMAL);")
        try_sql("DROP FUNCTION IF EXISTS py_unique_tracker_mod(INT, INT, VARCHAR);")
        try_sql("DROP FUNCTION IF EXISTS py_category_summary_mod(VARCHAR, DECIMAL, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_hierarchical_agg_mod(VARCHAR, VARCHAR, VARCHAR, DECIMAL, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_user_profile_mod2(INT, VARCHAR, VARCHAR, DECIMAL, INT);")
        try_sql("DROP TABLE IF EXISTS complex_transactions_mod;")
        try_sql("DROP TABLE IF EXISTS complex_nulls_mod;")
    }
}
