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

suite("test_pythonudtf_basic_module") {
    // Basic Python UDTF tests using module-based deployment
    
    def pyPath = """${context.file.parent}/udtf_scripts/pyudtf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${pyPath}".toString())
    
    try {
        // ========================================
        // Test 1: Simple String Split UDTF
        // Input: Single string
        // Output: Multiple rows (one per split part)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_split_string_module(STRING); """
        sql """
        CREATE TABLES FUNCTION py_split_string_module(STRING)
        RETURNS ARRAY<STRING>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.basic_udtf.split_string_udtf",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS temp_input_module; """
        sql """
        CREATE TABLE temp_input_module (
            id INT,
            input_str STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO temp_input_module VALUES (1, 'apple,banana,cherry');
        """
        
        qt_split_string """
            SELECT part 
            FROM temp_input_module 
            LATERAL VIEW py_split_string_module(input_str) tmp AS part;
        """
        
        // ========================================
        // Test 2: Generate Series UDTF
        // Input: start, end integers
        // Output: Sequence of integers
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_generate_series_module(INT, INT); """
        sql """
        CREATE TABLES FUNCTION py_generate_series_module(INT, INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.basic_udtf.generate_series_udtf",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS temp_series_module; """
        sql """
        CREATE TABLE temp_series_module (
            id INT,
            start_val INT,
            end_val INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO temp_series_module VALUES (1, 1, 5), (2, 10, 12);
        """
        
        qt_generate_series """
            SELECT tmp.value 
            FROM temp_series_module 
            LATERAL VIEW py_generate_series_module(start_val, end_val) tmp AS value;
        """
        
        qt_generate_series_multiple """
            SELECT tmp.value 
            FROM temp_series_module 
            LATERAL VIEW py_generate_series_module(start_val, end_val) tmp AS value
            ORDER BY tmp.value;
        """
        
        // ========================================
        // Test 3: Running Sum UDTF (without state management)
        // Note: Function-based UDTFs cannot maintain state across calls
        // Each row is processed independently
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_running_sum_module(INT); """
        sql """
        CREATE TABLES FUNCTION py_running_sum_module(INT)
        RETURNS ARRAY<STRUCT<original_value:INT, cumulative_sum:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.basic_udtf.running_sum_udtf",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS numbers_table_module; """
        sql """
        CREATE TABLE numbers_table_module (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO numbers_table_module VALUES
        (1, 10),
        (2, 20),
        (3, 30),
        (4, 40);
        """
        
        qt_running_sum """
            SELECT original_value, cumulative_sum
            FROM numbers_table_module
            LATERAL VIEW py_running_sum_module(value) tmp AS original_value, cumulative_sum
            ORDER BY original_value;
        """

        // ========================================
        // Test 4: Explode Array UDTF
        // Similar to LATERAL VIEW explode in Hive
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_explode_json_array_module(STRING); """
        sql """
        CREATE TABLES FUNCTION py_explode_json_array_module(STRING)
        RETURNS ARRAY<STRING>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.basic_udtf.explode_json_udtf",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS temp_json_module; """
        sql """
        CREATE TABLE temp_json_module (
            id INT,
            json_data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO temp_json_module VALUES (1, '["apple", "banana", "cherry"]');
        """
        
        qt_explode_json """
            SELECT element
            FROM temp_json_module
            LATERAL VIEW py_explode_json_array_module(json_data) tmp AS element;
        """
        
        // ========================================
        // Test 5: Top-N UDTF (stateless version)
        // Note: Without state, this simply returns first n values per row
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_top_n_module(INT, INT); """
        sql """
        CREATE TABLES FUNCTION py_top_n_module(INT, INT)
        RETURNS ARRAY<STRUCT<value:INT, rank:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.basic_udtf.top_n_udtf",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS ranked_data_module; """
        sql """
        CREATE TABLE ranked_data_module (
            id INT,
            category STRING,
            value INT,
            top_n INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO ranked_data_module VALUES
        (1, 'A', 100, 2),
        (2, 'A', 90, 2),
        (3, 'A', 80, 2),
        (4, 'A', 70, 2),
        (5, 'B', 200, 2),
        (6, 'B', 190, 2);
        """
        
        qt_top_n """
            SELECT category, tmp.value, tmp.rank
            FROM ranked_data_module
            LATERAL VIEW py_top_n_module(value, top_n) tmp AS value, rank
            ORDER BY category, tmp.rank;
        """
        
        // ========================================
        // Test 6: Multiple Outputs per Input
        // One input row can generate multiple output rows
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_duplicate_n_times_module(STRING, INT); """
        sql """
        CREATE TABLES FUNCTION py_duplicate_n_times_module(STRING, INT)
        RETURNS ARRAY<STRUCT<output:STRING, idx:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.basic_udtf.duplicate_udtf",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS temp_dup_module; """
        sql """
        CREATE TABLE temp_dup_module (
            id INT,
            text STRING,
            times INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO temp_dup_module VALUES (1, 'Hello', 3);
        """
        
        qt_duplicate """
            SELECT output, idx
            FROM temp_dup_module
            LATERAL VIEW py_duplicate_n_times_module(text, times) tmp AS output, idx;
        """
        
        // ========================================
        // Test 7: Conditional Output (Skip Rows)
        // UDTF can skip rows by not yielding
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_filter_positive_module(INT); """
        sql """
        CREATE TABLES FUNCTION py_filter_positive_module(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.basic_udtf.filter_positive_udtf",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS mixed_numbers_module; """
        sql """
        CREATE TABLE mixed_numbers_module (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO mixed_numbers_module VALUES (1, -5), (2, 0), (3, 3), (4, -2), (5, 7), (6, 1);
        """
        
        qt_filter_positive """
            SELECT positive_value
            FROM mixed_numbers_module
            LATERAL VIEW py_filter_positive_module(value) tmp AS positive_value
            ORDER BY positive_value;
        """
        
        // ========================================
        // Test 8: Cartesian Product UDTF
        // Generate all combinations
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_cartesian_module(STRING, STRING); """
        sql """
        CREATE TABLES FUNCTION py_cartesian_module(STRING, STRING)
        RETURNS ARRAY<STRUCT<item1:STRING, item2:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.basic_udtf.cartesian_udtf",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS temp_cart_module; """
        sql """
        CREATE TABLE temp_cart_module (
            id INT,
            list1 STRING,
            list2 STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO temp_cart_module VALUES (1, 'A,B', 'X,Y,Z');
        """
        
        qt_cartesian """
            SELECT item1, item2
            FROM temp_cart_module
            LATERAL VIEW py_cartesian_module(list1, list2) tmp AS item1, item2
            ORDER BY item1, item2;
        """
        
        // ========================================
        // Test 9: All Rows Filtered (Empty Output)
        // Tests data_batch = None case
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_filter_negative_module(INT); """
        sql """
        CREATE TABLES FUNCTION py_filter_negative_module(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.basic_udtf.filter_negative_udtf",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS temp_all_positive_module; """
        sql """
        CREATE TABLE temp_all_positive_module (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        // Insert only positive numbers - all should be filtered
        sql """
        INSERT INTO temp_all_positive_module VALUES (1, 10), (2, 20), (3, 30);
        """
        
        // Expected: No output rows (all filtered), but should not crash
        qt_all_filtered """
            SELECT id, neg_value
            FROM temp_all_positive_module 
            LATERAL VIEW py_filter_negative_module(value) tmp AS neg_value
            ORDER BY id;
        """

        // ========================================
        // Test 10: Mixed - Some Filtered, Some Not
        // ========================================
        sql """ DROP TABLE IF EXISTS temp_mixed_module; """
        sql """
        CREATE TABLE temp_mixed_module (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        // Mix of positive and negative - only negative should pass
        sql """
        INSERT INTO temp_mixed_module VALUES (1, 10), (2, -5), (3, 20), (4, -3);
        """
        
        qt_mixed_filter """
            SELECT id, neg_value
            FROM temp_mixed_module 
            LATERAL VIEW py_filter_negative_module(value) tmp AS neg_value
            ORDER BY id, neg_value;
        """

        // ========================================
        // Test 11: Empty Input Table
        // Tests empty batch case
        // ========================================
        sql """ DROP TABLE IF EXISTS temp_empty_module; """
        sql """
        CREATE TABLE temp_empty_module (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        // No data inserted - empty table
        qt_empty_input """
            SELECT id, neg_value
            FROM temp_empty_module 
            LATERAL VIEW py_filter_negative_module(value) tmp AS neg_value;
        """

        // ========================================
        // Test 12: always_nullable = true (default)
        // Function can return NULL even with NOT NULL input
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_nullable_processor_module(INT); """
        sql """
        CREATE TABLES FUNCTION py_nullable_processor_module(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.basic_udtf.nullable_processor_udtf",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_split_string_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_generate_series_module(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_running_sum_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_explode_json_array_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_top_n_module(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_duplicate_n_times_module(STRING, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_filter_positive_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_cartesian_module(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_filter_negative_module(INT);")
        try_sql("DROP TABLE IF EXISTS temp_input_module;")
        try_sql("DROP TABLE IF EXISTS numbers_table_module;")
        try_sql("DROP TABLE IF EXISTS ranked_data_module;")
        try_sql("DROP TABLE IF EXISTS mixed_numbers_module;")
        try_sql("DROP TABLE IF EXISTS temp_all_positive_module;")
        try_sql("DROP TABLE IF EXISTS temp_mixed_module;")
        try_sql("DROP TABLE IF EXISTS temp_empty_module;")
    }
}
