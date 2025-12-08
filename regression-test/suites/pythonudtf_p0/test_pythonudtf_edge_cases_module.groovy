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

suite("test_pythonudtf_edge_cases_module") {
    // Test Python UDTF Edge Cases and Boundary Conditions
    // Coverage: NULL handling, extreme cases, special values
    
    def pyPath = """${context.file.parent}/udtf_scripts/pyudtf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${pyPath}".toString())
    
    try {
        // ========================================
        // Section 1: NULL Value Handling
        // ========================================
        
        // Test 1.1: NULL Integer Input
        sql """ DROP FUNCTION IF EXISTS udtf_null_int_module(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_null_int_module(INT)
        RETURNS ARRAY<STRUCT<input_value:INT, is_null:BOOLEAN, result:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.edge_cases_udtf.handle_null_int",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_null_int_module; """
        sql """
        CREATE TABLE test_null_int_module (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_null_int_module VALUES (1, NULL), (2, 0), (3, 10), (4, NULL);
        """
        
        qt_null_int """
            SELECT id, tmp.input_value, tmp.is_null, tmp.result
            FROM test_null_int_module
            LATERAL VIEW udtf_null_int_module(value) tmp AS input_value, is_null, result
            ORDER BY id;
        """
        
        // Test 1.2: Empty String vs NULL String
        sql """ DROP FUNCTION IF EXISTS udtf_null_string_module(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_null_string_module(STRING)
        RETURNS ARRAY<STRUCT<value_type:STRING, length:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.edge_cases_udtf.handle_null_string",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_null_string_module; """
        sql """
        CREATE TABLE test_null_string_module (
            id INT,
            value STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_null_string_module VALUES (1, NULL), (2, ''), (3, 'hello'), (4, NULL);
        """
        
        qt_null_string """
            SELECT id, tmp.value_type, tmp.length
            FROM test_null_string_module
            LATERAL VIEW udtf_null_string_module(value) tmp AS value_type, length
            ORDER BY id;
        """
        
        // Test 1.3: Empty Array
        sql """ DROP FUNCTION IF EXISTS udtf_empty_array_module(ARRAY<INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_empty_array_module(ARRAY<INT>)
        RETURNS ARRAY<STRUCT<array_type:STRING, size:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.edge_cases_udtf.handle_empty_array",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_empty_array_module; """
        sql """
        CREATE TABLE test_empty_array_module (
            id INT,
            value ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_empty_array_module VALUES 
        (1, NULL), 
        (2, []), 
        (3, [1, 2, 3]);
        """
        
        qt_empty_array """
            SELECT id, tmp.array_type, tmp.size
            FROM test_empty_array_module
            LATERAL VIEW udtf_empty_array_module(value) tmp AS array_type, size
            ORDER BY id;
        """
        
        // Test 1.4: NULL Fields in STRUCT
        sql """ DROP FUNCTION IF EXISTS udtf_null_struct_module(STRUCT<name:STRING, age:INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_null_struct_module(STRUCT<name:STRING, age:INT>)
        RETURNS ARRAY<STRUCT<has_name:BOOLEAN, has_age:BOOLEAN, summary:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.edge_cases_udtf.handle_null_struct",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_null_struct_module; """
        sql """
        CREATE TABLE test_null_struct_module (
            id INT,
            person STRUCT<name:STRING, age:INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_null_struct_module VALUES 
        (1, named_struct('name', 'Alice', 'age', 25)),
        (2, named_struct('name', 'Bob', 'age', NULL)),
        (3, named_struct('name', NULL, 'age', 30)),
        (4, named_struct('name', NULL, 'age', NULL));
        """
        
        qt_null_struct """
            SELECT id, tmp.has_name, tmp.has_age, tmp.summary
            FROM test_null_struct_module
            LATERAL VIEW udtf_null_struct_module(person) tmp AS has_name, has_age, summary
            ORDER BY id;
        """
        
        // ========================================
        // Section 2: Extreme Cases
        // ========================================
        
        // Test 2.1: Empty Table (0 rows)
        sql """ DROP FUNCTION IF EXISTS udtf_empty_table_module(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_empty_table_module(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.edge_cases_udtf.process_empty_table",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_empty_table_module; """
        sql """
        CREATE TABLE test_empty_table_module (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        // No INSERT - table remains empty
        
        qt_empty_table """
            SELECT tmp.value
            FROM test_empty_table_module
            LATERAL VIEW udtf_empty_table_module(value) tmp AS value;
        """
        
        // Test 2.2: Single Row Table
        sql """ DROP FUNCTION IF EXISTS udtf_single_row_module(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_single_row_module(INT)
        RETURNS ARRAY<STRUCT<original:INT, generated:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.edge_cases_udtf.process_single_row",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_single_row_module; """
        sql """
        CREATE TABLE test_single_row_module (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_single_row_module VALUES (1, 100);
        """
        
        qt_single_row """
            SELECT tmp.original, tmp.generated
            FROM test_single_row_module
            LATERAL VIEW udtf_single_row_module(value) tmp AS original, generated
            ORDER BY tmp.generated;
        """
        
        // Test 2.3: Large Field - Long String
        sql """ DROP FUNCTION IF EXISTS udtf_long_string_module(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_long_string_module(STRING)
        RETURNS ARRAY<STRUCT<length:INT, first_10:STRING, last_10:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.edge_cases_udtf.process_long_string",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_long_string_module; """
        sql """
        CREATE TABLE test_long_string_module (
            id INT,
            value STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_long_string_module VALUES 
        (1, REPEAT('A', 1000)),
        (2, REPEAT('B', 5000));
        """
        
        qt_long_string """
            SELECT id, tmp.length, tmp.first_10, tmp.last_10
            FROM test_long_string_module
            LATERAL VIEW udtf_long_string_module(value) tmp AS length, first_10, last_10
            ORDER BY id;
        """
        
        // Test 2.4: Large Array
        sql """ DROP FUNCTION IF EXISTS udtf_large_array_module(ARRAY<INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_large_array_module(ARRAY<INT>)
        RETURNS ARRAY<STRUCT<total_elements:INT, sum_value:BIGINT, first_elem:INT, last_elem:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.edge_cases_udtf.process_large_array",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_large_array_module; """
        sql """
        CREATE TABLE test_large_array_module (
            id INT,
            value ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_large_array_module VALUES 
        (1, ARRAY_REPEAT(1, 100)),
        (2, ARRAY_REPEAT(5, 50));
        """
        
        qt_large_array """
            SELECT id, tmp.total_elements, tmp.sum_value, tmp.first_elem, tmp.last_elem
            FROM test_large_array_module
            LATERAL VIEW udtf_large_array_module(value) tmp AS total_elements, sum_value, first_elem, last_elem
            ORDER BY id;
        """
        
        // Test 2.5: Output Explosion - Controlled
        sql """ DROP FUNCTION IF EXISTS udtf_output_explosion_module(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_output_explosion_module(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.edge_cases_udtf.output_explosion",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_output_explosion_module; """
        sql """
        CREATE TABLE test_output_explosion_module (
            id INT,
            multiplier INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_output_explosion_module VALUES (1, 10), (2, 50);
        """
        
        qt_output_explosion """
            SELECT id, COUNT(*) as output_count, MIN(tmp.value) as min_val, MAX(tmp.value) as max_val
            FROM test_output_explosion_module
            LATERAL VIEW udtf_output_explosion_module(multiplier) tmp AS value
            GROUP BY id
            ORDER BY id;
        """
        
        // ========================================
        // Section 3: Special Values
        // ========================================
        
        // Test 3.1: Special Numeric Values (0, negative, boundary)
        sql """ DROP FUNCTION IF EXISTS udtf_special_numbers_module(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_special_numbers_module(INT)
        RETURNS ARRAY<STRUCT<original:INT, category:STRING, is_boundary:BOOLEAN>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.edge_cases_udtf.process_special_numbers",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_special_numbers_module; """
        sql """
        CREATE TABLE test_special_numbers_module (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_special_numbers_module VALUES 
        (1, -2147483648),  -- INT MIN
        (2, -1),
        (3, 0),
        (4, 1),
        (5, 2147483647),   -- INT MAX
        (6, NULL);
        """
        
        qt_special_numbers """
            SELECT id, tmp.original, tmp.category, tmp.is_boundary
            FROM test_special_numbers_module
            LATERAL VIEW udtf_special_numbers_module(value) tmp AS original, category, is_boundary
            ORDER BY id;
        """
        
        // Test 3.2: Special Double Values (infinity, very small numbers)
        sql """ DROP FUNCTION IF EXISTS udtf_special_doubles_module(DOUBLE); """
        sql """
        CREATE TABLES FUNCTION udtf_special_doubles_module(DOUBLE)
        RETURNS ARRAY<STRUCT<original:DOUBLE, classification:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.edge_cases_udtf.process_special_doubles",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_special_doubles_module; """
        sql """
        CREATE TABLE test_special_doubles_module (
            id INT,
            value DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_special_doubles_module VALUES 
        (1, 0.0),
        (2, 1e-15),
        (3, 1e15),
        (4, -1e15),
        (5, 3.14159);
        """
        
        qt_special_doubles """
            SELECT id, tmp.original, tmp.classification
            FROM test_special_doubles_module
            LATERAL VIEW udtf_special_doubles_module(value) tmp AS original, classification
            ORDER BY id;
        """
        
        // Test 3.3: Special String Values (special characters, Unicode)
        sql """ DROP FUNCTION IF EXISTS udtf_special_strings_module(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_special_strings_module(STRING)
        RETURNS ARRAY<STRUCT<length:INT, has_special:BOOLEAN, description:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.edge_cases_udtf.process_special_strings",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_special_strings_module; """
        sql """
        CREATE TABLE test_special_strings_module (
            id INT,
            value STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_special_strings_module VALUES 
        (1, 'normal text'),
        (2, 'hello@world.com'),
        (3, 'tab\\there'),
        (4, '你好世界'),
        (5, '');
        """
        
        qt_special_strings """
            SELECT id, tmp.length, tmp.has_special, tmp.description
            FROM test_special_strings_module
            LATERAL VIEW udtf_special_strings_module(value) tmp AS length, has_special, description
            ORDER BY id;
        """
        
        // Test 3.4: Boundary Dates
        sql """ DROP FUNCTION IF EXISTS udtf_boundary_dates_module(DATE); """
        sql """
        CREATE TABLES FUNCTION udtf_boundary_dates_module(DATE)
        RETURNS ARRAY<STRUCT<original:DATE, year:INT, is_boundary:BOOLEAN>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.edge_cases_udtf.process_boundary_dates",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_boundary_dates_module; """
        sql """
        CREATE TABLE test_boundary_dates_module (
            id INT,
            value DATE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_boundary_dates_module VALUES 
        (1, '1970-01-01'),
        (2, '2024-06-15'),
        (3, '9999-12-31');
        """
        
        qt_boundary_dates """
            SELECT id, tmp.original, tmp.year, tmp.is_boundary
            FROM test_boundary_dates_module
            LATERAL VIEW udtf_boundary_dates_module(value) tmp AS original, year, is_boundary
            ORDER BY id;
        """
        
    } finally {
        // Cleanup functions
        try_sql("DROP FUNCTION IF EXISTS udtf_null_int_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_null_string_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_empty_array_module(ARRAY<INT>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_null_struct_module(STRUCT<name:STRING, age:INT>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_empty_table_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_single_row_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_long_string_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_large_array_module(ARRAY<INT>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_output_explosion_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_special_numbers_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_special_doubles_module(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS udtf_special_strings_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_boundary_dates_module(DATE);")
        
        // Cleanup tables
        try_sql("DROP TABLE IF EXISTS test_null_int_module;")
        try_sql("DROP TABLE IF EXISTS test_null_string_module;")
        try_sql("DROP TABLE IF EXISTS test_empty_array_module;")
        try_sql("DROP TABLE IF EXISTS test_null_struct_module;")
        try_sql("DROP TABLE IF EXISTS test_empty_table_module;")
        try_sql("DROP TABLE IF EXISTS test_single_row_module;")
        try_sql("DROP TABLE IF EXISTS test_long_string_module;")
        try_sql("DROP TABLE IF EXISTS test_large_array_module;")
        try_sql("DROP TABLE IF EXISTS test_output_explosion_module;")
        try_sql("DROP TABLE IF EXISTS test_special_numbers_module;")
        try_sql("DROP TABLE IF EXISTS test_special_doubles_module;")
        try_sql("DROP TABLE IF EXISTS test_special_strings_module;")
        try_sql("DROP TABLE IF EXISTS test_boundary_dates_module;")
    }
}
