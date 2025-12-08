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

suite("test_pythonudtf_exceptions_module") {
    // Test Python UDTF Exception Handling
    // Coverage: Runtime errors, type errors, logic errors, edge cases
    
    def pyPath = """${context.file.parent}/udtf_scripts/pyudtf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${pyPath}".toString())
    
    try {
        // ========================================
        // Section 1: Arithmetic Exceptions
        // ========================================
        
        // Test 1.1: Division by Zero - Handled
        sql """ DROP FUNCTION IF EXISTS udtf_safe_divide_module(INT, INT); """
        sql """
        CREATE TABLES FUNCTION udtf_safe_divide_module(INT, INT)
        RETURNS ARRAY<STRUCT<numerator:INT, denominator:INT, result:DOUBLE, error_msg:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.exceptions_udtf.safe_divide",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_division_module; """
        sql """
        CREATE TABLE test_division_module (
            id INT,
            num INT,
            denom INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_division_module VALUES 
        (1, 10, 2),
        (2, 10, 0),
        (3, 0, 5),
        (4, -8, 4);
        """
        
        qt_safe_divide """
            SELECT id, tmp.numerator, tmp.denominator, tmp.result, tmp.error_msg
            FROM test_division_module
            LATERAL VIEW udtf_safe_divide_module(num, denom) tmp AS numerator, denominator, result, error_msg
            ORDER BY id;
        """
        
        // Test 1.2: Integer Overflow Detection
        sql """ DROP FUNCTION IF EXISTS udtf_overflow_check_module(BIGINT); """
        sql """
        CREATE TABLES FUNCTION udtf_overflow_check_module(BIGINT)
        RETURNS ARRAY<STRUCT<original:BIGINT, doubled:BIGINT, status:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.exceptions_udtf.check_overflow",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_overflow_module; """
        sql """
        CREATE TABLE test_overflow_module (
            id INT,
            big_val BIGINT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_overflow_module VALUES 
        (1, 100),
        (2, 5000000000000),
        (3, -5000000000000),
        (4, NULL);
        """
        
        qt_overflow_check """
            SELECT id, tmp.original, tmp.doubled, tmp.status
            FROM test_overflow_module
            LATERAL VIEW udtf_overflow_check_module(big_val) tmp AS original, doubled, status
            ORDER BY id;
        """
        
        // ========================================
        // Section 2: Type Conversion Errors
        // ========================================
        
        // Test 2.1: String to Number Conversion
        sql """ DROP FUNCTION IF EXISTS udtf_parse_number_module(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_parse_number_module(STRING)
        RETURNS ARRAY<STRUCT<input:STRING, parsed:DOUBLE, is_valid:BOOLEAN>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.exceptions_udtf.parse_number",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_parse_module; """
        sql """
        CREATE TABLE test_parse_module (
            id INT,
            text STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_parse_module VALUES 
        (1, '123'),
        (2, '45.67'),
        (3, 'abc'),
        (4, '12.34.56'),
        (5, ''),
        (6, NULL);
        """
        
        qt_parse_number """
            SELECT id, tmp.input, tmp.parsed, tmp.is_valid
            FROM test_parse_module
            LATERAL VIEW udtf_parse_number_module(text) tmp AS input, parsed, is_valid
            ORDER BY id;
        """
        
        // Test 2.2: Type Mismatch Handling
        sql """ DROP FUNCTION IF EXISTS udtf_type_check_module(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_type_check_module(STRING)
        RETURNS ARRAY<STRUCT<value:STRING, type_name:STRING, length:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.exceptions_udtf.check_type",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_types_module; """
        sql """
        CREATE TABLE test_types_module (
            id INT,
            val STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_types_module VALUES 
        (1, 'hello'),
        (2, ''),
        (3, '12345'),
        (4, NULL);
        """
        
        qt_type_check """
            SELECT id, tmp.value, tmp.type_name, tmp.length
            FROM test_types_module
            LATERAL VIEW udtf_type_check_module(val) tmp AS value, type_name, length
            ORDER BY id;
        """
        
        // ========================================
        // Section 3: Collection/Array Errors
        // ========================================
        
        // Test 3.1: Array Index Out of Bounds
        sql """ DROP FUNCTION IF EXISTS udtf_safe_index_module(ARRAY<INT>, INT); """
        sql """
        CREATE TABLES FUNCTION udtf_safe_index_module(ARRAY<INT>, INT)
        RETURNS ARRAY<STRUCT<arr_size:INT, target_pos:INT, value:INT, status:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.exceptions_udtf.safe_array_access",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_array_access_module; """
        sql """
        CREATE TABLE test_array_access_module (
            id INT,
            arr ARRAY<INT>,
            pos INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_array_access_module VALUES 
        (1, [10, 20, 30], 1),
        (2, [10, 20, 30], 5),
        (3, [10, 20, 30], -1),
        (4, [], 0),
        (5, NULL, 0);
        """
        
        qt_safe_index """
            SELECT id, tmp.arr_size, tmp.target_pos, tmp.value, tmp.status
            FROM test_array_access_module
            LATERAL VIEW udtf_safe_index_module(arr, pos) tmp AS arr_size, target_pos, value, status
            ORDER BY id;
        """
        
        // Test 3.2: Empty Collection Handling
        sql """ DROP FUNCTION IF EXISTS udtf_collection_stats_module(ARRAY<INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_collection_stats_module(ARRAY<INT>)
        RETURNS ARRAY<STRUCT<count:INT, total:BIGINT, avg:DOUBLE, status:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.exceptions_udtf.compute_stats",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_collection_stats_module; """
        sql """
        CREATE TABLE test_collection_stats_module (
            id INT,
            data ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_collection_stats_module VALUES 
        (1, [1, 2, 3, 4, 5]),
        (2, []),
        (3, NULL),
        (4, [10, 20]);
        """
        
        qt_collection_stats """
            SELECT id, tmp.count, tmp.total, tmp.avg, tmp.status
            FROM test_collection_stats_module
            LATERAL VIEW udtf_collection_stats_module(data) tmp AS count, total, avg, status
            ORDER BY id;
        """
        
        // ========================================
        // Section 4: Dictionary/STRUCT Errors
        // ========================================
        
        // Test 4.1: Missing Dictionary Keys
        sql """ DROP FUNCTION IF EXISTS udtf_safe_struct_access_module(STRUCT<name:STRING, age:INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_safe_struct_access_module(STRUCT<name:STRING, age:INT>)
        RETURNS ARRAY<STRUCT<has_name:BOOLEAN, has_age:BOOLEAN, name_val:STRING, age_val:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.exceptions_udtf.access_struct_fields",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_struct_access_module; """
        sql """
        CREATE TABLE test_struct_access_module (
            id INT,
            person STRUCT<name:STRING, age:INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_struct_access_module VALUES 
        (1, named_struct('name', 'Alice', 'age', 30)),
        (2, named_struct('name', 'Bob', 'age', NULL)),
        (3, named_struct('name', NULL, 'age', 25)),
        (4, NULL);
        """
        
        qt_safe_struct_access """
            SELECT id, tmp.has_name, tmp.has_age, tmp.name_val, tmp.age_val
            FROM test_struct_access_module
            LATERAL VIEW udtf_safe_struct_access_module(person) tmp AS has_name, has_age, name_val, age_val
            ORDER BY id;
        """
        
        // ========================================
        // Section 5: String Processing Errors
        // ========================================
        
        // Test 5.1: Invalid String Operations
        sql """ DROP FUNCTION IF EXISTS udtf_string_slice_module(STRING, INT, INT); """
        sql """
        CREATE TABLES FUNCTION udtf_string_slice_module(STRING, INT, INT)
        RETURNS ARRAY<STRUCT<original:STRING, start_pos:INT, end_pos:INT, slice:STRING, status:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.exceptions_udtf.slice_string",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_string_slice_module; """
        sql """
        CREATE TABLE test_string_slice_module (
            id INT,
            text STRING,
            start_pos INT,
            end_pos INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_string_slice_module VALUES 
        (1, 'hello world', 0, 5),
        (2, 'hello world', 6, 11),
        (3, 'hello world', 20, 30),
        (4, 'hello world', 5, 2),
        (5, NULL, 0, 5);
        """
        
        qt_string_slice """
            SELECT id, tmp.original, tmp.start_pos, tmp.end_pos, tmp.slice, tmp.status
            FROM test_string_slice_module
            LATERAL VIEW udtf_string_slice_module(text, start_pos, end_pos) tmp AS original, start_pos, end_pos, slice, status
            ORDER BY id;
        """
        
        // Test 5.2: Encoding/Decoding Errors
        sql """ DROP FUNCTION IF EXISTS udtf_check_encoding_module(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_check_encoding_module(STRING)
        RETURNS ARRAY<STRUCT<text:STRING, byte_length:INT, char_length:INT, has_unicode:BOOLEAN>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.exceptions_udtf.check_text_encoding",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_encoding_module; """
        sql """
        CREATE TABLE test_encoding_module (
            id INT,
            text STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_encoding_module VALUES 
        (1, 'hello'),
        (2, '你好世界'),
        (3, 'café'),
        (4, ''),
        (5, NULL);
        """
        
        qt_check_encoding """
            SELECT id, tmp.text, tmp.byte_length, tmp.char_length, tmp.has_unicode
            FROM test_encoding_module
            LATERAL VIEW udtf_check_encoding_module(text) tmp AS text, byte_length, char_length, has_unicode
            ORDER BY id;
        """
        
        // ========================================
        // Section 6: Logic and State Errors
        // ========================================
        
        // Test 6.1: Conditional Logic Errors
        sql """ DROP FUNCTION IF EXISTS udtf_conditional_process_module(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_conditional_process_module(INT)
        RETURNS ARRAY<STRUCT<input:INT, category:STRING, result:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.exceptions_udtf.process_conditional",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_conditional_module; """
        sql """
        CREATE TABLE test_conditional_module (
            id INT,
            val INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_conditional_module VALUES 
        (1, -10),
        (2, 0),
        (3, 50),
        (4, 200),
        (5, NULL);
        """
        
        qt_conditional_process """
            SELECT id, tmp.input, tmp.category, tmp.result
            FROM test_conditional_module
            LATERAL VIEW udtf_conditional_process_module(val) tmp AS input, category, result
            ORDER BY id;
        """
        
        // Test 6.2: Yield Control - No Output Case
        sql """ DROP FUNCTION IF EXISTS udtf_filter_yield_module(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_filter_yield_module(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.exceptions_udtf.conditional_yield",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_filter_yield_module; """
        sql """
        CREATE TABLE test_filter_yield_module (
            id INT,
            val INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_filter_yield_module VALUES 
        (1, 10),
        (2, 15),
        (3, -4),
        (4, 0),
        (5, 22),
        (6, NULL);
        """
        
        qt_filter_yield """
            SELECT id, tmp.value
            FROM test_filter_yield_module
            LATERAL VIEW udtf_filter_yield_module(val) tmp AS value
            ORDER BY id;
        """
        
        // ========================================
        // Section 7: Edge Cases in Computation
        // ========================================
        
        // Test 7.1: Very Small and Very Large Numbers
        sql """ DROP FUNCTION IF EXISTS udtf_number_range_module(DOUBLE); """
        sql """
        CREATE TABLES FUNCTION udtf_number_range_module(DOUBLE)
        RETURNS ARRAY<STRUCT<value:DOUBLE, magnitude:STRING, is_finite:BOOLEAN>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.exceptions_udtf.classify_number_range",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_number_range_module; """
        sql """
        CREATE TABLE test_number_range_module (
            id INT,
            val DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_number_range_module VALUES 
        (1, 0.0),
        (2, 1e-150),
        (3, 1e150),
        (4, 0.5),
        (5, 123.456),
        (6, NULL);
        """
        
        qt_number_range """
            SELECT id, tmp.value, tmp.magnitude, tmp.is_finite
            FROM test_number_range_module
            LATERAL VIEW udtf_number_range_module(val) tmp AS value, magnitude, is_finite
            ORDER BY id;
        """
        
        // Test 7.2: Date/Time Edge Cases
        sql """ DROP FUNCTION IF EXISTS udtf_date_validation_module(DATE); """
        sql """
        CREATE TABLES FUNCTION udtf_date_validation_module(DATE)
        RETURNS ARRAY<STRUCT<input_date:DATE, year:INT, is_leap_year:BOOLEAN, status:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.exceptions_udtf.validate_date",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_date_validation_module; """
        sql """
        CREATE TABLE test_date_validation_module (
            id INT,
            dt DATE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_date_validation_module VALUES 
        (1, '2024-01-01'),
        (2, '2000-02-29'),
        (3, '1970-01-01'),
        (4, '9999-12-31'),
        (5, NULL);
        """
        
        qt_date_validation """
            SELECT id, tmp.input_date, tmp.year, tmp.is_leap_year, tmp.status
            FROM test_date_validation_module
            LATERAL VIEW udtf_date_validation_module(dt) tmp AS input_date, year, is_leap_year, status
            ORDER BY id;
        """
        
    } finally {
        // Cleanup functions
        try_sql("DROP FUNCTION IF EXISTS udtf_safe_divide_module(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_overflow_check_module(BIGINT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_parse_number_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_type_check_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_safe_index_module(ARRAY<INT>, INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_collection_stats_module(ARRAY<INT>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_safe_struct_access_module(STRUCT<name:STRING, age:INT>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_string_slice_module(STRING, INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_check_encoding_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_conditional_process_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_filter_yield_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_number_range_module(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS udtf_date_validation_module(DATE);")
        
        // Cleanup tables
        try_sql("DROP TABLE IF EXISTS test_division_module;")
        try_sql("DROP TABLE IF EXISTS test_overflow_module;")
        try_sql("DROP TABLE IF EXISTS test_parse_module;")
        try_sql("DROP TABLE IF EXISTS test_types_module;")
        try_sql("DROP TABLE IF EXISTS test_array_access_module;")
        try_sql("DROP TABLE IF EXISTS test_collection_stats_module;")
        try_sql("DROP TABLE IF EXISTS test_struct_access_module;")
        try_sql("DROP TABLE IF EXISTS test_string_slice_module;")
        try_sql("DROP TABLE IF EXISTS test_encoding_module;")
        try_sql("DROP TABLE IF EXISTS test_conditional_module;")
        try_sql("DROP TABLE IF EXISTS test_filter_yield_module;")
        try_sql("DROP TABLE IF EXISTS test_number_range_module;")
        try_sql("DROP TABLE IF EXISTS test_date_validation_module;")
    }
}
