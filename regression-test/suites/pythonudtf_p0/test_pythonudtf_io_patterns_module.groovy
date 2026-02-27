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

suite("test_pythonudtf_io_patterns_module") {
    // Test Python UDTF Input/Output Patterns
    // Testing different cardinality patterns: 1-to-1, 1-to-N, 1-to-0, N-to-M
    
    def pyPath = """${context.file.parent}/udtf_scripts/pyudtf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${pyPath}".toString())
    
    try {
        // ========================================
        // Pattern 1: One-to-One (1 input row → 1 output row)
        // Each input row produces exactly one output row
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_one_to_one_module(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_one_to_one_module(INT)
        RETURNS ARRAY<STRUCT<input:INT, doubled:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.io_patterns_udtf.one_to_one",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_one_to_one_module; """
        sql """
        CREATE TABLE test_one_to_one_module (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_one_to_one_module VALUES (1, 10), (2, 20), (3, 30);
        """
        
        qt_one_to_one """
            SELECT tmp.input, tmp.doubled
            FROM test_one_to_one_module
            LATERAL VIEW udtf_one_to_one_module(value) tmp AS input, doubled
            ORDER BY tmp.input;
        """
        
        // ========================================
        // Pattern 2: One-to-Many (1 input row → N output all_rows)
        // Each input row produces multiple output all_rows
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_one_to_many_module(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_one_to_many_module(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.io_patterns_udtf.one_to_many",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_one_to_many_module; """
        sql """
        CREATE TABLE test_one_to_many_module (
            id INT,
            count INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_one_to_many_module VALUES (1, 3), (2, 2), (3, 4);
        """
        
        qt_one_to_many """
            SELECT id, tmp.value
            FROM test_one_to_many_module
            LATERAL VIEW udtf_one_to_many_module(count) tmp AS value
            ORDER BY id, tmp.value;
        """
        
        // ========================================
        // Pattern 3: One-to-Zero (1 input row → 0 output all_rows)
        // Some input all_rows produce no output (filtering)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_one_to_zero_module(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_one_to_zero_module(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.io_patterns_udtf.one_to_zero",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_one_to_zero_module; """
        sql """
        CREATE TABLE test_one_to_zero_module (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_one_to_zero_module VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6);
        """
        
        qt_one_to_zero """
            SELECT tmp.value
            FROM test_one_to_zero_module
            LATERAL VIEW udtf_one_to_zero_module(value) tmp AS value
            ORDER BY tmp.value;
        """
        
        // ========================================
        // Pattern 4: One-to-Variable (1 input row → 0/1/N output all_rows)
        // Different input all_rows produce different numbers of output all_rows
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_one_to_variable_module(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_one_to_variable_module(STRING)
        RETURNS ARRAY<STRING>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.io_patterns_udtf.one_to_variable",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_one_to_variable_module; """
        sql """
        CREATE TABLE test_one_to_variable_module (
            id INT,
            text STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_one_to_variable_module VALUES 
        (1, 'hello'),           -- 1 output
        (2, 'hello world'),     -- 2 outputs
        (3, ''),                -- 0 outputs
        (4, 'a b c');           -- 3 outputs
        """
        
        qt_one_to_variable """
            SELECT id, tmp.word
            FROM test_one_to_variable_module
            LATERAL VIEW udtf_one_to_variable_module(text) tmp AS word
            ORDER BY id, tmp.word;
        """
        
        // ========================================
        // Pattern 5: Many-to-One (N input all_rows → aggregate to fewer all_rows)
        // Note: This simulates batch processing where each row independently
        // produces output, but conceptually represents aggregation pattern
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_aggregate_pattern_module(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_aggregate_pattern_module(INT)
        RETURNS ARRAY<STRUCT<value:INT, category:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.io_patterns_udtf.aggregate_pattern",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_aggregate_pattern_module; """
        sql """
        CREATE TABLE test_aggregate_pattern_module (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_aggregate_pattern_module VALUES 
        (1, 5), (2, 50), (3, 500), (4, 8), (5, 80), (6, 800);
        """
        
        qt_aggregate_pattern """
            SELECT tmp.category, COUNT(*) as count
            FROM test_aggregate_pattern_module
            LATERAL VIEW udtf_aggregate_pattern_module(value) tmp AS value, category
            GROUP BY tmp.category
            ORDER BY tmp.category;
        """
        
        // ========================================
        // Pattern 6: Explosive Growth (1 input row → many output all_rows)
        // Testing large multiplication factor
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_explosive_module(INT, INT); """
        sql """
        CREATE TABLES FUNCTION udtf_explosive_module(INT, INT)
        RETURNS ARRAY<STRUCT<row_id:INT, col_id:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.io_patterns_udtf.explosive",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_explosive_module; """
        sql """
        CREATE TABLE test_explosive_module (
            id INT,
            all_rows INT,
            all_cols INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_explosive_module VALUES (1, 2, 3);
        """
        
        qt_explosive """
            SELECT tmp.row_id, tmp.col_id
            FROM test_explosive_module
            LATERAL VIEW udtf_explosive_module(all_rows, all_cols) tmp AS row_id, col_id
            ORDER BY tmp.row_id, tmp.col_id;
        """
        
        // ========================================
        // Pattern 7: Conditional Branching (different logic paths)
        // Same function but different output counts based on condition
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_conditional_module(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_conditional_module(INT)
        RETURNS ARRAY<STRUCT<value:INT, type:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.io_patterns_udtf.conditional",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_conditional_module_io; """
        sql """
        CREATE TABLE test_conditional_module_io (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_conditional_module_io VALUES (1, 10), (2, -5), (3, 0), (4, 7);
        """
        
        qt_conditional """
            SELECT tmp.value, tmp.type
            FROM test_conditional_module_io
            LATERAL VIEW udtf_conditional_module(value) tmp AS value, type
            ORDER BY tmp.value, tmp.type;
        """
        
        // ========================================
        // Pattern 8: All-or-Nothing (either all all_rows or no all_rows)
        // Based on a condition, output all or nothing
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_all_or_nothing_module(STRING, INT); """
        sql """
        CREATE TABLES FUNCTION udtf_all_or_nothing_module(STRING, INT)
        RETURNS ARRAY<STRUCT<char:STRING, pos:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.io_patterns_udtf.all_or_nothing",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_all_or_nothing_module; """
        sql """
        CREATE TABLE test_all_or_nothing_module (
            id INT,
            text STRING,
            min_len INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_all_or_nothing_module VALUES 
        (1, 'hello', 3),    -- 5 outputs (length=5 >= 3)
        (2, 'hi', 5),       -- 0 outputs (length=2 < 5)
        (3, 'world', 4);    -- 5 outputs (length=5 >= 4)
        """
        
        qt_all_or_nothing """
            SELECT id, tmp.char, tmp.pos
            FROM test_all_or_nothing_module
            LATERAL VIEW udtf_all_or_nothing_module(text, min_len) tmp AS char, pos
            ORDER BY id, tmp.pos;
        """
        
        // ========================================
        // Pattern 9: Empty Input Table (0 input all_rows)
        // Test behavior with no input data
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_empty_input_module(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_empty_input_module(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.io_patterns_udtf.empty_input",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_empty_input_module; """
        sql """
        CREATE TABLE test_empty_input_module (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        // No INSERT - table is empty
        
        qt_empty_input """
            SELECT tmp.value
            FROM test_empty_input_module
            LATERAL VIEW udtf_empty_input_module(value) tmp AS value;
        """
        
        // ========================================
        // Pattern 10: Batch Processing Simulation
        // Multiple input all_rows, each producing variable outputs
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_batch_process_module(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_batch_process_module(INT)
        RETURNS ARRAY<STRUCT<original:INT, factor:INT, result:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.io_patterns_udtf.batch_process",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_batch_process_module; """
        sql """
        CREATE TABLE test_batch_process_module (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_batch_process_module VALUES (1, 10), (2, 20);
        """
        
        qt_batch_process """
            SELECT tmp.original, tmp.factor, tmp.result
            FROM test_batch_process_module
            LATERAL VIEW udtf_batch_process_module(value) tmp AS original, factor, result
            ORDER BY tmp.original, tmp.factor;
        """
        
    } finally {
        // Cleanup functions
        try_sql("DROP FUNCTION IF EXISTS udtf_one_to_one_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_one_to_many_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_one_to_zero_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_one_to_variable_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_aggregate_pattern_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_explosive_module(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_conditional_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_all_or_nothing_module(STRING, INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_empty_input_module(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_batch_process_module(INT);")
        
        // Cleanup tables
        try_sql("DROP TABLE IF EXISTS test_one_to_one_module;")
        try_sql("DROP TABLE IF EXISTS test_one_to_many_module;")
        try_sql("DROP TABLE IF EXISTS test_one_to_zero_module;")
        try_sql("DROP TABLE IF EXISTS test_one_to_variable_module;")
        try_sql("DROP TABLE IF EXISTS test_aggregate_pattern_module;")
        try_sql("DROP TABLE IF EXISTS test_explosive_module;")
        try_sql("DROP TABLE IF EXISTS test_conditional_module_io;")
        try_sql("DROP TABLE IF EXISTS test_all_or_nothing_module;")
        try_sql("DROP TABLE IF EXISTS test_empty_input_module;")
        try_sql("DROP TABLE IF EXISTS test_batch_process_module;")
    }
}
