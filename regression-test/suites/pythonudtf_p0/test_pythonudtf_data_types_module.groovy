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

suite("test_pythonudtf_data_types_module") {
    // Test Python UDTF with Various Data Types using module-based deployment
    // UDTFs are loaded from pyudtf.zip file
    
    def pyPath = """${context.file.parent}/udtf_scripts/pyudtf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${pyPath}".toString())
    
    try {
        // ========================================
        // Type 1: TINYINT (1-byte integer: -128 to 127)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_tinyint_module(TINYINT); """
        sql """
        CREATE TABLES FUNCTION udtf_tinyint_module(TINYINT)
        RETURNS ARRAY<STRUCT<original:TINYINT, doubled:TINYINT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_tinyint",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_tinyint_module; """
        sql """
        CREATE TABLE test_tinyint_module (
            id INT,
            v TINYINT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_tinyint_module VALUES (1, -60), (2, 0), (3, 63);
        """
        
        qt_tinyint """
            SELECT tmp.original, tmp.doubled
            FROM test_tinyint_module
            LATERAL VIEW udtf_tinyint_module(v) tmp AS original, doubled
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 2: SMALLINT (2-byte integer: -32768 to 32767)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_smallint_module(SMALLINT); """
        sql """
        CREATE TABLES FUNCTION udtf_smallint_module(SMALLINT)
        RETURNS ARRAY<STRUCT<original:SMALLINT, squared:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_smallint",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_smallint_module; """
        sql """
        CREATE TABLE test_smallint_module (
            id INT,
            v SMALLINT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_smallint_module VALUES (1, -1000), (2, 0), (3, 1000);
        """
        
        qt_smallint """
            SELECT tmp.original, tmp.squared
            FROM test_smallint_module
            LATERAL VIEW udtf_smallint_module(v) tmp AS original, squared
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 3: BIGINT (8-byte integer)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_bigint_module(BIGINT); """
        sql """
        CREATE TABLES FUNCTION udtf_bigint_module(BIGINT)
        RETURNS ARRAY<STRUCT<original:BIGINT, incremented:BIGINT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_bigint",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_bigint_module; """
        sql """
        CREATE TABLE test_bigint_module (
            id INT,
            v BIGINT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_bigint_module VALUES (1, -1000000000000), (2, 0), (3, 1000000000000);
        """
        
        qt_bigint """
            SELECT tmp.original, tmp.incremented
            FROM test_bigint_module
            LATERAL VIEW udtf_bigint_module(v) tmp AS original, incremented
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 4: FLOAT (4-byte floating point)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_float_module(FLOAT); """
        sql """
        CREATE TABLES FUNCTION udtf_float_module(FLOAT)
        RETURNS ARRAY<STRUCT<original:FLOAT, halved:FLOAT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_float",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_float_module; """
        sql """
        CREATE TABLE test_float_module (
            id INT,
            v FLOAT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_float_module VALUES (1, -3.14), (2, 0.0), (3, 2.718);
        """
        
        qt_float """
            SELECT tmp.original, tmp.halved
            FROM test_float_module
            LATERAL VIEW udtf_float_module(v) tmp AS original, halved
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 5: DOUBLE (8-byte floating point)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_double_module(DOUBLE); """
        sql """
        CREATE TABLES FUNCTION udtf_double_module(DOUBLE)
        RETURNS ARRAY<STRUCT<original:DOUBLE, sqrt_value:DOUBLE>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_double",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_double_module; """
        sql """
        CREATE TABLE test_double_module (
            id INT,
            v DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_double_module VALUES (1, 0.0), (2, 4.0), (3, 16.0), (4, 100.0);
        """
        
        qt_double """
            SELECT tmp.original, tmp.sqrt_value
            FROM test_double_module
            LATERAL VIEW udtf_double_module(v) tmp AS original, sqrt_value
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 6: BOOLEAN
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_boolean_module(BOOLEAN); """
        sql """
        CREATE TABLES FUNCTION udtf_boolean_module(BOOLEAN)
        RETURNS ARRAY<STRUCT<original:BOOLEAN, negated:BOOLEAN, as_string:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_boolean",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_boolean_module; """
        sql """
        CREATE TABLE test_boolean_module (
            id INT,
            v BOOLEAN
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_boolean_module VALUES (1, true), (2, false);
        """
        
        qt_boolean """
            SELECT tmp.original, tmp.negated, tmp.as_string
            FROM test_boolean_module
            LATERAL VIEW udtf_boolean_module(v) tmp AS original, negated, as_string
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 7: STRING (Variable length text)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_string_module(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_string_module(STRING)
        RETURNS ARRAY<STRUCT<original:STRING, length:INT, upper:STRING, lower:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_string",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_string_module; """
        sql """
        CREATE TABLE test_string_module (
            id INT,
            v STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_string_module VALUES (1, 'Hello'), (2, 'WORLD'), (3, 'DoRiS');
        """
        
        qt_string """
            SELECT tmp.original, tmp.length, tmp.upper, tmp.lower
            FROM test_string_module
            LATERAL VIEW udtf_string_module(v) tmp AS original, length, upper, lower
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 8: DATE (Date without time)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_date_module(DATE); """
        sql """
        CREATE TABLES FUNCTION udtf_date_module(DATE)
        RETURNS ARRAY<STRUCT<original:DATE, year:INT, month:INT, day:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_date",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_date_module; """
        sql """
        CREATE TABLE test_date_module (
            id INT,
            v DATE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_date_module VALUES (1, '2024-01-01'), (2, '2024-06-15'), (3, '2024-12-31');
        """
        
        qt_date """
            SELECT tmp.original, tmp.year, tmp.month, tmp.day
            FROM test_date_module
            LATERAL VIEW udtf_date_module(v) tmp AS original, year, month, day
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 9: DATETIME (Date with time)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_datetime_module(DATETIME); """
        sql """
        CREATE TABLES FUNCTION udtf_datetime_module(DATETIME)
        RETURNS ARRAY<STRUCT<original:DATETIME, hour:INT, minute:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_datetime",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_datetime_module; """
        sql """
        CREATE TABLE test_datetime_module (
            id INT,
            v DATETIME
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_datetime_module VALUES 
        (1, '2024-01-01 08:30:00'), 
        (2, '2024-06-15 12:00:00'), 
        (3, '2024-12-31 23:59:00');
        """
        
        qt_datetime """
            SELECT tmp.original, tmp.hour, tmp.minute
            FROM test_datetime_module
            LATERAL VIEW udtf_datetime_module(v) tmp AS original, hour, minute
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 10: ARRAY<INT> (Array of integers)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_array_int_module(ARRAY<INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_array_int_module(ARRAY<INT>)
        RETURNS ARRAY<STRUCT<arr_pos:INT, element:INT, doubled:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_array_int",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_array_int_module; """
        sql """
        CREATE TABLE test_array_int_module (
            id INT,
            v ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_array_int_module VALUES 
        (1, [1, 2, 3]), 
        (2, [10, 20]), 
        (3, [100]);
        """
        
        qt_array_int """
            SELECT id, tmp.arr_pos, tmp.element, tmp.doubled
            FROM test_array_int_module
            LATERAL VIEW udtf_array_int_module(v) tmp AS arr_pos, element, doubled
            ORDER BY id, tmp.arr_pos;
        """
        
        // ========================================
        // Type 11: ARRAY<STRING> (Array of strings)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_array_string_module(ARRAY<STRING>); """
        sql """
        CREATE TABLES FUNCTION udtf_array_string_module(ARRAY<STRING>)
        RETURNS ARRAY<STRUCT<element:STRING, length:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_array_string",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_array_string_module; """
        sql """
        CREATE TABLE test_array_string_module (
            id INT,
            v ARRAY<STRING>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_array_string_module VALUES 
        (1, ['apple', 'banana']), 
        (2, ['cat', 'dog', 'bird']);
        """
        
        qt_array_string """
            SELECT id, tmp.element, tmp.length
            FROM test_array_string_module
            LATERAL VIEW udtf_array_string_module(v) tmp AS element, length
            ORDER BY id, tmp.element;
        """
        
        // ========================================
        // Type 12: STRUCT (Structured data)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_struct_module(STRUCT<name:STRING, age:INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_struct_module(STRUCT<name:STRING, age:INT>)
        RETURNS ARRAY<STRUCT<name:STRING, age:INT, category:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_struct",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_struct_module; """
        sql """
        CREATE TABLE test_struct_module (
            id INT,
            person STRUCT<name:STRING, age:INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_struct_module VALUES 
        (1, named_struct('name', 'Alice', 'age', 25)), 
        (2, named_struct('name', 'Bob', 'age', 15)), 
        (3, named_struct('name', 'Charlie', 'age', 30));
        """
        
        qt_struct """
            SELECT tmp.name, tmp.age, tmp.category
            FROM test_struct_module
            LATERAL VIEW udtf_struct_module(person) tmp AS name, age, category
            ORDER BY tmp.name;
        """
        
        // ========================================
        // Type 13: Multiple Input Types (INT, STRING)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_multi_types_module(INT, STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_multi_types_module(INT, STRING)
        RETURNS ARRAY<STRUCT<number:INT, text:STRING, combined:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_multi_types",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_multi_types_module; """
        sql """
        CREATE TABLE test_multi_types_module (
            id INT,
            num INT,
            text STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_multi_types_module VALUES (1, 100, 'apple'), (2, 200, 'banana');
        """
        
        qt_multi_types """
            SELECT tmp.number, tmp.text, tmp.combined
            FROM test_multi_types_module
            LATERAL VIEW udtf_multi_types_module(num, text) tmp AS number, text, combined
            ORDER BY tmp.number;
        """
        
        // ========================================
        // Type 14: DECIMAL (High precision decimal)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_decimal_module(DECIMAL(10,2)); """
        sql """
        CREATE TABLES FUNCTION udtf_decimal_module(DECIMAL(10,2))
        RETURNS ARRAY<STRUCT<original:DECIMAL(10,2), doubled:DECIMAL(11,2)>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_decimal",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_decimal_module; """
        sql """
        CREATE TABLE test_decimal_module (
            id INT,
            v DECIMAL(10,2)
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_decimal_module VALUES (1, 123.45), (2, 678.90), (3, 999.99);
        """
        
        qt_decimal """
            SELECT tmp.original, tmp.doubled
            FROM test_decimal_module
            LATERAL VIEW udtf_decimal_module(v) tmp AS original, doubled
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Section: P1 - Complex Data Types
        // ========================================
        
        // Test P1.1: MAP<STRING, INT> type (if supported)
        // Note: Doris may not fully support MAP in UDTF, test with workaround
        sql """ DROP FUNCTION IF EXISTS udtf_map_processor_module(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_map_processor_module(STRING)
        RETURNS ARRAY<STRUCT<k:STRING, v:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_map_string",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_map_like_module; """
        sql """
        CREATE TABLE test_map_like_module (
            id INT,
            map_data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_map_like_module VALUES 
        (1, 'age:25,score:90'),
        (2, 'age:30,score:85,level:3');
        """
        
        qt_map_like """
            SELECT id, tmp.k, tmp.v
            FROM test_map_like_module
            LATERAL VIEW udtf_map_processor_module(map_data) tmp AS k, v
            ORDER BY id, tmp.k;
        """
        
        // Test P1.2: Nested ARRAY (ARRAY<ARRAY<INT>> simulated)
        sql """ DROP FUNCTION IF EXISTS udtf_nested_array_module(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_nested_array_module(STRING)
        RETURNS ARRAY<STRUCT<group_idx:INT, element:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_nested_array",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_nested_array_module; """
        sql """
        CREATE TABLE test_nested_array_module (
            id INT,
            nested_data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_nested_array_module VALUES 
        (1, '[[10,20],[30,40]]'),
        (2, '[[50],[60,70,80]]');
        """
        
        qt_nested_array """
            SELECT id, tmp.group_idx, tmp.element
            FROM test_nested_array_module
            LATERAL VIEW udtf_nested_array_module(nested_data) tmp AS group_idx, element
            ORDER BY id, tmp.group_idx, tmp.element;
        """
        
        // Test P1.3: ARRAY<STRUCT<...>>
        sql """ DROP FUNCTION IF EXISTS udtf_array_of_structs_module(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_array_of_structs_module(STRING)
        RETURNS ARRAY<STRUCT<name:STRING, age:INT, score:INT>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_array_structs",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_array_structs_module; """
        sql """
        CREATE TABLE test_array_structs_module (
            id INT,
            data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_array_structs_module VALUES 
        (1, 'Alice:25:90|Bob:30:85'),
        (2, 'Charlie:28:88');
        """
        
        qt_array_structs """
            SELECT id, tmp.name, tmp.age, tmp.score
            FROM test_array_structs_module
            LATERAL VIEW udtf_array_of_structs_module(data) tmp AS name, age, score
            ORDER BY id, tmp.name;
        """
        
        // Test P1.4: STRUCT with nested ARRAY
        sql """ DROP FUNCTION IF EXISTS udtf_struct_with_array_module(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_struct_with_array_module(STRING)
        RETURNS ARRAY<STRUCT<person_name:STRING, tag_count:INT, tags:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_struct_array",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_struct_array_module; """
        sql """
        CREATE TABLE test_struct_array_module (
            id INT,
            person_tags STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_struct_array_module VALUES 
        (1, 'Alice:sports,music,reading'),
        (2, 'Bob:coding,gaming');
        """
        
        qt_struct_array """
            SELECT id, tmp.person_name, tmp.tag_count, tmp.tags
            FROM test_struct_array_module
            LATERAL VIEW udtf_struct_with_array_module(person_tags) tmp AS person_name, tag_count, tags
            ORDER BY id;
        """
        
        // Test P1.5: JSON-like data processing
        sql """ DROP FUNCTION IF EXISTS udtf_json_extract_module(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_json_extract_module(STRING)
        RETURNS ARRAY<STRUCT<field:STRING, v:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.extract_json_fields",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_json_data_module; """
        sql """
        CREATE TABLE test_json_data_module (
            id INT,
            json_content STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_json_data_module VALUES 
        (1, '{"name":"Alice","age":25,"city":"NYC"}'),
        (2, '{"name":"Bob","age":30}');
        """
        
        qt_json_extract """
            SELECT id, tmp.field, tmp.v
            FROM test_json_data_module
            LATERAL VIEW udtf_json_extract_module(json_content) tmp AS field, v
            ORDER BY id, tmp.field;
        """
        
        // Test P1.6: Complex nested STRUCT
        sql """ DROP FUNCTION IF EXISTS udtf_complex_struct_module(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_complex_struct_module(STRING)
        RETURNS ARRAY<STRUCT<user_id:INT, user_name:STRING, address_city:STRING, address_zip:STRING>>
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "pyudtf_module.data_types_udtf.process_complex_struct",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        sql """ DROP TABLE IF EXISTS test_complex_struct_module; """
        sql """
        CREATE TABLE test_complex_struct_module (
            id INT,
            user_data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_complex_struct_module VALUES 
        (1, '101:Alice:NYC:10001'),
        (2, '102:Bob:LA:90001');
        """
        
        qt_complex_struct """
            SELECT id, tmp.user_id, tmp.user_name, tmp.address_city, tmp.address_zip
            FROM test_complex_struct_module
            LATERAL VIEW udtf_complex_struct_module(user_data) tmp AS user_id, user_name, address_city, address_zip
            ORDER BY id;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS udtf_tinyint_module(TINYINT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_smallint_module(SMALLINT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_bigint_module(BIGINT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_float_module(FLOAT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_double_module(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS udtf_boolean_module(BOOLEAN);")
        try_sql("DROP FUNCTION IF EXISTS udtf_string_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_date_module(DATE);")
        try_sql("DROP FUNCTION IF EXISTS udtf_datetime_module(DATETIME);")
        try_sql("DROP FUNCTION IF EXISTS udtf_array_int_module(ARRAY<INT>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_array_string_module(ARRAY<STRING>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_struct_module(STRUCT<name:STRING, age:INT>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_multi_types_module(INT, STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_decimal_module(DECIMAL(10,2));")
        try_sql("DROP FUNCTION IF EXISTS udtf_map_processor_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_nested_array_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_array_of_structs_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_struct_with_array_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_json_extract_module(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_complex_struct_module(STRING);")
        try_sql("DROP TABLE IF EXISTS test_tinyint_module;")
        try_sql("DROP TABLE IF EXISTS test_smallint_module;")
        try_sql("DROP TABLE IF EXISTS test_bigint_module;")
        try_sql("DROP TABLE IF EXISTS test_float_module;")
        try_sql("DROP TABLE IF EXISTS test_double_module;")
        try_sql("DROP TABLE IF EXISTS test_boolean_module;")
        try_sql("DROP TABLE IF EXISTS test_string_module;")
        try_sql("DROP TABLE IF EXISTS test_date_module;")
        try_sql("DROP TABLE IF EXISTS test_datetime_module;")
        try_sql("DROP TABLE IF EXISTS test_array_int_module;")
        try_sql("DROP TABLE IF EXISTS test_array_string_module;")
        try_sql("DROP TABLE IF EXISTS test_struct_module;")
        try_sql("DROP TABLE IF EXISTS test_multi_types_module;")
        try_sql("DROP TABLE IF EXISTS test_decimal_module;")
        try_sql("DROP TABLE IF EXISTS test_map_like_module;")
        try_sql("DROP TABLE IF EXISTS test_nested_array_module;")
        try_sql("DROP TABLE IF EXISTS test_array_structs_module;")
        try_sql("DROP TABLE IF EXISTS test_struct_array_module;")
        try_sql("DROP TABLE IF EXISTS test_json_data_module;")
        try_sql("DROP TABLE IF EXISTS test_complex_struct_module;")
    }
}
