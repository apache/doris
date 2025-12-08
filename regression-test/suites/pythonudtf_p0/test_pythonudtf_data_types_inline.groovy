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

suite("test_pythonudtf_data_types_inline") {
    // Test Python UDTF with Various Data Types
    // Coverage: Basic types, numeric types, date/time types, complex types
    
    def runtime_version = "3.8.10"
    
    try {
        // ========================================
        // Type 1: TINYINT (1-byte integer: -128 to 127)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_tinyint(TINYINT); """
        sql """
        CREATE TABLES FUNCTION udtf_tinyint(TINYINT)
        RETURNS ARRAY<STRUCT<original:TINYINT, doubled:TINYINT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_tinyint",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_tinyint(v):
    '''Process TINYINT: test small integer range'''
    if v is not None:
        yield (v, v * 2)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_tinyint; """
        sql """
        CREATE TABLE test_tinyint (
            id INT,
            v TINYINT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_tinyint VALUES (1, -60), (2, 0), (3, 63);
        """
        
        qt_tinyint """
            SELECT tmp.original, tmp.doubled
            FROM test_tinyint
            LATERAL VIEW udtf_tinyint(v) tmp AS original, doubled
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 2: SMALLINT (2-byte integer: -32768 to 32767)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_smallint(SMALLINT); """
        sql """
        CREATE TABLES FUNCTION udtf_smallint(SMALLINT)
        RETURNS ARRAY<STRUCT<original:SMALLINT, squared:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_smallint",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_smallint(v):
    '''Process SMALLINT: test medium integer range'''
    if v is not None:
        yield (v, v * v)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_smallint; """
        sql """
        CREATE TABLE test_smallint (
            id INT,
            v SMALLINT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_smallint VALUES (1, -1000), (2, 0), (3, 1000);
        """
        
        qt_smallint """
            SELECT tmp.original, tmp.squared
            FROM test_smallint
            LATERAL VIEW udtf_smallint(v) tmp AS original, squared
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 3: BIGINT (8-byte integer)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_bigint(BIGINT); """
        sql """
        CREATE TABLES FUNCTION udtf_bigint(BIGINT)
        RETURNS ARRAY<STRUCT<original:BIGINT, incremented:BIGINT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_bigint",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_bigint(v):
    '''Process BIGINT: test large integer range'''
    if v is not None:
        yield (v, v + 1)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_bigint; """
        sql """
        CREATE TABLE test_bigint (
            id INT,
            v BIGINT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_bigint VALUES (1, -1000000000000), (2, 0), (3, 1000000000000);
        """
        
        qt_bigint """
            SELECT tmp.original, tmp.incremented
            FROM test_bigint
            LATERAL VIEW udtf_bigint(v) tmp AS original, incremented
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 4: FLOAT (4-byte floating point)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_float(FLOAT); """
        sql """
        CREATE TABLES FUNCTION udtf_float(FLOAT)
        RETURNS ARRAY<STRUCT<original:FLOAT, halved:FLOAT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_float",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_float(v):
    '''Process FLOAT: test floating point numbers'''
    if v is not None:
        yield (v, v / 2.0)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_float; """
        sql """
        CREATE TABLE test_float (
            id INT,
            v FLOAT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_float VALUES (1, -3.14), (2, 0.0), (3, 2.718);
        """
        
        qt_float """
            SELECT tmp.original, tmp.halved
            FROM test_float
            LATERAL VIEW udtf_float(v) tmp AS original, halved
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 5: DOUBLE (8-byte floating point)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_double(DOUBLE); """
        sql """
        CREATE TABLES FUNCTION udtf_double(DOUBLE)
        RETURNS ARRAY<STRUCT<original:DOUBLE, sqrt_value:DOUBLE>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_double",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
import math

def process_double(v):
    '''Process DOUBLE: test high precision floating point'''
    if v is not None and v >= 0:
        yield (v, math.sqrt(v))
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_double; """
        sql """
        CREATE TABLE test_double (
            id INT,
            v DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_double VALUES (1, 0.0), (2, 4.0), (3, 16.0), (4, 100.0);
        """
        
        qt_double """
            SELECT tmp.original, tmp.sqrt_value
            FROM test_double
            LATERAL VIEW udtf_double(v) tmp AS original, sqrt_value
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 6: BOOLEAN
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_boolean(BOOLEAN); """
        sql """
        CREATE TABLES FUNCTION udtf_boolean(BOOLEAN)
        RETURNS ARRAY<STRUCT<original:BOOLEAN, negated:BOOLEAN, as_string:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_boolean",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_boolean(v):
    '''Process BOOLEAN: test true/false values'''
    if v is not None:
        yield (v, not v, 'TRUE' if v else 'FALSE')
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_boolean; """
        sql """
        CREATE TABLE test_boolean (
            id INT,
            v BOOLEAN
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_boolean VALUES (1, true), (2, false);
        """
        
        qt_boolean """
            SELECT tmp.original, tmp.negated, tmp.as_string
            FROM test_boolean
            LATERAL VIEW udtf_boolean(v) tmp AS original, negated, as_string
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 7: STRING (Variable length text)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_string(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_string(STRING)
        RETURNS ARRAY<STRUCT<original:STRING, length:INT, upper:STRING, lower:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_string",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_string(v):
    '''Process STRING: test text manipulation'''
    if v is not None:
        yield (v, len(v), v.upper(), v.lower())
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_string; """
        sql """
        CREATE TABLE test_string (
            id INT,
            v STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_string VALUES (1, 'Hello'), (2, 'WORLD'), (3, 'DoRiS');
        """
        
        qt_string """
            SELECT tmp.original, tmp.length, tmp.upper, tmp.lower
            FROM test_string
            LATERAL VIEW udtf_string(v) tmp AS original, length, upper, lower
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 8: DATE (Date without time)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_date(DATE); """
        sql """
        CREATE TABLES FUNCTION udtf_date(DATE)
        RETURNS ARRAY<STRUCT<original:DATE, year:INT, month:INT, day:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_date",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_date(v):
    '''Process DATE: extract date components'''
    if v is not None:
        # v is a datetime.date object
        yield (v, v.year, v.month, v.day)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_date; """
        sql """
        CREATE TABLE test_date (
            id INT,
            v DATE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_date VALUES (1, '2024-01-01'), (2, '2024-06-15'), (3, '2024-12-31');
        """
        
        qt_date """
            SELECT tmp.original, tmp.year, tmp.month, tmp.day
            FROM test_date
            LATERAL VIEW udtf_date(v) tmp AS original, year, month, day
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 9: DATETIME (Date with time)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_datetime(DATETIME); """
        sql """
        CREATE TABLES FUNCTION udtf_datetime(DATETIME)
        RETURNS ARRAY<STRUCT<original:DATETIME, hour:INT, minute:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_datetime",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_datetime(v):
    '''Process DATETIME: extract time components'''
    if v is not None:
        # v is a datetime.datetime object
        yield (v, v.hour, v.minute)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_datetime; """
        sql """
        CREATE TABLE test_datetime (
            id INT,
            v DATETIME
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_datetime VALUES 
        (1, '2024-01-01 08:30:00'), 
        (2, '2024-06-15 12:00:00'), 
        (3, '2024-12-31 23:59:00');
        """
        
        qt_datetime """
            SELECT tmp.original, tmp.hour, tmp.minute
            FROM test_datetime
            LATERAL VIEW udtf_datetime(v) tmp AS original, hour, minute
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Type 10: ARRAY<INT> (Array of integers)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_array_int(ARRAY<INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_array_int(ARRAY<INT>)
        RETURNS ARRAY<STRUCT<arr_pos:INT, element:INT, doubled:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_array_int",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_array_int(arr):
    '''Process ARRAY<INT>: explode array and process each element'''
    if arr is not None:
        for i, elem in enumerate(arr):
            if elem is not None:
                yield (i, elem, elem * 2)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_array_int; """
        sql """
        CREATE TABLE test_array_int (
            id INT,
            v ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_array_int VALUES 
        (1, [1, 2, 3]), 
        (2, [10, 20]), 
        (3, [100]);
        """
        
        qt_array_int """
            SELECT id, tmp.arr_pos, tmp.element, tmp.doubled
            FROM test_array_int
            LATERAL VIEW udtf_array_int(v) tmp AS arr_pos, element, doubled
            ORDER BY id, tmp.arr_pos;
        """
        
        // ========================================
        // Type 11: ARRAY<STRING> (Array of strings)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_array_string(ARRAY<STRING>); """
        sql """
        CREATE TABLES FUNCTION udtf_array_string(ARRAY<STRING>)
        RETURNS ARRAY<STRUCT<element:STRING, length:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_array_string",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_array_string(arr):
    '''Process ARRAY<STRING>: explode and get string lengths'''
    if arr is not None:
        for elem in arr:
            if elem is not None:
                yield (elem, len(elem))
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_array_string; """
        sql """
        CREATE TABLE test_array_string (
            id INT,
            v ARRAY<STRING>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_array_string VALUES 
        (1, ['apple', 'banana']), 
        (2, ['cat', 'dog', 'bird']);
        """
        
        qt_array_string """
            SELECT id, tmp.element, tmp.length
            FROM test_array_string
            LATERAL VIEW udtf_array_string(v) tmp AS element, length
            ORDER BY id, tmp.element;
        """
        
        // ========================================
        // Type 12: STRUCT (Structured data)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_struct(STRUCT<name:STRING, age:INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_struct(STRUCT<name:STRING, age:INT>)
        RETURNS ARRAY<STRUCT<name:STRING, age:INT, category:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_struct",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_struct(person):
    '''Process STRUCT: access struct fields'''
    if person is not None:
        name = person['name'] if 'name' in person else None
        age = person['age'] if 'age' in person else None
        
        if name is not None and age is not None:
            category = 'child' if age < 18 else 'adult'
            yield (name, age, category)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_struct; """
        sql """
        CREATE TABLE test_struct (
            id INT,
            person STRUCT<name:STRING, age:INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_struct VALUES 
        (1, named_struct('name', 'Alice', 'age', 25)), 
        (2, named_struct('name', 'Bob', 'age', 15)), 
        (3, named_struct('name', 'Charlie', 'age', 30));
        """
        
        qt_struct """
            SELECT tmp.name, tmp.age, tmp.category
            FROM test_struct
            LATERAL VIEW udtf_struct(person) tmp AS name, age, category
            ORDER BY tmp.name;
        """
        
        // ========================================
        // Type 13: Multiple Input Types (INT, STRING)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_multi_types(INT, STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_multi_types(INT, STRING)
        RETURNS ARRAY<STRUCT<number:INT, text:STRING, combined:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_multi_types",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_multi_types(num, text):
    '''Process multiple input types'''
    if num is not None and text is not None:
        yield (num, text, f"{text}_{num}")
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_multi_types; """
        sql """
        CREATE TABLE test_multi_types (
            id INT,
            num INT,
            text STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_multi_types VALUES (1, 100, 'apple'), (2, 200, 'banana');
        """
        
        qt_multi_types """
            SELECT tmp.number, tmp.text, tmp.combined
            FROM test_multi_types
            LATERAL VIEW udtf_multi_types(num, text) tmp AS number, text, combined
            ORDER BY tmp.number;
        """
        
        // ========================================
        // Type 14: DECIMAL (High precision decimal)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_decimal(DECIMAL(10,2)); """
        sql """
        CREATE TABLES FUNCTION udtf_decimal(DECIMAL(10,2))
        RETURNS ARRAY<STRUCT<original:DECIMAL(10,2), doubled:DECIMAL(11,2)>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_decimal",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
from decimal import Decimal

def process_decimal(v):
    '''Process DECIMAL: high precision arithmetic'''
    if v is not None:
        doubled = v * 2
        yield (v, doubled)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_decimal; """
        sql """
        CREATE TABLE test_decimal (
            id INT,
            v DECIMAL(10,2)
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_decimal VALUES (1, 123.45), (2, 678.90), (3, 999.99);
        """
        
        qt_decimal """
            SELECT tmp.original, tmp.doubled
            FROM test_decimal
            LATERAL VIEW udtf_decimal(v) tmp AS original, doubled
            ORDER BY tmp.original;
        """
        
        // ========================================
        // Section: P1 - Complex Data Types
        // ========================================
        
        // Test P1.1: MAP<STRING, INT> type (if supported)
        // Note: Doris may not fully support MAP in UDTF, test with workaround
        sql """ DROP FUNCTION IF EXISTS udtf_map_processor(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_map_processor(STRING)
        RETURNS ARRAY<STRUCT<k:STRING, v:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_map_string",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_map_string(map_str):
    '''Process map-like string (key1:val1,key2:val2)'''
    if map_str:
        pairs = map_str.split(',')
        for pair in pairs:
            if ':' in pair:
                k, val = pair.split(':', 1)
                try:
                    yield (k.strip(), int(val.strip()))
                except ValueError:
                    pass
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_map_like; """
        sql """
        CREATE TABLE test_map_like (
            id INT,
            map_data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_map_like VALUES 
        (1, 'age:25,score:90'),
        (2, 'age:30,score:85,level:3');
        """
        
        qt_map_like """
            SELECT id, tmp.k, tmp.v
            FROM test_map_like
            LATERAL VIEW udtf_map_processor(map_data) tmp AS k, v
            ORDER BY id, tmp.k;
        """
        
        // Test P1.2: Nested ARRAY (ARRAY<ARRAY<INT>> simulated)
        sql """ DROP FUNCTION IF EXISTS udtf_nested_array(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_nested_array(STRING)
        RETURNS ARRAY<STRUCT<group_idx:INT, element:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_nested_array",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_nested_array(nested_str):
    '''Process nested array string ([[1,2],[3,4]])'''
    if nested_str:
        # Remove brackets and split by ],[
        nested_str = nested_str.strip('[]')
        groups = nested_str.split('],[')
        
        for group_idx, group in enumerate(groups):
            elements = group.strip('[]').split(',')
            for elem in elements:
                try:
                    yield (group_idx, int(elem.strip()))
                except ValueError:
                    pass
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_nested_array; """
        sql """
        CREATE TABLE test_nested_array (
            id INT,
            nested_data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_nested_array VALUES 
        (1, '[[10,20],[30,40]]'),
        (2, '[[50],[60,70,80]]');
        """
        
        qt_nested_array """
            SELECT id, tmp.group_idx, tmp.element
            FROM test_nested_array
            LATERAL VIEW udtf_nested_array(nested_data) tmp AS group_idx, element
            ORDER BY id, tmp.group_idx, tmp.element;
        """
        
        // Test P1.3: ARRAY<STRUCT<...>>
        sql """ DROP FUNCTION IF EXISTS udtf_array_of_structs(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_array_of_structs(STRING)
        RETURNS ARRAY<STRUCT<name:STRING, age:INT, score:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_array_structs",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_array_structs(data):
    '''Process array of structs (name:age:score|name:age:score)'''
    if data:
        items = data.split('|')
        for item in items:
            parts = item.split(':')
            if len(parts) == 3:
                try:
                    yield (parts[0], int(parts[1]), int(parts[2]))
                except ValueError:
                    pass
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_array_structs; """
        sql """
        CREATE TABLE test_array_structs (
            id INT,
            data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_array_structs VALUES 
        (1, 'Alice:25:90|Bob:30:85'),
        (2, 'Charlie:28:88');
        """
        
        qt_array_structs """
            SELECT id, tmp.name, tmp.age, tmp.score
            FROM test_array_structs
            LATERAL VIEW udtf_array_of_structs(data) tmp AS name, age, score
            ORDER BY id, tmp.name;
        """
        
        // Test P1.4: STRUCT with nested ARRAY
        sql """ DROP FUNCTION IF EXISTS udtf_struct_with_array(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_struct_with_array(STRING)
        RETURNS ARRAY<STRUCT<person_name:STRING, tag_count:INT, tags:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_struct_array",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_struct_array(data):
    '''Process struct with array (name:tag1,tag2,tag3)'''
    if data and ':' in data:
        name, tags = data.split(':', 1)
        tag_list = tags.split(',')
        yield (name, len(tag_list), ','.join(tag_list))
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_struct_array; """
        sql """
        CREATE TABLE test_struct_array (
            id INT,
            person_tags STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_struct_array VALUES 
        (1, 'Alice:sports,music,reading'),
        (2, 'Bob:coding,gaming');
        """
        
        qt_struct_array """
            SELECT id, tmp.person_name, tmp.tag_count, tmp.tags
            FROM test_struct_array
            LATERAL VIEW udtf_struct_with_array(person_tags) tmp AS person_name, tag_count, tags
            ORDER BY id;
        """
        
        // Test P1.5: JSON-like data processing
        sql """ DROP FUNCTION IF EXISTS udtf_json_extract(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_json_extract(STRING)
        RETURNS ARRAY<STRUCT<field:STRING, v:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "extract_json_fields",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
import json

def extract_json_fields(json_str):
    '''Extract JSON fields'''
    if json_str:
        try:
            data = json.loads(json_str)
            if isinstance(data, dict):
                for k, v in data.items():
                    yield (k, str(v))
        except:
            pass
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_json_data; """
        sql """
        CREATE TABLE test_json_data (
            id INT,
            json_content STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_json_data VALUES 
        (1, '{"name":"Alice","age":25,"city":"NYC"}'),
        (2, '{"name":"Bob","age":30}');
        """
        
        qt_json_extract """
            SELECT id, tmp.field, tmp.v
            FROM test_json_data
            LATERAL VIEW udtf_json_extract(json_content) tmp AS field, v
            ORDER BY id, tmp.field;
        """
        
        // Test P1.6: Complex nested STRUCT
        sql """ DROP FUNCTION IF EXISTS udtf_complex_struct(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_complex_struct(STRING)
        RETURNS ARRAY<STRUCT<user_id:INT, user_name:STRING, address_city:STRING, address_zip:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_complex_struct",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_complex_struct(data):
    '''Process complex struct (id:name:city:zip)'''
    if data:
        parts = data.split(':')
        if len(parts) == 4:
            try:
                yield (int(parts[0]), parts[1], parts[2], parts[3])
            except ValueError:
                pass
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_complex_struct; """
        sql """
        CREATE TABLE test_complex_struct (
            id INT,
            user_data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_complex_struct VALUES 
        (1, '101:Alice:NYC:10001'),
        (2, '102:Bob:LA:90001');
        """
        
        qt_complex_struct """
            SELECT id, tmp.user_id, tmp.user_name, tmp.address_city, tmp.address_zip
            FROM test_complex_struct
            LATERAL VIEW udtf_complex_struct(user_data) tmp AS user_id, user_name, address_city, address_zip
            ORDER BY id;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS udtf_tinyint(TINYINT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_smallint(SMALLINT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_bigint(BIGINT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_float(FLOAT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_double(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS udtf_boolean(BOOLEAN);")
        try_sql("DROP FUNCTION IF EXISTS udtf_string(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_date(DATE);")
        try_sql("DROP FUNCTION IF EXISTS udtf_datetime(DATETIME);")
        try_sql("DROP FUNCTION IF EXISTS udtf_array_int(ARRAY<INT>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_array_string(ARRAY<STRING>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_struct(STRUCT<name:STRING, age:INT>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_multi_types(INT, STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_decimal(DECIMAL(10,2));")
        try_sql("DROP FUNCTION IF EXISTS udtf_map_processor(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_nested_array(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_array_of_structs(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_struct_with_array(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_json_extract(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_complex_struct(STRING);")
        try_sql("DROP TABLE IF EXISTS test_tinyint;")
        try_sql("DROP TABLE IF EXISTS test_smallint;")
        try_sql("DROP TABLE IF EXISTS test_bigint;")
        try_sql("DROP TABLE IF EXISTS test_float;")
        try_sql("DROP TABLE IF EXISTS test_double;")
        try_sql("DROP TABLE IF EXISTS test_boolean;")
        try_sql("DROP TABLE IF EXISTS test_string;")
        try_sql("DROP TABLE IF EXISTS test_date;")
        try_sql("DROP TABLE IF EXISTS test_datetime;")
        try_sql("DROP TABLE IF EXISTS test_array_int;")
        try_sql("DROP TABLE IF EXISTS test_array_string;")
        try_sql("DROP TABLE IF EXISTS test_struct;")
        try_sql("DROP TABLE IF EXISTS test_multi_types;")
        try_sql("DROP TABLE IF EXISTS test_decimal;")
        try_sql("DROP TABLE IF EXISTS test_map_like;")
        try_sql("DROP TABLE IF EXISTS test_nested_array;")
        try_sql("DROP TABLE IF EXISTS test_array_structs;")
        try_sql("DROP TABLE IF EXISTS test_struct_array;")
        try_sql("DROP TABLE IF EXISTS test_json_data;")
        try_sql("DROP TABLE IF EXISTS test_complex_struct;")
    }
}
