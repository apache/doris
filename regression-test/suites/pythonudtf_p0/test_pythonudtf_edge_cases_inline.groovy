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

suite("test_pythonudtf_edge_cases_inline") {
    // Test Python UDTF Edge Cases and Boundary Conditions
    // Coverage: NULL handling, extreme cases, special values
    
    def runtime_version = "3.8.10"
    
    try {
        // ========================================
        // Section 1: NULL Value Handling
        // ========================================
        
        // Test 1.1: NULL Integer Input
        sql """ DROP FUNCTION IF EXISTS udtf_null_int(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_null_int(INT)
        RETURNS ARRAY<STRUCT<input_value:INT, is_null:BOOLEAN, result:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "handle_null_int",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def handle_null_int(value):
    '''Handle NULL integer values'''
    if value is None:
        yield (None, True, -1)  # NULL indicator
    else:
        yield (value, False, value * 2)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_null_int; """
        sql """
        CREATE TABLE test_null_int (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_null_int VALUES (1, NULL), (2, 0), (3, 10), (4, NULL);
        """
        
        qt_null_int """
            SELECT id, tmp.input_value, tmp.is_null, tmp.result
            FROM test_null_int
            LATERAL VIEW udtf_null_int(value) tmp AS input_value, is_null, result
            ORDER BY id;
        """
        
        // Test 1.2: Empty String vs NULL String
        sql """ DROP FUNCTION IF EXISTS udtf_null_string(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_null_string(STRING)
        RETURNS ARRAY<STRUCT<value_type:STRING, length:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "handle_null_string",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def handle_null_string(value):
    '''Distinguish NULL from empty string'''
    if value is None:
        yield ('NULL', -1)
    elif value == '':
        yield ('EMPTY', 0)
    else:
        yield ('NORMAL', len(value))
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_null_string; """
        sql """
        CREATE TABLE test_null_string (
            id INT,
            value STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_null_string VALUES (1, NULL), (2, ''), (3, 'hello'), (4, NULL);
        """
        
        qt_null_string """
            SELECT id, tmp.value_type, tmp.length
            FROM test_null_string
            LATERAL VIEW udtf_null_string(value) tmp AS value_type, length
            ORDER BY id;
        """
        
        // Test 1.3: Empty Array
        sql """ DROP FUNCTION IF EXISTS udtf_empty_array(ARRAY<INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_empty_array(ARRAY<INT>)
        RETURNS ARRAY<STRUCT<array_type:STRING, size:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "handle_empty_array",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def handle_empty_array(arr):
    '''Handle NULL vs empty array'''
    if arr is None:
        yield ('NULL', -1)
    elif len(arr) == 0:
        yield ('EMPTY', 0)
    else:
        yield ('NORMAL', len(arr))
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_empty_array; """
        sql """
        CREATE TABLE test_empty_array (
            id INT,
            value ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_empty_array VALUES 
        (1, NULL), 
        (2, []), 
        (3, [1, 2, 3]);
        """
        
        qt_empty_array """
            SELECT id, tmp.array_type, tmp.size
            FROM test_empty_array
            LATERAL VIEW udtf_empty_array(value) tmp AS array_type, size
            ORDER BY id;
        """
        
        // Test 1.4: NULL Fields in STRUCT
        sql """ DROP FUNCTION IF EXISTS udtf_null_struct(STRUCT<name:STRING, age:INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_null_struct(STRUCT<name:STRING, age:INT>)
        RETURNS ARRAY<STRUCT<has_name:BOOLEAN, has_age:BOOLEAN, summary:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "handle_null_struct",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def handle_null_struct(person):
    '''Handle NULL fields in STRUCT'''
    if person is None:
        yield (False, False, 'struct_is_null')
    else:
        name = person.get('name')
        age = person.get('age')
        has_name = name is not None
        has_age = age is not None
        
        if has_name and has_age:
            summary = f"{name}_{age}"
        elif has_name:
            summary = f"{name}_no_age"
        elif has_age:
            summary = f"no_name_{age}"
        else:
            summary = "all_fields_null"
        
        yield (has_name, has_age, summary)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_null_struct; """
        sql """
        CREATE TABLE test_null_struct (
            id INT,
            person STRUCT<name:STRING, age:INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_null_struct VALUES 
        (1, named_struct('name', 'Alice', 'age', 25)),
        (2, named_struct('name', 'Bob', 'age', NULL)),
        (3, named_struct('name', NULL, 'age', 30)),
        (4, named_struct('name', NULL, 'age', NULL));
        """
        
        qt_null_struct """
            SELECT id, tmp.has_name, tmp.has_age, tmp.summary
            FROM test_null_struct
            LATERAL VIEW udtf_null_struct(person) tmp AS has_name, has_age, summary
            ORDER BY id;
        """
        
        // ========================================
        // Section 2: Extreme Cases
        // ========================================
        
        // Test 2.1: Empty Table (0 rows)
        sql """ DROP FUNCTION IF EXISTS udtf_empty_table(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_empty_table(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_empty_table",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_empty_table(value):
    '''This should never be called for empty table'''
    if value is not None:
        yield (value * 2,)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_empty_table; """
        sql """
        CREATE TABLE test_empty_table (
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
            FROM test_empty_table
            LATERAL VIEW udtf_empty_table(value) tmp AS value;
        """
        
        // Test 2.2: Single Row Table
        sql """ DROP FUNCTION IF EXISTS udtf_single_row(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_single_row(INT)
        RETURNS ARRAY<STRUCT<original:INT, generated:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_single_row",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_single_row(value):
    '''Process single row input'''
    if value is not None:
        for i in range(3):
            yield (value, value + i)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_single_row; """
        sql """
        CREATE TABLE test_single_row (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_single_row VALUES (1, 100);
        """
        
        qt_single_row """
            SELECT tmp.original, tmp.generated
            FROM test_single_row
            LATERAL VIEW udtf_single_row(value) tmp AS original, generated
            ORDER BY tmp.generated;
        """
        
        // Test 2.3: Large Field - Long String
        sql """ DROP FUNCTION IF EXISTS udtf_long_string(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_long_string(STRING)
        RETURNS ARRAY<STRUCT<length:INT, first_10:STRING, last_10:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_long_string",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_long_string(text):
    '''Process very long string'''
    if text is not None:
        length = len(text)
        first_10 = text[:10] if length >= 10 else text
        last_10 = text[-10:] if length >= 10 else text
        yield (length, first_10, last_10)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_long_string; """
        sql """
        CREATE TABLE test_long_string (
            id INT,
            value STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_long_string VALUES 
        (1, REPEAT('A', 1000)),
        (2, REPEAT('B', 5000));
        """
        
        qt_long_string """
            SELECT id, tmp.length, tmp.first_10, tmp.last_10
            FROM test_long_string
            LATERAL VIEW udtf_long_string(value) tmp AS length, first_10, last_10
            ORDER BY id;
        """
        
        // Test 2.4: Large Array
        sql """ DROP FUNCTION IF EXISTS udtf_large_array(ARRAY<INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_large_array(ARRAY<INT>)
        RETURNS ARRAY<STRUCT<total_elements:INT, sum_value:BIGINT, first_elem:INT, last_elem:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_large_array",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_large_array(arr):
    '''Process large array - compute statistics instead of exploding'''
    if arr is not None and len(arr) > 0:
        total = len(arr)
        total_sum = sum(x for x in arr if x is not None)
        first = arr[0] if len(arr) > 0 else None
        last = arr[-1] if len(arr) > 0 else None
        yield (total, total_sum, first, last)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_large_array; """
        sql """
        CREATE TABLE test_large_array (
            id INT,
            value ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_large_array VALUES 
        (1, ARRAY_REPEAT(1, 100)),
        (2, ARRAY_REPEAT(5, 50));
        """
        
        qt_large_array """
            SELECT id, tmp.total_elements, tmp.sum_value, tmp.first_elem, tmp.last_elem
            FROM test_large_array
            LATERAL VIEW udtf_large_array(value) tmp AS total_elements, sum_value, first_elem, last_elem
            ORDER BY id;
        """
        
        // Test 2.5: Output Explosion - Controlled
        sql """ DROP FUNCTION IF EXISTS udtf_output_explosion(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_output_explosion(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "output_explosion",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def output_explosion(n):
    '''Generate many outputs from single input (controlled explosion)'''
    if n is not None and 0 < n <= 100:  # Safety limit
        for i in range(n):
            yield (i,)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_output_explosion; """
        sql """
        CREATE TABLE test_output_explosion (
            id INT,
            multiplier INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_output_explosion VALUES (1, 10), (2, 50);
        """
        
        qt_output_explosion """
            SELECT id, COUNT(*) as output_count, MIN(tmp.value) as min_val, MAX(tmp.value) as max_val
            FROM test_output_explosion
            LATERAL VIEW udtf_output_explosion(multiplier) tmp AS value
            GROUP BY id
            ORDER BY id;
        """
        
        // ========================================
        // Section 3: Special Values
        // ========================================
        
        // Test 3.1: Special Numeric Values (0, negative, boundary)
        sql """ DROP FUNCTION IF EXISTS udtf_special_numbers(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_special_numbers(INT)
        RETURNS ARRAY<STRUCT<original:INT, category:STRING, is_boundary:BOOLEAN>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_special_numbers",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_special_numbers(value):
    '''Categorize special numeric values'''
    INT_MIN = -2147483648
    INT_MAX = 2147483647
    
    if value is None:
        yield (None, 'NULL', False)
    elif value == 0:
        yield (value, 'ZERO', False)
    elif value == INT_MIN or value == INT_MAX:
        category = 'POSITIVE' if value > 0 else 'NEGATIVE'
        yield (value, category, True)  # is_boundary = True
    elif value > 0:
        yield (value, 'POSITIVE', False)
    else:
        yield (value, 'NEGATIVE', False)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_special_numbers; """
        sql """
        CREATE TABLE test_special_numbers (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_special_numbers VALUES 
        (1, -2147483648),  -- INT MIN
        (2, -1),
        (3, 0),
        (4, 1),
        (5, 2147483647),   -- INT MAX
        (6, NULL);
        """
        
        qt_special_numbers """
            SELECT id, tmp.original, tmp.category, tmp.is_boundary
            FROM test_special_numbers
            LATERAL VIEW udtf_special_numbers(value) tmp AS original, category, is_boundary
            ORDER BY id;
        """
        
        // Test 3.2: Special Double Values (infinity, very small numbers)
        sql """ DROP FUNCTION IF EXISTS udtf_special_doubles(DOUBLE); """
        sql """
        CREATE TABLES FUNCTION udtf_special_doubles(DOUBLE)
        RETURNS ARRAY<STRUCT<original:DOUBLE, classification:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_special_doubles",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
import math

def process_special_doubles(value):
    '''Classify special double values'''
    if value is None:
        yield (None, 'NULL')
    elif math.isnan(value):
        yield (value, 'NAN')
    elif math.isinf(value):
        if value > 0:
            yield (value, 'POSITIVE_INF')
        else:
            yield (value, 'NEGATIVE_INF')
    elif value == 0.0:
        yield (value, 'ZERO')
    elif abs(value) < 1e-10:
        yield (value, 'VERY_SMALL')
    elif abs(value) > 1e10:
        yield (value, 'VERY_LARGE')
    else:
        yield (value, 'NORMAL')
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_special_doubles; """
        sql """
        CREATE TABLE test_special_doubles (
            id INT,
            value DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_special_doubles VALUES 
        (1, 0.0),
        (2, 1e-15),
        (3, 1e15),
        (4, -1e15),
        (5, 3.14159);
        """
        
        qt_special_doubles """
            SELECT id, tmp.original, tmp.classification
            FROM test_special_doubles
            LATERAL VIEW udtf_special_doubles(value) tmp AS original, classification
            ORDER BY id;
        """
        
        // Test 3.3: Special String Values (special characters, Unicode)
        sql """ DROP FUNCTION IF EXISTS udtf_special_strings(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_special_strings(STRING)
        RETURNS ARRAY<STRUCT<length:INT, has_special:BOOLEAN, description:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_special_strings",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_special_strings(text):
    '''Process strings with special characters'''
    if text is None:
        yield (0, False, 'NULL')
    elif text == '':
        yield (0, False, 'EMPTY')
    else:
        length = len(text)
        has_special = any(ord(c) > 127 for c in text)
        
        if has_special:
            desc = 'HAS_UNICODE'
        elif any(c in text for c in ['\\n', '\\t', '\\r']):
            desc = 'HAS_WHITESPACE'
        elif any(c in text for c in ['!', '@', '#', '\$', '%']):
            desc = 'HAS_SYMBOLS'
        else:
            desc = 'NORMAL'
        
        yield (length, has_special, desc)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_special_strings; """
        sql """
        CREATE TABLE test_special_strings (
            id INT,
            value STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_special_strings VALUES 
        (1, 'normal text'),
        (2, 'hello@world.com'),
        (3, 'tab\\there'),
        (4, '你好世界'),
        (5, '');
        """
        
        qt_special_strings """
            SELECT id, tmp.length, tmp.has_special, tmp.description
            FROM test_special_strings
            LATERAL VIEW udtf_special_strings(value) tmp AS length, has_special, description
            ORDER BY id;
        """
        
        // Test 3.4: Boundary Dates
        sql """ DROP FUNCTION IF EXISTS udtf_boundary_dates(DATE); """
        sql """
        CREATE TABLES FUNCTION udtf_boundary_dates(DATE)
        RETURNS ARRAY<STRUCT<original:DATE, year:INT, is_boundary:BOOLEAN>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_boundary_dates",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_boundary_dates(dt):
    '''Process boundary date values'''
    if dt is None:
        yield (None, 0, False)
    else:
        year = dt.year
        # Check if it's a boundary date
        is_boundary = year in [1970, 9999] or (year == 1970 and dt.month == 1 and dt.day == 1)
        yield (dt, year, is_boundary)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_boundary_dates; """
        sql """
        CREATE TABLE test_boundary_dates (
            id INT,
            value DATE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_boundary_dates VALUES 
        (1, '1970-01-01'),
        (2, '2024-06-15'),
        (3, '9999-12-31');
        """
        
        qt_boundary_dates """
            SELECT id, tmp.original, tmp.year, tmp.is_boundary
            FROM test_boundary_dates
            LATERAL VIEW udtf_boundary_dates(value) tmp AS original, year, is_boundary
            ORDER BY id;
        """
        
    } finally {
        // Cleanup functions
        try_sql("DROP FUNCTION IF EXISTS udtf_null_int(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_null_string(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_empty_array(ARRAY<INT>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_null_struct(STRUCT<name:STRING, age:INT>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_empty_table(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_single_row(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_long_string(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_large_array(ARRAY<INT>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_output_explosion(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_special_numbers(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_special_doubles(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS udtf_special_strings(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_boundary_dates(DATE);")
        
        // Cleanup tables
        try_sql("DROP TABLE IF EXISTS test_null_int;")
        try_sql("DROP TABLE IF EXISTS test_null_string;")
        try_sql("DROP TABLE IF EXISTS test_empty_array;")
        try_sql("DROP TABLE IF EXISTS test_null_struct;")
        try_sql("DROP TABLE IF EXISTS test_empty_table;")
        try_sql("DROP TABLE IF EXISTS test_single_row;")
        try_sql("DROP TABLE IF EXISTS test_long_string;")
        try_sql("DROP TABLE IF EXISTS test_large_array;")
        try_sql("DROP TABLE IF EXISTS test_output_explosion;")
        try_sql("DROP TABLE IF EXISTS test_special_numbers;")
        try_sql("DROP TABLE IF EXISTS test_special_doubles;")
        try_sql("DROP TABLE IF EXISTS test_special_strings;")
        try_sql("DROP TABLE IF EXISTS test_boundary_dates;")
    }
}
