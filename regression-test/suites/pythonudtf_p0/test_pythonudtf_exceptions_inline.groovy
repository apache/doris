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

suite("test_pythonudtf_exceptions_inline") {
    // Test Python UDTF Exception Handling
    // Coverage: Runtime errors, type errors, logic errors, edge cases
    
    def runtime_version = "3.8.10"
    
    try {
        // ========================================
        // Section 1: Arithmetic Exceptions
        // ========================================
        
        // Test 1.1: Division by Zero - Handled
        sql """ DROP FUNCTION IF EXISTS udtf_safe_divide(INT, INT); """
        sql """
        CREATE TABLES FUNCTION udtf_safe_divide(INT, INT)
        RETURNS ARRAY<STRUCT<numerator:INT, denominator:INT, result:DOUBLE, error_msg:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "safe_divide",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def safe_divide(a, b):
    '''Safe division with error handling'''
    try:
        if b == 0:
            yield (a, b, None, 'division_by_zero')
        else:
            result = a / b
            yield (a, b, result, 'success')
    except Exception as e:
        yield (a, b, None, f'error_{type(e).__name__}')
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_division; """
        sql """
        CREATE TABLE test_division (
            id INT,
            num INT,
            denom INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_division VALUES 
        (1, 10, 2),
        (2, 10, 0),
        (3, 0, 5),
        (4, -8, 4);
        """
        
        qt_safe_divide """
            SELECT id, tmp.numerator, tmp.denominator, tmp.result, tmp.error_msg
            FROM test_division
            LATERAL VIEW udtf_safe_divide(num, denom) tmp AS numerator, denominator, result, error_msg
            ORDER BY id;
        """
        
        // Test 1.2: Integer Overflow Detection
        sql """ DROP FUNCTION IF EXISTS udtf_overflow_check(BIGINT); """
        sql """
        CREATE TABLES FUNCTION udtf_overflow_check(BIGINT)
        RETURNS ARRAY<STRUCT<original:BIGINT, doubled:BIGINT, status:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "check_overflow",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def check_overflow(value):
    '''Check for potential overflow in operations'''
    if value is None:
        yield (None, None, 'null_input')
    else:
        # BIGINT range: -2^63 to 2^63-1
        MAX_BIGINT = 9223372036854775807
        MIN_BIGINT = -9223372036854775808
        
        doubled = value * 2
        
        # Check if doubled value is within safe range
        if doubled > MAX_BIGINT or doubled < MIN_BIGINT:
            yield (value, None, 'would_overflow')
        else:
            yield (value, doubled, 'safe')
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_overflow; """
        sql """
        CREATE TABLE test_overflow (
            id INT,
            big_val BIGINT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_overflow VALUES 
        (1, 100),
        (2, 5000000000000),
        (3, -5000000000000),
        (4, NULL);
        """
        
        qt_overflow_check """
            SELECT id, tmp.original, tmp.doubled, tmp.status
            FROM test_overflow
            LATERAL VIEW udtf_overflow_check(big_val) tmp AS original, doubled, status
            ORDER BY id;
        """
        
        // ========================================
        // Section 2: Type Conversion Errors
        // ========================================
        
        // Test 2.1: String to Number Conversion
        sql """ DROP FUNCTION IF EXISTS udtf_parse_number(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_parse_number(STRING)
        RETURNS ARRAY<STRUCT<input:STRING, parsed:DOUBLE, is_valid:BOOLEAN>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "parse_number",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def parse_number(text):
    '''Parse string to number with error handling'''
    if text is None:
        yield (None, None, False)
    else:
        try:
            num = float(text)
            yield (text, num, True)
        except ValueError:
            yield (text, None, False)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_parse; """
        sql """
        CREATE TABLE test_parse (
            id INT,
            text STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_parse VALUES 
        (1, '123'),
        (2, '45.67'),
        (3, 'abc'),
        (4, '12.34.56'),
        (5, ''),
        (6, NULL);
        """
        
        qt_parse_number """
            SELECT id, tmp.input, tmp.parsed, tmp.is_valid
            FROM test_parse
            LATERAL VIEW udtf_parse_number(text) tmp AS input, parsed, is_valid
            ORDER BY id;
        """
        
        // Test 2.2: Type Mismatch Handling
        sql """ DROP FUNCTION IF EXISTS udtf_type_check(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_type_check(STRING)
        RETURNS ARRAY<STRUCT<value:STRING, type_name:STRING, length:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "check_type",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def check_type(value):
    '''Check and report value type'''
    type_name = type(value).__name__
    
    if value is None:
        yield (None, 'NoneType', 0)
    elif isinstance(value, str):
        yield (value, type_name, len(value))
    else:
        # Unexpected type - convert to string
        yield (str(value), type_name, len(str(value)))
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_types; """
        sql """
        CREATE TABLE test_types (
            id INT,
            val STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_types VALUES 
        (1, 'hello'),
        (2, ''),
        (3, '12345'),
        (4, NULL);
        """
        
        qt_type_check """
            SELECT id, tmp.value, tmp.type_name, tmp.length
            FROM test_types
            LATERAL VIEW udtf_type_check(val) tmp AS value, type_name, length
            ORDER BY id;
        """
        
        // ========================================
        // Section 3: Collection/Array Errors
        // ========================================
        
        // Test 3.1: Array Index Out of Bounds
        sql """ DROP FUNCTION IF EXISTS udtf_safe_index(ARRAY<INT>, INT); """
        sql """
        CREATE TABLES FUNCTION udtf_safe_index(ARRAY<INT>, INT)
        RETURNS ARRAY<STRUCT<arr_size:INT, target_pos:INT, value:INT, status:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "safe_array_access",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def safe_array_access(arr, position):
    '''Safe array element access'''
    if arr is None:
        yield (0, position, None, 'null_array')
    elif len(arr) == 0:
        yield (0, position, None, 'empty_array')
    elif position < 0 or position >= len(arr):
        yield (len(arr), position, None, 'out_of_bounds')
    else:
        yield (len(arr), position, arr[position], 'success')
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_array_access; """
        sql """
        CREATE TABLE test_array_access (
            id INT,
            arr ARRAY<INT>,
            pos INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_array_access VALUES 
        (1, [10, 20, 30], 1),
        (2, [10, 20, 30], 5),
        (3, [10, 20, 30], -1),
        (4, [], 0),
        (5, NULL, 0);
        """
        
        qt_safe_index """
            SELECT id, tmp.arr_size, tmp.target_pos, tmp.value, tmp.status
            FROM test_array_access
            LATERAL VIEW udtf_safe_index(arr, pos) tmp AS arr_size, target_pos, value, status
            ORDER BY id;
        """
        
        // Test 3.2: Empty Collection Handling
        sql """ DROP FUNCTION IF EXISTS udtf_collection_stats(ARRAY<INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_collection_stats(ARRAY<INT>)
        RETURNS ARRAY<STRUCT<count:INT, total:BIGINT, avg:DOUBLE, status:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "compute_stats",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def compute_stats(arr):
    '''Compute statistics with empty array handling'''
    if arr is None:
        yield (0, 0, 0.0, 'null_array')
    elif len(arr) == 0:
        yield (0, 0, 0.0, 'empty_array')
    else:
        count = len(arr)
        total = sum(x for x in arr if x is not None)
        avg = total / count if count > 0 else 0.0
        yield (count, total, avg, 'computed')
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_collection_stats; """
        sql """
        CREATE TABLE test_collection_stats (
            id INT,
            data ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_collection_stats VALUES 
        (1, [1, 2, 3, 4, 5]),
        (2, []),
        (3, NULL),
        (4, [10, 20]);
        """
        
        qt_collection_stats """
            SELECT id, tmp.count, tmp.total, tmp.avg, tmp.status
            FROM test_collection_stats
            LATERAL VIEW udtf_collection_stats(data) tmp AS count, total, avg, status
            ORDER BY id;
        """
        
        // ========================================
        // Section 4: Dictionary/STRUCT Errors
        // ========================================
        
        // Test 4.1: Missing Dictionary Keys
        sql """ DROP FUNCTION IF EXISTS udtf_safe_struct_access(STRUCT<name:STRING, age:INT>); """
        sql """
        CREATE TABLES FUNCTION udtf_safe_struct_access(STRUCT<name:STRING, age:INT>)
        RETURNS ARRAY<STRUCT<has_name:BOOLEAN, has_age:BOOLEAN, name_val:STRING, age_val:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "access_struct_fields",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def access_struct_fields(person):
    '''Safe STRUCT field access'''
    if person is None:
        yield (False, False, None, None)
    else:
        # Use .get() to safely access dictionary keys
        name = person.get('name')
        age = person.get('age')
        
        has_name = name is not None
        has_age = age is not None
        
        yield (has_name, has_age, name, age)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_struct_access; """
        sql """
        CREATE TABLE test_struct_access (
            id INT,
            person STRUCT<name:STRING, age:INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_struct_access VALUES 
        (1, named_struct('name', 'Alice', 'age', 30)),
        (2, named_struct('name', 'Bob', 'age', NULL)),
        (3, named_struct('name', NULL, 'age', 25)),
        (4, NULL);
        """
        
        qt_safe_struct_access """
            SELECT id, tmp.has_name, tmp.has_age, tmp.name_val, tmp.age_val
            FROM test_struct_access
            LATERAL VIEW udtf_safe_struct_access(person) tmp AS has_name, has_age, name_val, age_val
            ORDER BY id;
        """
        
        // ========================================
        // Section 5: String Processing Errors
        // ========================================
        
        // Test 5.1: Invalid String Operations
        sql """ DROP FUNCTION IF EXISTS udtf_string_slice(STRING, INT, INT); """
        sql """
        CREATE TABLES FUNCTION udtf_string_slice(STRING, INT, INT)
        RETURNS ARRAY<STRUCT<original:STRING, start_pos:INT, end_pos:INT, slice:STRING, status:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "slice_string",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def slice_string(text, start, end):
    '''Safe string slicing'''
    if text is None:
        yield (None, start, end, None, 'null_string')
    elif start is None or end is None:
        yield (text, start, end, None, 'null_index')
    else:
        length = len(text)
        
        # Clamp indices to valid range
        safe_start = max(0, min(start, length))
        safe_end = max(0, min(end, length))
        
        if safe_start >= safe_end:
            yield (text, start, end, '', 'empty_slice')
        else:
            result = text[safe_start:safe_end]
            yield (text, start, end, result, 'success')
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_string_slice; """
        sql """
        CREATE TABLE test_string_slice (
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
        INSERT INTO test_string_slice VALUES 
        (1, 'hello world', 0, 5),
        (2, 'hello world', 6, 11),
        (3, 'hello world', 20, 30),
        (4, 'hello world', 5, 2),
        (5, NULL, 0, 5);
        """
        
        qt_string_slice """
            SELECT id, tmp.original, tmp.start_pos, tmp.end_pos, tmp.slice, tmp.status
            FROM test_string_slice
            LATERAL VIEW udtf_string_slice(text, start_pos, end_pos) tmp AS original, start_pos, end_pos, slice, status
            ORDER BY id;
        """
        
        // Test 5.2: Encoding/Decoding Errors
        sql """ DROP FUNCTION IF EXISTS udtf_check_encoding(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_check_encoding(STRING)
        RETURNS ARRAY<STRUCT<text:STRING, byte_length:INT, char_length:INT, has_unicode:BOOLEAN>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "check_text_encoding",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def check_text_encoding(text):
    '''Check string encoding properties'''
    if text is None:
        yield (None, 0, 0, False)
    else:
        byte_len = len(text.encode('utf-8'))
        char_len = len(text)
        has_unicode = byte_len > char_len
        
        yield (text, byte_len, char_len, has_unicode)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_encoding; """
        sql """
        CREATE TABLE test_encoding (
            id INT,
            text STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_encoding VALUES 
        (1, 'hello'),
        (2, '你好世界'),
        (3, 'café'),
        (4, ''),
        (5, NULL);
        """
        
        qt_check_encoding """
            SELECT id, tmp.text, tmp.byte_length, tmp.char_length, tmp.has_unicode
            FROM test_encoding
            LATERAL VIEW udtf_check_encoding(text) tmp AS text, byte_length, char_length, has_unicode
            ORDER BY id;
        """
        
        // ========================================
        // Section 6: Logic and State Errors
        // ========================================
        
        // Test 6.1: Conditional Logic Errors
        sql """ DROP FUNCTION IF EXISTS udtf_conditional_process(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_conditional_process(INT)
        RETURNS ARRAY<STRUCT<input:INT, category:STRING, result:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_conditional",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_conditional(value):
    '''Process value based on multiple conditions'''
    if value is None:
        yield (None, 'null', 0)
    elif value < 0:
        # For negative: take absolute value
        yield (value, 'negative', abs(value))
    elif value == 0:
        # Zero case: return 1
        yield (value, 'zero', 1)
    elif value > 0 and value <= 100:
        # Small positive: double it
        yield (value, 'small_positive', value * 2)
    else:
        # Large positive: return as-is
        yield (value, 'large_positive', value)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_conditional; """
        sql """
        CREATE TABLE test_conditional (
            id INT,
            val INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_conditional VALUES 
        (1, -10),
        (2, 0),
        (3, 50),
        (4, 200),
        (5, NULL);
        """
        
        qt_conditional_process """
            SELECT id, tmp.input, tmp.category, tmp.result
            FROM test_conditional
            LATERAL VIEW udtf_conditional_process(val) tmp AS input, category, result
            ORDER BY id;
        """
        
        // Test 6.2: Yield Control - No Output Case
        sql """ DROP FUNCTION IF EXISTS udtf_filter_yield(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_filter_yield(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "conditional_yield",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def conditional_yield(value):
    '''Only yield for even positive numbers'''
    if value is not None and value > 0 and value % 2 == 0:
        yield (value,)
    # For other cases, yield nothing (filter out)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_filter_yield; """
        sql """
        CREATE TABLE test_filter_yield (
            id INT,
            val INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_filter_yield VALUES 
        (1, 10),
        (2, 15),
        (3, -4),
        (4, 0),
        (5, 22),
        (6, NULL);
        """
        
        qt_filter_yield """
            SELECT id, tmp.value
            FROM test_filter_yield
            LATERAL VIEW udtf_filter_yield(val) tmp AS value
            ORDER BY id;
        """
        
        // ========================================
        // Section 7: Edge Cases in Computation
        // ========================================
        
        // Test 7.1: Very Small and Very Large Numbers
        sql """ DROP FUNCTION IF EXISTS udtf_number_range(DOUBLE); """
        sql """
        CREATE TABLES FUNCTION udtf_number_range(DOUBLE)
        RETURNS ARRAY<STRUCT<value:DOUBLE, magnitude:STRING, is_finite:BOOLEAN>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "classify_number_range",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
import math

def classify_number_range(value):
    '''Classify number by magnitude'''
    if value is None:
        yield (None, 'null', True)
    elif math.isnan(value):
        yield (value, 'nan', False)
    elif math.isinf(value):
        yield (value, 'infinity', False)
    elif value == 0.0:
        yield (value, 'zero', True)
    elif abs(value) < 1e-100:
        yield (value, 'extremely_small', True)
    elif abs(value) > 1e100:
        yield (value, 'extremely_large', True)
    elif abs(value) < 1.0:
        yield (value, 'small', True)
    else:
        yield (value, 'normal', True)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_number_range; """
        sql """
        CREATE TABLE test_number_range (
            id INT,
            val DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_number_range VALUES 
        (1, 0.0),
        (2, 1e-150),
        (3, 1e150),
        (4, 0.5),
        (5, 123.456),
        (6, NULL);
        """
        
        qt_number_range """
            SELECT id, tmp.value, tmp.magnitude, tmp.is_finite
            FROM test_number_range
            LATERAL VIEW udtf_number_range(val) tmp AS value, magnitude, is_finite
            ORDER BY id;
        """
        
        // Test 7.2: Date/Time Edge Cases
        sql """ DROP FUNCTION IF EXISTS udtf_date_validation(DATE); """
        sql """
        CREATE TABLES FUNCTION udtf_date_validation(DATE)
        RETURNS ARRAY<STRUCT<input_date:DATE, year:INT, is_leap_year:BOOLEAN, status:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "validate_date",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def validate_date(dt):
    '''Validate and classify dates'''
    if dt is None:
        yield (None, 0, False, 'null_date')
    else:
        year = dt.year
        
        # Check if leap year
        is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
        
        # Classify date
        if year < 1900:
            status = 'very_old'
        elif year > 2100:
            status = 'far_future'
        else:
            status = 'normal'
        
        yield (dt, year, is_leap, status)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_date_validation; """
        sql """
        CREATE TABLE test_date_validation (
            id INT,
            dt DATE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_date_validation VALUES 
        (1, '2024-01-01'),
        (2, '2000-02-29'),
        (3, '1970-01-01'),
        (4, '9999-12-31'),
        (5, NULL);
        """
        
        qt_date_validation """
            SELECT id, tmp.input_date, tmp.year, tmp.is_leap_year, tmp.status
            FROM test_date_validation
            LATERAL VIEW udtf_date_validation(dt) tmp AS input_date, year, is_leap_year, status
            ORDER BY id;
        """
        
    } finally {
        // Cleanup functions
        try_sql("DROP FUNCTION IF EXISTS udtf_safe_divide(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_overflow_check(BIGINT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_parse_number(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_type_check(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_safe_index(ARRAY<INT>, INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_collection_stats(ARRAY<INT>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_safe_struct_access(STRUCT<name:STRING, age:INT>);")
        try_sql("DROP FUNCTION IF EXISTS udtf_string_slice(STRING, INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_check_encoding(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_conditional_process(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_filter_yield(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_number_range(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS udtf_date_validation(DATE);")
        
        // Cleanup tables
        try_sql("DROP TABLE IF EXISTS test_division;")
        try_sql("DROP TABLE IF EXISTS test_overflow;")
        try_sql("DROP TABLE IF EXISTS test_parse;")
        try_sql("DROP TABLE IF EXISTS test_types;")
        try_sql("DROP TABLE IF EXISTS test_array_access;")
        try_sql("DROP TABLE IF EXISTS test_collection_stats;")
        try_sql("DROP TABLE IF EXISTS test_struct_access;")
        try_sql("DROP TABLE IF EXISTS test_string_slice;")
        try_sql("DROP TABLE IF EXISTS test_encoding;")
        try_sql("DROP TABLE IF EXISTS test_conditional;")
        try_sql("DROP TABLE IF EXISTS test_filter_yield;")
        try_sql("DROP TABLE IF EXISTS test_number_range;")
        try_sql("DROP TABLE IF EXISTS test_date_validation;")
    }
}
