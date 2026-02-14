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

suite("test_pythonudtf_basic_inline") {
    // Basic Python UDTF tests following Snowflake syntax
    // UDTF (User-Defined Table Function) returns table (multiple rows) from scalar/table input
    
    def runtime_version = "3.8.10"
    
    try {
        // ========================================
        // Test 1: Simple String Split UDTF
        // Input: Single string
        // Output: Multiple rows (one per split part)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_split_string(STRING); """
        sql """
        CREATE TABLES FUNCTION py_split_string(STRING)
        RETURNS ARRAY<STRING>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "split_string_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def split_string_udtf(input_str):
    '''Split comma-separated string into rows'''
    if input_str:
        parts = input_str.split(',')
        for part in parts:
            yield (part.strip(),)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS temp_input; """
        sql """
        CREATE TABLE temp_input (
            id INT,
            input_str STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO temp_input VALUES (1, 'apple,banana,cherry');
        """
        
        qt_split_string """
            SELECT part 
            FROM temp_input 
            LATERAL VIEW py_split_string(input_str) tmp AS part;
        """
        
        // ========================================
        // Test 2: Generate Series UDTF
        // Input: start, end integers
        // Output: Sequence of integers
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_generate_series(INT, INT); """
        sql """
        CREATE TABLES FUNCTION py_generate_series(INT, INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "generate_series_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def generate_series_udtf(start, end):
    '''Generate integer series from start to end'''
    if start is not None and end is not None:
        for i in range(start, end + 1):
            yield (i,)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS temp_series; """
        sql """
        CREATE TABLE temp_series (
            id INT,
            start_val INT,
            end_val INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO temp_series VALUES (1, 1, 5), (2, 10, 12);
        """
        
        qt_generate_series """
            SELECT tmp.value 
            FROM temp_series 
            LATERAL VIEW py_generate_series(start_val, end_val) tmp AS value;
        """
        
        qt_generate_series_multiple """
            SELECT tmp.value 
            FROM temp_series 
            LATERAL VIEW py_generate_series(start_val, end_val) tmp AS value
            ORDER BY tmp.value;
        """
        
        // ========================================
        // Test 3: Running Sum UDTF (without state management)
        // Note: Function-based UDTFs cannot maintain state across calls
        // Each row is processed independently
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_running_sum(INT); """
        sql """
        CREATE TABLES FUNCTION py_running_sum(INT)
        RETURNS ARRAY<STRUCT<original_value:INT, cumulative_sum:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "running_sum_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def running_sum_udtf(value):
    '''Return value with itself as cumulative sum (stateless)'''
    # Note: Function-based UDTF cannot maintain state
    # This is simplified to return (value, value)
    if value is not None:
        yield (value, value)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS numbers_table; """
        sql """
        CREATE TABLE numbers_table (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO numbers_table VALUES
        (1, 10),
        (2, 20),
        (3, 30),
        (4, 40);
        """
        
        qt_running_sum """
            SELECT original_value, cumulative_sum
            FROM numbers_table
            LATERAL VIEW py_running_sum(value) tmp AS original_value, cumulative_sum
            ORDER BY original_value;
        """

        // ========================================
        // Test 4: Explode Array UDTF
        // Similar to LATERAL VIEW explode in Hive
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_explode_json_array(STRING); """
        sql """
        CREATE TABLES FUNCTION py_explode_json_array(STRING)
        RETURNS ARRAY<STRING>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "explode_json_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
import json

def explode_json_udtf(json_str):
    '''Explode JSON ARRAY into rows'''
    if json_str:
        try:
            data = json.loads(json_str)
            if isinstance(data, list):
                for item in data:
                    yield (str(item),)
        except:
            pass  # Skip invalid JSON
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS temp_json; """
        sql """
        CREATE TABLE temp_json (
            id INT,
            json_data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO temp_json VALUES (1, '["apple", "banana", "cherry"]');
        """
        
        qt_explode_json """
            SELECT element
            FROM temp_json
            LATERAL VIEW py_explode_json_array(json_data) tmp AS element;
        """
        
        // ========================================
        // Test 5: Top-N UDTF (stateless version)
        // Note: Without state, this simply returns first n values per row
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_top_n(INT, INT); """
        sql """
        CREATE TABLES FUNCTION py_top_n(INT, INT)
        RETURNS ARRAY<STRUCT<value:INT, rank:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "top_n_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def top_n_udtf(value, n):
    '''Return single value with rank 1 (stateless)'''
    # Without state, each row is independent
    if value is not None and n is not None and n > 0:
        yield (value, 1)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS ranked_data; """
        sql """
        CREATE TABLE ranked_data (
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
        INSERT INTO ranked_data VALUES
        (1, 'A', 100, 2),
        (2, 'A', 90, 2),
        (3, 'A', 80, 2),
        (4, 'A', 70, 2),
        (5, 'B', 200, 2),
        (6, 'B', 190, 2);
        """
        
        qt_top_n """
            SELECT category, tmp.value, tmp.rank
            FROM ranked_data
            LATERAL VIEW py_top_n(value, top_n) tmp AS value, rank
            ORDER BY category, tmp.rank;
        """
        
        // ========================================
        // Test 6: Multiple Outputs per Input
        // One input row can generate multiple output rows
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_duplicate_n_times(STRING, INT); """
        sql """
        CREATE TABLES FUNCTION py_duplicate_n_times(STRING, INT)
        RETURNS ARRAY<STRUCT<output:STRING, idx:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "duplicate_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def duplicate_udtf(text, n):
    '''Duplicate input text N times'''
    if text and n:
        for i in range(n):
            yield (text, i + 1)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS temp_dup; """
        sql """
        CREATE TABLE temp_dup (
            id INT,
            text STRING,
            times INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO temp_dup VALUES (1, 'Hello', 3);
        """
        
        qt_duplicate """
            SELECT output, idx
            FROM temp_dup
            LATERAL VIEW py_duplicate_n_times(text, times) tmp AS output, idx;
        """
        
        // ========================================
        // Test 7: Conditional Output (Skip Rows)
        // UDTF can skip rows by not yielding
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_filter_positive(INT); """
        sql """
        CREATE TABLES FUNCTION py_filter_positive(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "filter_positive_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def filter_positive_udtf(value):
    '''Only output positive values'''
    if value is not None and value > 0:
        yield (value,)
    # If value <= 0, don't yield (skip this row)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS mixed_numbers; """
        sql """
        CREATE TABLE mixed_numbers (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO mixed_numbers VALUES (1, -5), (2, 0), (3, 3), (4, -2), (5, 7), (6, 1);
        """
        
        qt_filter_positive """
            SELECT positive_value
            FROM mixed_numbers
            LATERAL VIEW py_filter_positive(value) tmp AS positive_value
            ORDER BY positive_value;
        """
        
        // ========================================
        // Test 8: Cartesian Product UDTF
        // Generate all combinations
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_cartesian(STRING, STRING); """
        sql """
        CREATE TABLES FUNCTION py_cartesian(STRING, STRING)
        RETURNS ARRAY<STRUCT<item1:STRING, item2:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "cartesian_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def cartesian_udtf(list1, list2):
    '''Generate cartesian product of two comma-separated lists'''
    if list1 and list2:
        items1 = [x.strip() for x in list1.split(',')]
        items2 = [y.strip() for y in list2.split(',')]
        
        for x in items1:
            for y in items2:
                yield (x, y)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS temp_cart; """
        sql """
        CREATE TABLE temp_cart (
            id INT,
            list1 STRING,
            list2 STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO temp_cart VALUES (1, 'A,B', 'X,Y,Z');
        """
        
        qt_cartesian """
            SELECT item1, item2
            FROM temp_cart
            LATERAL VIEW py_cartesian(list1, list2) tmp AS item1, item2
            ORDER BY item1, item2;
        """
        
        // ========================================
        // Test 9: All Rows Filtered (Empty Output)
        // Tests data_batch = None case
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_filter_negative(INT); """
        sql """
        CREATE TABLES FUNCTION py_filter_negative(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "filter_negative_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def filter_negative_udtf(value):
    '''Only output negative values (filter all positive numbers)'''
    if value is not None and value < 0:
        yield (value,)
    # For positive numbers, don't yield anything
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS temp_all_positive; """
        sql """
        CREATE TABLE temp_all_positive (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        // Insert only positive numbers - all should be filtered
        sql """
        INSERT INTO temp_all_positive VALUES (1, 10), (2, 20), (3, 30);
        """
        
        // Expected: No output rows (all filtered), but should not crash
        qt_all_filtered """
            SELECT id, neg_value
            FROM temp_all_positive 
            LATERAL VIEW py_filter_negative(value) tmp AS neg_value
            ORDER BY id;
        """

        // ========================================
        // Test 10: Mixed - Some Filtered, Some Not
        // ========================================
        sql """ DROP TABLE IF EXISTS temp_mixed; """
        sql """
        CREATE TABLE temp_mixed (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        // Mix of positive and negative - only negative should pass
        sql """
        INSERT INTO temp_mixed VALUES (1, 10), (2, -5), (3, 20), (4, -3);
        """
        
        qt_mixed_filter """
            SELECT id, neg_value
            FROM temp_mixed 
            LATERAL VIEW py_filter_negative(value) tmp AS neg_value
            ORDER BY id, neg_value;
        """

        // ========================================
        // Test 11: Empty Input Table
        // Tests empty batch case
        // ========================================
        sql """ DROP TABLE IF EXISTS temp_empty; """
        sql """
        CREATE TABLE temp_empty (
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
            FROM temp_empty 
            LATERAL VIEW py_filter_negative(value) tmp AS neg_value;
        """

        // ========================================
        // Test 12: always_nullable = true (default)
        // Function can return NULL even with NOT NULL input
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_nullable_processor(INT); """
        sql """
        CREATE TABLES FUNCTION py_nullable_processor(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "nullable_processor_udtf",
            "runtime_version" = "3.8.10",
            "always_nullable" = "true"
        )
        AS \$\$
def nullable_processor_udtf(value):
    '''Return NULL for even numbers, value for odd numbers'''
    if value is None:
        yield (None,)
    elif value % 2 == 0:
        yield (None,)  # Return NULL for even numbers
    else:
        yield (value,)  # Return original value for odd numbers
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_nullable; """
        sql """
        CREATE TABLE test_nullable (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_nullable VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);
        """
        
        // Should return NULL for even values, original value for odd
        qt_nullable_true """
            SELECT id, result
            FROM test_nullable 
            LATERAL VIEW py_nullable_processor(value) tmp AS result
            ORDER BY id;
        """

        // ========================================
        // Test 13: always_nullable = false
        // Function guarantees NOT NULL output with NOT NULL input
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_non_nullable_processor(INT); """
        sql """
        CREATE TABLES FUNCTION py_non_nullable_processor(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "non_nullable_processor_udtf",
            "runtime_version" = "3.8.10",
            "always_nullable" = "false"
        )
        AS \$\$
def non_nullable_processor_udtf(value):
    '''Always return non-NULL value, double the input'''
    if value is None:
        yield (0,)  # Return 0 for NULL input
    else:
        yield (value * 2,)  # Always return non-NULL
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_non_nullable; """
        sql """
        CREATE TABLE test_non_nullable (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_non_nullable VALUES (1, 10), (2, 20), (3, 30);
        """
        
        // Should return doubled values, all NOT NULL
        qt_non_nullable_false """
            SELECT id, result
            FROM test_non_nullable 
            LATERAL VIEW py_non_nullable_processor(value) tmp AS result
            ORDER BY id;
        """

        // ========================================
        // Test 14: always_nullable with NULL inputs
        // Test how both modes handle NULL inputs
        // ========================================
        sql """ DROP TABLE IF EXISTS test_null_inputs; """
        sql """
        CREATE TABLE test_null_inputs (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_null_inputs VALUES (1, NULL), (2, 10), (3, NULL), (4, 20);
        """
        
        // Test with always_nullable = true (can return NULL)
        qt_nullable_with_nulls """
            SELECT id, result
            FROM test_null_inputs 
            LATERAL VIEW py_nullable_processor(value) tmp AS result
            ORDER BY id;
        """
        
        // Test with always_nullable = false (converts NULL to 0)
        qt_non_nullable_with_nulls """
            SELECT id, result
            FROM test_null_inputs 
            LATERAL VIEW py_non_nullable_processor(value) tmp AS result
            ORDER BY id;
        """

        // ========================================
        // Test 15: always_nullable default behavior
        // If not specified, should default to true
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_default_nullable(STRING); """
        sql """
        CREATE TABLES FUNCTION py_default_nullable(STRING)
        RETURNS ARRAY<STRING>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "default_nullable_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def default_nullable_udtf(text):
    '''Return NULL for empty strings, uppercase for non-empty'''
    if not text or text.strip() == '':
        yield (None,)
    else:
        yield (text.upper(),)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_default_nullable; """
        sql """
        CREATE TABLE test_default_nullable (
            id INT,
            text STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_default_nullable VALUES (1, 'hello'), (2, ''), (3, 'world'), (4, '  ');
        """
        
        // Should return NULL for empty/blank strings (default nullable behavior)
        qt_default_nullable """
            SELECT id, result
            FROM test_default_nullable 
            LATERAL VIEW py_default_nullable(text) tmp AS result
            ORDER BY id;
        """

        // ========================================
        // Test 16: always_nullable with multiple outputs
        // Test nullable behavior with functions returning multiple rows
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_nullable_explode(STRING); """
        sql """
        CREATE TABLES FUNCTION py_nullable_explode(STRING)
        RETURNS ARRAY<STRING>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "nullable_explode_udtf",
            "runtime_version" = "3.8.10",
            "always_nullable" = "true"
        )
        AS \$\$
def nullable_explode_udtf(csv_string):
    '''Split CSV and return NULL for empty parts'''
    if not csv_string:
        return
    parts = csv_string.split(',')
    for part in parts:
        stripped = part.strip()
        if stripped:
            yield (stripped,)
        else:
            yield (None,)  # Return NULL for empty parts
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_multi_nullable; """
        sql """
        CREATE TABLE test_multi_nullable (
            id INT,
            data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_multi_nullable VALUES 
            (1, 'a,b,c'), 
            (2, 'x,,z'),
            (3, ',,');
        """
        
        // Should return NULL for empty parts in CSV
        qt_multi_nullable """
            SELECT id, part
            FROM test_multi_nullable 
            LATERAL VIEW py_nullable_explode(data) tmp AS part
            ORDER BY id, part;
        """
        
        // ========================================
        // Test: Scalar Value Support (New Feature)
        // Single-field UDTF can yield scalar values directly
        // ========================================
        
        // Test Case 1: yield scalar (int)
        sql """ DROP FUNCTION IF EXISTS py_scalar_int(INT, INT); """
        sql """
        CREATE TABLES FUNCTION py_scalar_int(INT, INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "scalar_int_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def scalar_int_udtf(start, end):
    '''Yield scalar integers directly (no tuple wrapping)'''
    if start is not None and end is not None:
        for i in range(start, end + 1):
            yield i  # Direct scalar, not (i,)
\$\$;
        """
        
        qt_scalar_int """
            SELECT tmp.value 
            FROM (SELECT 1 as start_val, 5 as end_val) t
            LATERAL VIEW py_scalar_int(start_val, end_val) tmp AS value
            ORDER BY tmp.value;
        """
        
        // Test Case 2: yield scalar (string)
        sql """ DROP FUNCTION IF EXISTS py_scalar_string(STRING); """
        sql """
        CREATE TABLES FUNCTION py_scalar_string(STRING)
        RETURNS ARRAY<STRING>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "scalar_string_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def scalar_string_udtf(text):
    '''Split string and yield parts directly as scalars'''
    if text:
        for part in text.split(','):
            yield part.strip()  # Direct string, not (part.strip(),)
\$\$;
        """
        
        qt_scalar_string """
            SELECT tmp.value 
            FROM (SELECT 'apple,banana,cherry' as text) t
            LATERAL VIEW py_scalar_string(text) tmp AS value
            ORDER BY tmp.value;
        """
        
        // Test Case 3: Mixed - both scalar and tuple should work
        sql """ DROP FUNCTION IF EXISTS py_mixed_style(INT); """
        sql """
        CREATE TABLES FUNCTION py_mixed_style(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "mixed_style_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def mixed_style_udtf(n):
    '''Test mixing scalar and tuple yields (should both work)'''
    if n is not None and n > 0:
        # First yield as scalar
        yield n
        # Then yield as tuple
        yield (n * 2,)
        # Then scalar again
        yield n * 3
\$\$;
        """
        
        qt_mixed_style """
            SELECT tmp.value 
            FROM (SELECT 10 as n) t
            LATERAL VIEW py_mixed_style(n) tmp AS value
            ORDER BY tmp.value;
        """
        
        // Test Case 4: return scalar (not yield)
        sql """ DROP FUNCTION IF EXISTS py_return_scalar(STRING); """
        sql """
        CREATE TABLES FUNCTION py_return_scalar(STRING)
        RETURNS ARRAY<STRING>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "return_scalar_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def return_scalar_udtf(text):
    '''Return scalar value instead of yielding'''
    if text:
        return text.upper()  # Direct return, not (text.upper(),)
\$\$;
        """
        
        qt_return_scalar """
            SELECT tmp.value 
            FROM (SELECT 'hello' as text) t
            LATERAL VIEW py_return_scalar(text) tmp AS value;
        """
        
        // Test Case 5: Verify multi-field still requires tuples
        sql """ DROP FUNCTION IF EXISTS py_multi_field_check(INT); """
        sql """
        CREATE TABLES FUNCTION py_multi_field_check(INT)
        RETURNS ARRAY<STRUCT<original:INT, doubled:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "multi_field_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def multi_field_udtf(n):
    '''Multi-field UDTF still requires tuples'''
    if n is not None:
        yield (n, n * 2)  # Must be tuple for multi-field
\$\$;
        """
        
        qt_multi_field_check """
            SELECT tmp.original, tmp.doubled
            FROM (SELECT 42 as n) t
            LATERAL VIEW py_multi_field_check(n) tmp AS original, doubled;
        """
        
        // ========================================
        // Test: OUTER Semantics
        // When function is registered as func, both func and func_outer are available
        // func: skips NULL/empty results (no output row)
        // func_outer: outputs NULL for NULL/empty results (guaranteed output row)
        // ========================================

        // Test Case 1: Simple UDTF with NULL handling
        sql """ DROP FUNCTION IF EXISTS py_process_value(INT); """
        sql """
        CREATE TABLES FUNCTION py_process_value(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "process_value_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def process_value_udtf(value):
    '''Process value: if positive, yield doubled value; otherwise yield nothing'''
    if value is not None and value > 0:
        yield (value * 2,)
    # If value is None or <= 0, don't yield anything
\$\$;
        """

        sql """ DROP TABLE IF EXISTS test_outer_basic; """
        sql """
        CREATE TABLE test_outer_basic (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO test_outer_basic VALUES 
            (1, 10),      -- positive: should output 20
            (2, NULL),    -- NULL: func skips, func_outer outputs NULL
            (3, 0),       -- zero: func skips, func_outer outputs NULL
            (4, -5),      -- negative: func skips, func_outer outputs NULL
            (5, 15);      -- positive: should output 30
        """

        // Test without _outer: NULL/non-positive values are skipped (no output row)
        qt_outer_without """
            SELECT id, result
            FROM test_outer_basic
            LATERAL VIEW py_process_value(value) tmp AS result
            ORDER BY id;
        """

        // Test with _outer: NULL/non-positive values output NULL (guaranteed row per input)
        qt_outer_with """
            SELECT id, result
            FROM test_outer_basic
            LATERAL VIEW py_process_value_outer(value) tmp AS result
            ORDER BY id;
        """

        // Test Case 2: String split with empty/NULL strings
        sql """ DROP FUNCTION IF EXISTS py_split_words(STRING); """
        sql """
        CREATE TABLES FUNCTION py_split_words(STRING)
        RETURNS ARRAY<STRING>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "split_words_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def split_words_udtf(text):
    '''Split text by spaces. Empty/NULL strings yield nothing.'''
    if text and text.strip():
        words = text.strip().split()
        for word in words:
            yield (word,)
    # Empty or NULL strings: no output
\$\$;
        """

        sql """ DROP TABLE IF EXISTS test_outer_strings; """
        sql """
        CREATE TABLE test_outer_strings (
            id INT,
            text STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO test_outer_strings VALUES 
            (1, 'hello world'),      -- should split into 2 rows
            (2, NULL),               -- NULL
            (3, ''),                 -- empty string
            (4, '   '),              -- whitespace only
            (5, 'single');           -- single word
        """

        // Without _outer: only rows 1 and 5 produce output
        qt_outer_string_without """
            SELECT id, word
            FROM test_outer_strings
            LATERAL VIEW py_split_words(text) tmp AS word
            ORDER BY id, word;
        """

        // With _outer: all rows produce at least one output (NULL for rows 2,3,4)
        qt_outer_string_with """
            SELECT id, word
            FROM test_outer_strings
            LATERAL VIEW py_split_words_outer(text) tmp AS word
            ORDER BY id, word;
        """

        // Test Case 3: Array expansion with empty arrays
        sql """ DROP FUNCTION IF EXISTS py_expand_range(INT); """
        sql """
        CREATE TABLES FUNCTION py_expand_range(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "expand_range_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def expand_range_udtf(n):
    '''Generate numbers from 1 to n. If n <= 0 or NULL, yield nothing.'''
    if n is not None and n > 0:
        for i in range(1, n + 1):
            yield (i,)
    # If n is None or <= 0, no output
\$\$;
        """

        sql """ DROP TABLE IF EXISTS test_outer_range; """
        sql """
        CREATE TABLE test_outer_range (
            id INT,
            count INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO test_outer_range VALUES 
            (1, 3),       -- should generate 1,2,3
            (2, NULL),    -- NULL
            (3, 0),       -- zero (no range)
            (4, -2),      -- negative (no range)
            (5, 1);       -- should generate 1
        """

        // Without _outer: only rows 1 and 5 produce output
        qt_outer_range_without """
            SELECT id, num
            FROM test_outer_range
            LATERAL VIEW py_expand_range(count) tmp AS num
            ORDER BY id, num;
        """

        // With _outer: all rows produce output (NULL for rows 2,3,4)
        qt_outer_range_with """
            SELECT id, num
            FROM test_outer_range
            LATERAL VIEW py_expand_range_outer(count) tmp AS num
            ORDER BY id, num;
        """

        // Test Case 4: Multiple column output with OUTER
        sql """ DROP FUNCTION IF EXISTS py_parse_csv(STRING); """
        sql """
        CREATE TABLES FUNCTION py_parse_csv(STRING)
        RETURNS ARRAY<STRUCT<field1:STRING, field2:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "parse_csv_udtf",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def parse_csv_udtf(csv_line):
    '''Parse CSV line into field1,field2. Empty/NULL yields nothing.'''
    if csv_line and csv_line.strip():
        parts = csv_line.split(',')
        if len(parts) >= 2:
            yield (parts[0].strip(), parts[1].strip())
    # Empty or invalid CSV: no output
\$\$;
        """

        sql """ DROP TABLE IF EXISTS test_outer_multifield; """
        sql """
        CREATE TABLE test_outer_multifield (
            id INT,
            data STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO test_outer_multifield VALUES 
            (1, 'Alice,30'),      -- valid CSV
            (2, NULL),            -- NULL
            (3, ''),              -- empty
            (4, 'Bob'),           -- incomplete CSV (only 1 field)
            (5, 'Charlie,25');    -- valid CSV
        """

        // Without _outer: only rows 1 and 5 produce output
        qt_outer_multifield_without """
            SELECT id, field1, field2
            FROM test_outer_multifield
            LATERAL VIEW py_parse_csv(data) tmp AS field1, field2
            ORDER BY id;
        """

        // With _outer: all rows produce output (NULL,NULL for rows 2,3,4)
        qt_outer_multifield_with """
            SELECT id, field1, field2
            FROM test_outer_multifield
            LATERAL VIEW py_parse_csv_outer(data) tmp AS field1, field2
            ORDER BY id;
        """

        // Test Case 5: Combining regular and outer table functions
        sql """ DROP TABLE IF EXISTS test_outer_combined; """
        sql """
        CREATE TABLE test_outer_combined (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO test_outer_combined VALUES 
            (1, 5),
            (2, NULL),
            (3, 3);
        """

        // Mix regular and outer: regular filters, outer preserves
        qt_outer_mixed_functions """
            SELECT id, r1.num as regular_num, r2.num as outer_num
            FROM test_outer_combined
            LATERAL VIEW py_expand_range(value) r1 AS num
            LATERAL VIEW py_expand_range_outer(value) r2 AS num
            ORDER BY id, regular_num, outer_num;
        """

        // Test Case 6: Verify outer behavior with built-in functions
        sql """ DROP TABLE IF EXISTS test_outer_builtin; """
        sql """
        CREATE TABLE test_outer_builtin (
            id INT,
            arr ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO test_outer_builtin VALUES 
            (1, [1, 2, 3]),      -- normal array
            (2, NULL),           -- NULL array
            (3, []),             -- empty array
            (4, [5]);            -- single element
        """

        // Built-in explode (no outer): skips NULL and empty
        qt_outer_builtin_explode """
            SELECT id, elem
            FROM test_outer_builtin
            LATERAL VIEW explode(arr) tmp AS elem
            ORDER BY id, elem;
        """

        // Built-in explode_outer: preserves NULL and empty rows
        qt_outer_builtin_explode_outer """
            SELECT id, elem
            FROM test_outer_builtin
            LATERAL VIEW explode_outer(arr) tmp AS elem
            ORDER BY id, elem;
        """

        // Test Case 7: Documentation example - LEFT OUTER JOIN semantics
        sql """ DROP TABLE IF EXISTS orders; """
        sql """
        CREATE TABLE orders (
            order_id INT,
            items STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(order_id)
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO orders VALUES 
            (1, 'apple,banana'),           -- order with items
            (2, NULL),                     -- order with NULL items
            (3, ''),                       -- order with empty items
            (4, 'cherry');                 -- order with one item
        """

        // Without outer: orders 2 and 3 disappear (like INNER JOIN)
        qt_outer_doc_inner """
            SELECT order_id, item
            FROM orders
            LATERAL VIEW py_split_words(items) tmp AS item
            ORDER BY order_id, item;
        """

        // With outer: all orders preserved (like LEFT OUTER JOIN)
        qt_outer_doc_outer """
            SELECT order_id, item
            FROM orders
            LATERAL VIEW py_split_words_outer(items) tmp AS item
            ORDER BY order_id, item;
        """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_split_string(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_generate_series(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_running_sum(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_explode_json_array(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_top_n(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_duplicate_n_times(STRING, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_filter_positive(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_cartesian(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_filter_negative(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_nullable_processor(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_non_nullable_processor(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_default_nullable(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_nullable_explode(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_scalar_int(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_scalar_string(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_mixed_style(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_return_scalar(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_multi_field_check(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_process_value(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_split_words(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_expand_range(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_parse_csv(STRING);")
        try_sql("DROP TABLE IF EXISTS temp_input;")
        try_sql("DROP TABLE IF EXISTS numbers_table;")
        try_sql("DROP TABLE IF EXISTS ranked_data;")
        try_sql("DROP TABLE IF EXISTS mixed_numbers;")
        try_sql("DROP TABLE IF EXISTS temp_all_positive;")
        try_sql("DROP TABLE IF EXISTS temp_mixed;")
        try_sql("DROP TABLE IF EXISTS temp_empty;")
        try_sql("DROP TABLE IF EXISTS test_nullable;")
        try_sql("DROP TABLE IF EXISTS test_non_nullable;")
        try_sql("DROP TABLE IF EXISTS test_null_inputs;")
        try_sql("DROP TABLE IF EXISTS test_default_nullable;")
        try_sql("DROP TABLE IF EXISTS test_multi_nullable;")
        try_sql("DROP TABLE IF EXISTS test_outer_basic;")
        try_sql("DROP TABLE IF EXISTS test_outer_strings;")
        try_sql("DROP TABLE IF EXISTS test_outer_range;")
        try_sql("DROP TABLE IF EXISTS test_outer_multifield;")
        try_sql("DROP TABLE IF EXISTS test_outer_combined;")
        try_sql("DROP TABLE IF EXISTS test_outer_builtin;")
        try_sql("DROP TABLE IF EXISTS orders;")
    }
}
