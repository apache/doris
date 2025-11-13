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
    
    def runtime_version = "3.10.12"
    
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
            "runtime_version" = "3.10.12"
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
            "runtime_version" = "3.10.12"
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
            "runtime_version" = "3.10.12"
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
            "runtime_version" = "3.10.12"
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
            "runtime_version" = "3.10.12"
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
            "runtime_version" = "3.10.12"
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
            "runtime_version" = "3.10.12"
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
            "runtime_version" = "3.10.12"
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
            "runtime_version" = "3.10.12"
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
            "runtime_version" = "3.10.12",
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
            "runtime_version" = "3.10.12",
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
            "runtime_version" = "3.10.12"
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
            "runtime_version" = "3.10.12",
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
    }
}
