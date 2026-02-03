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

suite("test_pythonudtf_io_patterns_inline") {
    // Test Python UDTF Input/Output Patterns
    // Testing different cardinality patterns: 1-to-1, 1-to-N, 1-to-0, N-to-M
    
    def runtime_version = "3.8.10"
    
    try {
        // ========================================
        // Pattern 1: One-to-One (1 input row → 1 output row)
        // Each input row produces exactly one output row
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_one_to_one(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_one_to_one(INT)
        RETURNS ARRAY<STRUCT<input:INT, doubled:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "one_to_one",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def one_to_one(value):
    '''Each input row produces exactly one output row'''
    if value is not None:
        yield (value, value * 2)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_one_to_one; """
        sql """
        CREATE TABLE test_one_to_one (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_one_to_one VALUES (1, 10), (2, 20), (3, 30);
        """
        sql """ sync """
        
        qt_one_to_one """
            SELECT tmp.input, tmp.doubled
            FROM test_one_to_one
            LATERAL VIEW udtf_one_to_one(value) tmp AS input, doubled
            ORDER BY tmp.input;
        """
        
        // ========================================
        // Pattern 2: One-to-Many (1 input row → N output all_rows)
        // Each input row produces multiple output all_rows
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_one_to_many(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_one_to_many(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "one_to_many",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def one_to_many(n):
    '''Each input row produces N output all_rows (1 to n)'''
    if n is not None and n > 0:
        for i in range(1, n + 1):
            yield (i,)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_one_to_many; """
        sql """
        CREATE TABLE test_one_to_many (
            id INT,
            count INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_one_to_many VALUES (1, 3), (2, 2), (3, 4);
        """
        sql """ sync """
        
        qt_one_to_many """
            SELECT id, tmp.value
            FROM test_one_to_many
            LATERAL VIEW udtf_one_to_many(count) tmp AS value
            ORDER BY id, tmp.value;
        """
        
        // ========================================
        // Pattern 3: One-to-Zero (1 input row → 0 output all_rows)
        // Some input all_rows produce no output (filtering)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_one_to_zero(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_one_to_zero(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "one_to_zero",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def one_to_zero(value):
    '''Only output even numbers, skip odd numbers (zero output)'''
    if value is not None and value % 2 == 0:
        yield (value,)
    # Odd numbers: no yield, zero output all_rows
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_one_to_zero; """
        sql """
        CREATE TABLE test_one_to_zero (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_one_to_zero VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6);
        """
        sql """ sync """
        
        qt_one_to_zero """
            SELECT tmp.value
            FROM test_one_to_zero
            LATERAL VIEW udtf_one_to_zero(value) tmp AS value
            ORDER BY tmp.value;
        """
        
        // ========================================
        // Pattern 4: One-to-Variable (1 input row → 0/1/N output all_rows)
        // Different input all_rows produce different numbers of output all_rows
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_one_to_variable(STRING); """
        sql """
        CREATE TABLES FUNCTION udtf_one_to_variable(STRING)
        RETURNS ARRAY<STRING>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "one_to_variable",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def one_to_variable(text):
    '''
    - Empty string → 0 all_rows
    - Single word → 1 row
    - Multiple words → N all_rows
    '''
    if text:
        words = text.split()
        for word in words:
            yield (word,)
    # Empty or None: no yield, zero output
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_one_to_variable; """
        sql """
        CREATE TABLE test_one_to_variable (
            id INT,
            text STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_one_to_variable VALUES 
        (1, 'hello'),           -- 1 output
        (2, 'hello world'),     -- 2 outputs
        (3, ''),                -- 0 outputs
        (4, 'a b c');           -- 3 outputs
        """
        sql """ sync """
        
        qt_one_to_variable """
            SELECT id, tmp.word
            FROM test_one_to_variable
            LATERAL VIEW udtf_one_to_variable(text) tmp AS word
            ORDER BY id, tmp.word;
        """
        
        // ========================================
        // Pattern 5: Many-to-One (N input all_rows → aggregate to fewer all_rows)
        // Note: This simulates batch processing where each row independently
        // produces output, but conceptually represents aggregation pattern
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_aggregate_pattern(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_aggregate_pattern(INT)
        RETURNS ARRAY<STRUCT<value:INT, category:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "aggregate_pattern",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def aggregate_pattern(value):
    '''Categorize numbers into ranges'''
    if value is not None:
        if value < 10:
            category = 'small'
        elif value < 100:
            category = 'medium'
        else:
            category = 'large'
        yield (value, category)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_aggregate_pattern; """
        sql """
        CREATE TABLE test_aggregate_pattern (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_aggregate_pattern VALUES 
        (1, 5), (2, 50), (3, 500), (4, 8), (5, 80), (6, 800);
        """
        sql """ sync """
        
        qt_aggregate_pattern """
            SELECT tmp.category, COUNT(*) as count
            FROM test_aggregate_pattern
            LATERAL VIEW udtf_aggregate_pattern(value) tmp AS value, category
            GROUP BY tmp.category
            ORDER BY tmp.category;
        """
        
        // ========================================
        // Pattern 6: Explosive Growth (1 input row → many output all_rows)
        // Testing large multiplication factor
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_explosive(INT, INT); """
        sql """
        CREATE TABLES FUNCTION udtf_explosive(INT, INT)
        RETURNS ARRAY<STRUCT<row_id:INT, col_id:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "explosive",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def explosive(all_rows, all_cols):
    '''Generate all_rows * all_cols output all_rows (cartesian product)'''
    if all_rows is not None and all_cols is not None and all_rows > 0 and all_cols > 0:
        for r in range(all_rows):
            for c in range(all_cols):
                yield (r, c)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_explosive; """
        sql """
        CREATE TABLE test_explosive (
            id INT,
            all_rows INT,
            all_cols INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_explosive VALUES (1, 2, 3);
        """
        sql """ sync """
        
        qt_explosive """
            SELECT tmp.row_id, tmp.col_id
            FROM test_explosive
            LATERAL VIEW udtf_explosive(all_rows, all_cols) tmp AS row_id, col_id
            ORDER BY tmp.row_id, tmp.col_id;
        """
        
        // ========================================
        // Pattern 7: Conditional Branching (different logic paths)
        // Same function but different output counts based on condition
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_conditional(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_conditional(INT)
        RETURNS ARRAY<STRUCT<value:INT, type:STRING>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "conditional",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def conditional(value):
    '''
    - Positive: output (value, 'positive')
    - Negative: output (abs(value), 'negative')
    - Zero: output both (0, 'zero') and (0, 'neutral')
    '''
    if value is not None:
        if value > 0:
            yield (value, 'positive')
        elif value < 0:
            yield (abs(value), 'negative')
        else:
            yield (0, 'zero')
            yield (0, 'neutral')
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_conditional_io; """
        sql """
        CREATE TABLE test_conditional_io (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_conditional_io VALUES (1, 10), (2, -5), (3, 0), (4, 7);
        """
        sql """ sync """
        
        qt_conditional """
            SELECT tmp.value, tmp.type
            FROM test_conditional_io
            LATERAL VIEW udtf_conditional(value) tmp AS value, type
            ORDER BY tmp.value, tmp.type;
        """
        
        // ========================================
        // Pattern 8: All-or-Nothing (either all all_rows or no all_rows)
        // Based on a condition, output all or nothing
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_all_or_nothing(STRING, INT); """
        sql """
        CREATE TABLES FUNCTION udtf_all_or_nothing(STRING, INT)
        RETURNS ARRAY<STRUCT<char:STRING, pos:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "all_or_nothing",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def all_or_nothing(text, min_length):
    '''
    If text length >= min_length: output each character with position
    Otherwise: output nothing
    '''
    if text and len(text) >= min_length:
        for i, char in enumerate(text):
            yield (char, i)
    # If condition not met: no yield
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_all_or_nothing; """
        sql """
        CREATE TABLE test_all_or_nothing (
            id INT,
            text STRING,
            min_len INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_all_or_nothing VALUES 
        (1, 'hello', 3),    -- 5 outputs (length=5 >= 3)
        (2, 'hi', 5),       -- 0 outputs (length=2 < 5)
        (3, 'world', 4);    -- 5 outputs (length=5 >= 4)
        """
        sql """ sync """
        
        qt_all_or_nothing """
            SELECT id, tmp.char, tmp.pos
            FROM test_all_or_nothing
            LATERAL VIEW udtf_all_or_nothing(text, min_len) tmp AS char, pos
            ORDER BY id, tmp.pos;
        """
        
        // ========================================
        // Pattern 9: Empty Input Table (0 input all_rows)
        // Test behavior with no input data
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_empty_input(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_empty_input(INT)
        RETURNS ARRAY<INT>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "empty_input",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def empty_input(value):
    '''Simple identity function'''
    if value is not None:
        yield (value,)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_empty_input; """
        sql """
        CREATE TABLE test_empty_input (
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
            FROM test_empty_input
            LATERAL VIEW udtf_empty_input(value) tmp AS value;
        """
        
        // ========================================
        // Pattern 10: Batch Processing Simulation
        // Multiple input all_rows, each producing variable outputs
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udtf_batch_process(INT); """
        sql """
        CREATE TABLES FUNCTION udtf_batch_process(INT)
        RETURNS ARRAY<STRUCT<original:INT, factor:INT, result:INT>>
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "batch_process",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
def batch_process(value):
    '''For each input, generate multiples (2x, 3x, 5x)'''
    if value is not None and value > 0:
        for factor in [2, 3, 5]:
            yield (value, factor, value * factor)
\$\$;
        """
        
        sql """ DROP TABLE IF EXISTS test_batch_process; """
        sql """
        CREATE TABLE test_batch_process (
            id INT,
            value INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO test_batch_process VALUES (1, 10), (2, 20);
        """
        sql """ sync """
        
        qt_batch_process """
            SELECT tmp.original, tmp.factor, tmp.result
            FROM test_batch_process
            LATERAL VIEW udtf_batch_process(value) tmp AS original, factor, result
            ORDER BY tmp.original, tmp.factor;
        """
        
    } finally {
        // Cleanup functions
        try_sql("DROP FUNCTION IF EXISTS udtf_one_to_one(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_one_to_many(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_one_to_zero(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_one_to_variable(STRING);")
        try_sql("DROP FUNCTION IF EXISTS udtf_aggregate_pattern(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_explosive(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_conditional(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_all_or_nothing(STRING, INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_empty_input(INT);")
        try_sql("DROP FUNCTION IF EXISTS udtf_batch_process(INT);")
        
        // Cleanup tables
        try_sql("DROP TABLE IF EXISTS test_one_to_one;")
        try_sql("DROP TABLE IF EXISTS test_one_to_many;")
        try_sql("DROP TABLE IF EXISTS test_one_to_zero;")
        try_sql("DROP TABLE IF EXISTS test_one_to_variable;")
        try_sql("DROP TABLE IF EXISTS test_aggregate_pattern;")
        try_sql("DROP TABLE IF EXISTS test_explosive;")
        try_sql("DROP TABLE IF EXISTS test_conditional_io;")
        try_sql("DROP TABLE IF EXISTS test_all_or_nothing;")
        try_sql("DROP TABLE IF EXISTS test_empty_input;")
        try_sql("DROP TABLE IF EXISTS test_batch_process;")
    }
}
