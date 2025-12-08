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

suite("test_pythonudf_error_handling") {
    // Test error handling and exception cases for Python UDF
    
    def runtime_version = "3.8.10"
    try {
        // Test 1: Division by zero error handling
        sql """ DROP FUNCTION IF EXISTS py_safe_divide(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_safe_divide(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(a, b):
    if a is None or b is None:
        return None
    if b == 0:
        return None
    return a / b
\$\$;
        """
        
        qt_select_divide_normal """ SELECT py_safe_divide(10.0, 2.0) AS result; """
        qt_select_divide_zero """ SELECT py_safe_divide(10.0, 0.0) AS result; """
        qt_select_divide_null """ SELECT py_safe_divide(10.0, NULL) AS result; """
        
        // Test 2: String index out of bounds handling
        sql """ DROP FUNCTION IF EXISTS py_safe_substring(STRING, INT); """
        sql """
        CREATE FUNCTION py_safe_substring(STRING, INT) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(s, index):
    if s is None or index is None:
        return None
    if index < 0 or index >= len(s):
        return None
    return s[index]
\$\$;
        """
        
        qt_select_substring_valid """ SELECT py_safe_substring('hello', 1) AS result; """
        qt_select_substring_invalid """ SELECT py_safe_substring('hello', 10) AS result; """
        qt_select_substring_negative """ SELECT py_safe_substring('hello', -1) AS result; """
        
        // Test 3: Type conversion error handling
        sql """ DROP FUNCTION IF EXISTS py_safe_int_parse(STRING); """
        sql """
        CREATE FUNCTION py_safe_int_parse(STRING) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(s):
    if s is None:
        return None
    try:
        return int(s)
    except (ValueError, TypeError):
        return None
\$\$;
        """
        
        qt_select_parse_valid """ SELECT py_safe_int_parse('123') AS result; """
        qt_select_parse_invalid """ SELECT py_safe_int_parse('abc') AS result; """
        qt_select_parse_empty """ SELECT py_safe_int_parse('') AS result; """
        
        // Test 4: Array out of bounds handling
        sql """ DROP FUNCTION IF EXISTS py_safe_array_get(ARRAY<INT>, INT); """
        sql """
        CREATE FUNCTION py_safe_array_get(ARRAY<INT>, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(arr, index):
    if arr is None or index is None:
        return None
    if index < 0 or index >= len(arr):
        return None
    return arr[index]
\$\$;
        """
        
        qt_select_array_valid """ SELECT py_safe_array_get([10, 20, 30], 1) AS result; """
        qt_select_array_invalid """ SELECT py_safe_array_get([10, 20, 30], 5) AS result; """
        
        // Test 5: Test error handling on table data
        sql """ DROP TABLE IF EXISTS error_handling_test_table; """
        sql """
        CREATE TABLE error_handling_test_table (
            id INT,
            numerator DOUBLE,
            denominator DOUBLE,
            text STRING,
            arr_index INT
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO error_handling_test_table VALUES
        (1, 100.0, 10.0, '123', 0),
        (2, 50.0, 0.0, 'abc', 1),
        (3, NULL, 5.0, '', 2),
        (4, 75.0, NULL, '456', -1),
        (5, 25.0, 5.0, 'xyz', 10);
        """
        
        qt_select_table_error_handling """ 
        SELECT 
            id,
            numerator,
            denominator,
            py_safe_divide(numerator, denominator) AS divide_result,
            text,
            py_safe_int_parse(text) AS parse_result
        FROM error_handling_test_table
        ORDER BY id;
        """
        
        // Test 6: Empty string handling
        sql """ DROP FUNCTION IF EXISTS py_safe_length(STRING); """
        sql """
        CREATE FUNCTION py_safe_length(STRING) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(s):
    if s is None:
        return None
    return len(s)
\$\$;
        """
        
        qt_select_length_normal """ SELECT py_safe_length('hello') AS result; """
        qt_select_length_empty """ SELECT py_safe_length('') AS result; """
        qt_select_length_null """ SELECT py_safe_length(NULL) AS result; """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_safe_divide(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_safe_substring(STRING, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_safe_int_parse(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_safe_array_get(ARRAY<INT>, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_safe_length(STRING);")
        try_sql("DROP TABLE IF EXISTS error_handling_test_table;")
    }
}
