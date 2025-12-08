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

suite("test_pythonudf_global_function") {
    // Test creating global Python UDF with GLOBAL keyword
    
    def runtime_version = "3.8.10"
    try {
        // Test 1: Create GLOBAL function
        sql """ DROP GLOBAL FUNCTION IF EXISTS py_global_multiply(INT, INT); """
        sql """
        CREATE GLOBAL FUNCTION py_global_multiply(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(a, b):
    if a is None or b is None:
        return None
    return a * b
\$\$;
        """
        
        qt_select_global_multiply """ SELECT py_global_multiply(7, 8) AS result; """
        
        // Test 2: Create GLOBAL string function
        sql """ DROP GLOBAL FUNCTION IF EXISTS py_global_lower(STRING); """
        sql """
        CREATE GLOBAL FUNCTION py_global_lower(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(s):
    if s is None:
        return None
    return s.lower()
\$\$;
        """
        
        qt_select_global_lower """ SELECT py_global_lower('HELLO WORLD') AS result; """
        
        // Test 3: Create regular (non-GLOBAL) function for comparison
        sql """ DROP FUNCTION IF EXISTS py_local_add(INT, INT); """
        sql """
        CREATE FUNCTION py_local_add(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(a, b):
    if a is None or b is None:
        return None
    return a + b
\$\$;
        """
        
        qt_select_local_add """ SELECT py_local_add(15, 25) AS result; """
        
        // Test 4: Test GLOBAL function on table data
        sql """ DROP TABLE IF EXISTS global_function_test_table; """
        sql """
        CREATE TABLE global_function_test_table (
            id INT,
            val1 INT,
            val2 INT,
            text STRING
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO global_function_test_table VALUES
        (1, 5, 6, 'APPLE'),
        (2, 10, 20, 'BANANA'),
        (3, 3, 7, 'CHERRY'),
        (4, NULL, 5, 'DATE'),
        (5, 8, 9, NULL);
        """
        
        qt_select_table_global """ 
        SELECT 
            id,
            val1,
            val2,
            py_global_multiply(val1, val2) AS multiply_result,
            text,
            py_global_lower(text) AS lower_result
        FROM global_function_test_table
        ORDER BY id;
        """
        
        // Test 5: Mathematical calculation with GLOBAL function
        sql """ DROP GLOBAL FUNCTION IF EXISTS py_global_power(DOUBLE, DOUBLE); """
        sql """
        CREATE GLOBAL FUNCTION py_global_power(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(base, exponent):
    if base is None or exponent is None:
        return None
    return base ** exponent
\$\$;
        """
        
        qt_select_global_power """ SELECT py_global_power(2.0, 3.0) AS result; """
        qt_select_global_power_decimal """ SELECT py_global_power(5.0, 0.5) AS result; """
        
    } finally {
        try_sql("DROP GLOBAL FUNCTION IF EXISTS py_global_multiply(INT, INT);")
        try_sql("DROP GLOBAL FUNCTION IF EXISTS py_global_lower(STRING);")
        try_sql("DROP GLOBAL FUNCTION IF EXISTS py_global_power(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_local_add(INT, INT);")
        try_sql("DROP TABLE IF EXISTS global_function_test_table;")
    }
}
