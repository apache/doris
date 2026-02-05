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

suite("test_pythonudf_always_nullable") {
    // Test different configurations of always_nullable parameter
    
    def runtime_version = "3.8.10"
    try {
        // Test 1: always_nullable = true (default value)
        sql """ DROP FUNCTION IF EXISTS py_nullable_true(INT); """
        sql """
        CREATE FUNCTION py_nullable_true(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(x):
    if x is None:
        return None
    if x < 0:
        return None
    return x * 2
\$\$;
        """
        
        qt_select_nullable_true_normal """ SELECT py_nullable_true(10) AS result; """
        qt_select_nullable_true_null """ SELECT py_nullable_true(NULL) AS result; """
        qt_select_nullable_true_negative """ SELECT py_nullable_true(-5) AS result; """
        
        // Test 2: always_nullable = false
        sql """ DROP FUNCTION IF EXISTS py_nullable_false(INT); """
        sql """
        CREATE FUNCTION py_nullable_false(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "always_nullable" = "false",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(x):
    if x is None:
        return 0
    return x * 2
\$\$;
        """
        
        qt_select_nullable_false_normal """ SELECT py_nullable_false(10) AS result; """
        qt_select_nullable_false_null """ SELECT py_nullable_false(NULL) AS result; """
        
        // Test 3: always_nullable = false but function returns None
        // This tests the edge case where the function violates the always_nullable contract
        sql """ DROP FUNCTION IF EXISTS py_nullable_false_returns_none(INT); """
        sql """
        CREATE FUNCTION py_nullable_false_returns_none(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "always_nullable" = "false",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(x):
    if x < 0:
        return None  # Returns None even though always_nullable is false
    return x * 2
\$\$;
        """
        
        qt_select_nullable_false_returns_none_normal """ SELECT py_nullable_false_returns_none(10) AS result; """

        test {
            sql """ SELECT py_nullable_false_returns_none(-5) AS result; """
            exception "but the return type is not nullable, please check the always_nullable property in create function statement, it should be true"
        }
        
        // Test 4: Test nullable behavior on table data
        sql """ DROP TABLE IF EXISTS nullable_test_table; """
        sql """
        CREATE TABLE nullable_test_table (
            id INT,
            value INT
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO nullable_test_table VALUES
        (1, 10),
        (2, NULL),
        (3, -5),
        (4, 0),
        (5, 100);
        """
        
        qt_select_table_nullable_true """ 
        SELECT 
            id,
            value,
            py_nullable_true(value) AS result
        FROM nullable_test_table
        ORDER BY id;
        """
        
        qt_select_table_nullable_false """ 
        SELECT 
            id,
            value,
            py_nullable_false(value) AS result
        FROM nullable_test_table
        ORDER BY id;
        """
        
        test {
            sql """
            SELECT
                id,
                value,
                py_nullable_false_returns_none(value) AS result
            FROM nullable_test_table
            ORDER BY id;
            """
            exception "'<' not supported between instances of 'NoneType' and 'int'"
        }
        
        // Test 5: Nullable test for string type
        sql """ DROP FUNCTION IF EXISTS py_string_nullable(STRING); """
        sql """
        CREATE FUNCTION py_string_nullable(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(s):
    if s is None or s == "":
        return None
    return s.upper()
\$\$;
        """
        
        qt_select_string_nullable """ SELECT py_string_nullable('hello') AS result; """
        qt_select_string_nullable_null """ SELECT py_string_nullable(NULL) AS result; """
        qt_select_string_nullable_empty """ SELECT py_string_nullable('') AS result; """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_nullable_true(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_nullable_false(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_nullable_false_returns_none(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_string_nullable(STRING);")
        try_sql("DROP TABLE IF EXISTS nullable_test_table;")
    }
}
