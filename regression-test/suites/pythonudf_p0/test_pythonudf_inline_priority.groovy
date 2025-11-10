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

suite("test_pythonudf_inline_priority") {
    // Test that inline code has higher priority when both file and inline code are specified
    
    // Disabled temporarily
    return
    
    def zipPath = """${context.file.parent}/udf_scripts/pyudf.zip"""
    scp_udf_file_to_all_be(zipPath)
    def runtime_version = "3.10.12"
    log.info("Python zip path: ${zipPath}".toString())
    
    try {
        // Test 1: Specify both file and inline code, verify inline code takes priority
        // Function in int_test.py returns arg + 1
        // But inline code returns arg * 10
        sql """ DROP FUNCTION IF EXISTS py_priority_test(INT); """
        sql """
        CREATE FUNCTION py_priority_test(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${zipPath}",
            "symbol" = "int_test.evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(arg):
    # inline code: returns arg * 10
    if arg is None:
        return None
    return arg * 10
\$\$;
        """
        
        // If using code from file, result should be 6 (5 + 1)
        // If using inline code, result should be 50 (5 * 10)
        qt_select_priority_inline """ SELECT py_priority_test(5) AS result; """
        
        // Test 2: Another priority test - string processing
        sql """ DROP FUNCTION IF EXISTS py_priority_string_test(STRING); """
        sql """
        CREATE FUNCTION py_priority_string_test(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(s):
    # inline code: returns reversed string
    if s is None:
        return None
    return s[::-1]
\$\$;
        """
        
        // inline code should return reversed string
        qt_select_priority_string """ SELECT py_priority_string_test('hello') AS result; """
        
        // Test 3: Verify priority on table data
        sql """ DROP TABLE IF EXISTS priority_test_table; """
        sql """
        CREATE TABLE priority_test_table (
            id INT,
            num INT
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO priority_test_table VALUES
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4),
        (5, 5);
        """
        
        // Verify inline code priority: should return num * 10
        qt_select_table_priority """ 
        SELECT 
            id,
            num,
            py_priority_test(num) AS result
        FROM priority_test_table
        ORDER BY id;
        """
        
        // Test 4: Only file parameter, no inline code
        sql """ DROP FUNCTION IF EXISTS py_file_only_test(INT); """
        sql """
        CREATE FUNCTION py_file_only_test(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${zipPath}",
            "symbol" = "int_test.evaluate",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        // Should use code from file: returns arg + 1
        qt_select_file_only """ SELECT py_file_only_test(5) AS result; """
        
        // Test 5: Only inline code, no file parameter
        sql """ DROP FUNCTION IF EXISTS py_inline_only_test(INT); """
        sql """
        CREATE FUNCTION py_inline_only_test(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(arg):
    if arg is None:
        return None
    return arg * 100
\$\$;
        """
        
        // Should use inline code: returns arg * 100
        qt_select_inline_only """ SELECT py_inline_only_test(5) AS result; """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_priority_test(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_priority_string_test(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_file_only_test(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_inline_only_test(INT);")
        try_sql("DROP TABLE IF EXISTS priority_test_table;")
    }
}
