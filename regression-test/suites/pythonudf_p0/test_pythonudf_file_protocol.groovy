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

suite("test_pythonudf_file_protocol") {
    // Test loading Python UDF from zip package using file:// protocol
    
    def zipPath = """${context.file.parent}/udf_scripts/pyudf.zip"""
    scp_udf_file_to_all_be(zipPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${zipPath}".toString())
    
    try {
        // Test 1: Load int_test.py from zip package using file:// protocol
        sql """ DROP FUNCTION IF EXISTS py_file_int_add(INT); """
        sql """
        CREATE FUNCTION py_file_int_add(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${zipPath}",
            "symbol" = "int_test.evaluate",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        qt_select_file_int """ SELECT py_file_int_add(99) AS result; """
        
        // Test 2: Load string_test.py from zip package using file:// protocol
        sql """ DROP FUNCTION IF EXISTS py_file_string_mask(STRING, INT, INT); """
        sql """
        CREATE FUNCTION py_file_string_mask(STRING, INT, INT) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${zipPath}",
            "symbol" = "string_test.evaluate",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        qt_select_file_string """ SELECT py_file_string_mask('1234567890', 3, 3) AS result; """
        
        // Test 3: Load float_test.py from zip package using file:// protocol
        sql """ DROP FUNCTION IF EXISTS py_file_float_process(FLOAT); """
        sql """
        CREATE FUNCTION py_file_float_process(FLOAT) 
        RETURNS FLOAT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${zipPath}",
            "symbol" = "float_test.evaluate",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        qt_select_file_float """ SELECT py_file_float_process(3.14) AS result; """
        
        // Test 4: Load boolean_test.py from zip package using file:// protocol
        sql """ DROP FUNCTION IF EXISTS py_file_bool_not(BOOLEAN); """
        sql """
        CREATE FUNCTION py_file_bool_not(BOOLEAN) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${zipPath}",
            "symbol" = "boolean_test.evaluate",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        qt_select_file_bool_true """ SELECT py_file_bool_not(true) AS result; """
        qt_select_file_bool_false """ SELECT py_file_bool_not(false) AS result; """
        
        // Test 5: Test UDF with file:// protocol on table data
        sql """ DROP TABLE IF EXISTS file_protocol_test_table; """
        sql """
        CREATE TABLE file_protocol_test_table (
            id INT,
            num INT,
            text STRING
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO file_protocol_test_table VALUES
        (1, 10, 'hello'),
        (2, 20, 'world'),
        (3, 30, 'python'),
        (4, 40, 'doris');
        """
        
        qt_select_table_file """ 
        SELECT 
            id,
            num,
            py_file_int_add(num) AS num_result,
            text,
            py_file_string_mask(text, 1, 1) AS text_result
        FROM file_protocol_test_table
        ORDER BY id;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_file_int_add(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_file_string_mask(STRING, INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_file_float_process(FLOAT);")
        try_sql("DROP FUNCTION IF EXISTS py_file_bool_not(BOOLEAN);")
        try_sql("DROP TABLE IF EXISTS file_protocol_test_table;")
    }
}
