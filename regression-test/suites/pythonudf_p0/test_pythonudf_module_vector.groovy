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

suite("test_pythonudf_module_vector") {
    // Test vectorized Python UDF using module mode with pandas.Series
    
    def pyPath = """${context.file.parent}/udf_scripts/python_udf_vector_ops.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    
    log.info("Python module path: ${pyPath}".toString())
    
    try {
        // Create test table
        sql """ DROP TABLE IF EXISTS vector_module_test_table; """
        sql """
        CREATE TABLE vector_module_test_table (
            id INT,
            int_a INT,
            int_b INT,
            double_a DOUBLE,
            double_b DOUBLE,
            string_a STRING,
            string_b STRING,
            bool_a BOOLEAN,
            bool_b BOOLEAN
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO vector_module_test_table VALUES
        (1, 10, 20, 1.5, 2.5, 'hello world', 'python udf', true, true),
        (2, 30, 15, 3.5, 4.5, 'foo bar', 'test case', false, true),
        (3, 50, 50, 5.5, 2.0, 'data science', 'machine learning', true, false),
        (4, 5, 25, 7.5, 1.5, 'apache doris', 'database system', false, false),
        (5, 100, 10, 9.5, 3.5, 'vector operations', 'pandas series', true, true);
        """
        
        // Test 1: Vector addition with constant
        sql """ DROP FUNCTION IF EXISTS py_vec_add_const(INT, INT); """
        sql """
        CREATE FUNCTION py_vec_add_const(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_add_with_constant",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_add_const """ 
        SELECT 
            id,
            int_a,
            int_b,
            py_vec_add_const(int_a, int_b) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
        // Test 2: Vector multiplication and rounding
        sql """ DROP FUNCTION IF EXISTS py_vec_multiply_round(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_multiply_round(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_multiply_and_round",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_multiply_round """ 
        SELECT 
            id,
            double_a,
            double_b,
            py_vec_multiply_round(double_a, double_b) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
        // Test 3: Vector string concatenation with separator
        sql """ DROP FUNCTION IF EXISTS py_vec_concat_sep(STRING, STRING); """
        sql """
        CREATE FUNCTION py_vec_concat_sep(STRING, STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_string_concat_with_separator",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_concat_sep """ 
        SELECT 
            id,
            string_a,
            string_b,
            py_vec_concat_sep(string_a, string_b) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
        // Test 4: Vector string title case
        sql """ DROP FUNCTION IF EXISTS py_vec_title_case(STRING); """
        sql """
        CREATE FUNCTION py_vec_title_case(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_string_title_case",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_title_case """ 
        SELECT 
            id,
            string_a,
            py_vec_title_case(string_a) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
        // Test 5: Vector conditional value (max of two values)
        sql """ DROP FUNCTION IF EXISTS py_vec_conditional(INT, INT); """
        sql """
        CREATE FUNCTION py_vec_conditional(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_conditional_value",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_conditional """ 
        SELECT 
            id,
            int_a,
            int_b,
            py_vec_conditional(int_a, int_b) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
        // Test 6: Vector percentage calculation
        sql """ DROP FUNCTION IF EXISTS py_vec_percentage(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_percentage(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_percentage_calculation",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_percentage """ 
        SELECT 
            id,
            double_a,
            double_b,
            py_vec_percentage(double_a, double_b) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
        // Test 7: Vector range check
        sql """ DROP FUNCTION IF EXISTS py_vec_in_range(INT, INT, INT); """
        sql """
        CREATE FUNCTION py_vec_in_range(INT, INT, INT) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_is_in_range",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_in_range """ 
        SELECT 
            id,
            int_a,
            py_vec_in_range(int_a, 10, 50) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
        // Test 8: Vector safe division
        sql """ DROP FUNCTION IF EXISTS py_vec_safe_div(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_safe_div(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_safe_divide",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_safe_div """ 
        SELECT 
            id,
            double_a,
            double_b,
            py_vec_safe_div(double_a, double_b) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
        // Test 9: Vector exponential decay
        sql """ DROP FUNCTION IF EXISTS py_vec_exp_decay(DOUBLE, INT); """
        sql """
        CREATE FUNCTION py_vec_exp_decay(DOUBLE, INT) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_exponential_decay",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_exp_decay """ 
        SELECT 
            id,
            double_a,
            int_a,
            py_vec_exp_decay(double_a, int_a) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
        // Test 10: Vector string extract first word
        sql """ DROP FUNCTION IF EXISTS py_vec_first_word(STRING); """
        sql """
        CREATE FUNCTION py_vec_first_word(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_string_extract_first_word",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_first_word """ 
        SELECT 
            id,
            string_a,
            py_vec_first_word(string_a) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
        // Test 11: Vector absolute difference
        sql """ DROP FUNCTION IF EXISTS py_vec_abs_diff(INT, INT); """
        sql """
        CREATE FUNCTION py_vec_abs_diff(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_abs_difference",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_abs_diff """ 
        SELECT 
            id,
            int_a,
            int_b,
            py_vec_abs_diff(int_a, int_b) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
        // Test 12: Vector power operation
        sql """ DROP FUNCTION IF EXISTS py_vec_power(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_power(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_power",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_power """ 
        SELECT 
            id,
            double_a,
            py_vec_power(double_a, 2.0) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
        // Test 13: Vector boolean AND operation
        sql """ DROP FUNCTION IF EXISTS py_vec_bool_and(BOOLEAN, BOOLEAN); """
        sql """
        CREATE FUNCTION py_vec_bool_and(BOOLEAN, BOOLEAN) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_boolean_and",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_bool_and """ 
        SELECT 
            id,
            bool_a,
            bool_b,
            py_vec_bool_and(bool_a, bool_b) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
        // Test 14: Vector boolean OR operation
        sql """ DROP FUNCTION IF EXISTS py_vec_bool_or(BOOLEAN, BOOLEAN); """
        sql """
        CREATE FUNCTION py_vec_bool_or(BOOLEAN, BOOLEAN) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_boolean_or",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_bool_or """ 
        SELECT 
            id,
            bool_a,
            bool_b,
            py_vec_bool_or(bool_a, bool_b) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
        // Test 15: Vector clip values
        sql """ DROP FUNCTION IF EXISTS py_vec_clip(INT, INT, INT); """
        sql """
        CREATE FUNCTION py_vec_clip(INT, INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${pyPath}",
            "symbol" = "python_udf_vector_ops.vec_clip_values",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_vec_clip """ 
        SELECT 
            id,
            int_a,
            py_vec_clip(int_a, 20, 60) AS result
        FROM vector_module_test_table
        ORDER BY id;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_vec_add_const(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_multiply_round(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_concat_sep(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_title_case(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_conditional(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_percentage(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_in_range(INT, INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_safe_div(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_exp_decay(DOUBLE, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_first_word(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_abs_diff(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_power(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_bool_and(BOOLEAN, BOOLEAN);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_bool_or(BOOLEAN, BOOLEAN);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_clip(INT, INT, INT);")
        try_sql("DROP TABLE IF EXISTS vector_module_test_table;")
    }
}
