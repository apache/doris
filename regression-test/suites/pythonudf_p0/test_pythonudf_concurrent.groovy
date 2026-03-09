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

suite("test_pythonudf_concurrent") {
    // Test multiple Python UDFs executing concurrently in the same SQL query
    
    def runtime_version = "3.8.10"
    
    try {
        // Create test table
        sql """ DROP TABLE IF EXISTS concurrent_udf_test; """
        sql """
        CREATE TABLE concurrent_udf_test (
            id INT,
            value1 INT,
            value2 INT,
            value3 DOUBLE,
            value4 DOUBLE,
            str1 STRING,
            str2 STRING
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO concurrent_udf_test VALUES
        (1, 10, 20, 1.5, 2.5, 'hello', 'world'),
        (2, 30, 40, 3.5, 4.5, 'foo', 'bar'),
        (3, 50, 60, 5.5, 6.5, 'test', 'case'),
        (4, 70, 80, 7.5, 8.5, 'python', 'udf'),
        (5, 90, 100, 9.5, 10.5, 'doris', 'db');
        """
        
        // Create multiple scalar UDFs with different operations
        
        // UDF 1: Integer addition
        sql """ DROP FUNCTION IF EXISTS py_add_int(INT, INT); """
        sql """
        CREATE FUNCTION py_add_int(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "add_int",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
def add_int(a, b):
    if a is None or b is None:
        return None
    return a + b
\$\$;
        """
        
        // UDF 2: Integer multiplication
        sql """ DROP FUNCTION IF EXISTS py_multiply_int(INT, INT); """
        sql """
        CREATE FUNCTION py_multiply_int(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "multiply_int",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
def multiply_int(a, b):
    if a is None or b is None:
        return None
    return a * b
\$\$;
        """
        
        // UDF 3: Double division
        sql """ DROP FUNCTION IF EXISTS py_divide_double(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_divide_double(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "divide_double",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
def divide_double(a, b):
    if a is None or b is None or b == 0:
        return None
    return a / b
\$\$;
        """
        
        // UDF 4: String concatenation
        sql """ DROP FUNCTION IF EXISTS py_concat_str(STRING, STRING); """
        sql """
        CREATE FUNCTION py_concat_str(STRING, STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "concat_str",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
def concat_str(s1, s2):
    if s1 is None or s2 is None:
        return None
    return s1 + '_' + s2
\$\$;
        """
        
        // UDF 5: String length
        sql """ DROP FUNCTION IF EXISTS py_str_len(STRING); """
        sql """
        CREATE FUNCTION py_str_len(STRING) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "str_len",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
def str_len(s):
    if s is None:
        return None
    return len(s)
\$\$;
        """
        
        // Test 1: Multiple scalar UDFs in SELECT clause
        qt_concurrent_scalar_1 """
        SELECT 
            id,
            py_add_int(value1, value2) AS add_result,
            py_multiply_int(value1, value2) AS multiply_result,
            py_divide_double(value3, value4) AS divide_result,
            py_concat_str(str1, str2) AS concat_result,
            py_str_len(str1) AS len_result
        FROM concurrent_udf_test
        ORDER BY id;
        """
        
        // Test 2: Multiple scalar UDFs with nested calls
        qt_concurrent_scalar_2 """
        SELECT 
            id,
            py_add_int(py_multiply_int(value1, 2), value2) AS nested_result1,
            py_str_len(py_concat_str(str1, str2)) AS nested_result2
        FROM concurrent_udf_test
        ORDER BY id;
        """
        
        // Test 3: Multiple scalar UDFs in WHERE clause
        qt_concurrent_scalar_3 """
        SELECT 
            id,
            value1,
            value2
        FROM concurrent_udf_test
        WHERE py_add_int(value1, value2) > 50 
          AND py_str_len(str1) > 3
        ORDER BY id;
        """
        
        // Test 4: Multiple vectorized UDFs (using pandas)
        sql """ DROP FUNCTION IF EXISTS py_vec_add(INT, INT); """
        sql """
        CREATE FUNCTION py_vec_add(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "vec_add",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def vec_add(a: pd.Series, b: pd.Series) -> pd.Series:
    return a + b
\$\$;
        """
        
        sql """ DROP FUNCTION IF EXISTS py_vec_sub(INT, INT); """
        sql """
        CREATE FUNCTION py_vec_sub(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "vec_sub",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def vec_sub(a: pd.Series, b: pd.Series) -> pd.Series:
    return a - b
\$\$;
        """
        
        sql """ DROP FUNCTION IF EXISTS py_vec_max(INT, INT); """
        sql """
        CREATE FUNCTION py_vec_max(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "vec_max",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd
import numpy as np

def vec_max(a: pd.Series, b: pd.Series) -> pd.Series:
    return pd.Series(np.maximum(a, b))
\$\$;
        """
        
        // Test 5: Multiple vectorized UDFs in the same query
        qt_concurrent_vector_1 """
        SELECT 
            id,
            py_vec_add(value1, value2) AS vec_add_result,
            py_vec_sub(value2, value1) AS vec_sub_result,
            py_vec_max(value1, value2) AS vec_max_result
        FROM concurrent_udf_test
        ORDER BY id;
        """
        
        // Test 6: Mix of scalar and vectorized UDFs
        qt_concurrent_mixed_1 """
        SELECT 
            id,
            py_add_int(value1, value2) AS scalar_add,
            py_vec_add(value1, value2) AS vector_add,
            py_concat_str(str1, str2) AS scalar_concat,
            py_multiply_int(value1, 10) AS scalar_mul
        FROM concurrent_udf_test
        ORDER BY id;
        """
        
        // Test 7: Multiple UDFs with aggregation
        qt_concurrent_with_agg """
        SELECT 
            COUNT(*) AS total_count,
            SUM(py_add_int(value1, value2)) AS sum_add,
            AVG(py_multiply_int(value1, 2)) AS avg_mul,
            MAX(py_str_len(str1)) AS max_len
        FROM concurrent_udf_test;
        """
        
        // Test 8: Multiple UDFs with GROUP BY
        qt_concurrent_group_by """
        SELECT 
            CASE WHEN id <= 3 THEN 'group1' ELSE 'group2' END AS grp,
            COUNT(*) AS cnt,
            SUM(py_add_int(value1, value2)) AS sum_result,
            AVG(py_divide_double(value3, value4)) AS avg_result
        FROM concurrent_udf_test
        GROUP BY grp
        ORDER BY grp;
        """
        
        // Test 9: Multiple UDFs with different parameter types
        sql """ DROP FUNCTION IF EXISTS py_int_to_str(INT); """
        sql """
        CREATE FUNCTION py_int_to_str(INT) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "int_to_str",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
def int_to_str(n):
    if n is None:
        return None
    return str(n)
\$\$;
        """
        
        qt_concurrent_type_mix """
        SELECT 
            id,
            py_add_int(value1, value2) AS int_result,
            py_divide_double(value3, value4) AS double_result,
            py_concat_str(py_int_to_str(value1), str1) AS mixed_result
        FROM concurrent_udf_test
        ORDER BY id;
        """
        
        // Test 10: Stress test - many UDFs in one query
        qt_concurrent_stress """
        SELECT 
            id,
            py_add_int(value1, value2) AS r1,
            py_multiply_int(value1, value2) AS r2,
            py_add_int(value1, 100) AS r3,
            py_multiply_int(value2, 5) AS r4,
            py_divide_double(value3, value4) AS r5,
            py_divide_double(value4, value3) AS r6,
            py_concat_str(str1, str2) AS r7,
            py_str_len(str1) AS r8,
            py_str_len(str2) AS r9,
            py_int_to_str(value1) AS r10
        FROM concurrent_udf_test
        ORDER BY id;
        """
        
    } finally {
        // Cleanup
        try_sql("DROP FUNCTION IF EXISTS py_add_int(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_multiply_int(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_divide_double(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_concat_str(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_str_len(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_add(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_sub(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_max(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_int_to_str(INT);")
        try_sql("DROP TABLE IF EXISTS concurrent_udf_test;")
    }
}


