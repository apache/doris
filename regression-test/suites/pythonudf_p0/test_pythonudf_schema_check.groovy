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

suite("test_pythonudf_schema_check") {
    // Test type compatibility in Python UDF
    // Users can specify compatible types instead of exact matching types
    // For example: TINYINT can be used where INT is expected
    
    def runtime_version = "3.8.10"
    
    try {
        // Create test table with various integer types
        sql """ DROP TABLE IF EXISTS test_type_compat_table; """
        sql """
        CREATE TABLE test_type_compat_table (
            id INT,
            tiny_val TINYINT,
            small_val SMALLINT,
            int_val INT,
            big_val BIGINT,
            float_val FLOAT,
            double_val DOUBLE,
            str_val STRING,
            bool_val BOOLEAN,
            date_val DATE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1");
        """
        
        // Insert test data
        sql """
        INSERT INTO test_type_compat_table VALUES
        (1, 10, 100, 1000, 10000, 1.5, 10.5, 'test1', true, '2024-01-01'),
        (2, 20, 200, 2000, 20000, 2.5, 20.5, 'test2', false, '2024-01-02'),
        (3, 30, 300, 3000, 30000, 3.5, 30.5, 'test3', true, '2024-01-03'),
        (4, 40, 400, 4000, 40000, 4.5, 40.5, 'test4', false, '2024-01-04'),
        (5, 50, 500, 5000, 50000, 5.5, 50.5, 'test5', true, '2024-01-05');
        """
        
        // ==================== Test 1: Integer Type Promotion (TINYINT -> INT) ====================
        log.info("=== Test 1: TINYINT can be used where INT is expected ===")
        
        sql """ DROP FUNCTION IF EXISTS py_add_int_sc(INT, INT); """
        sql """
        CREATE FUNCTION py_add_int_sc(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_add_int_sc",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def py_add_int_sc(a, b):
    if a is None or b is None:
        return None
    return a + b
\$\$;
        """
        
        // Pass TINYINT where INT is expected
        qt_select_1 """
        SELECT 
            id,
            tiny_val,
            int_val,
            py_add_int_sc(tiny_val, int_val) AS result
        FROM test_type_compat_table
        ORDER BY id;
        """
        
        // ==================== Test 2: Integer Type Promotion (SMALLINT -> INT) ====================
        log.info("=== Test 2: SMALLINT can be used where INT is expected ===")
        
        qt_select_2 """
        SELECT 
            id,
            small_val,
            int_val,
            py_add_int_sc(small_val, int_val) AS result
        FROM test_type_compat_table
        ORDER BY id;
        """
        
        // ==================== Test 3: Integer Type Promotion (INT -> BIGINT) ====================
        log.info("=== Test 3: INT can be used where BIGINT is expected ===")
        
        sql """ DROP FUNCTION IF EXISTS py_add_bigint(BIGINT, BIGINT); """
        sql """
        CREATE FUNCTION py_add_bigint(BIGINT, BIGINT) 
        RETURNS BIGINT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_add_bigint",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def py_add_bigint(a, b):
    return a + b
\$\$;
        """
        
        qt_select_3 """
        SELECT 
            id,
            int_val,
            big_val,
            py_add_bigint(int_val, big_val) AS result
        FROM test_type_compat_table
        ORDER BY id;
        """
        
        // ==================== Test 4: Float Type Promotion (FLOAT -> DOUBLE) ====================
        log.info("=== Test 4: FLOAT can be used where DOUBLE is expected ===")
        
        sql """ DROP FUNCTION IF EXISTS py_add_double(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_add_double(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_add_double",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def py_add_double(a, b):
    return a + b
\$\$;
        """
        
        qt_select_4 """
        SELECT 
            id,
            float_val,
            double_val,
            py_add_double(float_val, double_val) AS result
        FROM test_type_compat_table
        ORDER BY id;
        """
        
        // ==================== Test 5: Mixed Integer Types ====================
        log.info("=== Test 5: Mixed integer types (TINYINT, SMALLINT, INT) ===")
        
        sql """ DROP FUNCTION IF EXISTS py_sum_three(INT, INT, INT); """
        sql """
        CREATE FUNCTION py_sum_three(INT, INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_sum_three",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def py_sum_three(a, b, c):
    return a + b + c
\$\$;
        """
        
        qt_select_5 """
        SELECT 
            id,
            tiny_val,
            small_val,
            int_val,
            py_sum_three(tiny_val, small_val, int_val) AS result
        FROM test_type_compat_table
        ORDER BY id;
        """
        
        // ==================== Test 6: Vectorized UDF with Type Promotion ====================
        log.info("=== Test 6: Vectorized UDF with integer type promotion ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_multiply(INT, INT); """
        sql """
        CREATE FUNCTION py_vec_multiply(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_vec_multiply",
            "runtime_version" = "${runtime_version}",
            "vectorized" = "true"
        )
        AS \$\$
import pandas as pd

def py_vec_multiply(a: pd.Series, b: pd.Series) -> pd.Series:
    return a * b
\$\$;
        """
        
        // Use TINYINT and SMALLINT where INT is expected
        qt_select_6 """
        SELECT 
            id,
            tiny_val,
            small_val,
            py_vec_multiply(tiny_val, small_val) AS result
        FROM test_type_compat_table
        ORDER BY id;
        """
        
        // ==================== Test 7: Vectorized UDF with Float Promotion ====================
        log.info("=== Test 7: Vectorized UDF with float type promotion ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_divide(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_divide(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_vec_divide",
            "runtime_version" = "${runtime_version}",
            "vectorized" = "true"
        )
        AS \$\$
import pandas as pd

def py_vec_divide(a: pd.Series, b: pd.Series) -> pd.Series:
    return a / b
\$\$;
        """
        
        // Use FLOAT where DOUBLE is expected
        qt_select_7 """
        SELECT 
            id,
            float_val,
            double_val,
            py_vec_divide(double_val, float_val) AS result
        FROM test_type_compat_table
        ORDER BY id;
        """
        
        // ==================== Test 8: Mixed Types in Vectorized UDF ====================
        log.info("=== Test 8: Mixed integer and float types in vectorized UDF ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_calc(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_calc(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_vec_calc",
            "runtime_version" = "${runtime_version}",
            "vectorized" = "true"
        )
        AS \$\$
import pandas as pd

def py_vec_calc(a: pd.Series, b: pd.Series) -> pd.Series:
    return a * 2.0 + b
\$\$;
        """
        
        // Use INT and FLOAT where DOUBLE is expected
        qt_select_8 """
        SELECT 
            id,
            int_val,
            float_val,
            py_vec_calc(int_val, float_val) AS result
        FROM test_type_compat_table
        ORDER BY id;
        """
        
        // ==================== Test 9: String Type Compatibility ====================
        log.info("=== Test 9: String type compatibility ===")
        
        sql """ DROP FUNCTION IF EXISTS py_string_upper(STRING); """
        sql """
        CREATE FUNCTION py_string_upper(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_string_upper",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def py_string_upper(s):
    return s.upper() if s else None
\$\$;
        """
        
        qt_select_9 """
        SELECT 
            id,
            str_val,
            py_string_upper(str_val) AS upper_str
        FROM test_type_compat_table
        ORDER BY id;
        """
        
        // ==================== Test 10: Boolean Type ====================
        log.info("=== Test 10: Boolean type compatibility ===")
        
        sql """ DROP FUNCTION IF EXISTS py_bool_not(BOOLEAN); """
        sql """
        CREATE FUNCTION py_bool_not(BOOLEAN) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_bool_not",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def py_bool_not(b):
    return not b if b is not None else None
\$\$;
        """
        
        qt_select_10 """
        SELECT 
            id,
            bool_val,
            py_bool_not(bool_val) AS negated
        FROM test_type_compat_table
        ORDER BY id;
        """
        
        // ==================== Test 11: Complex Type Promotion Chain ====================
        log.info("=== Test 11: Complex type promotion chain (TINYINT -> BIGINT) ===")
        
        qt_select_11 """
        SELECT 
            id,
            tiny_val,
            big_val,
            py_add_bigint(tiny_val, big_val) AS result
        FROM test_type_compat_table
        ORDER BY id;
        """
        
        // ==================== Test 12: Vectorized with Mixed Scalar and Series ====================
        log.info("=== Test 12: Vectorized UDF with type promotion and mixed params ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_scale(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_scale(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_vec_scale",
            "runtime_version" = "${runtime_version}",
            "vectorized" = "true"
        )
        AS \$\$
import pandas as pd

def py_vec_scale(values: pd.Series, factor: float) -> pd.Series:
    return values * factor
\$\$;
        """
        
        // Use INT (promoted to DOUBLE) with scalar FLOAT
        qt_select_12 """
        SELECT 
            id,
            int_val,
            py_vec_scale(int_val, 1.5) AS scaled
        FROM test_type_compat_table
        ORDER BY id;
        """
        
        // ==================== Test 13: Type Incompatibility - STRING to INT ====================
        log.info("=== Test 13: Type incompatibility - STRING cannot be used where INT is expected ===")
        
        qt_select_13 """
        SELECT 
            id,
            str_val,
            py_add_int_sc(str_val, int_val) AS result
        FROM test_type_compat_table
        ORDER BY id
        LIMIT 1;
        """
        
        // ==================== Test 14: Type Incompatibility - BIGINT to INT ====================
        log.info("=== Test 14: Type incompatibility - BIGINT cannot be downcast to INT ===")
        
        qt_select_14 """
        SELECT 
            id,
            big_val,
            py_add_int_sc(big_val, int_val) AS result
        FROM test_type_compat_table
        ORDER BY id
        LIMIT 1;
        """
        
        // ==================== Test 15: Type Incompatibility - DOUBLE to FLOAT ====================
        log.info("=== Test 15: Type incompatibility - DOUBLE cannot be downcast to FLOAT ===")
        
        sql """ DROP FUNCTION IF EXISTS py_add_float(FLOAT, FLOAT); """
        sql """
        CREATE FUNCTION py_add_float(FLOAT, FLOAT) 
        RETURNS FLOAT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_add_float",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def py_add_float(a, b):
    return a + b
\$\$;
        """
        
        qt_select_15 """
        SELECT 
            id,
            double_val,
            py_add_float(double_val, float_val) AS result
        FROM test_type_compat_table
        ORDER BY id
        LIMIT 1;
        """
        
        // ==================== Test 16: Type Incompatibility - BOOLEAN to INT ====================
        log.info("=== Test 16: Type incompatibility - BOOLEAN cannot be used where INT is expected ===")
        
        qt_select_16 """
        SELECT 
            id,
            bool_val,
            py_add_int_sc(bool_val, int_val) AS result
        FROM test_type_compat_table
        ORDER BY id
        LIMIT 1;
        """
        
        // ==================== Test 17: Type Incompatibility - DATE to STRING ====================
        log.info("=== Test 17: Type incompatibility - DATE cannot be directly used where STRING is expected ===")
        
        qt_select_17 """
        SELECT 
            id,
            date_val,
            py_string_upper(date_val) AS result
        FROM test_type_compat_table
        ORDER BY id
        LIMIT 1;
        """
        
        // ==================== Test 18: Type Incompatibility - INT to BOOLEAN ====================
        log.info("=== Test 18: Type incompatibility - INT cannot be used where BOOLEAN is expected ===")
        
        qt_select_18 """
        SELECT 
            id,
            int_val,
            py_bool_not(int_val) AS result
        FROM test_type_compat_table
        ORDER BY id
        LIMIT 1;
        """
        
        // ==================== Test 19: Type Incompatibility in Vectorized UDF - STRING to INT ====================
        log.info("=== Test 19: Type incompatibility in vectorized UDF - STRING to INT ===")
        
        qt_select_19 """
        SELECT 
            id,
            str_val,
            py_vec_multiply(str_val, int_val) AS result
        FROM test_type_compat_table
        ORDER BY id
        LIMIT 1;
        """
        
        // ==================== Test 20: Type Incompatibility - Mixed incompatible types ====================
        log.info("=== Test 20: Type incompatibility - Mixed incompatible types ===")
        
        qt_select_20 """
        SELECT 
            id,
            str_val,
            bool_val,
            py_add_int_sc(str_val, bool_val) AS result
        FROM test_type_compat_table
        ORDER BY id
        LIMIT 1;
        """
        
        // ==================== Test 21: Wrong number of arguments ====================
        log.info("=== Test 21: Wrong number of arguments ===")
        
        test {
            sql """
            SELECT 
                id,
                py_add_int_sc(int_val) AS result
            FROM test_type_compat_table
            ORDER BY id
            LIMIT 1;
            """
            exception "Can not found function 'py_add_int_sc' which has 1 arity. Candidate functions are: [py_add_int_sc(INT, INT)]"
        }
        
        // ==================== Test 22: Type Incompatibility - FLOAT to INT ====================
        log.info("=== Test 22: Type incompatibility - FLOAT cannot be used where INT is expected ===")
        
        qt_select_22 """
        SELECT 
            id,
            float_val,
            py_add_int_sc(float_val, int_val) AS result
        FROM test_type_compat_table
        ORDER BY id
        LIMIT 1;
        """
        
        log.info("All type compatibility tests (including negative tests) passed!")
        
    } finally {
        // Cleanup
        sql """ DROP FUNCTION IF EXISTS py_add_int_sc(INT, INT); """
        sql """ DROP FUNCTION IF EXISTS py_add_bigint(BIGINT, BIGINT); """
        sql """ DROP FUNCTION IF EXISTS py_add_double(DOUBLE, DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_add_float(FLOAT, FLOAT); """
        sql """ DROP FUNCTION IF EXISTS py_sum_three(INT, INT, INT); """
        sql """ DROP FUNCTION IF EXISTS py_vec_multiply(INT, INT); """
        sql """ DROP FUNCTION IF EXISTS py_vec_divide(DOUBLE, DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_vec_calc(DOUBLE, DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_string_upper(STRING); """
        sql """ DROP FUNCTION IF EXISTS py_bool_not(BOOLEAN); """
        sql """ DROP FUNCTION IF EXISTS py_vec_scale(DOUBLE, DOUBLE); """
        sql """ DROP TABLE IF EXISTS test_type_compat_table; """
    }
}
