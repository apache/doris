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

suite("test_pythonudf_inline_vector") {
    // Test vectorized Python UDF using Inline mode with pandas.Series
    
    def runtime_version = "3.8.10"
    try {
        // Create test table
        sql """ DROP TABLE IF EXISTS vector_udf_test_table; """
        sql """
        CREATE TABLE vector_udf_test_table (
            id INT,
            int_col1 INT,
            int_col2 INT,
            double_col1 DOUBLE,
            double_col2 DOUBLE,
            string_col1 STRING,
            string_col2 STRING,
            bool_col BOOLEAN
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO vector_udf_test_table VALUES
        (1, 10, 20, 1.5, 2.5, 'hello', 'world', true),
        (2, 30, 40, 3.5, 4.5, 'foo', 'bar', false),
        (3, NULL, 50, 5.5, NULL, NULL, 'test', true),
        (4, 60, NULL, NULL, 6.5, 'data', NULL, false),
        (5, 70, 80, 7.5, 8.5, 'python', 'udf', true);
        """
        
        // Test 1: Vector INT addition with pandas.Series
        sql """ DROP FUNCTION IF EXISTS py_vec_add_int(INT, INT); """
        sql """
        CREATE FUNCTION py_vec_add_int(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "add",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def add(a: pd.Series, b: pd.Series) -> pd.Series:
    return a + b + 1
\$\$;
        """
        
        qt_vec_add_int """ 
        SELECT 
            id,
            int_col1,
            int_col2,
            py_vec_add_int(int_col1, int_col2) AS result
        FROM vector_udf_test_table
        ORDER BY id;
        """
        
        // Test 2: Vector DOUBLE multiplication with pandas.Series
        sql """ DROP FUNCTION IF EXISTS py_vec_multiply_double(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_multiply_double(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "multiply",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def multiply(a: pd.Series, b: pd.Series) -> pd.Series:
    return a * b
\$\$;
        """
        
        qt_vec_multiply_double """ 
        SELECT 
            id,
            double_col1,
            double_col2,
            py_vec_multiply_double(double_col1, double_col2) AS result
        FROM vector_udf_test_table
        ORDER BY id;
        """
        
        // Test 3: Vector STRING concatenation with pandas.Series
        sql """ DROP FUNCTION IF EXISTS py_vec_concat_string(STRING, STRING); """
        sql """
        CREATE FUNCTION py_vec_concat_string(STRING, STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "concat",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def concat(s1: pd.Series, s2: pd.Series) -> pd.Series:
    return s1 + '_' + s2
\$\$;
        """
        
        qt_vec_concat_string """ 
        SELECT 
            id,
            string_col1,
            string_col2,
            py_vec_concat_string(string_col1, string_col2) AS result
        FROM vector_udf_test_table
        ORDER BY id;
        """
        
        // Test 4: Vector INT with conditional logic using pandas.Series
        sql """ DROP FUNCTION IF EXISTS py_vec_max_int(INT, INT); """
        sql """
        CREATE FUNCTION py_vec_max_int(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "get_max",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd
import numpy as np

def get_max(a: pd.Series, b: pd.Series) -> pd.Series:
    return pd.Series(np.maximum(a, b))
\$\$;
        """
        
        qt_vec_max_int """ 
        SELECT 
            id,
            int_col1,
            int_col2,
            py_vec_max_int(int_col1, int_col2) AS result
        FROM vector_udf_test_table
        ORDER BY id;
        """
        
        // Test 5: Vector DOUBLE with mathematical operations
        sql """ DROP FUNCTION IF EXISTS py_vec_sqrt_double(DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_sqrt_double(DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "sqrt",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd
import numpy as np

def sqrt(x: pd.Series) -> pd.Series:
    return np.sqrt(x)
\$\$;
        """
        
        qt_vec_sqrt_double """ 
        SELECT 
            id,
            double_col1,
            py_vec_sqrt_double(double_col1) AS result
        FROM vector_udf_test_table
        ORDER BY id;
        """
        
        // Test 6: Vector STRING with upper case transformation
        sql """ DROP FUNCTION IF EXISTS py_vec_upper_string(STRING); """
        sql """
        CREATE FUNCTION py_vec_upper_string(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "to_upper",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def to_upper(s: pd.Series) -> pd.Series:
    return s.str.upper()
\$\$;
        """
        
        qt_vec_upper_string """ 
        SELECT 
            id,
            string_col1,
            py_vec_upper_string(string_col1) AS result
        FROM vector_udf_test_table
        ORDER BY id;
        """
        
        // Test 7: Vector INT with complex calculation
        sql """ DROP FUNCTION IF EXISTS py_vec_weighted_sum(INT, INT); """
        sql """
        CREATE FUNCTION py_vec_weighted_sum(INT, INT) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "weighted_sum",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def weighted_sum(a: pd.Series, b: pd.Series) -> pd.Series:
    return a * 0.3 + b * 0.7
\$\$;
        """
        
        qt_vec_weighted_sum """ 
        SELECT 
            id,
            int_col1,
            int_col2,
            py_vec_weighted_sum(int_col1, int_col2) AS result
        FROM vector_udf_test_table
        ORDER BY id;
        """
        
        // Test 8: Vector BOOLEAN operations
        sql """ DROP FUNCTION IF EXISTS py_vec_not_bool(BOOLEAN); """
        sql """
        CREATE FUNCTION py_vec_not_bool(BOOLEAN) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "negate",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def negate(b: pd.Series) -> pd.Series:
    return ~b
\$\$;
        """
        
        qt_vec_not_bool """ 
        SELECT 
            id,
            bool_col,
            py_vec_not_bool(bool_col) AS result
        FROM vector_udf_test_table
        ORDER BY id;
        """
        
        // Test 9: Vector INT comparison returning BOOLEAN
        sql """ DROP FUNCTION IF EXISTS py_vec_greater_than(INT, INT); """
        sql """
        CREATE FUNCTION py_vec_greater_than(INT, INT) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "greater",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def greater(a: pd.Series, b: pd.Series) -> pd.Series:
    return a > b
\$\$;
        """
        
        qt_vec_greater_than """ 
        SELECT 
            id,
            int_col1,
            int_col2,
            py_vec_greater_than(int_col1, int_col2) AS result
        FROM vector_udf_test_table
        ORDER BY id;
        """
        
        // Test 10: Vector STRING length calculation
        sql """ DROP FUNCTION IF EXISTS py_vec_string_length(STRING); """
        sql """
        CREATE FUNCTION py_vec_string_length(STRING) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "str_len",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def str_len(s: pd.Series) -> pd.Series:
    return s.str.len()
\$\$;
        """
        
        qt_vec_string_length """ 
        SELECT 
            id,
            string_col1,
            py_vec_string_length(string_col1) AS result
        FROM vector_udf_test_table
        ORDER BY id;
        """
        
        // Test 11: Vector with NULL handling using fillna
        sql """ DROP FUNCTION IF EXISTS py_vec_fill_null_int(INT); """
        sql """
        CREATE FUNCTION py_vec_fill_null_int(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "fill_null",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def fill_null(x: pd.Series) -> pd.Series:
    return x.fillna(0)
\$\$;
        """
        
        qt_vec_fill_null_int """ 
        SELECT 
            id,
            int_col1,
            py_vec_fill_null_int(int_col1) AS result
        FROM vector_udf_test_table
        ORDER BY id;
        """
        
        // Test 12: Vector with aggregation-like operation (cumulative sum)
        sql """ DROP FUNCTION IF EXISTS py_vec_cumsum_int(INT); """
        sql """
        CREATE FUNCTION py_vec_cumsum_int(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "cumsum",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def cumsum(x: pd.Series) -> pd.Series:
    return x.cumsum()
\$\$;
        """
        
        qt_vec_cumsum_int """ 
        SELECT 
            id,
            int_col1,
            py_vec_cumsum_int(int_col1) AS result
        FROM vector_udf_test_table
        ORDER BY id;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_vec_add_int(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_multiply_double(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_concat_string(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_max_int(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_sqrt_double(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_upper_string(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_weighted_sum(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_not_bool(BOOLEAN);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_greater_than(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_string_length(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_fill_null_int(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_vec_cumsum_int(INT);")
        try_sql("DROP TABLE IF EXISTS vector_udf_test_table;")
    }
}
