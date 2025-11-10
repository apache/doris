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

suite("test_pythonudf_performance") {
    // Test Python UDF performance and correctness with large data volumes
    
    def runtime_version = "3.10.12"
    try {
        // Create simple Python UDF
        sql """ DROP FUNCTION IF EXISTS py_perf_double(INT); """
        sql """
        CREATE FUNCTION py_perf_double(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x * 2
\$\$;
        """
        
        sql """ DROP FUNCTION IF EXISTS py_perf_concat(STRING, STRING); """
        sql """
        CREATE FUNCTION py_perf_concat(STRING, STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(s1, s2):
    if s1 is None or s2 is None:
        return None
    return s1 + '_' + s2
\$\$;
        """
        
        // Create test table
        sql """ DROP TABLE IF EXISTS performance_test_table; """
        sql """
        CREATE TABLE performance_test_table (
            id INT,
            value INT,
            category STRING,
            amount DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES("replication_num" = "1");
        """
        
        // Insert test data (large data volume)
        sql """
        INSERT INTO performance_test_table 
        SELECT 
            number AS id,
            number % 1000 AS value,
            CASE 
                WHEN number % 4 = 0 THEN 'A'
                WHEN number % 4 = 1 THEN 'B'
                WHEN number % 4 = 2 THEN 'C'
                ELSE 'D'
            END AS category,
            number * 1.5 AS amount
        FROM numbers("number" = "10000");
        """
        
        // Test 1: Simple UDF call performance
        qt_select_perf_simple """ 
        SELECT COUNT(*) AS total_count
        FROM performance_test_table
        WHERE py_perf_double(value) > 1000;
        """
        
        // Test 2: UDF performance in aggregate queries
        qt_select_perf_aggregate """ 
        SELECT 
            category,
            COUNT(*) AS count,
            AVG(py_perf_double(value)) AS avg_doubled_value
        FROM performance_test_table
        GROUP BY category
        ORDER BY category;
        """
        
        // Test 3: Multiple UDFs combined usage
        qt_select_perf_multiple_udf """ 
        SELECT 
            category,
            COUNT(*) AS count
        FROM performance_test_table
        WHERE py_perf_double(value) > 500
        GROUP BY category
        ORDER BY count DESC
        LIMIT 10;
        """
        
        // Test 4: String UDF performance
        qt_select_perf_string """ 
        SELECT 
            category,
            COUNT(DISTINCT py_perf_concat(category, CAST(value AS STRING))) AS unique_combinations
        FROM performance_test_table
        GROUP BY category
        ORDER BY category;
        """
        
        // Test 5: UDF with complex calculations
        sql """ DROP FUNCTION IF EXISTS py_perf_complex(INT, DOUBLE); """
        sql """
        CREATE FUNCTION py_perf_complex(INT, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(v, a):
    if v is None or a is None:
        return None
    result = (v * 1.5 + a * 0.8) / 2.0
    return result
\$\$;
        """
        
        qt_select_perf_complex """ 
        SELECT 
            category,
            AVG(py_perf_complex(value, amount)) AS avg_complex_result,
            MAX(py_perf_complex(value, amount)) AS max_complex_result,
            MIN(py_perf_complex(value, amount)) AS min_complex_result
        FROM performance_test_table
        GROUP BY category
        ORDER BY category;
        """
        
        // Test 6: UDF in nested queries
        qt_select_perf_nested """ 
        SELECT 
            category,
            avg_doubled
        FROM (
            SELECT 
                category,
                AVG(py_perf_double(value)) AS avg_doubled
            FROM performance_test_table
            GROUP BY category
        ) t
        WHERE avg_doubled > 500
        ORDER BY avg_doubled DESC;
        """
        
        // Test 7: Performance test with NULL value handling
        sql """ DROP TABLE IF EXISTS performance_null_test; """
        sql """
        CREATE TABLE performance_null_test (
            id INT,
            value INT,
            nullable_value INT
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO performance_null_test 
        SELECT 
            number AS id,
            number % 100 AS value,
            CASE WHEN number % 5 = 0 THEN NULL ELSE number % 50 END AS nullable_value
        FROM numbers("number" = "5000");
        """
        
        qt_select_perf_null """ 
        SELECT 
            COUNT(*) AS total,
            COUNT(py_perf_double(nullable_value)) AS non_null_count,
            AVG(py_perf_double(nullable_value)) AS avg_result
        FROM performance_null_test;
        """
        
        // Test 8: Sorting performance
        qt_select_perf_order """ 
        SELECT 
            id,
            value,
            py_perf_double(value) AS doubled_value
        FROM performance_test_table
        ORDER BY py_perf_double(value) DESC, id DESC
        LIMIT 20;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_perf_double(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_perf_concat(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_perf_complex(INT, DOUBLE);")
        try_sql("DROP TABLE IF EXISTS performance_test_table;")
        try_sql("DROP TABLE IF EXISTS performance_null_test;")
    }
}
