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

suite("test_pythonudf_data_types") {
    // Test various data types supported by Python UDF
    def runtime_version = "3.8.10"
    
    try {
        // Test 1: TINYINT type
        sql """ DROP FUNCTION IF EXISTS py_tinyint_test(TINYINT); """
        sql """
        CREATE FUNCTION py_tinyint_test(TINYINT) 
        RETURNS TINYINT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x + 1
\$\$;
        """
        
        qt_select_tinyint """ SELECT py_tinyint_test(CAST(10 AS TINYINT)) AS result; """
        
        // Test 2: SMALLINT type
        sql """ DROP FUNCTION IF EXISTS py_smallint_test(SMALLINT); """
        sql """
        CREATE FUNCTION py_smallint_test(SMALLINT) 
        RETURNS SMALLINT 
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
        
        qt_select_smallint """ SELECT py_smallint_test(CAST(1000 AS SMALLINT)) AS result; """
        
        // Test 3: BIGINT type
        sql """ DROP FUNCTION IF EXISTS py_bigint_test(BIGINT); """
        sql """
        CREATE FUNCTION py_bigint_test(BIGINT) 
        RETURNS BIGINT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(x):
    if x is None:
        return None
    return x + 1000000
\$\$;
        """
        
        qt_select_bigint """ SELECT py_bigint_test(1000000000000) AS result; """
        
        // Test 4: DECIMAL type
        sql """ DROP FUNCTION IF EXISTS py_decimal_test(DECIMAL(10,2)); """
        sql """
        CREATE FUNCTION py_decimal_test(DECIMAL(10,2)) 
        RETURNS DECIMAL(10,2) 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(x):
    if x is None:
        return None
    from decimal import Decimal
    return x * Decimal('1.1')
\$\$;
        """
        
        qt_select_decimal """ SELECT py_decimal_test(100.50) AS result; """
        
        // Test 5: DATE type
        sql """ DROP FUNCTION IF EXISTS py_date_test(DATE); """
        sql """
        CREATE FUNCTION py_date_test(DATE) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(d):
    if d is None:
        return None
    return str(d)
\$\$;
        """
        
        qt_select_date """ SELECT py_date_test('2024-01-15') AS result; """
        
        // Test 6: DATETIME type
        sql """ DROP FUNCTION IF EXISTS py_datetime_test(DATETIME); """
        sql """
        CREATE FUNCTION py_datetime_test(DATETIME) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(dt):
    if dt is None:
        return None
    return str(dt)
\$\$;
        """
        
        qt_select_datetime """ SELECT py_datetime_test('2024-01-15 10:30:45') AS result; """
        
        // Test 7: Comprehensive test - create table and test multiple data types
        sql """ DROP TABLE IF EXISTS data_types_test_table; """
        sql """
        CREATE TABLE data_types_test_table (
            id INT,
            tiny_val TINYINT,
            small_val SMALLINT,
            int_val INT,
            big_val BIGINT,
            float_val FLOAT,
            double_val DOUBLE,
            decimal_val DECIMAL(10,2),
            string_val STRING,
            bool_val BOOLEAN
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO data_types_test_table VALUES
        (1, 10, 100, 1000, 10000, 1.5, 2.5, 100.50, 'test1', true),
        (2, 20, 200, 2000, 20000, 2.5, 3.5, 200.75, 'test2', false),
        (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
        """
        
        qt_select_table_types """ 
        SELECT 
            id,
            py_tinyint_test(tiny_val) AS tiny_result,
            py_smallint_test(small_val) AS small_result,
            py_bigint_test(big_val) AS big_result
        FROM data_types_test_table
        ORDER BY id;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_tinyint_test(TINYINT);")
        try_sql("DROP FUNCTION IF EXISTS py_smallint_test(SMALLINT);")
        try_sql("DROP FUNCTION IF EXISTS py_bigint_test(BIGINT);")
        try_sql("DROP FUNCTION IF EXISTS py_decimal_test(DECIMAL(10,2));")
        try_sql("DROP FUNCTION IF EXISTS py_date_test(DATE);")
        try_sql("DROP FUNCTION IF EXISTS py_datetime_test(DATETIME);")
        try_sql("DROP TABLE IF EXISTS data_types_test_table;")
    }
}
