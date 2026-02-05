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

suite("test_pythonudaf_data_types") {
    // Test Python UDAFs with various data types
    // Including: TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, STRING, DATE, DATETIME
    
    def runtime_version = "3.8.10"
    
    try {
        // Create test table with various data types
        sql """ DROP TABLE IF EXISTS data_types_test; """
        sql """
        CREATE TABLE data_types_test (
            id INT,
            tiny_val TINYINT,
            small_val SMALLINT,
            int_val INT,
            big_val BIGINT,
            float_val FLOAT,
            double_val DOUBLE,
            decimal_val DECIMAL(10, 2),
            str_val STRING,
            date_val DATE,
            bool_val BOOLEAN
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO data_types_test VALUES
        (1, 10, 100, 1000, 10000, 1.5, 10.55, 100.50, 'apple', '2024-01-01', true),
        (2, 20, 200, 2000, 20000, 2.5, 20.55, 200.50, 'banana', '2024-01-02', false),
        (3, 30, 300, 3000, 30000, 3.5, 30.55, 300.50, 'cherry', '2024-01-03', true),
        (4, 40, 400, 4000, 40000, 4.5, 40.55, 400.50, 'date', '2024-01-04', true),
        (5, 50, 500, 5000, 50000, 5.5, 50.55, 500.50, 'elderberry', '2024-01-05', false);
        """
        
        qt_select_data """ SELECT * FROM data_types_test ORDER BY id; """
        
        // ========================================
        // Test 1: TINYINT aggregation
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_sum_tinyint(TINYINT); """
        sql """
        CREATE AGGREGATE FUNCTION py_sum_tinyint(TINYINT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SumTinyIntUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class SumTinyIntUDAF:
    def __init__(self):
        self.sum = 0
    
    @property
    def aggregate_state(self):
        return self.sum
    
    def accumulate(self, value):
        if value is not None:
            self.sum += value
    
    def merge(self, other_state):
        self.sum += other_state
    
    def finish(self):
        return self.sum
\$\$;
        """
        
        qt_tinyint """ SELECT py_sum_tinyint(tiny_val) as sum_tiny FROM data_types_test; """
        
        // ========================================
        // Test 2: SMALLINT aggregation
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_sum_smallint(SMALLINT); """
        sql """
        CREATE AGGREGATE FUNCTION py_sum_smallint(SMALLINT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SumSmallIntUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class SumSmallIntUDAF:
    def __init__(self):
        self.sum = 0
    
    @property
    def aggregate_state(self):
        return self.sum
    
    def accumulate(self, value):
        if value is not None:
            self.sum += value
    
    def merge(self, other_state):
        self.sum += other_state
    
    def finish(self):
        return self.sum
\$\$;
        """
        
        qt_smallint """ SELECT py_sum_smallint(small_val) as sum_small FROM data_types_test; """
        
        // ========================================
        // Test 3: BIGINT aggregation
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_sum_bigint(BIGINT); """
        sql """
        CREATE AGGREGATE FUNCTION py_sum_bigint(BIGINT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SumBigIntUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class SumBigIntUDAF:
    def __init__(self):
        self.sum = 0
    
    @property
    def aggregate_state(self):
        return self.sum
    
    def accumulate(self, value):
        if value is not None:
            self.sum += value
    
    def merge(self, other_state):
        self.sum += other_state
    
    def finish(self):
        return self.sum
\$\$;
        """
        
        qt_bigint """ SELECT py_sum_bigint(big_val) as sum_big FROM data_types_test; """
        
        // ========================================
        // Test 4: FLOAT aggregation
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_sum_float(FLOAT); """
        sql """
        CREATE AGGREGATE FUNCTION py_sum_float(FLOAT)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SumFloatUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class SumFloatUDAF:
    def __init__(self):
        self.sum = 0.0
    
    @property
    def aggregate_state(self):
        return self.sum
    
    def accumulate(self, value):
        if value is not None:
            self.sum += float(value)
    
    def merge(self, other_state):
        self.sum += other_state
    
    def finish(self):
        return self.sum
\$\$;
        """
        
        qt_float """ SELECT py_sum_float(float_val) as sum_float FROM data_types_test; """
        
        // ========================================
        // Test 5: DECIMAL aggregation
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_sum_decimal(DECIMAL); """
        sql """
        CREATE AGGREGATE FUNCTION py_sum_decimal(DECIMAL)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SumDecimalUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class SumDecimalUDAF:
    def __init__(self):
        self.sum = 0.0
    
    @property
    def aggregate_state(self):
        return self.sum
    
    def accumulate(self, value):
        if value is not None:
            self.sum += float(value)
    
    def merge(self, other_state):
        self.sum += other_state
    
    def finish(self):
        return self.sum
\$\$;
        """
        
        qt_decimal """ SELECT py_sum_decimal(decimal_val) as sum_decimal FROM data_types_test; """
        
        // ========================================
        // Test 6: STRING concatenation
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_concat_str(STRING); """
        sql """
        CREATE AGGREGATE FUNCTION py_concat_str(STRING)
        RETURNS STRING
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "ConcatStrUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class ConcatStrUDAF:
    def __init__(self):
        self.values = []
    
    @property
    def aggregate_state(self):
        return self.values
    
    def accumulate(self, value):
        if value is not None:
            self.values.append(value)
    
    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state)
    
    def finish(self):
        if not self.values:
            return None
        return ','.join(sorted(self.values))
\$\$;
        """
        
        qt_string """ SELECT py_concat_str(str_val) as concat_result FROM data_types_test; """
        
        // ========================================
        // Test 7: BOOLEAN count
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_count_true(BOOLEAN); """
        sql """
        CREATE AGGREGATE FUNCTION py_count_true(BOOLEAN)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "CountTrueUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class CountTrueUDAF:
    def __init__(self):
        self.count = 0
    
    @property
    def aggregate_state(self):
        return self.count
    
    def accumulate(self, value):
        if value is True:
            self.count += 1
    
    def merge(self, other_state):
        self.count += other_state
    
    def finish(self):
        return self.count
\$\$;
        """
        
        qt_boolean """ SELECT py_count_true(bool_val) as true_count FROM data_types_test; """
        
        // ========================================
        // Test 8: Mixed data types in one query
        // ========================================
        qt_mixed_types """
            SELECT 
                py_sum_tinyint(tiny_val) as sum_tiny,
                py_sum_smallint(small_val) as sum_small,
                py_sum_bigint(big_val) as sum_big,
                py_sum_float(float_val) as sum_float,
                py_sum_decimal(decimal_val) as sum_decimal,
                py_concat_str(str_val) as concat_str,
                py_count_true(bool_val) as true_count
            FROM data_types_test;
        """
        
        // ========================================
        // Test 9: Type conversion in UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_int_to_double_sum(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_int_to_double_sum(INT)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "IntToDoubleSumUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class IntToDoubleSumUDAF:
    def __init__(self):
        self.sum = 0.0
    
    @property
    def aggregate_state(self):
        return self.sum
    
    def accumulate(self, value):
        if value is not None:
            self.sum += float(value)
    
    def merge(self, other_state):
        self.sum += other_state
    
    def finish(self):
        return self.sum
\$\$;
        """
        
        qt_type_conversion """ SELECT py_int_to_double_sum(int_val) as sum_as_double FROM data_types_test; """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_sum_tinyint(TINYINT);")
        try_sql("DROP FUNCTION IF EXISTS py_sum_smallint(SMALLINT);")
        try_sql("DROP FUNCTION IF EXISTS py_sum_bigint(BIGINT);")
        try_sql("DROP FUNCTION IF EXISTS py_sum_float(FLOAT);")
        try_sql("DROP FUNCTION IF EXISTS py_sum_decimal(DECIMAL);")
        try_sql("DROP FUNCTION IF EXISTS py_concat_str(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_count_true(BOOLEAN);")
        try_sql("DROP FUNCTION IF EXISTS py_int_to_double_sum(INT);")
        try_sql("DROP TABLE IF EXISTS data_types_test;")
    }
}
