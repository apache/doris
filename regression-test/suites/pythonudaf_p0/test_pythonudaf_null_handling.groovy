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

suite("test_pythonudaf_null_handling") {
    // Test NULL handling in Python UDAFs
    // This is critical for data quality and edge cases
    
    def runtime_version = "3.8.10"
    
    try {
        // Create test table with NULLs
        sql """ DROP TABLE IF EXISTS null_test; """
        sql """
        CREATE TABLE null_test (
            id INT,
            category STRING,
            int_val INT,
            double_val DOUBLE,
            str_val STRING
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO null_test VALUES
        (1, 'A', 10, 10.5, 'apple'),
        (2, 'A', NULL, 20.3, 'banana'),
        (3, 'A', 30, NULL, NULL),
        (4, 'A', NULL, NULL, 'cherry'),
        (5, 'B', 40, 40.2, NULL),
        (6, 'B', NULL, NULL, NULL),
        (7, 'B', 60, 60.8, 'date'),
        (8, 'C', NULL, NULL, NULL),
        (9, 'C', NULL, NULL, NULL),
        (10, 'C', NULL, NULL, NULL);
        """
        
        qt_select_data """ SELECT * FROM null_test ORDER BY id; """
        
        // ========================================
        // Test 1: Count with NULL handling
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_count_nonnull(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_count_nonnull(INT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "CountNonNullUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class CountNonNullUDAF:
    def __init__(self):
        self.count = 0
    
    @property
    def aggregate_state(self):
        return self.count
    
    def accumulate(self, value):
        if value is not None:
            self.count += 1
    
    def merge(self, other_state):
        self.count += other_state
    
    def finish(self):
        return self.count
\$\$;
        """
        
        qt_count_null_all """ SELECT py_count_nonnull(int_val) as count_nonnull FROM null_test; """
        qt_count_null_group """ SELECT category, py_count_nonnull(int_val) as count_nonnull 
                                FROM null_test GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 2: Sum with NULL handling
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_sum_null(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_sum_null(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SumNullUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class SumNullUDAF:
    def __init__(self):
        self.sum = 0.0
        self.has_value = False
    
    @property
    def aggregate_state(self):
        return (self.sum, self.has_value)
    
    def accumulate(self, value):
        if value is not None:
            self.sum += value
            self.has_value = True
    
    def merge(self, other_state):
        other_sum, other_has_value = other_state
        self.sum += other_sum
        self.has_value = self.has_value or other_has_value
    
    def finish(self):
        return self.sum if self.has_value else None
\$\$;
        """
        
        qt_sum_null_all """ SELECT py_sum_null(double_val) as sum_result FROM null_test; """
        qt_sum_null_group """ SELECT category, py_sum_null(double_val) as sum_result 
                              FROM null_test GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 3: First Non-NULL Value UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_first_nonnull(STRING); """
        sql """
        CREATE AGGREGATE FUNCTION py_first_nonnull(STRING)
        RETURNS STRING
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "FirstNonNullUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class FirstNonNullUDAF:
    def __init__(self):
        self.first_value = None
    
    @property
    def aggregate_state(self):
        return self.first_value
    
    def accumulate(self, value):
        if self.first_value is None and value is not None:
            self.first_value = value
    
    def merge(self, other_state):
        if self.first_value is None and other_state is not None:
            self.first_value = other_state
    
    def finish(self):
        return self.first_value
\$\$;
        """
        
        qt_first_nonnull_all """ SELECT py_first_nonnull(str_val) as first_str FROM null_test; """
        qt_first_nonnull_group """ SELECT category, py_first_nonnull(str_val) as first_str 
                                   FROM null_test GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 4: NULL Count UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_count_null(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_count_null(INT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "CountNullUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class CountNullUDAF:
    def __init__(self):
        self.null_count = 0
    
    @property
    def aggregate_state(self):
        return self.null_count
    
    def accumulate(self, value):
        if value is None:
            self.null_count += 1
    
    def merge(self, other_state):
        self.null_count += other_state
    
    def finish(self):
        return self.null_count
\$\$;
        """
        
        qt_null_count_all """ SELECT py_count_null(int_val) as null_count FROM null_test; """
        qt_null_count_group """ SELECT category, py_count_null(int_val) as null_count 
                                FROM null_test GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 5: Coalesce Average (ignore NULLs)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_avg_coalesce(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_avg_coalesce(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "AvgCoalesceUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class AvgCoalesceUDAF:
    def __init__(self):
        self.sum = 0.0
        self.count = 0
    
    @property
    def aggregate_state(self):
        return (self.sum, self.count)
    
    def accumulate(self, value):
        if value is not None:
            self.sum += value
            self.count += 1
    
    def merge(self, other_state):
        other_sum, other_count = other_state
        self.sum += other_sum
        self.count += other_count
    
    def finish(self):
        if self.count == 0:
            return None
        return self.sum / self.count
\$\$;
        """
        
        qt_avg_coalesce_all """ SELECT py_avg_coalesce(double_val) as avg_result FROM null_test; """
        qt_avg_coalesce_group """ SELECT category, py_avg_coalesce(double_val) as avg_result 
                                  FROM null_test GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 6: All NULLs scenario
        // ========================================
        sql """ DROP TABLE IF EXISTS all_null_test; """
        sql """
        CREATE TABLE all_null_test (
            id INT,
            val INT
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO all_null_test VALUES
        (1, NULL),
        (2, NULL),
        (3, NULL);
        """
        
        qt_all_null_count """ SELECT py_count_nonnull(val) as count_result FROM all_null_test; """
        qt_all_null_sum """ SELECT py_sum_null(CAST(val AS DOUBLE)) as sum_result FROM all_null_test; """
        
        // ========================================
        // Test 7: Empty table scenario
        // ========================================
        sql """ DROP TABLE IF EXISTS empty_test; """
        sql """
        CREATE TABLE empty_test (
            id INT,
            val INT
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        qt_empty_count """ SELECT py_count_nonnull(val) as count_result FROM empty_test; """
        qt_empty_sum """ SELECT py_sum_null(CAST(val AS DOUBLE)) as sum_result FROM empty_test; """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_count_nonnull(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_sum_null(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_first_nonnull(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_count_null(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_avg_coalesce(DOUBLE);")
        try_sql("DROP TABLE IF EXISTS null_test;")
        try_sql("DROP TABLE IF EXISTS all_null_test;")
        try_sql("DROP TABLE IF EXISTS empty_test;")
    }
}
