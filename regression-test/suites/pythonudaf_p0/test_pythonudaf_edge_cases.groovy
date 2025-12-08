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

suite("test_pythonudaf_edge_cases") {
    // Test Python UDAFs with edge cases and boundary values
    // Including: very large numbers, very small numbers, negative numbers, zero, duplicates
    
    def runtime_version = "3.8.10"
    
    try {
        // Create test table with edge cases
        sql """ DROP TABLE IF EXISTS edge_cases_test; """
        sql """
        CREATE TABLE edge_cases_test (
            id INT,
            category STRING,
            int_val BIGINT,
            double_val DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO edge_cases_test VALUES
        (1, 'positive', 2147483647, 1.7976931348623157e+308),  -- Max INT, near max DOUBLE
        (2, 'positive', 1000000000, 999999999.999),
        (3, 'negative', -2147483648, -1.7976931348623157e+308),  -- Min INT, near min DOUBLE
        (4, 'negative', -1000000000, -999999999.999),
        (5, 'zero', 0, 0.0),
        (6, 'zero', 0, 0.0),
        (7, 'small', 1, 0.0000000001),  -- Very small positive
        (8, 'small', -1, -0.0000000001),  -- Very small negative
        (9, 'duplicate', 100, 100.5),
        (10, 'duplicate', 100, 100.5),
        (11, 'duplicate', 100, 100.5),
        (12, 'mixed', 50, -50.5),
        (13, 'mixed', -50, 50.5);
        """
        
        qt_select_data """ SELECT * FROM edge_cases_test ORDER BY id; """
        
        // ========================================
        // Test 1: Sum with large numbers
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_sum_large(BIGINT); """
        sql """
        CREATE AGGREGATE FUNCTION py_sum_large(BIGINT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SumLargeUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class SumLargeUDAF:
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
        
        qt_large_numbers """ SELECT category, py_sum_large(int_val) as sum_result 
                             FROM edge_cases_test 
                             GROUP BY category 
                             ORDER BY category; """
        
        // ========================================
        // Test 2: Min with negative numbers
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_min_val(BIGINT); """
        sql """
        CREATE AGGREGATE FUNCTION py_min_val(BIGINT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "MinValUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class MinValUDAF:
    def __init__(self):
        self.min_value = None
    
    @property
    def aggregate_state(self):
        return self.min_value
    
    def accumulate(self, value):
        if value is not None:
            if self.min_value is None or value < self.min_value:
                self.min_value = value
    
    def merge(self, other_state):
        if other_state is not None:
            if self.min_value is None or other_state < self.min_value:
                self.min_value = other_state
    
    def finish(self):
        return self.min_value
\$\$;
        """
        
        qt_negative_min """ SELECT py_min_val(int_val) as min_value FROM edge_cases_test; """
        
        // ========================================
        // Test 3: Max with large numbers
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_max_val(BIGINT); """
        sql """
        CREATE AGGREGATE FUNCTION py_max_val(BIGINT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "MaxValUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class MaxValUDAF:
    def __init__(self):
        self.max_value = None
    
    @property
    def aggregate_state(self):
        return self.max_value
    
    def accumulate(self, value):
        if value is not None:
            if self.max_value is None or value > self.max_value:
                self.max_value = value
    
    def merge(self, other_state):
        if other_state is not None:
            if self.max_value is None or other_state > self.max_value:
                self.max_value = other_state
    
    def finish(self):
        return self.max_value
\$\$;
        """
        
        qt_large_max """ SELECT py_max_val(int_val) as max_value FROM edge_cases_test; """
        
        // ========================================
        // Test 4: Count distinct values
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_count_distinct(BIGINT); """
        sql """
        CREATE AGGREGATE FUNCTION py_count_distinct(BIGINT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "CountDistinctUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class CountDistinctUDAF:
    def __init__(self):
        self.distinct_values = set()
    
    @property
    def aggregate_state(self):
        return list(self.distinct_values)
    
    def accumulate(self, value):
        if value is not None:
            self.distinct_values.add(value)
    
    def merge(self, other_state):
        if other_state:
            self.distinct_values.update(other_state)
    
    def finish(self):
        return len(self.distinct_values)
\$\$;
        """
        
        qt_distinct_count """ SELECT category, py_count_distinct(int_val) as distinct_count 
                              FROM edge_cases_test 
                              GROUP BY category 
                              ORDER BY category; """
        
        // ========================================
        // Test 5: Product aggregation (handles overflow)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_product(BIGINT); """
        sql """
        CREATE AGGREGATE FUNCTION py_product(BIGINT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "ProductUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class ProductUDAF:
    def __init__(self):
        self.product = 1
        self.has_value = False
    
    @property
    def aggregate_state(self):
        return (self.product, self.has_value)
    
    def accumulate(self, value):
        if value is not None:
            self.product *= value
            self.has_value = True
    
    def merge(self, other_state):
        other_product, other_has_value = other_state
        if other_has_value:
            self.product *= other_product
            self.has_value = True
    
    def finish(self):
        return self.product if self.has_value else None
\$\$;
        """
        
        qt_product """ SELECT category, py_product(int_val) as product_result 
                       FROM edge_cases_test 
                       WHERE category = 'small'
                       GROUP BY category; """
        
        // ========================================
        // Test 6: Absolute sum (sum of absolute values)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_abs_sum(BIGINT); """
        sql """
        CREATE AGGREGATE FUNCTION py_abs_sum(BIGINT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "AbsSumUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class AbsSumUDAF:
    def __init__(self):
        self.sum = 0
    
    @property
    def aggregate_state(self):
        return self.sum
    
    def accumulate(self, value):
        if value is not None:
            self.sum += abs(value)
    
    def merge(self, other_state):
        self.sum += other_state
    
    def finish(self):
        return self.sum
\$\$;
        """
        
        qt_abs_sum """ SELECT category, py_abs_sum(int_val) as abs_sum 
                       FROM edge_cases_test 
                       GROUP BY category 
                       ORDER BY category; """
        
        // ========================================
        // Test 7: Safe division (handles division by zero)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_safe_avg(BIGINT); """
        sql """
        CREATE AGGREGATE FUNCTION py_safe_avg(BIGINT)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SafeAvgUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class SafeAvgUDAF:
    def __init__(self):
        self.sum = 0
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
            return 0.0  # Safe default
        return float(self.sum) / float(self.count)
\$\$;
        """
        
        qt_safe_avg """ SELECT category, py_safe_avg(int_val) as safe_avg 
                        FROM edge_cases_test 
                        GROUP BY category 
                        ORDER BY category; """
        
        // ========================================
        // Test 8: Sign count (count positive, negative, zero)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_sign_summary(BIGINT); """
        sql """
        CREATE AGGREGATE FUNCTION py_sign_summary(BIGINT)
        RETURNS STRING
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SignSummaryUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class SignSummaryUDAF:
    def __init__(self):
        self.positive_count = 0
        self.negative_count = 0
        self.zero_count = 0
    
    @property
    def aggregate_state(self):
        return (self.positive_count, self.negative_count, self.zero_count)
    
    def accumulate(self, value):
        if value is not None:
            if value > 0:
                self.positive_count += 1
            elif value < 0:
                self.negative_count += 1
            else:
                self.zero_count += 1
    
    def merge(self, other_state):
        other_pos, other_neg, other_zero = other_state
        self.positive_count += other_pos
        self.negative_count += other_neg
        self.zero_count += other_zero
    
    def finish(self):
        return f"pos:{self.positive_count},neg:{self.negative_count},zero:{self.zero_count}"
\$\$;
        """
        
        qt_sign_summary """ SELECT py_sign_summary(int_val) as sign_summary FROM edge_cases_test; """
        
        // ========================================
        // Test 9: Test with only zeros
        // ========================================
        sql """ DROP TABLE IF EXISTS zero_test; """
        sql """
        CREATE TABLE zero_test (
            id INT,
            val BIGINT
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO zero_test VALUES
        (1, 0), (2, 0), (3, 0), (4, 0), (5, 0);
        """
        
        qt_all_zeros_sum """ SELECT py_sum_large(val) as sum_zeros FROM zero_test; """
        qt_all_zeros_avg """ SELECT py_safe_avg(val) as avg_zeros FROM zero_test; """
        qt_all_zeros_product """ SELECT py_product(val) as product_zeros FROM zero_test; """
        
        // ========================================
        // Test 10: Single value aggregation
        // ========================================
        sql """ DROP TABLE IF EXISTS single_value_test; """
        sql """
        CREATE TABLE single_value_test (
            id INT,
            val BIGINT
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO single_value_test VALUES (1, 42);
        """
        
        qt_single_value """ SELECT 
                            py_sum_large(val) as sum_val,
                            py_min_val(val) as min_val,
                            py_max_val(val) as max_val,
                            py_safe_avg(val) as avg_val
                            FROM single_value_test; """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_sum_large(BIGINT);")
        try_sql("DROP FUNCTION IF EXISTS py_min_val(BIGINT);")
        try_sql("DROP FUNCTION IF EXISTS py_max_val(BIGINT);")
        try_sql("DROP FUNCTION IF EXISTS py_count_distinct(BIGINT);")
        try_sql("DROP FUNCTION IF EXISTS py_product(BIGINT);")
        try_sql("DROP FUNCTION IF EXISTS py_abs_sum(BIGINT);")
        try_sql("DROP FUNCTION IF EXISTS py_safe_avg(BIGINT);")
        try_sql("DROP FUNCTION IF EXISTS py_sign_summary(BIGINT);")
        try_sql("DROP TABLE IF EXISTS edge_cases_test;")
        try_sql("DROP TABLE IF EXISTS zero_test;")
        try_sql("DROP TABLE IF EXISTS single_value_test;")
    }
}
