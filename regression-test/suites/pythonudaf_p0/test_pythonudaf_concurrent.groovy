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

suite("test_pythonudaf_concurrent") {
    // Test multiple Python UDAFs executing concurrently in the same SQL query
    // This is the key test case to verify the fix for multi-UDAF state management
    
    try {
        // Create test table
        sql """ DROP TABLE IF EXISTS concurrent_udaf_test; """
        sql """
        CREATE TABLE concurrent_udaf_test (
            id INT,
            category STRING,
            value INT,
            price DOUBLE,
            quantity INT,
            amount DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO concurrent_udaf_test VALUES
        (1, 'A', 10, 1.5, 5, 7.5),
        (2, 'A', 20, 2.5, 3, 7.5),
        (3, 'B', 30, 3.5, 4, 14.0),
        (4, 'B', 40, 4.5, 2, 9.0),
        (5, 'C', 50, 5.5, 6, 33.0),
        (6, 'C', 60, 6.5, 1, 6.5),
        (7, 'A', 70, 7.5, 8, 60.0),
        (8, 'B', 80, 8.5, 7, 59.5),
        (9, 'C', 90, 9.5, 9, 85.5),
        (10, 'A', 100, 10.5, 10, 105.0);
        """
        
        // UDAF 1: Sum aggregation
        sql """ DROP FUNCTION IF EXISTS inline_udaf_sum(INT); """
        sql """
        CREATE AGGREGATE FUNCTION inline_udaf_sum(INT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SumUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class SumUDAF:
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
        
        // UDAF 2: Count aggregation
        sql """ DROP FUNCTION IF EXISTS inline_udaf_count(INT); """
        sql """
        CREATE AGGREGATE FUNCTION inline_udaf_count(INT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "CountUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class CountUDAF:
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
        
        // UDAF 3: Average aggregation
        sql """ DROP FUNCTION IF EXISTS inline_udaf_avg(INT); """
        sql """
        CREATE AGGREGATE FUNCTION inline_udaf_avg(INT)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "AvgUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class AvgUDAF:
    def __init__(self):
        self.count = 0
        self.sum = 0

    @property
    def aggregate_state(self):
        return (self.count, self.sum)

    def accumulate(self, value):
        if value is not None:
            self.count += 1
            self.sum += value

    def merge(self, other_state):
        other_count, other_sum = other_state
        self.count += other_count
        self.sum += other_sum

    def finish(self):
        if self.count == 0:
            return None
        return self.sum / self.count
\$\$;
        """
        
        // UDAF 4: Max aggregation
        sql """ DROP FUNCTION IF EXISTS inline_udaf_max(INT); """
        sql """
        CREATE AGGREGATE FUNCTION inline_udaf_max(INT)
        RETURNS INT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "MaxUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class MaxUDAF:
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
        
        // UDAF 5: Min aggregation
        sql """ DROP FUNCTION IF EXISTS inline_udaf_min(INT); """
        sql """
        CREATE AGGREGATE FUNCTION inline_udaf_min(INT)
        RETURNS INT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "MinUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class MinUDAF:
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
        
        // UDAF 6: Sum for DOUBLE type
        sql """ DROP FUNCTION IF EXISTS inline_udaf_sum_double(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION inline_udaf_sum_double(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SumDoubleUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class SumDoubleUDAF:
    def __init__(self):
        self.sum = 0.0

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
        
        // Test 1: Two different UDAFs in the same query (Critical test case!)
        qt_concurrent_two_udaf """
        SELECT 
            inline_udaf_sum(value) AS total_sum,
            inline_udaf_count(value) AS total_count
        FROM concurrent_udaf_test;
        """
        
        // Test 2: Three different UDAFs in the same query
        qt_concurrent_three_udaf """
        SELECT 
            inline_udaf_sum(value) AS total_sum,
            inline_udaf_count(value) AS total_count,
            inline_udaf_avg(value) AS avg_value
        FROM concurrent_udaf_test;
        """
        
        // Test 3: Multiple UDAFs with GROUP BY (Critical test case!)
        qt_concurrent_udaf_group_by """
        SELECT 
            category,
            inline_udaf_sum(value) AS sum_value,
            inline_udaf_count(value) AS count_value,
            inline_udaf_avg(value) AS avg_value
        FROM concurrent_udaf_test
        GROUP BY category
        ORDER BY category;
        """
        
        // Test 4: Five different UDAFs in the same query
        qt_concurrent_five_udaf """
        SELECT 
            inline_udaf_sum(value) AS total_sum,
            inline_udaf_count(value) AS total_count,
            inline_udaf_avg(value) AS avg_value,
            inline_udaf_max(value) AS max_value,
            inline_udaf_min(value) AS min_value
        FROM concurrent_udaf_test;
        """
        
        // Test 5: Multiple UDAFs with different types (INT and DOUBLE)
        qt_concurrent_mixed_types """
        SELECT 
            inline_udaf_sum(value) AS sum_int,
            inline_udaf_sum_double(price) AS sum_double,
            inline_udaf_count(value) AS count_value,
            inline_udaf_avg(quantity) AS avg_quantity
        FROM concurrent_udaf_test;
        """
        
        // Test 6: Multiple UDAFs with GROUP BY on different columns
        qt_concurrent_complex_group """
        SELECT 
            category,
            inline_udaf_sum(value) AS sum_value,
            inline_udaf_count(value) AS count_value,
            inline_udaf_avg(value) AS avg_value,
            inline_udaf_max(value) AS max_value,
            inline_udaf_min(value) AS min_value,
            inline_udaf_sum_double(amount) AS sum_amount
        FROM concurrent_udaf_test
        GROUP BY category
        ORDER BY category;
        """
        
        // Test 7: Same UDAF function called multiple times on different columns
        qt_concurrent_same_udaf """
        SELECT 
            inline_udaf_sum(value) AS sum_value,
            inline_udaf_sum(quantity) AS sum_quantity
        FROM concurrent_udaf_test;
        """
        
        // Test 8: Nested aggregation - UDAFs with WHERE clause
        qt_concurrent_with_filter """
        SELECT 
            inline_udaf_sum(value) AS sum_value,
            inline_udaf_count(value) AS count_value,
            inline_udaf_avg(value) AS avg_value
        FROM concurrent_udaf_test
        WHERE value > 30;
        """
        
        // Test 9: Multiple UDAFs with HAVING clause
        qt_concurrent_with_having """
        SELECT 
            category,
            inline_udaf_sum(value) AS sum_value,
            inline_udaf_count(value) AS count_value,
            inline_udaf_avg(value) AS avg_value
        FROM concurrent_udaf_test
        GROUP BY category
        HAVING inline_udaf_sum(value) > 100
        ORDER BY category;
        """
        
        // Test 10: Stress test - Multiple UDAFs called multiple times
        qt_concurrent_stress """
        SELECT 
            category,
            inline_udaf_sum(value) AS sum_value,
            inline_udaf_sum(quantity) AS sum_quantity,
            inline_udaf_count(value) AS count_value,
            inline_udaf_count(quantity) AS count_quantity,
            inline_udaf_avg(value) AS avg_value,
            inline_udaf_avg(quantity) AS avg_quantity,
            inline_udaf_max(value) AS max_value,
            inline_udaf_min(value) AS min_value,
            inline_udaf_sum_double(price) AS sum_price,
            inline_udaf_sum_double(amount) AS sum_amount
        FROM concurrent_udaf_test
        GROUP BY category
        ORDER BY category;
        """
        
        // Test 11: Verify correctness - Compare with native functions
        qt_concurrent_verify_sum """
        SELECT 
            inline_udaf_sum(value) AS python_sum,
            SUM(value) AS native_sum,
            inline_udaf_count(value) AS python_count,
            COUNT(value) AS native_count
        FROM concurrent_udaf_test;
        """
        
        qt_concurrent_verify_group """
        SELECT 
            category,
            inline_udaf_sum(value) AS python_sum,
            SUM(value) AS native_sum,
            inline_udaf_count(value) AS python_count,
            COUNT(value) AS native_count,
            inline_udaf_avg(value) AS python_avg,
            AVG(value) AS native_avg
        FROM concurrent_udaf_test
        GROUP BY category
        ORDER BY category;
        """
        
    } finally {
        // Cleanup
        try_sql("DROP FUNCTION IF EXISTS inline_udaf_sum(INT);")
        try_sql("DROP FUNCTION IF EXISTS inline_udaf_count(INT);")
        try_sql("DROP FUNCTION IF EXISTS inline_udaf_avg(INT);")
        try_sql("DROP FUNCTION IF EXISTS inline_udaf_max(INT);")
        try_sql("DROP FUNCTION IF EXISTS inline_udaf_min(INT);")
        try_sql("DROP FUNCTION IF EXISTS inline_udaf_sum_double(DOUBLE);")
        try_sql("DROP TABLE IF EXISTS concurrent_udaf_test;")
    }
}
