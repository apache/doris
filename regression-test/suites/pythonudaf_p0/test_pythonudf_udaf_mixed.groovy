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

suite("test_pythonudf_udaf_mixed") {
    // Test mixing Python scalar UDFs and aggregate UDAFs in the same query
    // This verifies that UDFs and UDAFs can coexist and work correctly together
    
    def runtime_version = "3.8.10"
    
    try {
        // Create test table
        sql """ DROP TABLE IF EXISTS mixed_test; """
        sql """
        CREATE TABLE mixed_test (
            id INT,
            category STRING,
            value INT,
            price DOUBLE,
            discount DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO mixed_test VALUES
        (1, 'Electronics', 100, 99.9, 0.1),
        (2, 'Electronics', 200, 199.9, 0.15),
        (3, 'Books', 50, 29.9, 0.05),
        (4, 'Books', 80, 49.9, 0.1),
        (5, 'Clothing', 150, 79.9, 0.2),
        (6, 'Clothing', 120, 59.9, 0.15),
        (7, 'Electronics', 300, 299.9, 0.25),
        (8, 'Books', 60, 39.9, 0.08);
        """
        
        // Create scalar UDFs
        
        // UDF 1: Calculate final price after discount
        sql """ DROP FUNCTION IF EXISTS py_final_price(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_final_price(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "final_price",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
def final_price(price, discount):
    if price is None or discount is None:
        return None
    return price * (1 - discount)
\$\$;
        """
        
        // UDF 2: Double the value
        sql """ DROP FUNCTION IF EXISTS py_double(INT); """
        sql """
        CREATE FUNCTION py_double(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "double_value",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
def double_value(x):
    if x is None:
        return None
    return x * 2
\$\$;
        """
        
        // UDF 3: Get category prefix (first 3 characters)
        sql """ DROP FUNCTION IF EXISTS py_category_prefix(STRING); """
        sql """
        CREATE FUNCTION py_category_prefix(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "get_prefix",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
def get_prefix(s):
    if s is None or len(s) == 0:
        return None
    return s[:3].upper()
\$\$;
        """
        
        // Create aggregate UDAFs
        
        // UDAF 1: Sum
        sql """ DROP FUNCTION IF EXISTS py_udaf_sum(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_udaf_sum(INT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SumUDAF",
            "runtime_version" = "${runtime_version}"
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
        
        // UDAF 2: Count
        sql """ DROP FUNCTION IF EXISTS py_udaf_count(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_udaf_count(INT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "CountUDAF",
            "runtime_version" = "${runtime_version}"
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
        
        // UDAF 3: Average for DOUBLE
        sql """ DROP FUNCTION IF EXISTS py_udaf_avg_double(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_udaf_avg_double(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "AvgDoubleUDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class AvgDoubleUDAF:
    def __init__(self):
        self.count = 0
        self.sum = 0.0

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
        
        // Test 1: Mix scalar UDF with UDAF in SELECT
        // Use UDF for constant calculation, UDAF for aggregation
        qt_mixed_1 """
        SELECT 
            py_udaf_sum(value) AS total_value,
            py_udaf_count(value) AS total_count,
            py_double(100) AS doubled_constant
        FROM mixed_test;
        """
        
        // Test 2: UDF in GROUP BY expression, UDAF in SELECT
        qt_mixed_2 """
        SELECT 
            py_category_prefix(category) AS prefix,
            py_udaf_sum(value) AS total_value,
            py_udaf_count(value) AS item_count
        FROM mixed_test
        GROUP BY py_category_prefix(category)
        ORDER BY prefix;
        """
        
        // Test 3: UDAF aggregating UDF results
        // Apply UDF to each row, then aggregate with UDAF
        qt_mixed_3 """
        SELECT 
            category,
            py_udaf_sum(py_double(value)) AS sum_doubled_value,
            py_udaf_count(value) AS count_value
        FROM mixed_test
        GROUP BY category
        ORDER BY category;
        """
        
        // Test 4: Multiple UDAFs with UDF in GROUP BY
        qt_mixed_4 """
        SELECT 
            py_category_prefix(category) AS prefix,
            py_udaf_sum(value) AS total_value,
            py_udaf_count(value) AS item_count,
            py_udaf_avg_double(price) AS avg_price
        FROM mixed_test
        GROUP BY py_category_prefix(category)
        ORDER BY prefix;
        """
        
        // Test 5: UDF and UDAF with complex expressions
        // Calculate final price with UDF, then aggregate with UDAF
        qt_mixed_5 """
        SELECT 
            category,
            py_udaf_sum(value) AS total_value,
            py_udaf_avg_double(py_final_price(price, discount)) AS avg_final_price,
            py_udaf_count(value) AS count_value
        FROM mixed_test
        GROUP BY category
        ORDER BY category;
        """
        
        // Test 6: Multiple UDFs and UDAFs together
        qt_mixed_6 """
        SELECT 
            category,
            py_category_prefix(category) AS prefix,
            py_udaf_sum(py_double(value)) AS sum_doubled,
            py_udaf_count(value) AS count_items,
            py_udaf_avg_double(price) AS avg_price
        FROM mixed_test
        GROUP BY category
        ORDER BY category;
        """
        
        // Test 7: HAVING clause with UDAF
        qt_mixed_7 """
        SELECT 
            category,
            py_udaf_sum(value) AS total_value,
            py_udaf_count(value) AS item_count
        FROM mixed_test
        GROUP BY category
        HAVING py_udaf_sum(value) > 150
        ORDER BY category;
        """
        
        // Test 8: Subquery with UDF, outer query with UDAF
        qt_mixed_8 """
        SELECT 
            py_udaf_sum(doubled_value) AS total_doubled,
            py_udaf_count(doubled_value) AS count_doubled
        FROM (
            SELECT py_double(value) AS doubled_value
            FROM mixed_test
        ) t;
        """
        
        // Test 9: UDF in WHERE clause, UDAF in SELECT
        qt_mixed_9 """
        SELECT 
            category,
            py_udaf_sum(value) AS total_value,
            py_udaf_count(value) AS item_count
        FROM mixed_test
        WHERE py_double(value) > 100
        GROUP BY category
        ORDER BY category;
        """
        
        // Test 10: Complex case - Multiple UDFs and UDAFs with different operations
        qt_mixed_10 """
        SELECT 
            py_category_prefix(category) AS prefix,
            py_udaf_sum(value) AS sum_value,
            py_udaf_sum(py_double(value)) AS sum_doubled,
            py_udaf_count(value) AS count_value,
            py_udaf_avg_double(price) AS avg_price,
            py_udaf_avg_double(py_final_price(price, discount)) AS avg_final_price
        FROM mixed_test
        GROUP BY py_category_prefix(category)
        ORDER BY prefix;
        """
        
        // Test 11: Verify UDAF results match native functions
        qt_mixed_verify """
        SELECT 
            category,
            py_udaf_sum(value) AS python_sum,
            SUM(value) AS native_sum,
            py_udaf_count(value) AS python_count,
            COUNT(value) AS native_count
        FROM mixed_test
        GROUP BY category
        ORDER BY category;
        """
        
    } finally {
        // Cleanup
        try_sql("DROP FUNCTION IF EXISTS py_final_price(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_double(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_category_prefix(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_udaf_sum(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_udaf_count(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_udaf_avg_double(DOUBLE);")
        try_sql("DROP TABLE IF EXISTS mixed_test;")
    }
}
