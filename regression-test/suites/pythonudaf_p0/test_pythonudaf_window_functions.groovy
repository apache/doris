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

suite("test_pythonudaf_window_functions") {
    // Test Python UDAFs with window functions (OVER clause)
    // This tests PARTITION BY, ORDER BY, and frame specifications
    
    def runtime_version = "3.8.10"
    
    try {
        // Create sales data table for window function tests
        sql """ DROP TABLE IF EXISTS sales_data; """
        sql """
        CREATE TABLE sales_data (
            id INT,
            sales_date DATE,
            region STRING,
            product STRING,
            sales_amount DOUBLE,
            quantity INT,
            salesperson STRING
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO sales_data VALUES
        (1, '2024-01-01', 'North', 'Laptop', 1200.00, 2, 'Alice'),
        (2, '2024-01-02', 'North', 'Mouse', 25.00, 10, 'Alice'),
        (3, '2024-01-03', 'North', 'Keyboard', 75.00, 5, 'Bob'),
        (4, '2024-01-04', 'South', 'Laptop', 1150.00, 1, 'Charlie'),
        (5, '2024-01-05', 'South', 'Monitor', 300.00, 3, 'Charlie'),
        (6, '2024-01-06', 'South', 'Mouse', 20.00, 15, 'David'),
        (7, '2024-01-07', 'East', 'Laptop', 1300.00, 2, 'Eve'),
        (8, '2024-01-08', 'East', 'Keyboard', 80.00, 4, 'Eve'),
        (9, '2024-01-09', 'East', 'Monitor', 320.00, 2, 'Frank'),
        (10, '2024-01-10', 'West', 'Laptop', 1250.00, 3, 'Grace'),
        (11, '2024-01-11', 'West', 'Mouse', 22.00, 12, 'Grace'),
        (12, '2024-01-12', 'West', 'Keyboard', 70.00, 6, 'Henry'),
        (13, '2024-01-13', 'North', 'Monitor', 310.00, 2, 'Alice'),
        (14, '2024-01-14', 'South', 'Keyboard', 78.00, 3, 'Charlie'),
        (15, '2024-01-15', 'East', 'Mouse', 24.00, 8, 'Eve');
        """
        
        qt_select_data """ SELECT * FROM sales_data ORDER BY id; """
        
        // Create Python UDAFs for window functions
        
        // ========================================
        // UDAF 1: Running Sum (Cumulative Sum)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_running_sum(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_running_sum(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "RunningSumUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class RunningSumUDAF:
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
        
        // ========================================
        // UDAF 2: Running Count
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_running_count(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_running_count(INT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "RunningCountUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class RunningCountUDAF:
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
        
        // ========================================
        // UDAF 3: Running Average
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_running_avg(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_running_avg(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "RunningAvgUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class RunningAvgUDAF:
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
        
        // ========================================
        // Test 1: Simple window with PARTITION BY
        // ========================================
        qt_window_partition_by """
            SELECT 
                id,
                region,
                sales_amount,
                py_running_sum(sales_amount) OVER (PARTITION BY region ORDER BY id) as region_running_sum
            FROM sales_data
            ORDER BY region, id;
        """
        
        // ========================================
        // Test 2: Window with ORDER BY only (no partition)
        // ========================================
        qt_window_order_by """
            SELECT 
                id,
                sales_date,
                sales_amount,
                py_running_sum(sales_amount) OVER (ORDER BY sales_date) as cumulative_sales,
                py_running_count(quantity) OVER (ORDER BY sales_date) as cumulative_count
            FROM sales_data
            ORDER BY sales_date;
        """
        
        // ========================================
        // Test 3: Multiple partitions with different UDAFs
        // ========================================
        qt_window_multi_partition """
            SELECT 
                region,
                product,
                sales_amount,
                py_running_sum(sales_amount) OVER (PARTITION BY region ORDER BY id) as region_sum,
                py_running_sum(sales_amount) OVER (PARTITION BY product ORDER BY id) as product_sum,
                py_running_avg(sales_amount) OVER (PARTITION BY region ORDER BY id) as region_avg
            FROM sales_data
            ORDER BY region, id;
        """
        
        // ========================================
        // Test 4: Window with frame specification - ROWS BETWEEN
        // ========================================
        qt_window_rows_between """
            SELECT 
                id,
                sales_date,
                sales_amount,
                py_running_sum(sales_amount) OVER (
                    ORDER BY sales_date 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as moving_sum_3days
            FROM sales_data
            ORDER BY sales_date;
        """
        
        // ========================================
        // Test 5: Window with cumulative sum (default frame)
        // ========================================
        qt_window_unbounded """
            SELECT 
                region,
                sales_date,
                sales_amount,
                py_running_sum(sales_amount) OVER (
                    PARTITION BY region 
                    ORDER BY sales_date
                ) as region_cumulative
            FROM sales_data
            ORDER BY region, sales_date;
        """
        
        // ========================================
        // Test 6: Salesperson performance with running totals
        // ========================================
        qt_window_salesperson """
            SELECT 
                salesperson,
                sales_date,
                sales_amount,
                py_running_sum(sales_amount) OVER (
                    PARTITION BY salesperson 
                    ORDER BY sales_date
                ) as person_cumulative_sales,
                py_running_count(quantity) OVER (
                    PARTITION BY salesperson 
                    ORDER BY sales_date
                ) as person_total_transactions
            FROM sales_data
            ORDER BY salesperson, sales_date;
        """
        
        // ========================================
        // Test 7: Compare window function with regular aggregation
        // ========================================
        qt_window_vs_group_by """
            SELECT 
                region,
                sales_amount,
                py_running_sum(sales_amount) OVER (PARTITION BY region) as window_total,
                (SELECT py_running_sum(sales_amount) FROM sales_data s2 WHERE s2.region = s1.region) as subquery_total
            FROM sales_data s1
            ORDER BY region, id;
        """
        
        // ========================================
        // Test 8: Multiple window specifications
        // ========================================
        qt_window_multiple_specs """
            SELECT 
                product,
                sales_date,
                sales_amount,
                py_running_sum(sales_amount) OVER (PARTITION BY product ORDER BY sales_date) as product_running_sum,
                py_running_avg(sales_amount) OVER (ORDER BY sales_date) as overall_avg,
                py_running_count(quantity) OVER (PARTITION BY product ORDER BY sales_date) as product_count
            FROM sales_data
            ORDER BY product, sales_date;
        """
        
        // ========================================
        // Test 9: Window with complex ordering
        // ========================================
        qt_window_complex_order """
            SELECT 
                region,
                product,
                sales_amount,
                quantity,
                py_running_sum(sales_amount) OVER (
                    PARTITION BY region 
                    ORDER BY sales_amount DESC, quantity
                ) as ranked_cumulative
            FROM sales_data
            ORDER BY region, sales_amount DESC, quantity;
        """
        
        // ========================================
        // Test 10: Window function with WHERE clause
        // ========================================
        qt_window_with_where """
            SELECT 
                region,
                product,
                sales_amount,
                py_running_sum(sales_amount) OVER (
                    PARTITION BY region 
                    ORDER BY sales_date
                ) as region_cumulative
            FROM sales_data
            WHERE sales_amount > 50
            ORDER BY region, sales_date;
        """
        
        // ========================================
        // Test 11: Nested query with window functions
        // ========================================
        qt_window_nested """
            SELECT 
                region,
                sales_date,
                sales_amount,
                region_running_sum,
                region_running_sum - sales_amount as previous_sum
            FROM (
                SELECT 
                    region,
                    sales_date,
                    sales_amount,
                    py_running_sum(sales_amount) OVER (
                        PARTITION BY region 
                        ORDER BY sales_date
                    ) as region_running_sum
                FROM sales_data
            ) t
            ORDER BY region, sales_date;
        """
        
        // ========================================
        // Test 12: Window with RANGE frame (if supported)
        // ========================================
        qt_window_range_frame """
            SELECT 
                id,
                sales_date,
                sales_amount,
                py_running_sum(sales_amount) OVER (
                    ORDER BY id
                    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                ) as three_row_sum
            FROM sales_data
            ORDER BY id;
        """
        
        // ========================================
        // Test 13: Empty partition handling
        // ========================================
        sql """ DROP TABLE IF EXISTS sparse_data; """
        sql """
        CREATE TABLE sparse_data (
            id INT,
            category STRING,
            value DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO sparse_data VALUES
        (1, 'A', 100),
        (2, 'A', 200),
        (3, 'B', 300),
        (4, 'C', 400),
        (5, 'C', 500),
        (6, 'C', 600);
        """
        
        qt_window_sparse """
            SELECT 
                category,
                value,
                py_running_sum(value) OVER (PARTITION BY category ORDER BY id) as category_sum,
                py_running_count(CAST(value AS INT)) OVER (PARTITION BY category ORDER BY id) as category_count
            FROM sparse_data
            ORDER BY category, id;
        """
        
        // ========================================
        // Test 14: First and last value in window
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_window_first(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_window_first(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "WindowFirstUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class WindowFirstUDAF:
    def __init__(self):
        self.first_value = None
        self.has_value = False
    
    @property
    def aggregate_state(self):
        return (self.first_value, self.has_value)
    
    def accumulate(self, value):
        if not self.has_value and value is not None:
            self.first_value = value
            self.has_value = True
    
    def merge(self, other_state):
        other_first, other_has = other_state
        if not self.has_value and other_has:
            self.first_value = other_first
            self.has_value = True
    
    def finish(self):
        return self.first_value
\$\$;
        """
        
        qt_window_first_value """
            SELECT 
                region,
                sales_date,
                sales_amount,
                py_window_first(sales_amount) OVER (
                    PARTITION BY region 
                    ORDER BY sales_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) as first_sale_in_region
            FROM sales_data
            ORDER BY region, sales_date;
        """
        
        // ========================================
        // Test 15: Window with NULL values
        // ========================================
        sql """ DROP TABLE IF EXISTS window_null_test; """
        sql """
        CREATE TABLE window_null_test (
            id INT,
            category STRING,
            value DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO window_null_test VALUES
        (1, 'A', 10.0),
        (2, 'A', NULL),
        (3, 'A', 30.0),
        (4, 'B', NULL),
        (5, 'B', 50.0),
        (6, 'B', NULL);
        """
        
        qt_window_with_nulls """
            SELECT 
                category,
                value,
                py_running_sum(value) OVER (PARTITION BY category ORDER BY id) as running_sum,
                py_running_count(CAST(value AS INT)) OVER (PARTITION BY category ORDER BY id) as running_count
            FROM window_null_test
            ORDER BY category, id;
        """
        
        // ========================================
        // Test 16: Window function performance - larger dataset
        // ========================================
        sql """ DROP TABLE IF EXISTS large_window_test; """
        sql """
        CREATE TABLE large_window_test (
            id INT,
            group_id INT,
            value DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO large_window_test 
        SELECT 
            number as id,
            number % 5 as group_id,
            number * 1.5 as value
        FROM numbers("number" = "100");
        """
        
        qt_window_large_dataset """
            SELECT 
                group_id,
                COUNT(*) as total_rows,
                AVG(running_sum) as avg_running_sum
            FROM (
                SELECT 
                    group_id,
                    value,
                    py_running_sum(value) OVER (PARTITION BY group_id ORDER BY id) as running_sum
                FROM large_window_test
            ) t
            GROUP BY group_id
            ORDER BY group_id;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_running_sum(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_running_count(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_running_avg(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_window_first(DOUBLE);")
        try_sql("DROP TABLE IF EXISTS sales_data;")
        try_sql("DROP TABLE IF EXISTS sparse_data;")
        try_sql("DROP TABLE IF EXISTS window_null_test;")
        try_sql("DROP TABLE IF EXISTS large_window_test;")
    }
}
