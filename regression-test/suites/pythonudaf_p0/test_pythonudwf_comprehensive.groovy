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

suite("test_pythonudwf_comprehensive") {
    // Comprehensive test suite for Python User-Defined Window Functions (UDWF)
    // Tests cover: PARTITION BY, ORDER BY, frame specifications, edge cases, and complex scenarios
    
    def runtime_version = "3.8.10"
    
    try {
        // ========================================
        // Setup: Create test tables with diverse data
        // ========================================
        
        // Table 1: Sales data for basic window function tests
        sql """ DROP TABLE IF EXISTS udwf_sales; """
        sql """
        CREATE TABLE udwf_sales (
            order_id INT,
            order_date DATE,
            region VARCHAR(50),
            product VARCHAR(50),
            category VARCHAR(50),
            sales_amount DECIMAL(10,2),
            quantity INT,
            salesperson VARCHAR(50)
        ) ENGINE=OLAP 
        DUPLICATE KEY(order_id)
        DISTRIBUTED BY HASH(order_id) BUCKETS 3
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO udwf_sales VALUES
        (1, '2024-01-01', 'North', 'Laptop', 'Electronics', 1200.50, 2, 'Alice'),
        (2, '2024-01-02', 'North', 'Mouse', 'Electronics', 25.99, 10, 'Alice'),
        (3, '2024-01-03', 'North', 'Desk', 'Furniture', 350.00, 1, 'Bob'),
        (4, '2024-01-04', 'South', 'Laptop', 'Electronics', 1150.00, 1, 'Charlie'),
        (5, '2024-01-05', 'South', 'Chair', 'Furniture', 200.00, 4, 'Charlie'),
        (6, '2024-01-06', 'South', 'Monitor', 'Electronics', 300.00, 3, 'David'),
        (7, '2024-01-07', 'East', 'Laptop', 'Electronics', 1300.00, 2, 'Eve'),
        (8, '2024-01-08', 'East', 'Keyboard', 'Electronics', 80.00, 5, 'Eve'),
        (9, '2024-01-09', 'East', 'Bookshelf', 'Furniture', 180.00, 2, 'Frank'),
        (10, '2024-01-10', 'West', 'Laptop', 'Electronics', 1250.00, 3, 'Grace'),
        (11, '2024-01-11', 'West', 'Mouse', 'Electronics', 22.50, 12, 'Grace'),
        (12, '2024-01-12', 'West', 'Table', 'Furniture', 450.00, 1, 'Henry'),
        (13, '2024-01-13', 'North', 'Monitor', 'Electronics', 310.00, 2, 'Alice'),
        (14, '2024-01-14', 'South', 'Keyboard', 'Electronics', 78.00, 3, 'Charlie'),
        (15, '2024-01-15', 'East', 'Mouse', 'Electronics', 24.00, 8, 'Eve'),
        (16, '2024-01-16', 'West', 'Chair', 'Furniture', 195.00, 5, 'Grace'),
        (17, '2024-01-17', 'North', 'Desk', 'Furniture', 380.00, 1, 'Bob'),
        (18, '2024-01-18', 'South', 'Monitor', 'Electronics', 295.00, 2, 'David'),
        (19, '2024-01-19', 'East', 'Laptop', 'Electronics', 1280.00, 1, 'Frank'),
        (20, '2024-01-20', 'West', 'Keyboard', 'Electronics', 85.00, 6, 'Henry');
        """
        
        // Table 2: Stock prices for time-series analysis
        sql """ DROP TABLE IF EXISTS udwf_stock_prices; """
        sql """
        CREATE TABLE udwf_stock_prices (
            trade_id INT,
            trade_time DATETIME,
            symbol VARCHAR(20),
            price DOUBLE,
            volume INT
        ) ENGINE=OLAP 
        DUPLICATE KEY(trade_id)
        DISTRIBUTED BY HASH(trade_id) BUCKETS 2
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO udwf_stock_prices VALUES
        (1, '2024-01-01 09:30:00', 'AAPL', 150.25, 1000),
        (2, '2024-01-01 09:35:00', 'AAPL', 151.50, 1200),
        (3, '2024-01-01 09:40:00', 'AAPL', 150.75, 800),
        (4, '2024-01-01 09:45:00', 'AAPL', 152.00, 1500),
        (5, '2024-01-01 09:50:00', 'AAPL', 151.25, 900),
        (6, '2024-01-01 09:30:00', 'GOOGL', 2800.00, 500),
        (7, '2024-01-01 09:35:00', 'GOOGL', 2815.50, 600),
        (8, '2024-01-01 09:40:00', 'GOOGL', 2810.00, 400),
        (9, '2024-01-01 09:45:00', 'GOOGL', 2825.00, 700),
        (10, '2024-01-01 09:50:00', 'GOOGL', 2820.50, 550),
        (11, '2024-01-01 09:30:00', 'MSFT', 380.00, 2000),
        (12, '2024-01-01 09:35:00', 'MSFT', 382.50, 2200),
        (13, '2024-01-01 09:40:00', 'MSFT', 381.00, 1800),
        (14, '2024-01-01 09:45:00', 'MSFT', 383.75, 2500),
        (15, '2024-01-01 09:50:00', 'MSFT', 382.25, 1900);
        """
        
        // Table 3: Student scores for ranking tests
        sql """ DROP TABLE IF EXISTS udwf_student_scores; """
        sql """
        CREATE TABLE udwf_student_scores (
            student_id INT,
            student_name VARCHAR(50),
            class VARCHAR(20),
            subject VARCHAR(20),
            score INT,
            exam_date DATE
        ) ENGINE=OLAP 
        DUPLICATE KEY(student_id)
        DISTRIBUTED BY HASH(student_id) BUCKETS 2
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO udwf_student_scores VALUES
        (1, 'Alice', 'ClassA', 'Math', 95, '2024-01-15'),
        (1, 'Alice', 'ClassA', 'English', 88, '2024-01-15'),
        (1, 'Alice', 'ClassA', 'Physics', 92, '2024-01-15'),
        (2, 'Bob', 'ClassA', 'Math', 87, '2024-01-15'),
        (2, 'Bob', 'ClassA', 'English', 90, '2024-01-15'),
        (2, 'Bob', 'ClassA', 'Physics', 85, '2024-01-15'),
        (3, 'Charlie', 'ClassA', 'Math', 92, '2024-01-15'),
        (3, 'Charlie', 'ClassA', 'English', 85, '2024-01-15'),
        (3, 'Charlie', 'ClassA', 'Physics', 88, '2024-01-15'),
        (4, 'David', 'ClassB', 'Math', 78, '2024-01-15'),
        (4, 'David', 'ClassB', 'English', 82, '2024-01-15'),
        (4, 'David', 'ClassB', 'Physics', 80, '2024-01-15'),
        (5, 'Eve', 'ClassB', 'Math', 90, '2024-01-15'),
        (5, 'Eve', 'ClassB', 'English', 93, '2024-01-15'),
        (5, 'Eve', 'ClassB', 'Physics', 89, '2024-01-15'),
        (6, 'Frank', 'ClassB', 'Math', 85, '2024-01-15'),
        (6, 'Frank', 'ClassB', 'English', 87, '2024-01-15'),
        (6, 'Frank', 'ClassB', 'Physics', 91, '2024-01-15');
        """
        
        // ========================================
        // UDWF Definitions: Various window functions
        // ========================================
        
        // UDWF 1: Running Sum (Cumulative Sum)
        sql """ DROP FUNCTION IF EXISTS py_udwf_running_sum(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_udwf_running_sum(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "RunningSumUDWF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class RunningSumUDWF:
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
        
        // UDWF 2: Running Average
        sql """ DROP FUNCTION IF EXISTS py_udwf_running_avg(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_udwf_running_avg(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "RunningAvgUDWF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class RunningAvgUDWF:
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
        
        // UDWF 3: Running Count
        sql """ DROP FUNCTION IF EXISTS py_udwf_running_count(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_udwf_running_count(INT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "RunningCountUDWF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class RunningCountUDWF:
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
        
        // UDWF 4: Moving Average (for frame-based windows)
        sql """ DROP FUNCTION IF EXISTS py_udwf_moving_avg(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_udwf_moving_avg(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "MovingAvgUDWF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class MovingAvgUDWF:
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
        return sum(self.values) / len(self.values)
\$\$;
        """
        
        // UDWF 5: Standard Deviation (for volatility analysis)
        sql """ DROP FUNCTION IF EXISTS py_udwf_stddev(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_udwf_stddev(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "StdDevUDWF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
import math

class StdDevUDWF:
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
        if not self.values or len(self.values) < 2:
            return None
        mean = sum(self.values) / len(self.values)
        variance = sum((x - mean) ** 2 for x in self.values) / len(self.values)
        return math.sqrt(variance)
\$\$;
        """
        
        // UDWF 6: Min Value
        sql """ DROP FUNCTION IF EXISTS py_udwf_min(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_udwf_min(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "MinUDWF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class MinUDWF:
    def __init__(self):
        self.min_val = None
    
    @property
    def aggregate_state(self):
        return self.min_val
    
    def accumulate(self, value):
        if value is not None:
            if self.min_val is None or value < self.min_val:
                self.min_val = value
    
    def merge(self, other_state):
        if other_state is not None:
            if self.min_val is None or other_state < self.min_val:
                self.min_val = other_state
    
    def finish(self):
        return self.min_val
\$\$;
        """
        
        // UDWF 7: Max Value
        sql """ DROP FUNCTION IF EXISTS py_udwf_max(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_udwf_max(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "MaxUDWF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class MaxUDWF:
    def __init__(self):
        self.max_val = None
    
    @property
    def aggregate_state(self):
        return self.max_val
    
    def accumulate(self, value):
        if value is not None:
            if self.max_val is None or value > self.max_val:
                self.max_val = value
    
    def merge(self, other_state):
        if other_state is not None:
            if self.max_val is None or other_state > self.max_val:
                self.max_val = other_state
    
    def finish(self):
        return self.max_val
\$\$;
        """
        
        // UDWF 8: First Value
        sql """ DROP FUNCTION IF EXISTS py_udwf_first_value(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_udwf_first_value(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "FirstValueUDWF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class FirstValueUDWF:
    def __init__(self):
        self.first = None
        self.has_value = False
    
    @property
    def aggregate_state(self):
        return (self.first, self.has_value)
    
    def accumulate(self, value):
        if value is not None and not self.has_value:
            self.first = value
            self.has_value = True
    
    def merge(self, other_state):
        other_first, other_has_value = other_state
        if other_has_value and not self.has_value:
            self.first = other_first
            self.has_value = True
    
    def finish(self):
        return self.first
\$\$;
        """
        
        // UDWF 9: Last Value
        sql """ DROP FUNCTION IF EXISTS py_udwf_last_value(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_udwf_last_value(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "LastValueUDWF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class LastValueUDWF:
    def __init__(self):
        self.last = None
    
    @property
    def aggregate_state(self):
        return self.last
    
    def accumulate(self, value):
        if value is not None:
            self.last = value
    
    def merge(self, other_state):
        if other_state is not None:
            self.last = other_state
    
    def finish(self):
        return self.last
\$\$;
        """
        
        // UDWF 10: Rank (Dense Rank implementation)
        sql """ DROP FUNCTION IF EXISTS py_udwf_rank(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_udwf_rank(INT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "RankUDWF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class RankUDWF:
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
        // Test Category 1: Basic Window Functions with PARTITION BY
        // ========================================
        
        // Test 1.1: Simple PARTITION BY with running sum
        qt_test_partition_by_running_sum """
            SELECT 
                order_id,
                region,
                sales_amount,
                py_udwf_running_sum(sales_amount) OVER (PARTITION BY region ORDER BY order_id) as region_running_sum
            FROM udwf_sales
            ORDER BY region, order_id;
        """
        
        // Test 1.2: PARTITION BY with running average
        qt_test_partition_by_running_avg """
            SELECT 
                order_id,
                category,
                sales_amount,
                py_udwf_running_avg(sales_amount) OVER (PARTITION BY category ORDER BY order_date) as category_running_avg
            FROM udwf_sales
            ORDER BY category, order_date, order_id;
        """
        
        // Test 1.3: PARTITION BY with running count
        qt_test_partition_by_running_count """
            SELECT 
                order_id,
                salesperson,
                quantity,
                py_udwf_running_count(quantity) OVER (PARTITION BY salesperson ORDER BY order_id) as sales_count
            FROM udwf_sales
            ORDER BY salesperson, order_id;
        """
        
        // Test 1.4: Multiple PARTITION BY columns
        qt_test_multi_partition_columns """
            SELECT 
                order_id,
                region,
                category,
                sales_amount,
                py_udwf_running_sum(sales_amount) OVER (PARTITION BY region, category ORDER BY order_id) as segment_sum
            FROM udwf_sales
            ORDER BY region, category, order_id;
        """
        
        // ========================================
        // Test Category 2: Window Functions with ORDER BY only
        // ========================================
        
        // Test 2.1: ORDER BY with cumulative sum
        qt_test_order_by_cumulative_sum """
            SELECT 
                order_id,
                order_date,
                sales_amount,
                py_udwf_running_sum(sales_amount) OVER (ORDER BY order_date, order_id) as cumulative_sales
            FROM udwf_sales
            ORDER BY order_date, order_id;
        """
        
        // Test 2.2: ORDER BY with cumulative average
        qt_test_order_by_cumulative_avg """
            SELECT 
                order_id,
                order_date,
                sales_amount,
                py_udwf_running_avg(sales_amount) OVER (ORDER BY order_date) as cumulative_avg
            FROM udwf_sales
            ORDER BY order_date, order_id;
        """
        
        // Test 2.3: ORDER BY DESC
        qt_test_order_by_desc """
            SELECT 
                order_id,
                sales_amount,
                py_udwf_running_sum(sales_amount) OVER (ORDER BY sales_amount DESC) as sum_by_amount_desc
            FROM udwf_sales
            ORDER BY sales_amount DESC, order_id;
        """
        
        // ========================================
        // Test Category 3: Window Functions with Frame Specifications
        // ========================================
        
        // Test 3.1: ROWS BETWEEN frame (moving average - 3 row window)
        qt_test_rows_between_moving_avg """
            SELECT 
                order_id,
                order_date,
                sales_amount,
                py_udwf_moving_avg(sales_amount) OVER (
                    ORDER BY order_date 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as moving_avg_3
            FROM udwf_sales
            ORDER BY order_date, order_id;
        """
        
        // Test 3.2: ROWS BETWEEN with partition
        qt_test_rows_between_with_partition """
            SELECT 
                order_id,
                region,
                sales_amount,
                py_udwf_moving_avg(sales_amount) OVER (
                    PARTITION BY region 
                    ORDER BY order_id 
                    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                ) as moving_avg_region
            FROM udwf_sales
            ORDER BY region, order_id;
        """
        
        // Test 3.3: ROWS BETWEEN UNBOUNDED PRECEDING
        qt_test_rows_unbounded_preceding """
            SELECT 
                order_id,
                category,
                sales_amount,
                py_udwf_running_sum(sales_amount) OVER (
                    PARTITION BY category 
                    ORDER BY order_id 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) as category_cumsum
            FROM udwf_sales
            ORDER BY category, order_id;
        """
        
        // Test 3.4: ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        qt_test_rows_unbounded_following """
            SELECT 
                order_id,
                region,
                sales_amount,
                py_udwf_running_sum(sales_amount) OVER (
                    PARTITION BY region 
                    ORDER BY order_id 
                    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                ) as remaining_sum
            FROM udwf_sales
            ORDER BY region, order_id;
        """
        
        // ========================================
        // Test Category 4: Multiple Window Functions in Single Query
        // ========================================
        
        // Test 4.1: Multiple UDWFs with same partition
        qt_test_multiple_udwf_same_partition """
            SELECT 
                order_id,
                region,
                sales_amount,
                py_udwf_running_sum(sales_amount) OVER (PARTITION BY region ORDER BY order_id) as running_sum,
                py_udwf_running_avg(sales_amount) OVER (PARTITION BY region ORDER BY order_id) as running_avg,
                py_udwf_running_count(quantity) OVER (PARTITION BY region ORDER BY order_id) as running_count
            FROM udwf_sales
            ORDER BY region, order_id;
        """
        
        // Test 4.2: Multiple UDWFs with different partitions
        qt_test_multiple_udwf_diff_partition """
            SELECT 
                order_id,
                region,
                category,
                sales_amount,
                py_udwf_running_sum(sales_amount) OVER (PARTITION BY region ORDER BY order_id) as region_sum,
                py_udwf_running_sum(sales_amount) OVER (PARTITION BY category ORDER BY order_id) as category_sum,
                py_udwf_running_sum(sales_amount) OVER (ORDER BY order_id) as total_sum
            FROM udwf_sales
            ORDER BY order_id;
        """
        
        // Test 4.3: Mix of UDWFs and built-in functions
        qt_test_mix_udwf_builtin """
            SELECT 
                order_id,
                region,
                sales_amount,
                py_udwf_running_sum(sales_amount) OVER (PARTITION BY region ORDER BY order_id) as py_sum,
                SUM(sales_amount) OVER (PARTITION BY region ORDER BY order_id) as builtin_sum,
                py_udwf_running_avg(sales_amount) OVER (PARTITION BY region ORDER BY order_id) as py_avg,
                AVG(sales_amount) OVER (PARTITION BY region ORDER BY order_id) as builtin_avg
            FROM udwf_sales
            ORDER BY region, order_id;
        """
        
        // ========================================
        // Test Category 5: Statistical Analysis Functions
        // ========================================
        
        // Test 5.1: Standard deviation by partition
        qt_test_stddev_by_partition """
            SELECT 
                symbol,
                trade_time,
                price,
                py_udwf_stddev(price) OVER (
                    PARTITION BY symbol 
                    ORDER BY trade_time 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as price_volatility
            FROM udwf_stock_prices
            ORDER BY symbol, trade_time;
        """
        
        // Test 5.2: Min and Max in moving window
        qt_test_min_max_moving_window """
            SELECT 
                symbol,
                trade_time,
                price,
                py_udwf_min(price) OVER (
                    PARTITION BY symbol 
                    ORDER BY trade_time 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as rolling_min,
                py_udwf_max(price) OVER (
                    PARTITION BY symbol 
                    ORDER BY trade_time 
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as rolling_max
            FROM udwf_stock_prices
            ORDER BY symbol, trade_time;
        """
        
        // Test 5.3: First and Last value in window
        qt_test_first_last_value """
            SELECT 
                symbol,
                trade_time,
                price,
                py_udwf_first_value(price) OVER (
                    PARTITION BY symbol 
                    ORDER BY trade_time
                ) as opening_price,
                py_udwf_last_value(price) OVER (
                    PARTITION BY symbol 
                    ORDER BY trade_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as closing_price
            FROM udwf_stock_prices
            ORDER BY symbol, trade_time;
        """
        
        // ========================================
        // Test Category 6: Complex Analytical Queries
        // ========================================
        
        // Test 6.1: Category-based cumulative sales analysis
        qt_test_complex_growth_analysis """
            SELECT 
                region,
                category,
                total_sales,
                py_udwf_running_sum(total_sales) OVER (
                    PARTITION BY region 
                    ORDER BY category
                ) as cumulative_by_category
            FROM (
                SELECT 
                    region,
                    category,
                    SUM(sales_amount) as total_sales
                FROM udwf_sales
                GROUP BY region, category
            ) t
            ORDER BY region, category;
        """
        
        // Test 6.2: Top-N analysis with ranking
        qt_test_topn_analysis """
            SELECT 
                student_name,
                class,
                subject,
                score,
                py_udwf_rank(score) OVER (
                    PARTITION BY class, subject 
                    ORDER BY score DESC
                ) as rank_in_subject
            FROM udwf_student_scores
            ORDER BY class, subject, score DESC;
        """
        
        // Test 6.3: Percentile calculation using window
        qt_test_percentile_analysis """
            SELECT 
                class,
                subject,
                score,
                py_udwf_running_count(score) OVER (
                    PARTITION BY class, subject 
                    ORDER BY score
                ) as count_below_or_equal
            FROM udwf_student_scores
            ORDER BY class, subject, score;
        """
        
        // ========================================
        // Test Category 7: Edge Cases and Null Handling
        // ========================================
        
        // Test 7.1: Empty partition
        qt_test_empty_partition """
            SELECT 
                order_id,
                region,
                sales_amount,
                py_udwf_running_sum(sales_amount) OVER (
                    PARTITION BY product 
                    ORDER BY order_id
                ) as product_sum
            FROM udwf_sales
            WHERE region = 'North'
            ORDER BY product, order_id;
        """
        
        // Test 7.2: Single row partition
        sql """ DROP TABLE IF EXISTS udwf_single_row; """
        sql """
        CREATE TABLE udwf_single_row (
            id INT,
            category VARCHAR(10),
            value DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO udwf_single_row VALUES
        (1, 'A', 100.0),
        (2, 'B', 200.0),
        (3, 'C', 300.0);
        """
        
        qt_test_single_row_partition """
            SELECT 
                id,
                category,
                value,
                py_udwf_running_sum(value) OVER (PARTITION BY category ORDER BY id) as cat_sum
            FROM udwf_single_row
            ORDER BY id;
        """
        
        // Test 7.3: NULL values handling
        sql """ DROP TABLE IF EXISTS udwf_with_nulls; """
        sql """
        CREATE TABLE udwf_with_nulls (
            id INT,
            category VARCHAR(10),
            value DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO udwf_with_nulls VALUES
        (1, 'A', 100.0),
        (2, 'A', NULL),
        (3, 'A', 200.0),
        (4, 'A', NULL),
        (5, 'A', 300.0),
        (6, 'B', NULL),
        (7, 'B', 150.0),
        (8, 'B', NULL);
        """
        
        qt_test_null_values """
            SELECT 
                id,
                category,
                value,
                py_udwf_running_sum(value) OVER (PARTITION BY category ORDER BY id) as sum_ignore_null,
                py_udwf_running_count(value) OVER (PARTITION BY category ORDER BY id) as count_non_null,
                py_udwf_running_avg(value) OVER (PARTITION BY category ORDER BY id) as avg_ignore_null
            FROM udwf_with_nulls
            ORDER BY category, id;
        """
        
        // Test 7.4: All NULL values in partition
        sql """ DROP TABLE IF EXISTS udwf_all_nulls; """
        sql """
        CREATE TABLE udwf_all_nulls (
            id INT,
            category VARCHAR(10),
            value DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO udwf_all_nulls VALUES
        (1, 'A', NULL),
        (2, 'A', NULL),
        (3, 'B', 100.0);
        """
        
        qt_test_all_nulls_partition """
            SELECT 
                id,
                category,
                value,
                py_udwf_running_sum(value) OVER (PARTITION BY category ORDER BY id) as sum_result,
                py_udwf_running_avg(value) OVER (PARTITION BY category ORDER BY id) as avg_result
            FROM udwf_all_nulls
            ORDER BY category, id;
        """
        
        // ========================================
        // Test Category 8: Performance and Scalability
        // ========================================
        
        // Test 8.1: Large partition test (high cardinality)
        qt_test_large_partition """
            SELECT 
                order_id,
                salesperson,
                sales_amount,
                py_udwf_running_sum(sales_amount) OVER (
                    PARTITION BY salesperson 
                    ORDER BY order_id
                ) as salesperson_total
            FROM udwf_sales
            ORDER BY salesperson, order_id;
        """
        
        // Test 8.2: Multiple complex windows in one query
        qt_test_multiple_complex_windows """
            SELECT 
                order_id,
                region,
                category,
                product,
                sales_amount,
                py_udwf_running_sum(sales_amount) OVER (PARTITION BY region ORDER BY order_id) as region_sum,
                py_udwf_running_avg(sales_amount) OVER (PARTITION BY region ORDER BY order_id) as region_avg,
                py_udwf_min(sales_amount) OVER (PARTITION BY category ORDER BY order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as category_min,
                py_udwf_max(sales_amount) OVER (PARTITION BY category ORDER BY order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as category_max,
                py_udwf_running_count(quantity) OVER (PARTITION BY product ORDER BY order_id) as product_count
            FROM udwf_sales
            ORDER BY order_id;
        """
        
        // ========================================
        // Test Category 9: Data Type Coverage
        // ========================================
        
        // Test 9.1: INT type
        qt_test_int_type """
            SELECT 
                order_id,
                region,
                quantity,
                py_udwf_running_count(quantity) OVER (PARTITION BY region ORDER BY order_id) as qty_count
            FROM udwf_sales
            ORDER BY region, order_id;
        """
        
        // Test 9.2: DECIMAL type
        qt_test_decimal_type """
            SELECT 
                order_id,
                category,
                sales_amount,
                py_udwf_running_sum(sales_amount) OVER (PARTITION BY category ORDER BY order_id) as decimal_sum
            FROM udwf_sales
            ORDER BY category, order_id;
        """
        
        // Test 9.3: DOUBLE type
        qt_test_double_type """
            SELECT 
                trade_id,
                symbol,
                price,
                py_udwf_running_avg(price) OVER (PARTITION BY symbol ORDER BY trade_time) as avg_price
            FROM udwf_stock_prices
            ORDER BY symbol, trade_time;
        """
        
        // ========================================
        // Test Category 10: Subquery and CTE with Window Functions
        // ========================================
        
        // Test 10.1: Window function in subquery
        qt_test_window_in_subquery """
            SELECT 
                region,
                AVG(running_sum) as avg_running_sum
            FROM (
                SELECT 
                    region,
                    order_id,
                    py_udwf_running_sum(sales_amount) OVER (
                        PARTITION BY region 
                        ORDER BY order_id
                    ) as running_sum
                FROM udwf_sales
            ) t
            GROUP BY region
            ORDER BY region;
        """
        
        // Test 10.2: Window function with CTE
        qt_test_window_with_cte """
            WITH regional_sales AS (
                SELECT 
                    region,
                    order_id,
                    sales_amount,
                    py_udwf_running_sum(sales_amount) OVER (
                        PARTITION BY region 
                        ORDER BY order_id
                    ) as cumulative_sales
                FROM udwf_sales
            )
            SELECT 
                region,
                MAX(cumulative_sales) as max_cumulative
            FROM regional_sales
            GROUP BY region
            ORDER BY region;
        """
        
        // Test 10.3: Nested window functions (window over window result)
        qt_test_nested_windows """
            SELECT 
                region,
                order_id,
                sales_amount,
                running_sum,
                py_udwf_running_avg(running_sum) OVER (
                    PARTITION BY region 
                    ORDER BY order_id
                ) as avg_of_running_sum
            FROM (
                SELECT 
                    region,
                    order_id,
                    sales_amount,
                    py_udwf_running_sum(sales_amount) OVER (
                        PARTITION BY region 
                        ORDER BY order_id
                    ) as running_sum
                FROM udwf_sales
            ) t
            ORDER BY region, order_id;
        """
        
        // ========================================
        // Test Category 11: JOIN with Window Functions
        // ========================================
        
        // Test 11.1: Window function after JOIN
        sql """ DROP TABLE IF EXISTS udwf_customers; """
        sql """
        CREATE TABLE udwf_customers (
            salesperson VARCHAR(50),
            customer_level VARCHAR(20),
            commission_rate DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(salesperson)
        DISTRIBUTED BY HASH(salesperson) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO udwf_customers VALUES
        ('Alice', 'Gold', 0.15),
        ('Bob', 'Silver', 0.10),
        ('Charlie', 'Gold', 0.15),
        ('David', 'Bronze', 0.08),
        ('Eve', 'Gold', 0.15),
        ('Frank', 'Silver', 0.10),
        ('Grace', 'Gold', 0.15),
        ('Henry', 'Bronze', 0.08);
        """
        
        qt_test_window_after_join """
            SELECT 
                s.order_id,
                s.salesperson,
                c.customer_level,
                s.sales_amount,
                py_udwf_running_sum(s.sales_amount) OVER (
                    PARTITION BY c.customer_level 
                    ORDER BY s.order_id
                ) as level_running_sum
            FROM udwf_sales s
            JOIN udwf_customers c ON s.salesperson = c.salesperson
            ORDER BY c.customer_level, s.order_id;
        """
        
        // ========================================
        // Cleanup
        // ========================================
        
        sql """ DROP FUNCTION IF EXISTS py_udwf_running_sum(DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_udwf_running_avg(DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_udwf_running_count(INT); """
        sql """ DROP FUNCTION IF EXISTS py_udwf_moving_avg(DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_udwf_stddev(DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_udwf_min(DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_udwf_max(DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_udwf_first_value(DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_udwf_last_value(DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_udwf_rank(INT); """
        
        sql """ DROP TABLE IF EXISTS udwf_sales; """
        sql """ DROP TABLE IF EXISTS udwf_stock_prices; """
        sql """ DROP TABLE IF EXISTS udwf_student_scores; """
        sql """ DROP TABLE IF EXISTS udwf_single_row; """
        sql """ DROP TABLE IF EXISTS udwf_with_nulls; """
        sql """ DROP TABLE IF EXISTS udwf_all_nulls; """
        sql """ DROP TABLE IF EXISTS udwf_customers; """
        
    } finally {
        // Ensure cleanup even if tests fail
        try {
            sql """ DROP FUNCTION IF EXISTS py_udwf_running_sum(DOUBLE); """
            sql """ DROP FUNCTION IF EXISTS py_udwf_running_avg(DOUBLE); """
            sql """ DROP FUNCTION IF EXISTS py_udwf_running_count(INT); """
            sql """ DROP FUNCTION IF EXISTS py_udwf_moving_avg(DOUBLE); """
            sql """ DROP FUNCTION IF EXISTS py_udwf_stddev(DOUBLE); """
            sql """ DROP FUNCTION IF EXISTS py_udwf_min(DOUBLE); """
            sql """ DROP FUNCTION IF EXISTS py_udwf_max(DOUBLE); """
            sql """ DROP FUNCTION IF EXISTS py_udwf_first_value(DOUBLE); """
            sql """ DROP FUNCTION IF EXISTS py_udwf_last_value(DOUBLE); """
            sql """ DROP FUNCTION IF EXISTS py_udwf_rank(INT); """
            
            sql """ DROP TABLE IF EXISTS udwf_sales; """
            sql """ DROP TABLE IF EXISTS udwf_stock_prices; """
            sql """ DROP TABLE IF EXISTS udwf_student_scores; """
            sql """ DROP TABLE IF EXISTS udwf_single_row; """
            sql """ DROP TABLE IF EXISTS udwf_with_nulls; """
            sql """ DROP TABLE IF EXISTS udwf_all_nulls; """
            sql """ DROP TABLE IF EXISTS udwf_customers; """
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }
}
