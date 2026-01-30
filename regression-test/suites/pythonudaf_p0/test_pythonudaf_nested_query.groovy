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

suite("test_pythonudaf_nested_query") {
    // Test Python UDAFs in complex nested queries, subqueries, CTEs, and JOINs
    
    def runtime_version = "3.8.10"
    
    try {
        // Create orders table
        sql """ DROP TABLE IF EXISTS orders; """
        sql """
        CREATE TABLE orders (
            order_id INT,
            customer_id INT,
            product_id INT,
            quantity INT,
            price DOUBLE,
            order_date DATE
        ) ENGINE=OLAP 
        DUPLICATE KEY(order_id)
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO orders VALUES
        (1, 101, 1, 2, 29.99, '2024-01-01'),
        (2, 101, 2, 1, 49.99, '2024-01-02'),
        (3, 102, 1, 3, 29.99, '2024-01-03'),
        (4, 102, 3, 1, 99.99, '2024-01-04'),
        (5, 103, 2, 2, 49.99, '2024-01-05'),
        (6, 103, 1, 1, 29.99, '2024-01-06'),
        (7, 104, 3, 2, 99.99, '2024-01-07'),
        (8, 104, 2, 3, 49.99, '2024-01-08'),
        (9, 105, 1, 4, 29.99, '2024-01-09'),
        (10, 105, 3, 1, 99.99, '2024-01-10');
        """
        
        // Create customers table
        sql """ DROP TABLE IF EXISTS customers; """
        sql """
        CREATE TABLE customers (
            customer_id INT,
            customer_name STRING,
            city STRING,
            segment STRING
        ) ENGINE=OLAP 
        DUPLICATE KEY(customer_id)
        DISTRIBUTED BY HASH(customer_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO customers VALUES
        (101, 'Alice', 'New York', 'Premium'),
        (102, 'Bob', 'Los Angeles', 'Standard'),
        (103, 'Charlie', 'Chicago', 'Premium'),
        (104, 'David', 'Houston', 'Standard'),
        (105, 'Eve', 'Phoenix', 'Premium');
        """
        
        qt_select_orders """ SELECT * FROM orders ORDER BY order_id; """
        qt_select_customers """ SELECT * FROM customers ORDER BY customer_id; """
        
        // Create Python UDAFs
        
        // UDAF 1: Total Revenue
        sql """ DROP FUNCTION IF EXISTS py_total_revenue(INT, DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_total_revenue(INT, DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "TotalRevenueUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class TotalRevenueUDAF:
    def __init__(self):
        self.total = 0.0
    
    @property
    def aggregate_state(self):
        return self.total
    
    def accumulate(self, quantity, price):
        if quantity is not None and price is not None:
            self.total += quantity * price
    
    def merge(self, other_state):
        self.total += other_state
    
    def finish(self):
        return self.total
\$\$;
        """
        
        // UDAF 2: Order Count
        sql """ DROP FUNCTION IF EXISTS py_order_count(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_order_count(INT)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "OrderCountUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class OrderCountUDAF:
    def __init__(self):
        self.count = 0
    
    @property
    def aggregate_state(self):
        return self.count
    
    def accumulate(self, order_id):
        if order_id is not None:
            self.count += 1
    
    def merge(self, other_state):
        self.count += other_state
    
    def finish(self):
        return self.count
\$\$;
        """
        
        // UDAF 3: Average Order Value
        sql """ DROP FUNCTION IF EXISTS py_avg_order_value(INT, DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_avg_order_value(INT, DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "AvgOrderValueUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class AvgOrderValueUDAF:
    def __init__(self):
        self.total_value = 0.0
        self.count = 0
    
    @property
    def aggregate_state(self):
        return (self.total_value, self.count)
    
    def accumulate(self, quantity, price):
        if quantity is not None and price is not None:
            self.total_value += quantity * price
            self.count += 1
    
    def merge(self, other_state):
        other_value, other_count = other_state
        self.total_value += other_value
        self.count += other_count
    
    def finish(self):
        if self.count == 0:
            return None
        return self.total_value / self.count
\$\$;
        """
        
        // ========================================
        // Test 1: Subquery with UDAF
        // ========================================
        qt_subquery_1 """
            SELECT customer_id, total_revenue
            FROM (
                SELECT customer_id, py_total_revenue(quantity, price) as total_revenue
                FROM orders
                GROUP BY customer_id
            ) t
            WHERE total_revenue > 100
            ORDER BY customer_id;
        """
        
        // ========================================
        // Test 2: JOIN with UDAF aggregation
        // ========================================
        qt_join_1 """
            SELECT 
                c.customer_name,
                c.city,
                c.segment,
                py_total_revenue(o.quantity, o.price) as total_spent,
                py_order_count(o.order_id) as order_count
            FROM customers c
            INNER JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_name, c.city, c.segment
            ORDER BY c.customer_name;
        """
        
        // ========================================
        // Test 3: CTE with UDAF
        // ========================================
        qt_cte_1 """
            WITH customer_stats AS (
                SELECT 
                    customer_id,
                    py_total_revenue(quantity, price) as revenue,
                    py_order_count(order_id) as orders
                FROM orders
                GROUP BY customer_id
            )
            SELECT 
                cs.customer_id,
                c.customer_name,
                cs.revenue,
                cs.orders
            FROM customer_stats cs
            JOIN customers c ON cs.customer_id = c.customer_id
            WHERE cs.revenue > 100
            ORDER BY cs.revenue DESC;
        """
        
        // ========================================
        // Test 4: Multiple CTEs with UDAFs
        // ========================================
        qt_cte_2 """
            WITH premium_customers AS (
                SELECT customer_id, customer_name, segment
                FROM customers
                WHERE segment = 'Premium'
            ),
            customer_revenue AS (
                SELECT 
                    o.customer_id,
                    py_total_revenue(o.quantity, o.price) as revenue
                FROM orders o
                GROUP BY o.customer_id
            )
            SELECT 
                pc.customer_name,
                cr.revenue
            FROM premium_customers pc
            JOIN customer_revenue cr ON pc.customer_id = cr.customer_id
            ORDER BY cr.revenue DESC;
        """
        
        // ========================================
        // Test 5: Nested aggregation
        // ========================================
        qt_nested_agg """
            SELECT 
                segment,
                COUNT(*) as customer_count,
                SUM(total_revenue) as segment_revenue
            FROM (
                SELECT 
                    c.customer_id,
                    c.segment,
                    py_total_revenue(o.quantity, o.price) as total_revenue
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                GROUP BY c.customer_id, c.segment
            ) t
            GROUP BY segment
            ORDER BY segment;
        """
        
        // ========================================
        // Test 6: HAVING clause with UDAF
        // ========================================
        qt_having """
            SELECT 
                c.segment,
                py_total_revenue(o.quantity, o.price) as segment_revenue,
                py_order_count(o.order_id) as order_count
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.segment
            HAVING py_total_revenue(o.quantity, o.price) > 300
            ORDER BY segment_revenue DESC;
        """
        
        // ========================================
        // Test 7: Self-join with UDAF
        // ========================================
        qt_self_join """
            SELECT 
                o1.product_id,
                py_total_revenue(o1.quantity, o1.price) as total_revenue
            FROM orders o1
            WHERE EXISTS (
                SELECT 1 FROM orders o2 
                WHERE o1.product_id = o2.product_id 
                AND o1.order_id != o2.order_id
            )
            GROUP BY o1.product_id
            ORDER BY o1.product_id;
        """
        
        // ========================================
        // Test 8: UNION with UDAF
        // ========================================
        qt_union """
            SELECT 'Premium' as segment_type, py_total_revenue(o.quantity, o.price) as revenue
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            WHERE c.segment = 'Premium'
            UNION ALL
            SELECT 'Standard' as segment_type, py_total_revenue(o.quantity, o.price) as revenue
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            WHERE c.segment = 'Standard'
            ORDER BY segment_type;
        """
        
        // ========================================
        // Test 9: Complex nested query with multiple UDAFs
        // ========================================
        qt_complex_nested """
            SELECT 
                segment,
                avg_revenue,
                max_revenue,
                min_revenue
            FROM (
                SELECT 
                    segment,
                    AVG(customer_revenue) as avg_revenue,
                    MAX(customer_revenue) as max_revenue,
                    MIN(customer_revenue) as min_revenue
                FROM (
                    SELECT 
                        c.customer_id,
                        c.segment,
                        py_total_revenue(o.quantity, o.price) as customer_revenue
                    FROM customers c
                    JOIN orders o ON c.customer_id = o.customer_id
                    GROUP BY c.customer_id, c.segment
                ) customer_level
                GROUP BY segment
            ) segment_level
            ORDER BY segment;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_total_revenue(INT, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_order_count(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_avg_order_value(INT, DOUBLE);")
        try_sql("DROP TABLE IF EXISTS orders;")
        try_sql("DROP TABLE IF EXISTS customers;")
    }
}
