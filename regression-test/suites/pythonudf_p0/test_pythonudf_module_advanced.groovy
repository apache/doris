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

suite("test_pythonudf_module_advanced") {
    // Test advanced Python UDF features using Module mode
    
    def zipPath = """${context.file.parent}/udf_scripts/python_udf_module_test.zip"""
    scp_udf_file_to_all_be(zipPath)
    def runtime_version = "3.8.10"
    log.info("Python Zip path: ${zipPath}".toString())
    
    try {
        // Test 1: Use different module paths in zip package
        sql """ DROP FUNCTION IF EXISTS py_module_ltv(BIGINT, BIGINT, DOUBLE); """
        sql """
        CREATE FUNCTION py_module_ltv(BIGINT, BIGINT, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "file" = "file://${zipPath}",
            "symbol" = "python_udf_module_test.main.safe_ltv",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        );
        """
        
        qt_select_module_ltv_normal """ SELECT py_module_ltv(10, 100, 5000.0) AS result; """
        qt_select_module_ltv_null """ SELECT py_module_ltv(NULL, 100, 5000.0) AS result; """
        qt_select_module_ltv_zero """ SELECT py_module_ltv(0, 0, 5000.0) AS result; """
        
        // Test 2: Use Module UDF in complex queries
        sql """ DROP TABLE IF EXISTS customer_analytics; """
        sql """
        CREATE TABLE customer_analytics (
            customer_id BIGINT,
            days_inactive BIGINT,
            total_orders BIGINT,
            total_revenue DOUBLE,
            customer_segment STRING
        ) ENGINE=OLAP 
        DUPLICATE KEY(customer_id)
        DISTRIBUTED BY HASH(customer_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO customer_analytics VALUES
        (1001, 5, 50, 10000.0, 'Premium'),
        (1002, 30, 10, 2000.0, 'Regular'),
        (1003, 60, 5, 500.0, 'Inactive'),
        (1004, 2, 100, 25000.0, 'VIP'),
        (1005, 15, 25, 5000.0, 'Regular'),
        (1006, NULL, 30, 6000.0, 'Regular'),
        (1007, 10, NULL, 3000.0, 'Regular'),
        (1008, 45, 8, NULL, 'Inactive'),
        (1009, 0, 200, 50000.0, 'VIP'),
        (1010, 90, 2, 100.0, 'Churned');
        """
        
        qt_select_customer_analytics """ 
        SELECT 
            customer_id,
            customer_segment,
            days_inactive,
            total_orders,
            total_revenue,
            py_module_ltv(days_inactive, total_orders, total_revenue) AS ltv_score
        FROM customer_analytics
        ORDER BY customer_id;
        """
        
        // Test 3: Use Module UDF for group aggregation
        qt_select_segment_analysis """ 
        SELECT 
            customer_segment,
            COUNT(*) AS customer_count,
            AVG(total_revenue) AS avg_revenue,
            AVG(py_module_ltv(days_inactive, total_orders, total_revenue)) AS avg_ltv_score
        FROM customer_analytics
        GROUP BY customer_segment
        ORDER BY customer_segment;
        """
        
        // Test 4: Use Module UDF for filtering
        qt_select_high_value_customers """ 
        SELECT 
            customer_id,
            customer_segment,
            total_revenue,
            py_module_ltv(days_inactive, total_orders, total_revenue) AS ltv_score
        FROM customer_analytics
        WHERE py_module_ltv(days_inactive, total_orders, total_revenue) > 100
        ORDER BY ltv_score DESC;
        """
        
        // Test 5: Use Module UDF for sorting
        qt_select_sorted_by_ltv """ 
        SELECT 
            customer_id,
            customer_segment,
            py_module_ltv(days_inactive, total_orders, total_revenue) AS ltv_score
        FROM customer_analytics
        ORDER BY py_module_ltv(days_inactive, total_orders, total_revenue) DESC
        LIMIT 5;
        """
        
        // Test 6: Use Module UDF with multiple conditions
        qt_select_complex_query """ 
        SELECT 
            customer_id,
            customer_segment,
            days_inactive,
            total_orders,
            total_revenue,
            py_module_ltv(days_inactive, total_orders, total_revenue) AS ltv_score,
            CASE 
                WHEN py_module_ltv(days_inactive, total_orders, total_revenue) > 200 THEN 'High Value'
                WHEN py_module_ltv(days_inactive, total_orders, total_revenue) > 100 THEN 'Medium Value'
                WHEN py_module_ltv(days_inactive, total_orders, total_revenue) IS NOT NULL THEN 'Low Value'
                ELSE 'Unknown'
            END AS value_category
        FROM customer_analytics
        ORDER BY ltv_score DESC;
        """
        
        // Test 7: Use Module UDF with JOIN operations
        sql """ DROP TABLE IF EXISTS customer_info; """
        sql """
        CREATE TABLE customer_info (
            customer_id BIGINT,
            customer_name STRING,
            registration_date DATE
        ) ENGINE=OLAP 
        DUPLICATE KEY(customer_id)
        DISTRIBUTED BY HASH(customer_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO customer_info VALUES
        (1001, 'Alice Johnson', '2023-01-15'),
        (1002, 'Bob Smith', '2023-03-20'),
        (1003, 'Charlie Brown', '2022-11-10'),
        (1004, 'Diana Prince', '2023-05-01'),
        (1005, 'Eve Wilson', '2023-02-14');
        """
        
        qt_select_join_with_module_udf """ 
        SELECT 
            ci.customer_id,
            ci.customer_name,
            ca.customer_segment,
            ca.total_revenue,
            py_module_ltv(ca.days_inactive, ca.total_orders, ca.total_revenue) AS ltv_score
        FROM customer_info ci
        JOIN customer_analytics ca ON ci.customer_id = ca.customer_id
        WHERE py_module_ltv(ca.days_inactive, ca.total_orders, ca.total_revenue) IS NOT NULL
        ORDER BY ltv_score DESC;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_module_ltv(BIGINT, BIGINT, DOUBLE);")
        try_sql("DROP TABLE IF EXISTS customer_analytics;")
        try_sql("DROP TABLE IF EXISTS customer_info;")
    }
}
