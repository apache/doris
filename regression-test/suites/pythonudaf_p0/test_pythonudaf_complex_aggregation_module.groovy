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

suite("test_pythonudaf_complex_aggregation_module") {
    // Test complex aggregation scenarios with Python UDAFs using file-based deployment
    // UDAFs are loaded from pyudaf.zip file
    
    def pyPath = """${context.file.parent}/udaf_scripts/pyudaf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${pyPath}".toString())
    
    try {
        // Create test table with statistical data
        sql """ DROP TABLE IF EXISTS stats_test_file; """
        sql """
        CREATE TABLE stats_test_file (
            id INT,
            category VARCHAR(50),
            value DOUBLE,
            score INT,
            tag VARCHAR(50)
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO stats_test_file VALUES
        (1, 'A', 10.5, 85, 'alpha'),
        (2, 'A', 20.3, 92, 'beta'),
        (3, 'A', 15.7, 78, 'gamma'),
        (4, 'A', 30.2, 95, 'delta'),
        (5, 'A', 25.1, 88, 'alpha'),
        (6, 'B', 12.4, 70, 'beta'),
        (7, 'B', 18.9, 85, 'gamma'),
        (8, 'B', 22.5, 90, 'alpha'),
        (9, 'B', 16.3, 82, 'beta'),
        (10, 'C', 35.7, 98, 'delta'),
        (11, 'C', 28.4, 91, 'gamma'),
        (12, 'C', 31.2, 87, 'alpha'),
        (13, 'C', 26.8, 93, 'beta'),
        (14, 'C', 29.5, 89, 'delta'),
        (15, 'C', 33.1, 95, 'gamma');
        """
        
        qt_select_data """ SELECT * FROM stats_test_file ORDER BY id; """
        
        // ========================================
        // Test 1: Variance UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_variance_file(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_variance_file(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "stats_udaf.VarianceUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        qt_variance_all """ SELECT py_variance_file(value) as variance FROM stats_test_file; """
        qt_variance_group """ SELECT category, py_variance_file(value) as variance 
                             FROM stats_test_file GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 2: Standard Deviation UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_stddev_file(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_stddev_file(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "stats_udaf.StdDevUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        qt_stddev_all """ SELECT py_stddev_file(value) as stddev FROM stats_test_file; """
        qt_stddev_group """ SELECT category, py_stddev_file(value) as stddev 
                            FROM stats_test_file GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 3: Median UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_median_file(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_median_file(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "stats_udaf.MedianUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        qt_median_all """ SELECT py_median_file(value) as median FROM stats_test_file; """
        qt_median_group """ SELECT category, py_median_file(value) as median 
                            FROM stats_test_file GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 4: Collect List UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_collect_list_file(VARCHAR); """
        sql """
        CREATE AGGREGATE FUNCTION py_collect_list_file(VARCHAR)
        RETURNS VARCHAR
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "stats_udaf.CollectListUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        qt_collect_all """ SELECT py_collect_list_file(tag) as tags FROM stats_test_file; """
        qt_collect_group """ SELECT category, py_collect_list_file(tag) as tags 
                             FROM stats_test_file GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 5: Range (Max - Min) UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_value_range(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_value_range(INT)
        RETURNS INT
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "stats_udaf.RangeUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        qt_range_all """ SELECT py_value_range(score) as score_range FROM stats_test_file; """
        qt_range_group """ SELECT category, py_value_range(score) as score_range 
                           FROM stats_test_file GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 6: Geometric Mean UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_geomean_file(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_geomean_file(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "stats_udaf.GeometricMeanUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        qt_geomean_all """ SELECT py_geomean_file(value) as geomean FROM stats_test_file; """
        qt_geomean_group """ SELECT category, py_geomean_file(value) as geomean 
                             FROM stats_test_file GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 7: Weighted Average UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_weighted_avg_file(DOUBLE, INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_weighted_avg_file(DOUBLE, INT)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "stats_udaf.WeightedAvgUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        qt_weighted_avg_all """ SELECT py_weighted_avg_file(value, score) as weighted_avg FROM stats_test_file; """
        qt_weighted_avg_group """ SELECT category, py_weighted_avg_file(value, score) as weighted_avg 
                                  FROM stats_test_file GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 8: Multiple aggregations in one query
        // ========================================
        qt_multi_agg """ 
            SELECT 
                category,
                py_variance_file(value) as variance,
                py_stddev_file(value) as stddev,
                py_median_file(value) as median,
                py_value_range(score) as score_range,
                py_geomean_file(value) as geomean,
                py_weighted_avg_file(value, score) as weighted_avg
            FROM stats_test_file 
            GROUP BY category 
            ORDER BY category;
        """
        
        // ========================================
        // Test 9: Window Function Support
        // ========================================
        qt_window_partition """ 
            SELECT 
                id,
                category,
                value,
                py_variance_file(value) OVER(PARTITION BY category) as variance_by_category,
                py_median_file(value) OVER(PARTITION BY category) as median_by_category
            FROM stats_test_file 
            ORDER BY category, id;
        """
        
        qt_window_order """ 
            SELECT 
                id,
                category,
                value,
                py_variance_file(value) OVER(PARTITION BY category ORDER BY id) as running_variance,
                py_median_file(value) OVER(PARTITION BY category ORDER BY id) as running_median
            FROM stats_test_file 
            ORDER BY category, id;
        """
        
        // ========================================
        // Test 10: Comparison with Native Functions
        // ========================================
        qt_compare_native """ 
            SELECT 
                category,
                py_stddev_file(value) as py_stddev,
                stddev(value) as native_stddev,
                py_variance_file(value) as py_variance,
                variance(value) as native_variance
            FROM stats_test_file 
            GROUP BY category 
            ORDER BY category;
        """
        
        // ========================================
        // Test 11: NULL Handling
        // ========================================
        sql """ DROP TABLE IF EXISTS stats_nulls_file; """
        sql """
        CREATE TABLE stats_nulls_file (
            id INT,
            value DOUBLE,
            score INT,
            tag VARCHAR(50)
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO stats_nulls_file VALUES
        (1, 10.0, 80, 'alpha'),
        (2, NULL, 90, 'beta'),
        (3, 20.0, NULL, 'alpha'),
        (4, 30.0, 85, NULL),
        (5, NULL, NULL, NULL);
        """
        
        qt_null_handling """ 
            SELECT 
                py_variance_file(value) as variance,
                py_median_file(value) as median,
                py_value_range(score) as range_val,
                py_collect_list_file(tag) as tags
            FROM stats_nulls_file;
        """
        
        // ========================================
        // Test 12: Global Functions
        // ========================================
        sql """ DROP GLOBAL FUNCTION IF EXISTS py_variance_global(DOUBLE); """
        sql """
        CREATE GLOBAL AGGREGATE FUNCTION py_variance_global(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "stats_udaf.VarianceUDAF",
            "type" = "PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        );
        """
        
        qt_global_variance """ SELECT py_variance_global(value) as variance FROM stats_test_file; """
        qt_global_variance_group """ SELECT category, py_variance_global(value) as variance 
                                     FROM stats_test_file GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 13: Edge Cases
        // ========================================
        
        // Empty result set
        qt_empty """ SELECT py_variance_file(value) as variance FROM stats_test_file WHERE 1=0; """
        
        // Single value
        qt_single """ SELECT py_variance_file(value) as variance FROM stats_test_file WHERE id = 1; """
        
        // Two values
        qt_two """ SELECT py_median_file(value) as median FROM stats_test_file WHERE id IN (1, 2); """
        
    } finally {
        try_sql("DROP GLOBAL FUNCTION IF EXISTS py_variance_global(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_variance_file(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_stddev_file(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_median_file(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_collect_list_file(VARCHAR);")
        try_sql("DROP FUNCTION IF EXISTS py_value_range(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_geomean_file(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_weighted_avg_file(DOUBLE, INT);")
        try_sql("DROP TABLE IF EXISTS stats_test_file;")
        try_sql("DROP TABLE IF EXISTS stats_nulls_file;")
    }
}
