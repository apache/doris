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

suite("test_python_udaf_complex") {
    // Test complex UDAF functions using pandas and numpy
    // Dependencies: pandas, numpy

    def pyPath = """${context.file.parent}/py_udf_complex_scripts/py_udf_complex.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${pyPath}".toString())

    try {
        // Create test data table
        sql """ DROP TABLE IF EXISTS udaf_test_data; """
        sql """
        CREATE TABLE udaf_test_data (
            id INT,
            category STRING,
            value DOUBLE,
            weight DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO udaf_test_data VALUES
        (1, 'A', 10.0, 1.0),
        (2, 'A', 20.0, 2.0),
        (3, 'A', 30.0, 3.0),
        (4, 'A', 40.0, 2.0),
        (5, 'A', 50.0, 1.0),
        (6, 'B', 100.0, 1.0),
        (7, 'B', 110.0, 1.5),
        (8, 'B', 90.0, 2.0),
        (9, 'B', 105.0, 1.5),
        (10, 'B', 95.0, 2.0),
        (11, 'C', 5.0, 1.0),
        (12, 'C', 500.0, 1.0),
        (13, 'C', 10.0, 1.0),
        (14, 'C', 15.0, 1.0),
        (15, 'C', 8.0, 1.0);
        """

        // ========================================
        // Test 1: Weighted Average UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_weighted_avg(DOUBLE, DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_weighted_avg(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udaf.WeightedAverageUDAF",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_weighted_avg """
            SELECT category, py_weighted_avg(value, weight) AS weighted_avg
            FROM udaf_test_data
            GROUP BY category
            ORDER BY category;
        """

        // ========================================
        // Test 2: Skewness UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_skewness(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_skewness(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udaf.SkewnessUDAF",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_skewness """
            SELECT category, py_skewness(value) AS skewness
            FROM udaf_test_data
            GROUP BY category
            ORDER BY category;
        """

        // ========================================
        // Test 3: Kurtosis UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_kurtosis(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_kurtosis(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udaf.KurtosisUDAF",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_kurtosis """
            SELECT category, py_kurtosis(value) AS kurtosis
            FROM udaf_test_data
            GROUP BY category
            ORDER BY category;
        """

        // ========================================
        // Test 4: Correlation UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_correlation_agg(DOUBLE, DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_correlation_agg(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udaf.CorrelationUDAF",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_correlation_agg """
            SELECT category, py_correlation_agg(value, weight) AS correlation
            FROM udaf_test_data
            GROUP BY category
            ORDER BY category;
        """

        // ========================================
        // Test 5: Covariance UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_covariance_agg(DOUBLE, DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_covariance_agg(DOUBLE, DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udaf.CovarianceUDAF",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_covariance_agg """
            SELECT category, py_covariance_agg(value, weight) AS covariance
            FROM udaf_test_data
            GROUP BY category
            ORDER BY category;
        """

        // ========================================
        // Test 6: Retention Rate UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_retention_rate(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_retention_rate(INT)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udaf.RetentionRateUDAF",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS user_retention_test; """
        sql """
        CREATE TABLE user_retention_test (
            user_id INT,
            cohort STRING,
            is_retained INT
        ) ENGINE=OLAP
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO user_retention_test VALUES
        (1, '2024-01', 1),
        (2, '2024-01', 1),
        (3, '2024-01', 0),
        (4, '2024-01', 1),
        (5, '2024-01', 0),
        (6, '2024-02', 1),
        (7, '2024-02', 1),
        (8, '2024-02', 1),
        (9, '2024-02', 0),
        (10, '2024-02', 1);
        """

        qt_retention_rate """
            SELECT cohort, py_retention_rate(is_retained) AS retention_rate
            FROM user_retention_test
            GROUP BY cohort
            ORDER BY cohort;
        """

        // ========================================
        // Test 7: Funnel Conversion UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_funnel_conversion(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_funnel_conversion(INT)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udaf.ConversionFunnelUDAF",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS funnel_test; """
        sql """
        CREATE TABLE funnel_test (
            user_id INT,
            campaign STRING,
            max_stage_reached INT
        ) ENGINE=OLAP
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO funnel_test VALUES
        (1, 'Campaign_A', 4),
        (2, 'Campaign_A', 3),
        (3, 'Campaign_A', 2),
        (4, 'Campaign_A', 1),
        (5, 'Campaign_A', 4),
        (6, 'Campaign_A', 2),
        (7, 'Campaign_A', 1),
        (8, 'Campaign_A', 3),
        (9, 'Campaign_B', 3),
        (10, 'Campaign_B', 2),
        (11, 'Campaign_B', 1),
        (12, 'Campaign_B', 1);
        """

        qt_funnel_conversion """
            SELECT campaign, py_funnel_conversion(max_stage_reached) AS funnel_stats
            FROM funnel_test
            GROUP BY campaign
            ORDER BY campaign;
        """

        // ========================================
        // Test 8: Gini Coefficient UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_gini_coefficient(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_gini_coefficient(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udaf.GiniCoefficientUDAF",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS income_distribution_test; """
        sql """
        CREATE TABLE income_distribution_test (
            id INT,
            region STRING,
            income DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO income_distribution_test VALUES
        (1, 'Equal', 100.0),
        (2, 'Equal', 100.0),
        (3, 'Equal', 100.0),
        (4, 'Equal', 100.0),
        (5, 'Unequal', 10.0),
        (6, 'Unequal', 20.0),
        (7, 'Unequal', 70.0),
        (8, 'Unequal', 900.0);
        """

        qt_gini_coefficient """
            SELECT region, py_gini_coefficient(income) AS gini
            FROM income_distribution_test
            GROUP BY region
            ORDER BY region;
        """

        // ========================================
        // Test 9: HHI (Market Concentration) UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_hhi(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_hhi(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udaf.HerfindahlIndexUDAF",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS market_share_test; """
        sql """
        CREATE TABLE market_share_test (
            id INT,
            company VARCHAR(100),
            industry VARCHAR(100),
            market_share DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO market_share_test VALUES
        (1, 'A', 'Competitive', 10.0),
        (2, 'B', 'Competitive', 15.0),
        (3, 'C', 'Competitive', 12.0),
        (4, 'D', 'Competitive', 8.0),
        (5, 'E', 'Competitive', 20.0),
        (6, 'F', 'Competitive', 35.0),
        (7, 'X', 'Monopoly', 80.0),
        (8, 'Y', 'Monopoly', 15.0),
        (9, 'Z', 'Monopoly', 5.0);
        """

        qt_hhi """
            SELECT industry, py_hhi(market_share) AS hhi_index
            FROM market_share_test
            GROUP BY industry
            ORDER BY industry;
        """

        // ========================================
        // Test 10: Trend Slope UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_trend_slope(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_trend_slope(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udaf.TrendSlopeUDAF",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS time_series_test; """
        sql """
        CREATE TABLE time_series_test (
            ts INT,
            series_name STRING,
            value DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(ts)
        DISTRIBUTED BY HASH(ts) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO time_series_test VALUES
        (1, 'Increasing', 10.0),
        (2, 'Increasing', 15.0),
        (3, 'Increasing', 22.0),
        (4, 'Increasing', 28.0),
        (5, 'Increasing', 35.0),
        (1, 'Decreasing', 50.0),
        (2, 'Decreasing', 45.0),
        (3, 'Decreasing', 38.0),
        (4, 'Decreasing', 32.0),
        (5, 'Decreasing', 25.0),
        (1, 'Flat', 20.0),
        (2, 'Flat', 21.0),
        (3, 'Flat', 19.0),
        (4, 'Flat', 20.0),
        (5, 'Flat', 20.0);
        """

        qt_trend_slope """
            SELECT series_name, py_trend_slope(value) AS slope
            FROM time_series_test
            GROUP BY series_name
            ORDER BY series_name;
        """

        // ========================================
        // Test 11: Volatility UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_volatility(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_volatility(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udaf.VolatilityUDAF",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS stock_price_test; """
        sql """
        CREATE TABLE stock_price_test (
            day INT,
            stock STRING,
            price DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(day)
        DISTRIBUTED BY HASH(day) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO stock_price_test VALUES
        (1, 'Stable', 100.0),
        (2, 'Stable', 101.0),
        (3, 'Stable', 100.5),
        (4, 'Stable', 101.2),
        (5, 'Stable', 100.8),
        (1, 'Volatile', 100.0),
        (2, 'Volatile', 110.0),
        (3, 'Volatile', 95.0),
        (4, 'Volatile', 115.0),
        (5, 'Volatile', 90.0);
        """

        qt_volatility """
            SELECT stock, py_volatility(price) AS volatility
            FROM stock_price_test
            GROUP BY stock
            ORDER BY stock;
        """

        // ========================================
        // Test 12: JSON Array Aggregation UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_json_array_agg(STRING); """
        sql """
        CREATE AGGREGATE FUNCTION py_json_array_agg(STRING)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "complex_udaf.JsonArrayAggUDAF",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_json_array_agg """
            SELECT category, py_json_array_agg(CAST(id AS STRING)) AS id_array
            FROM udaf_test_data
            GROUP BY category
            ORDER BY category;
        """

        // ========================================
        // Test 13: Window Function with UDAF
        // ========================================
        qt_window_weighted_avg """
            SELECT
                id,
                category,
                value,
                weight,
                py_weighted_avg(value, weight) OVER (
                    PARTITION BY category
                    ORDER BY id
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) AS rolling_weighted_avg
            FROM udaf_test_data
            ORDER BY category, id;
        """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_weighted_avg(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_skewness(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_kurtosis(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_correlation_agg(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_covariance_agg(DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_retention_rate(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_funnel_conversion(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_gini_coefficient(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_hhi(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_trend_slope(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_volatility(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_json_array_agg(STRING);")
        try_sql("DROP TABLE IF EXISTS udaf_test_data;")
        try_sql("DROP TABLE IF EXISTS user_retention_test;")
        try_sql("DROP TABLE IF EXISTS funnel_test;")
        try_sql("DROP TABLE IF EXISTS income_distribution_test;")
        try_sql("DROP TABLE IF EXISTS market_share_test;")
        try_sql("DROP TABLE IF EXISTS time_series_test;")
        try_sql("DROP TABLE IF EXISTS stock_price_test;")
    }
}
