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

suite("test_python_udf_business_logic") {
    // Test complex business logic using pandas and numpy
    // Dependencies: pandas, numpy

    def pyPath = """${context.file.parent}/py_udf_complex_scripts/py_udf_complex.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python zip path: ${pyPath}".toString())

    try {
        // ========================================
        // Test 1: Moving Average Calculation
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_moving_average(STRING, INT); """
        sql """
        CREATE FUNCTION py_moving_average(STRING, INT)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.calculate_moving_average",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_moving_average """
            SELECT py_moving_average('[10, 20, 30, 40, 50, 60, 70]', 3) AS ma_result;
        """

        // ========================================
        // Test 2: Exponentially Weighted Moving Average
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_ewma(STRING, INT); """
        sql """
        CREATE FUNCTION py_ewma(STRING, INT)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.calculate_ewma",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_ewma """
            SELECT py_ewma('[100, 110, 105, 115, 120, 118, 125]', 3) AS ewma_result;
        """

        // ========================================
        // Test 3: Anomaly Detection (Z-Score)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_detect_anomalies(STRING, DOUBLE); """
        sql """
        CREATE FUNCTION py_detect_anomalies(STRING, DOUBLE)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.detect_anomalies_zscore",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_anomaly_detection """
            SELECT py_detect_anomalies('[10, 12, 11, 13, 100, 11, 12, 10, 11]', 2.0) AS anomalies;
        """

        // ========================================
        // Test 4: Trend Analysis
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_calculate_trend(STRING); """
        sql """
        CREATE FUNCTION py_calculate_trend(STRING)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.calculate_trend",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS sales_trend_test; """
        sql """
        CREATE TABLE sales_trend_test (
            id INT,
            product STRING,
            weekly_sales STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO sales_trend_test VALUES
        (1, 'Product A', '[100, 120, 130, 145, 160, 175, 190]'),
        (2, 'Product B', '[200, 195, 185, 180, 170, 165, 155]'),
        (3, 'Product C', '[50, 52, 48, 51, 49, 50, 51]');
        """

        qt_trend_analysis """
            SELECT id, product, py_calculate_trend(weekly_sales) AS trend_info
            FROM sales_trend_test
            ORDER BY id;
        """

        // ========================================
        // Test 5: Percentile Calculation
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_percentiles(STRING, STRING); """
        sql """
        CREATE FUNCTION py_percentiles(STRING, STRING)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.calculate_percentiles",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_percentiles """
            SELECT py_percentiles('[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 100]', '[10, 25, 50, 75, 90]') AS percentiles;
        """

        // ========================================
        // Test 6: Comprehensive Statistics
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_statistics(STRING); """
        sql """
        CREATE FUNCTION py_statistics(STRING)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.calculate_statistics",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_statistics """
            SELECT py_statistics('[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]') AS stats;
        """

        // ========================================
        // Test 7: Correlation Calculation
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_correlation(STRING, STRING); """
        sql """
        CREATE FUNCTION py_correlation(STRING, STRING)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.calculate_correlation",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_correlation """
            SELECT
                py_correlation('[1, 2, 3, 4, 5]', '[2, 4, 6, 8, 10]') AS perfect_positive,
                py_correlation('[1, 2, 3, 4, 5]', '[5, 4, 3, 2, 1]') AS perfect_negative,
                py_correlation('[1, 2, 3, 4, 5]', '[3, 1, 4, 1, 5]') AS weak;
        """

        // ========================================
        // Test 8: RFM Score Calculation
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_rfm_score(INT, INT, DOUBLE); """
        sql """
        CREATE FUNCTION py_rfm_score(INT, INT, DOUBLE)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.calculate_rfm_score",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS customer_rfm_test; """
        sql """
        CREATE TABLE customer_rfm_test (
            customer_id INT,
            days_since_last_purchase INT,
            purchase_count INT,
            total_spend DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(customer_id)
        DISTRIBUTED BY HASH(customer_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO customer_rfm_test VALUES
        (1, 5, 25, 15000.0),
        (2, 30, 5, 500.0),
        (3, 100, 2, 100.0),
        (4, 7, 15, 8000.0),
        (5, 200, 1, 50.0);
        """

        qt_rfm_score """
            SELECT customer_id,
                   days_since_last_purchase,
                   purchase_count,
                   total_spend,
                   py_rfm_score(days_since_last_purchase, purchase_count, total_spend) AS rfm_analysis
            FROM customer_rfm_test
            ORDER BY customer_id;
        """

        // ========================================
        // Test 9: Customer Lifetime Value
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_calculate_ltv(DOUBLE, DOUBLE, DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_calculate_ltv(DOUBLE, DOUBLE, DOUBLE, DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.calculate_ltv",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_ltv """
            SELECT
                py_calculate_ltv(100.0, 12.0, 3.0, 0.25) AS ltv_3year,
                py_calculate_ltv(50.0, 4.0, 5.0, 0.20) AS ltv_5year;
        """

        // ========================================
        // Test 10: Churn Probability
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_churn_probability(DOUBLE, DOUBLE, INT); """
        sql """
        CREATE FUNCTION py_churn_probability(DOUBLE, DOUBLE, INT)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.calculate_churn_probability",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        sql """ DROP TABLE IF EXISTS user_activity_test; """
        sql """
        CREATE TABLE user_activity_test (
            user_id INT,
            days_since_last_activity DOUBLE,
            avg_days_between_activities DOUBLE,
            total_activities INT
        ) ENGINE=OLAP
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """

        sql """
        INSERT INTO user_activity_test VALUES
        (1, 5.0, 7.0, 50),
        (2, 30.0, 7.0, 10),
        (3, 60.0, 14.0, 5),
        (4, 90.0, 30.0, 2),
        (5, 2.0, 3.0, 100);
        """

        qt_churn_probability """
            SELECT user_id,
                   days_since_last_activity,
                   avg_days_between_activities,
                   total_activities,
                   py_churn_probability(days_since_last_activity, avg_days_between_activities, total_activities) AS churn_prob
            FROM user_activity_test
            ORDER BY user_id;
        """

        // ========================================
        // Test 11: NPV Calculation
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_npv(STRING, DOUBLE); """
        sql """
        CREATE FUNCTION py_npv(STRING, DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.calculate_npv",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_npv """
            SELECT
                py_npv('[-1000, 300, 400, 500, 600]', 0.1) AS npv_10percent,
                py_npv('[-1000, 300, 400, 500, 600]', 0.05) AS npv_5percent;
        """

        // ========================================
        // Test 12: IRR Calculation
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_irr(STRING); """
        sql """
        CREATE FUNCTION py_irr(STRING)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.calculate_irr",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_irr """
            SELECT py_irr('[-1000, 300, 400, 500, 600]') AS irr;
        """

        // ========================================
        // Test 13: Sharpe Ratio
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_sharpe_ratio(STRING, DOUBLE); """
        sql """
        CREATE FUNCTION py_sharpe_ratio(STRING, DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.calculate_sharpe_ratio",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_sharpe_ratio """
            SELECT py_sharpe_ratio('[0.05, 0.03, -0.02, 0.04, 0.06, -0.01, 0.03]', 0.02) AS sharpe;
        """

        // ========================================
        // Test 14: Data Normalization
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_normalize(STRING, STRING); """
        sql """
        CREATE FUNCTION py_normalize(STRING, STRING)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.normalize_values",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_normalize """
            SELECT
                py_normalize('[10, 20, 30, 40, 50]', 'minmax') AS minmax_normalized,
                py_normalize('[10, 20, 30, 40, 50]', 'zscore') AS zscore_normalized;
        """

        // ========================================
        // Test 15: Value Binning
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_bin_values(STRING, INT, STRING); """
        sql """
        CREATE FUNCTION py_bin_values(STRING, INT, STRING)
        RETURNS STRING
        PROPERTIES (
            "file" = "file://${pyPath}",
            "symbol" = "business_logic.bin_values",
            "type" = "PYTHON_UDF",
            "runtime_version" = "${runtime_version}"
        );
        """

        qt_bin_values """
            SELECT py_bin_values('[15, 25, 35, 45, 55, 65, 75, 85]', 4, '["Low", "Medium", "High", "Very High"]') AS binned;
        """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_moving_average(STRING, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_ewma(STRING, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_detect_anomalies(STRING, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_calculate_trend(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_percentiles(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_statistics(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_correlation(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_rfm_score(INT, INT, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_calculate_ltv(DOUBLE, DOUBLE, DOUBLE, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_churn_probability(DOUBLE, DOUBLE, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_npv(STRING, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_irr(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_sharpe_ratio(STRING, DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_normalize(STRING, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_bin_values(STRING, INT, STRING);")
        try_sql("DROP TABLE IF EXISTS sales_trend_test;")
        try_sql("DROP TABLE IF EXISTS customer_rfm_test;")
        try_sql("DROP TABLE IF EXISTS user_activity_test;")
    }
}
