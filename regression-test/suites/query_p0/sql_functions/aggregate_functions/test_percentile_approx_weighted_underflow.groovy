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

suite("test_percentile_approx_weighted_underflow") {
    def scales = ["1e-100", "1", "1e100", "1e-300", "1e300",
                  "4.9406564584124654e-324", "1.7976931348623157e308"]
    scales.eachWithIndex { weight, index ->
        "order_qt_scale_${index}" """
            SELECT percentile_approx_weighted(x, w, 0.5),
                   percentile_approx_weighted(x, w, 0.5, 2048)
            FROM (
                SELECT CAST(0 AS DOUBLE) AS x, CAST('${weight}' AS DOUBLE) AS w
                UNION ALL SELECT CAST(10 AS DOUBLE), CAST('${weight}' AS DOUBLE)
            ) t;
        """
    }

    ["1e-100", "1", "1e100"].eachWithIndex { weight, index ->
        "order_qt_ratio_${index}" """
            SELECT percentile_approx_weighted(x, w, 0.5),
                   percentile_approx_weighted(x, w, 0.5, 2048)
            FROM (
                SELECT CAST(0 AS DOUBLE) AS x, CAST('${weight}' AS DOUBLE) AS w
                UNION ALL SELECT CAST(10 AS DOUBLE), 3 * CAST('${weight}' AS DOUBLE)
            ) t;
        """
    }

    order_qt_underflow """
        SELECT percentile_approx_weighted(x, w, 0.5) AS p
        FROM (
            SELECT CAST(0 AS DOUBLE) AS x, CAST(1e-100 AS DOUBLE) AS w
            UNION ALL SELECT CAST(10 AS DOUBLE), CAST(1e-100 AS DOUBLE)
        ) t;
    """

    order_qt_underflow_with_compression """
        SELECT percentile_approx_weighted(x, w, 0.5, 2048) AS p
        FROM (
            SELECT CAST(0 AS DOUBLE) AS x, CAST(1e-100 AS DOUBLE) AS w
            UNION ALL SELECT CAST(10 AS DOUBLE), CAST(1e-100 AS DOUBLE)
        ) t;
    """

    order_qt_zero_weights """
        SELECT percentile_approx_weighted(x, w, 0.5),
               percentile_approx_weighted(x, w, 0.5, 2048)
        FROM (
            SELECT CAST(0 AS DOUBLE) AS x, CAST(0 AS DOUBLE) AS w
            UNION ALL SELECT CAST(10 AS DOUBLE), CAST(0 AS DOUBLE)
        ) t;
    """

    order_qt_mixed_weights """
        SELECT percentile_approx_weighted(x, w, 0),
               percentile_approx_weighted(x, w, 0.5),
               percentile_approx_weighted(x, w, 1),
               percentile_approx_weighted(x, w, 0, 2048),
               percentile_approx_weighted(x, w, 0.5, 2048),
               percentile_approx_weighted(x, w, 1, 2048)
        FROM (
            SELECT CAST(-1000 AS DOUBLE) AS x, CAST(1e-100 AS DOUBLE) AS w
            UNION ALL SELECT CAST(0 AS DOUBLE), CAST(1 AS DOUBLE)
            UNION ALL SELECT CAST(10 AS DOUBLE), CAST(1 AS DOUBLE)
            UNION ALL SELECT CAST(1000 AS DOUBLE), CAST(1e-100 AS DOUBLE)
        ) t;
    """

    order_qt_positive_subnormal_weights """
        SELECT percentile_approx_weighted(x, w, 0.5),
               percentile_approx_weighted(x, w, 0.5, 2048)
        FROM (
            SELECT CAST(0 AS DOUBLE) AS x, CAST(1e-40 AS DOUBLE) AS w
            UNION ALL SELECT CAST(10 AS DOUBLE), CAST(1e-40 AS DOUBLE)
        ) t;
    """

    sql "DROP TABLE IF EXISTS weighted_percentile_scale"
    sql """
        CREATE TABLE weighted_percentile_scale (id INT, k INT, x DOUBLE, w DOUBLE)
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """
    sql """
        INSERT INTO weighted_percentile_scale VALUES
        (1, 1, 0, 1e-100), (2, 1, 10, 1e-100),
        (3, 2, 0, 1), (4, 2, 10, 1),
        (5, 3, 0, 1e100), (6, 3, 10, 1e100);
    """
    order_qt_grouped_weights """
        SELECT k, percentile_approx_weighted(x, w, 0.5),
                  percentile_approx_weighted(x, w, 0.5, 2048)
        FROM weighted_percentile_scale GROUP BY k;
    """

    sql "SET enable_agg_state = true"
    sql "DROP TABLE IF EXISTS weighted_percentile_scale_state"
    sql """
        CREATE TABLE weighted_percentile_scale_state (
            k INT,
            p AGG_STATE<percentile_approx_weighted(DOUBLE, DOUBLE, DOUBLE)> GENERIC,
            pc AGG_STATE<percentile_approx_weighted(DOUBLE, DOUBLE, DOUBLE, DOUBLE)> GENERIC
        )
        AGGREGATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """
    sql """
        INSERT INTO weighted_percentile_scale_state
        SELECT k, percentile_approx_weighted_state(x, w, 0.5),
                  percentile_approx_weighted_state(x, w, 0.5, 2048)
        FROM weighted_percentile_scale;
    """
    order_qt_persisted_weights """
        SELECT k, percentile_approx_weighted_merge(p), percentile_approx_weighted_merge(pc)
        FROM weighted_percentile_scale_state GROUP BY k;
    """

    test {
        sql "SELECT percentile_approx_weighted(0, CAST('NaN' AS DOUBLE), 0.5)"
        exception "percentile_approx_weighted requires finite weights"
    }
    test {
        sql "SELECT percentile_approx_weighted(0, CAST('Infinity' AS DOUBLE), 0.5, 2048)"
        exception "percentile_approx_weighted requires finite weights"
    }
}
