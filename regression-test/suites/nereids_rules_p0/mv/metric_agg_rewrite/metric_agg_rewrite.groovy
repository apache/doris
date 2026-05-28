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

/**
 * Materialized View Metric Aggregation Rewrite Test Suite
 *
 * This test suite validates materialized view (MV) query rewriting behavior for different
 * metric aggregation patterns. It covers 49 test cases organized by metric type and
 * aggregation scenarios.
 *
 * ============================================================================
 * METRIC TYPES TESTED
 * ============================================================================
 *
 * 1. ADDITIVE METRICS
 *    - Can be summed across all dimensions
 *    - Example: SUM(amt) for GMV (Gross Merchandise Value)
 *    - MV rewrite: Always possible when dimensions match or roll up
 *
 * 2. SEMI-ADDITIVE METRICS
 *    - Additive in some dimensions (e.g., time) but not others
 *    - Example: Daily UV summed over time periods
 *    - MV rewrite: Possible when the additive dimension is preserved
 *
 * 3. NON-ADDITIVE METRICS
 *    - Cannot be directly summed; require state-based aggregation
 *    - Example: APPROX_COUNT_DISTINCT(uuid) for UV (Unique Visitors)
 *    - MV rewrite: Requires partial aggregation with state functions
 *
 * ============================================================================
 * AGGREGATION PATTERNS
 * ============================================================================
 *
 * 1. FULL AGGREGATION
 *    - MV stores final aggregated values (e.g., APPROX_COUNT_DISTINCT(uuid))
 *    - Limitation: Cannot roll up non-additive metrics across dimensions
 *
 * 2. PARTIAL AGGREGATION
 *    - MV stores intermediate aggregation states
 *    - Uses: approx_count_distinct_state() + approx_count_distinct_union()
 *    - Benefit: Enables roll-up of non-additive metrics across dimensions
 *
 * ============================================================================
 * TEST CASES EXPECTED TO FAIL (mv_rewrite_fail)
 * ============================================================================
 *
 * These test cases validate that the optimizer correctly REJECTS MV rewrite
 * when the rewrite would produce incorrect results:
 *
 * Case 1:  Predicate pushdown with conditional aggregation
 *          - MV: SUM(IF(b=1, amt, 0)) grouped by (dt, c)
 *          - Query: SUM(amt) WHERE b=1
 *          - Why fail: MV pre-applied the filter; cannot re-filter
 *
 * Case 9:  Non-additive metric with full aggregation + multi-value dimensions
 *          - MV: APPROX_COUNT_DISTINCT(uuid) grouped by (dt, c, d)
 *          - Query: APPROX_COUNT_DISTINCT(uuid) WHERE c IN (1,2)
 *          - Why fail: Cannot sum distinct counts across c values
 *
 * Case 11: Non-additive metric with full aggregation + cross-day rollup
 *          - MV: APPROX_COUNT_DISTINCT(uuid) grouped by (dt, c, d)
 *          - Query: APPROX_COUNT_DISTINCT(uuid) across multiple days
 *          - Why fail: Cannot sum daily distinct counts
 *
 * Case 13: Semi-additive metric missing required grouping dimension
 *          - MV: APPROX_COUNT_DISTINCT(uuid) grouped by (dt, c)
 *          - Query: Inner query groups by dt only (drops c)
 *          - Why fail: MV cannot supply aggregation without c dimension
 *
 * Case 19: Semi-additive metric with full agg + window function
 *          - MV: APPROX_COUNT_DISTINCT(uuid) per (dt, c)
 *          - Query: SUM(daily_uv) OVER window
 *          - Why fail: Full agg MV cannot supply daily values for window
 *
 * Case 20: Non-additive metric with full agg + state merge window
 *          - MV: APPROX_COUNT_DISTINCT(uuid) per (dt, c)
 *          - Query: approx_count_distinct_merge(state) OVER window
 *          - Why fail: Full agg MV doesn't store state
 *
 * Case 27: Partial agg cumulative MV + non-additive metric window merge
 *          - MV: Cumulative state with APPROX_COUNT_DISTINCT_UNION over window
 *          - Query: approx_count_distinct_merge(state) OVER window
 *          - Why fail: State type mismatch or incompatible window semantics
 *
 * Case 41: Full agg time drill-down + non-additive metric
 *          - MV: Daily APPROX_COUNT_DISTINCT(uuid)
 *          - Query: Monthly APPROX_COUNT_DISTINCT(uuid)
 *          - Why fail: Cannot aggregate distinct counts across days to month
 *
 * Case 47: Full agg time roll-up + non-additive metric
 *          - MV: Monthly APPROX_COUNT_DISTINCT(uuid)
 *          - Query: Total APPROX_COUNT_DISTINCT(uuid) across months
 *          - Why fail: Cannot sum monthly distinct counts
 *
 * ============================================================================
 * HOW TO MODIFY TESTS FOR FUTURE OPTIMIZER IMPROVEMENTS
 * ============================================================================
 *
 * If the optimizer is enhanced to support previously unsupported patterns:
 *
 * 1. IDENTIFY THE FAILING TEST CASE
 *    - Find the case number in the list above
 *    - Locate it in the code (search for "Case X." comment)
 *
 * 2. CHANGE THE ASSERTION
 *    - Replace: mv_rewrite_fail(query, "mv_name")
 *    - With:    mv_rewrite_success(query, "mv_name",
 *                   is_partition_statistics_ready(db, ["tb_detail", "mv_name"]))
 *
 * 3. VERIFY THE CHANGE
 *    - Run: ./run-regression-test.sh --run -s mv_metric_aggregation_rewrite
 *    - Ensure the test passes and compare_res() validates correctness
 *
 * 4. UPDATE THIS COMMENT
 *    - Remove the case from the "EXPECTED TO FAIL" list above
 *    - Document what optimizer enhancement enabled the support
 *
 * EXAMPLE:
 *    If Case 41 is fixed by adding time-granularity-aware distinct count merging:
 *
 *    // 41. full agg, time drill-down, non-additive metric -- NOW SUPPORTED
 *    // (Previously failed; now works due to time-granularity-aware state merging)
 *    def query_41 = """..."""
 *    mv_rewrite_success(query_41, "mv_day_full",
 *            is_partition_statistics_ready(db, ["tb_detail", "mv_day_full"]))
 *    compare_res("${query_41} order by 1")
 *
 * ============================================================================
 * TEST EXECUTION
 * ============================================================================
 *
 * Run all tests:
 *   ./run-regression-test.sh --run -s metric_agg_rewrite
 *
 * Run with specific group:
 *   ./run-regression-test.sh --run -s metric_agg_rewrite -g nereids_rules_p0
 *
 * ============================================================================
 */

suite("metric_agg_rewrite") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"
    sql "set enable_agg_state=true"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_materialized_view_rewrite=true"

    sql """DROP TABLE IF EXISTS tb_detail"""

    sql """
        CREATE TABLE tb_detail (
            dt     DATE           NOT NULL,
            c      BIGINT         NOT NULL,
            d      BIGINT         NOT NULL,
            a      BIGINT         NOT NULL,
            b      BIGINT         NOT NULL,
            uuid   BIGINT         NOT NULL,
            pin    BIGINT         NOT NULL,
            amt    DECIMAL(18, 2) NOT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(dt, c, d, a)
        PARTITION BY RANGE(dt) (
            PARTITION p20250101 VALUES [("2025-01-01"),("2025-01-02")),
            PARTITION p20250102 VALUES [("2025-01-02"),("2025-01-03")),
            PARTITION p20250103 VALUES [("2025-01-03"),("2025-01-04")),
            PARTITION p20250201 VALUES [("2025-02-01"),("2025-02-02"))
        )
        DISTRIBUTED BY HASH(uuid) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO tb_detail VALUES
        ('2025-01-01', 1, 1, 1, 1, 1001, 2001, 100.00),
        ('2025-01-01', 1, 1, 0, 0, 1002, 2002, 200.00),
        ('2025-01-01', 1, 2, 1, 1, 1001, 2001, 150.00),
        ('2025-01-01', 2, 1, 1, 1, 1003, 2003, 50.00),
        ('2025-01-01', 2, 1, 1, 1, 1001, 2001, 300.00),
        ('2025-01-01', 2, 2, 1, 1, 1004, 2004, 250.00),
        ('2025-01-01', 3, 1, 0, 0, 1005, 2005, 180.00),
        ('2025-01-02', 1, 1, 1, 1, 1001, 2001, 120.00),
        ('2025-01-02', 1, 1, 1, 1, 1006, 2006, 250.00),
        ('2025-01-02', 1, 2, 0, 0, 1007, 2007, 220.00),
        ('2025-01-02', 2, 1, 1, 1, 1002, 2002, 170.00),
        ('2025-01-02', 2, 2, 1, 1, 1008, 2008, 280.00),
        ('2025-01-02', 3, 1, 1, 1, 1003, 2003, 90.00),
        ('2025-01-02', 3, 2, 0, 0, 1009, 2009, 310.00),
        ('2025-02-01', 1, 1, 1, 1, 1001, 2001, 100.00)
    """

    sql """analyze table tb_detail with sync"""
    sql """alter table tb_detail modify column amt set stats ('row_count'='15');"""

    def create_partitioned_mv = { mv_name, mv_sql, partition_col = "dt" ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql """
            CREATE MATERIALIZED VIEW ${mv_name}
            BUILD IMMEDIATE REFRESH ON MANUAL
            PARTITION BY (`${partition_col}`)
            DISTRIBUTED BY RANDOM BUCKETS AUTO
            PROPERTIES ('replication_num' = '1')
            AS ${mv_sql}
        """
        waitingMTMVTaskFinished(getJobName(db, mv_name))
        sql """sync;"""
    }

    def create_unpartitioned_mv = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql """
            CREATE MATERIALIZED VIEW ${mv_name}
            BUILD IMMEDIATE REFRESH ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS AUTO
            PROPERTIES ('replication_num' = '1')
            AS ${mv_sql}
        """
        waitingMTMVTaskFinished(getJobName(db, mv_name))
        sql """sync;"""
    }

    def compare_res = { stmt ->
        sql "SET enable_materialized_view_rewrite=false"
        def origin_res = sql stmt
        sql "SET enable_materialized_view_rewrite=true"
        def mv_res = sql stmt
        assertTrue((mv_res == [] && origin_res == []) || (mv_res.size() == origin_res.size()))
        for (int row = 0; row < mv_res.size(); row++) {
            assertTrue(mv_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_res[row].size(); col++) {
                assertTrue(mv_res[row][col] == origin_res[row][col])
            }
        }
    }

    // 1. predicate push down -- expect rewrite fail
    //   MV groups by (dt, c) and stores SUM(IF(b=1,amt,0)) which cannot serve a query
    //   with extra predicate "b = 1" on detail rows.
    def mv_pred_down_sql = """
        SELECT dt,
               c AS dim_c,
               approx_count_distinct_union(approx_count_distinct_state(IF(a = 1, uuid, NULL))) AS uv,
               SUM(IF(b = 1, amt, 0)) AS gmv
        FROM tb_detail GROUP BY dt, c
    """
    def query_pred_down = """
        SELECT c AS dim_c, sum(amt) AS gmv
        FROM tb_detail
        WHERE dt >= '2025-01-01' AND dt <= '2025-01-01' AND b = 1
        GROUP BY c
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_pred_down"""
    create_partitioned_mv("mv_pred_down", mv_pred_down_sql)
    mv_rewrite_fail(query_pred_down, "mv_pred_down")
    compare_res("${query_pred_down} order by 1")
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_pred_down"""


    // 2. aggregate function rollup with state union -- expect rewrite chosen
    def mv_agg_sql = """
        SELECT dt, c, d,
               approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state,
               BITMAP_UNION(TO_BITMAP(pin)) AS pin_bitmap
        FROM tb_detail GROUP BY dt, c, d
    """
    def query_agg = """
        SELECT c, d, APPROX_COUNT_DISTINCT(uuid) AS uv, COUNT(DISTINCT pin) AS pin_uv
        FROM tb_detail
        WHERE dt >= '2025-01-01' AND dt <= '2025-01-01'
        GROUP BY c, d
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_agg"""
    create_partitioned_mv("mv_agg", mv_agg_sql)
    mv_rewrite_success(query_agg, "mv_agg",
            is_partition_statistics_ready(db, ["tb_detail", "mv_agg"]))
    compare_res("${query_agg} order by 1, 2")
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_agg"""


    // 3, 4, 5. partial aggregation, single-dim scenarios -- expect rewrite chosen
    def mv_partial_sql = """
        SELECT dt, c,
               approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
        FROM tb_detail GROUP BY dt, c
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_partial"""
    create_partitioned_mv("mv_partial", mv_partial_sql)

    def query_partial_multi_value = """
        SELECT APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail
        WHERE dt >= '2025-01-01' AND dt <= '2025-01-01' AND c IN (1, 2)
    """
    mv_rewrite_success(query_partial_multi_value, "mv_partial",
            is_partition_statistics_ready(db, ["tb_detail", "mv_partial"]))
    compare_res(query_partial_multi_value)

    def query_partial_rollup = """
        SELECT APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail
        WHERE dt >= '2025-01-01' AND dt <= '2025-01-01' AND c = 1
    """
    mv_rewrite_success(query_partial_rollup, "mv_partial",
            is_partition_statistics_ready(db, ["tb_detail", "mv_partial"]))
    compare_res(query_partial_rollup)

    def query_partial_cross_time = """
        SELECT APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail
        WHERE dt >= '2025-01-01' AND dt <= '2025-01-02' AND c = 1
    """
    mv_rewrite_success(query_partial_cross_time, "mv_partial",
            is_partition_statistics_ready(db, ["tb_detail", "mv_partial"]))
    compare_res(query_partial_cross_time)

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_partial"""


    // 6. partial aggregation, multi-dim with cross-time -- expect rewrite chosen
    def mv_partial_dims_sql = """
        SELECT dt, c, d,
               approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
        FROM tb_detail GROUP BY dt, c, d
    """
    def query_partial_dims = """
        SELECT c, d, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
        GROUP BY c, d
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_partial_dims"""
    create_partitioned_mv("mv_partial_dims", mv_partial_dims_sql)
    mv_rewrite_success(query_partial_dims, "mv_partial_dims",
            is_partition_statistics_ready(db, ["tb_detail", "mv_partial_dims"]))
    compare_res("${query_partial_dims} order by 1, 2")
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_partial_dims"""


    // 7, 8. fully-aggregated MV at (dt, c) -- expect rewrite chosen
    def mv_full_sql = """
        SELECT dt, c, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail GROUP BY dt, c
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_full"""
    create_partitioned_mv("mv_full", mv_full_sql)

    def query_full_multi_value = """
        SELECT c, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-01' AND c IN (1, 2)
        GROUP BY c
    """
    mv_rewrite_success(query_full_multi_value, "mv_full",
            is_partition_statistics_ready(db, ["tb_detail", "mv_full"]))
    compare_res("${query_full_multi_value} order by 1")

    def query_full_single_value = """
        SELECT c, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-01' AND c = 1
        GROUP BY c
    """
    mv_rewrite_success(query_full_single_value, "mv_full",
            is_partition_statistics_ready(db, ["tb_detail", "mv_full"]))
    compare_res("${query_full_single_value} order by 1")

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_full"""


    // 9, 10, 11. fully-aggregated MV at (dt, c, d).
    //   case 9 / 11: rewrite must fail because non-additive UV cannot roll up across dims/days
    //   case 10: rewrite chosen since query keys match the MV grouping
    def mv_full_dims_sql = """
        SELECT dt, c, d, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail GROUP BY dt, c, d
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_full_dims"""
    create_partitioned_mv("mv_full_dims", mv_full_dims_sql)

    def query_full_dims_multi = """
        SELECT APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-01' AND c IN (1, 2)
    """
    mv_rewrite_fail(query_full_dims_multi, "mv_full_dims")
    compare_res(query_full_dims_multi)

    def query_full_dims_single = """
        SELECT c, d, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-01' AND c = 1 AND d = 1
        GROUP BY c, d
    """
    mv_rewrite_success(query_full_dims_single, "mv_full_dims",
            is_partition_statistics_ready(db, ["tb_detail", "mv_full_dims"]))
    compare_res("${query_full_dims_single} order by 1, 2")

    def query_full_dims_cross_day = """
        SELECT c, d, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
        GROUP BY c, d
    """
    mv_rewrite_fail(query_full_dims_cross_day, "mv_full_dims")
    compare_res("${query_full_dims_cross_day} order by 1, 2")

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_full_dims"""


    // 12, 13, 14. semi-additive metric -- full aggregation MV
    def mv_semi_full_sql = """
        SELECT dt, c, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail GROUP BY dt, c
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_semi_cumulative_full"""
    create_partitioned_mv("mv_semi_cumulative_full", mv_semi_full_sql)

    // 12. additive across time when grouped per day per c -- expect chosen
    def query_semi_time_sum = """
        SELECT c, SUM(daily_uv) AS uv FROM (
            SELECT dt, c, APPROX_COUNT_DISTINCT(uuid) AS daily_uv
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
            GROUP BY dt, c
        ) t GROUP BY c
    """
    mv_rewrite_success(query_semi_time_sum, "mv_semi_cumulative_full",
            is_partition_statistics_ready(db, ["tb_detail", "mv_semi_cumulative_full"]))
    compare_res("${query_semi_time_sum} order by 1")

    // 13. inner query drops c -- MV cannot supply the grouping -- expect fail
    def query_semi_no_c = """
        SELECT SUM(daily_uv) AS uv FROM (
            SELECT dt, APPROX_COUNT_DISTINCT(uuid) AS daily_uv
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02' AND c IN (1, 2)
            GROUP BY dt
        ) t
    """
    mv_rewrite_fail(query_semi_no_c, "mv_semi_cumulative_full")
    compare_res(query_semi_no_c)

    // 14. inner query keeps c, MV grouping covers it -- expect chosen
    def query_semi_with_c = """
        SELECT c, SUM(daily_uv) AS uv FROM (
            SELECT dt, c, APPROX_COUNT_DISTINCT(uuid) AS daily_uv
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02' AND c IN (1, 2)
            GROUP BY dt, c
        ) t GROUP BY c
    """
    mv_rewrite_success(query_semi_with_c, "mv_semi_cumulative_full",
            is_partition_statistics_ready(db, ["tb_detail", "mv_semi_cumulative_full"]))
    compare_res("${query_semi_with_c} order by 1")

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_semi_cumulative_full"""


    // 15. semi-additive metric, partial aggregation MV with c kept -- query drops c -- expect chosen
    def mv_semi_partial_sql = """
        SELECT dt, c,
               approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
        FROM tb_detail GROUP BY dt, c
    """
    def query_semi_partial = """
        SELECT SUM(daily_uv) AS uv FROM (
            SELECT dt, APPROX_COUNT_DISTINCT(uuid) AS daily_uv
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02' AND c IN (1, 2)
            GROUP BY dt
        ) t
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_semi_cumulative_partial"""
    create_partitioned_mv("mv_semi_cumulative_partial", mv_semi_partial_sql)
    mv_rewrite_success(query_semi_partial, "mv_semi_cumulative_partial")
    compare_res(query_semi_partial)
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_semi_cumulative_partial"""


    // 16. additive metric, single dim -- expect chosen
    def mv_cumulative_sql = """
        SELECT dt, c, SUM(amt) AS gmv
        FROM tb_detail GROUP BY dt, c
    """
    def query_cumulative = """
        SELECT SUM(amt) AS gmv
        FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02' AND c IN (1, 2)
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_cumulative"""
    create_partitioned_mv("mv_cumulative", mv_cumulative_sql)
    mv_rewrite_success(query_cumulative, "mv_cumulative",
            is_partition_statistics_ready(db, ["tb_detail", "mv_cumulative"]))
    compare_res(query_cumulative)
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_cumulative"""


    // 17. additive metric, multi-dim -- expect chosen
    def mv_cumulative_dims_sql = """
        SELECT dt, c, d, SUM(amt) AS gmv
        FROM tb_detail GROUP BY dt, c, d
    """
    def query_cumulative_dims = """
        SELECT SUM(amt) AS gmv
        FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02' AND c IN (1, 2)
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_cumulative_dims"""
    create_partitioned_mv("mv_cumulative_dims", mv_cumulative_dims_sql)
    mv_rewrite_success(query_cumulative_dims, "mv_cumulative_dims",
            is_partition_statistics_ready(db, ["tb_detail", "mv_cumulative_dims"]))
    compare_res(query_cumulative_dims)
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_cumulative_dims"""


    // 18, 19, 20. time-bucketed MV: SUM(amt) and APPROX_COUNT_DISTINCT(uuid) per (dt, c)
    def mv_full_agg_sql = """
        SELECT dt, c, SUM(amt) AS gmv, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail GROUP BY c, dt
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_full_agg"""
    create_partitioned_mv("mv_full_agg", mv_full_agg_sql)

    // 18. cumulative gmv via window over additive metric -- expect chosen
    def query_window_gmv = """
        SELECT dt,
            SUM(daily_gmv) OVER(ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_gmv
        FROM (
            SELECT dt, SUM(amt) AS daily_gmv
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
            GROUP BY dt
        ) t
    """
    mv_rewrite_success(query_window_gmv, "mv_full_agg",
            is_partition_statistics_ready(db, ["tb_detail", "mv_full_agg"]))
    compare_res("${query_window_gmv} order by 1")

    // 19. cumulative semi-additive uv with sum-of-daily values -- expect fail
    def query_window_uv_sum = """
        SELECT dt,
            SUM(daily_uv) OVER(ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_uv
        FROM (
            SELECT dt, APPROX_COUNT_DISTINCT(uuid) AS daily_uv
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
            GROUP BY dt
        ) t
    """
    mv_rewrite_fail(query_window_uv_sum, "mv_full_agg")
    compare_res("${query_window_uv_sum} order by 1")

    // 20. cumulative non-additive uv via state-merge over window -- expect fail (full-agg MV cannot supply state)
    def query_window_uv_merge = """
        SELECT dt,
            approx_count_distinct_merge(uv_state) OVER(
                ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_uv
        FROM (
            SELECT dt, approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
            GROUP BY dt
        ) t
    """
    mv_rewrite_fail(query_window_uv_merge, "mv_full_agg")
    compare_res("${query_window_uv_merge} order by 1")

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_full_agg"""


    // 21, 22. partial agg MV with state -- expects chosen for both window-sum and window-merge variants
    def mv_partial_agg_sql = """
        SELECT dt, c, SUM(amt) AS gmv,
               APPROX_COUNT_DISTINCT_UNION(APPROX_COUNT_DISTINCT_STATE(uuid)) AS uv
        FROM tb_detail GROUP BY c, dt
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_partial_agg"""
    create_partitioned_mv("mv_partial_agg", mv_partial_agg_sql)

    // 21. semi-additive uv via window-sum from daily counts -- expect chosen
    def query_partial_window_sum = """
        SELECT dt,
            SUM(daily_uv) OVER(ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_uv
        FROM (
            SELECT dt, APPROX_COUNT_DISTINCT(uuid) AS daily_uv
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
            GROUP BY dt
        ) t
    """
    mv_rewrite_success(query_partial_window_sum, "mv_partial_agg",
            is_partition_statistics_ready(db, ["tb_detail", "mv_partial_agg"]))
    compare_res("${query_partial_window_sum} order by 1")

    // 22. non-additive uv via window-merge over state -- expect chosen
    def query_partial_window_merge = """
        SELECT dt,
            approx_count_distinct_merge(uv_state) OVER(
                ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_uv
        FROM (
            SELECT dt, approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
            GROUP BY dt
        ) t
    """
    mv_rewrite_success(query_partial_window_merge, "mv_partial_agg",
            is_partition_statistics_ready(db, ["tb_detail", "mv_partial_agg"]))
    compare_res("${query_partial_window_merge} order by 1")

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_partial_agg"""


    // 23, 24. cumulative materialization -- one row per dt with cumulative aggregates
    def mv_accumulative_sql = """
        SELECT
            dt,
            SUM(gmv) OVER (ORDER BY dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS gmv,
            APPROX_COUNT_DISTINCT_MERGE(uv) OVER (ORDER BY dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS uv
        FROM (
            SELECT
                dt,
                SUM(amt) AS gmv,
                APPROX_COUNT_DISTINCT_UNION(APPROX_COUNT_DISTINCT_STATE(uuid)) AS uv
            FROM tb_detail
            GROUP BY dt
        ) t
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_accumulative"""
    create_unpartitioned_mv("mv_accumulative", mv_accumulative_sql)

    // 23. fully-aggregated cumulative additive metric -- expect chosen
    def query_acc_gmv = """
        SELECT dt,
            SUM(gmv) OVER(PARTITION BY 1 ORDER BY dt ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_gmv
        FROM (
            SELECT dt, SUM(amt) AS gmv
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
            GROUP BY dt
        ) t
    """
    mv_rewrite_success(query_acc_gmv, "mv_accumulative",
            is_partition_statistics_ready(db, ["tb_detail", "mv_accumulative"]))
    compare_res("${query_acc_gmv} order by 1")

    // 24. fully-aggregated cumulative non-additive metric via state merge -- expect chosen
    def query_acc_uv = """
        SELECT dt,
            approx_count_distinct_merge(uv_state) OVER(PARTITION BY 1 ORDER BY dt ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_uv
        FROM (
            SELECT dt,
                approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
            GROUP BY dt
        ) t
    """
    mv_rewrite_success(query_acc_uv, "mv_accumulative",
            is_partition_statistics_ready(db, ["tb_detail", "mv_accumulative"]))
    compare_res("${query_acc_uv} order by 1")

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_accumulative"""


    // 25. cumulative materialization with semi-additive metric -- expect chosen
    def mv_accumulative_semi_sql = """
        SELECT
            dt,
            SUM(uv) OVER (ORDER BY dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS uv
        FROM (
            SELECT
                dt,
                APPROX_COUNT_DISTINCT_MERGE(APPROX_COUNT_DISTINCT_STATE(uuid)) AS uv
            FROM tb_detail
            GROUP BY dt
        ) t
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_accumulative_semi"""
    create_unpartitioned_mv("mv_accumulative_semi", mv_accumulative_semi_sql)

    def query_acc_semi = """
        SELECT dt,
            SUM(uv) OVER(PARTITION BY 1 ORDER BY dt ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_uv
        FROM (
            SELECT dt, APPROX_COUNT_DISTINCT_MERGE(APPROX_COUNT_DISTINCT_STATE(uuid)) AS uv
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
            GROUP BY dt
        ) t
    """
    mv_rewrite_success(query_acc_semi, "mv_accumulative_semi",
            is_partition_statistics_ready(db, ["tb_detail", "mv_accumulative_semi"]))
    compare_res("${query_acc_semi} order by 1")

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_accumulative_semi"""


    // 26, 27. cumulative materialization with partial aggregation (state)
    def mv_accumulative_partial_sql = """
        SELECT
            dt,
            SUM(gmv) OVER (ORDER BY dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS gmv,
            APPROX_COUNT_DISTINCT_UNION(uv) OVER (ORDER BY dt ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS uv
        FROM (
            SELECT
                dt,
                SUM(amt) AS gmv,
                APPROX_COUNT_DISTINCT_UNION(APPROX_COUNT_DISTINCT_STATE(uuid)) AS uv
            FROM tb_detail
            GROUP BY dt
        ) t
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_accumulative_partial"""
    create_unpartitioned_mv("mv_accumulative_partial", mv_accumulative_partial_sql)

    // 26. partial-agg cumulative additive metric -- expect chosen
    def query_acc_partial_gmv = """
        SELECT dt,
            SUM(gmv) OVER(PARTITION BY 1 ORDER BY dt ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_gmv
        FROM (
            SELECT dt, sum(amt) AS gmv
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
            GROUP BY dt
        ) t
    """
    mv_rewrite_success(query_acc_partial_gmv, "mv_accumulative_partial",
            is_partition_statistics_ready(db, ["tb_detail", "mv_accumulative_partial"]))
    compare_res("${query_acc_partial_gmv} order by 1")

    // 27. partial-agg cumulative non-additive metric -- expect fail
    def query_acc_partial_uv = """
        SELECT dt,
            APPROX_COUNT_DISTINCT_MERGE(uv_state) OVER(PARTITION BY 1 ORDER BY dt ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_uv
        FROM (
            SELECT dt,
                APPROX_COUNT_DISTINCT_UNION(APPROX_COUNT_DISTINCT_STATE(uuid)) AS uv_state
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
            GROUP BY dt
        ) t
    """
    mv_rewrite_fail(query_acc_partial_uv, "mv_accumulative_partial")
    compare_res("${query_acc_partial_uv} order by 1")

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_accumulative_partial"""


    // 28. time-bucketed MV, query groups by dt -- expect chosen
    def mv_time_sql = """
        SELECT dt, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail GROUP BY dt
    """
    def query_time = """
        SELECT dt, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
        GROUP BY dt
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_time"""
    create_partitioned_mv("mv_time", mv_time_sql)
    mv_rewrite_success(query_time, "mv_time",
            is_partition_statistics_ready(db, ["tb_detail", "mv_time"]))
    compare_res("${query_time} order by 1")
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_time"""


    // 29. UNION ALL recognition -- expect chosen for both branches
    def mv_union_sql = """
        SELECT dt, c, d, SUM(amt) AS gmv, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail GROUP BY dt, c, d
    """
    def query_union = """
        SELECT dt, APPROX_COUNT_DISTINCT(uuid) AS uv, null
        FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02' GROUP BY dt
        UNION ALL
        SELECT dt, null, SUM(amt) AS gmv
        FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02' GROUP BY dt
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_union"""
    create_partitioned_mv("mv_union", mv_union_sql)
    mv_rewrite_success(query_union, "mv_union",
            is_partition_statistics_ready(db, ["tb_detail", "mv_union"]))
    compare_res("${query_union} order by 1")
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_union"""


    // 30, 31, 32. common MV for SELECT *, subquery, and CTE penetration
    def mv_common_sql = """
        SELECT dt, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail GROUP BY dt
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_common"""
    create_partitioned_mv("mv_common", mv_common_sql)

    // 30. SELECT * recognition -- expect chosen
    def query_select_star = """
        SELECT * FROM (
            SELECT dt, APPROX_COUNT_DISTINCT(uuid) AS uv
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02' GROUP BY dt
        ) t
    """
    mv_rewrite_success(query_select_star, "mv_common",
            is_partition_statistics_ready(db, ["tb_detail", "mv_common"]))
    compare_res("${query_select_star} order by 1")

    // 31. multi-layer SQL penetration -- expect chosen
    def query_multi_layer = """
        SELECT dt, APPROX_COUNT_DISTINCT(uuid) AS uv FROM (
            SELECT dt, uuid FROM tb_detail
            WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
        ) t GROUP BY dt
    """
    mv_rewrite_success(query_multi_layer, "mv_common",
            is_partition_statistics_ready(db, ["tb_detail", "mv_common"]))
    compare_res("${query_multi_layer} order by 1")

    // 32. CTE node penetration -- expect chosen
    def query_cte = """
        WITH detail AS (
            SELECT dt, uuid
            FROM tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
        ),
        sub_detail AS (
            SELECT * from tb_detail WHERE dt >= '2025-01-01' AND dt <= '2025-01-02'
        )
        SELECT dt, APPROX_COUNT_DISTINCT(uuid) as uv
        FROM sub_detail
        GROUP BY dt
    """
    mv_rewrite_success(query_cte, "mv_common",
            is_partition_statistics_ready(db, ["tb_detail", "mv_common"]))
    compare_res("${query_cte} order by 1")

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_common"""


    // 33-38. MV selection: full vs partial aggregation
    def mv_common_full_sql = """
        SELECT c, d, APPROX_COUNT_DISTINCT(uuid) AS uv, SUM(amt) AS gmv
        FROM tb_detail WHERE dt = '2025-01-01' GROUP BY c, d
    """
    def mv_common_partial_sql = """
        SELECT c, d,
               approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state,
               SUM(amt) AS gmv
        FROM tb_detail WHERE dt = '2025-01-01' GROUP BY c, d
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_common_full"""
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_common_partial"""
    create_unpartitioned_mv("mv_common_full", mv_common_full_sql)
    create_unpartitioned_mv("mv_common_partial", mv_common_partial_sql)

    // 33. additive + non-multi-dim-multi-value -- expect chosen (either MV)
    def query_33 = """
        SELECT c, d, SUM(amt) AS gmv
        FROM tb_detail WHERE dt = '2025-01-01' GROUP BY c, d
    """
    mv_rewrite_any_success(query_33, ["mv_common_partial", "mv_common_full"],
            is_partition_statistics_ready(db, ["tb_detail", "mv_common_partial", "mv_common_full"]))
    compare_res("${query_33} order by 1, 2")

    // 34. additive + multi-dim-multi-value -- expect chosen (either MV)
    def query_34 = """
        SELECT d, SUM(amt) AS gmv
        FROM tb_detail WHERE dt = '2025-01-01' AND c IN (1, 2) GROUP BY d
    """
    mv_rewrite_any_success(query_34, ["mv_common_partial", "mv_common_full"],
            is_partition_statistics_ready(db, ["tb_detail", "mv_common_partial", "mv_common_full"]))
    compare_res("${query_34} order by 1")

    // 35. non-additive + non-multi-dim-multi-value -- expect chosen (either MV)
    def query_35 = """
        SELECT c, d, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail WHERE dt = '2025-01-01' GROUP BY c, d
    """
    mv_rewrite_any_success(query_35, ["mv_common_partial", "mv_common_full"],
            is_partition_statistics_ready(db, ["tb_detail", "mv_common_partial", "mv_common_full"]))
    compare_res("${query_35} order by 1, 2")

    // 36. non-additive + multi-dim-multi-value -- expect chosen (mv_common_partial)
    def query_36 = """
        SELECT d, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail WHERE dt = '2025-01-01' AND c IN (1, 2) GROUP BY d
    """
    mv_rewrite_success(query_36, "mv_common_partial",
            is_partition_statistics_ready(db, ["tb_detail", "mv_common_partial", "mv_common_full"]))
    compare_res("${query_36} order by 1")

    // 37. additive + dimension path not fully matched -- expect chosen (either MV)
    def query_37 = """
        SELECT d, SUM(amt) AS gmv
        FROM tb_detail WHERE dt = '2025-01-01' GROUP BY d
    """
    mv_rewrite_any_success(query_37, ["mv_common_partial", "mv_common_full"],
            is_partition_statistics_ready(db, ["tb_detail", "mv_common_partial", "mv_common_full"]))
    compare_res("${query_37} order by 1")

    // 38. non-additive + dimension path not fully matched -- expect chosen (mv_common_partial)
    def query_38 = """
        SELECT d, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail WHERE dt = '2025-01-01' GROUP BY d
    """
    mv_rewrite_success(query_38, "mv_common_partial",
            is_partition_statistics_ready(db, ["tb_detail", "mv_common_partial", "mv_common_full"]))
    compare_res("${query_38} order by 1")

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_common_full"""
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_common_partial"""


    // 39. composite metric (multi-table join query) -- expect both MVs chosen
    sql """DROP TABLE IF EXISTS tb_name_detail1"""
    sql """DROP TABLE IF EXISTS tb_name_detail2"""
    sql """CREATE TABLE tb_name_detail1 LIKE tb_detail"""
    sql """INSERT INTO tb_name_detail1 SELECT * FROM tb_detail"""
    sql """CREATE TABLE tb_name_detail2 LIKE tb_detail"""
    sql """INSERT INTO tb_name_detail2 SELECT * FROM tb_detail"""

    def mv_uv_only_sql = """
        SELECT APPROX_COUNT_DISTINCT(uuid) AS uv, dt
        FROM tb_name_detail1 GROUP BY dt
    """
    def mv_gmv_only_sql = """
        SELECT SUM(amt) AS gmv, dt
        FROM tb_name_detail2 GROUP BY dt
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_uv_only"""
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_gmv_only"""
    create_partitioned_mv("mv_uv_only", mv_uv_only_sql)
    create_partitioned_mv("mv_gmv_only", mv_gmv_only_sql)

    def query_39 = """
        SELECT
            dt,
            SUM(uv) AS uv,
            SUM(gmv) AS gmv,
            SUM(uv) / SUM(gmv) AS rate
        FROM (
            SELECT
                APPROX_COUNT_DISTINCT(uuid) AS uv,
                null as gmv,
                dt
            FROM tb_name_detail1
            WHERE dt >= '2025-01-01' AND dt <= '2025-01-01'
            GROUP BY dt
            UNION ALL
            SELECT
                null as uv,
                SUM(amt) AS gmv,
                dt
            FROM tb_name_detail2
            WHERE dt >= '2025-01-01' AND dt <= '2025-01-01'
            GROUP BY dt
        ) t
        GROUP BY dt
    """
    mv_rewrite_all_success(query_39, ["mv_uv_only", "mv_gmv_only"],
            is_partition_statistics_ready(db, ["tb_name_detail1", "tb_name_detail2", "mv_uv_only", "mv_gmv_only"]))
    compare_res("${query_39} order by 1")

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_gmv_only"""
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_uv_only"""
    sql """DROP TABLE IF EXISTS tb_name_detail2"""
    sql """DROP TABLE IF EXISTS tb_name_detail1"""


    // 40-42. full aggregation MV, time granularity drill-down
    def mv_day_full_sql = """
        SELECT dt, APPROX_COUNT_DISTINCT(uuid) AS uv, SUM(amt) AS gmv
        FROM tb_detail GROUP BY dt
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_day_full"""
    create_partitioned_mv("mv_day_full", mv_day_full_sql)

    // 40. full agg, time drill-down, additive metric -- expect chosen
    def query_40 = """
        SELECT substring(dt, 1, 7) AS month, SUM(amt) AS gmv
        FROM tb_detail
        WHERE dt >= '2025-01-01' AND dt <= '2025-01-31'
        GROUP BY month
    """
    mv_rewrite_success(query_40, "mv_day_full",
            is_partition_statistics_ready(db, ["tb_detail", "mv_day_full"]))
    compare_res("${query_40} order by 1")

    // 41. full agg, time drill-down, non-additive metric -- expect fail
    def query_41 = """
        SELECT substring(dt, 1, 7) AS month, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail
        WHERE dt >= '2025-01-01' AND dt <= '2025-01-31'
        GROUP BY month
    """
    mv_rewrite_fail(query_41, "mv_day_full")
    compare_res("${query_41} order by 1")

    // 42. full agg, time drill-down, semi-additive metric -- expect chosen
    def query_42 = """
        SELECT
            substring(dt, 1, 7) AS month,
            SUM(uv) AS uv
        FROM (
            SELECT
                APPROX_COUNT_DISTINCT(uuid) AS uv,
                dt
            FROM tb_detail
            WHERE dt >= '2025-01-01' AND dt <= '2025-01-31'
            GROUP BY dt
        ) t
        GROUP BY month
    """
    mv_rewrite_success(query_42, "mv_day_full",
            is_partition_statistics_ready(db, ["tb_detail", "mv_day_full"]))
    compare_res("${query_42} order by 1")

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_day_full"""


    // 43-45. partial aggregation MV, time granularity drill-down
    def mv_day_partial_sql = """
        SELECT dt,
               approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state,
               SUM(amt) AS gmv
        FROM tb_detail GROUP BY dt
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_day_partial"""
    create_partitioned_mv("mv_day_partial", mv_day_partial_sql)

    // 43. partial agg, time drill-down, additive metric -- expect chosen
    def query_43 = """
        SELECT substring(dt, 1, 7) AS month, SUM(amt) AS gmv
        FROM tb_detail
        WHERE dt >= '2025-01-01' AND dt <= '2025-01-31'
        GROUP BY month
    """
    mv_rewrite_success(query_43, "mv_day_partial",
            is_partition_statistics_ready(db, ["tb_detail", "mv_day_partial"]))
    compare_res("${query_43} order by 1")

    // 44. partial agg, time drill-down, non-additive metric -- expect chosen
    def query_44 = """
        SELECT substring(dt, 1, 7) AS month, APPROX_COUNT_DISTINCT(uuid) AS uv
        FROM tb_detail
        WHERE dt >= '2025-01-01' AND dt <= '2025-01-31'
        GROUP BY month
    """
    mv_rewrite_success(query_44, "mv_day_partial",
            is_partition_statistics_ready(db, ["tb_detail", "mv_day_partial"]))
    compare_res("${query_44} order by 1")

    // 45. partial agg, time drill-down, semi-additive metric -- expect chosen
    def query_45 = """
        SELECT
            substring(dt, 1, 7) AS month,
            SUM(uv) AS uv
        FROM (
            SELECT
                APPROX_COUNT_DISTINCT(uuid) AS uv,
                dt
            FROM tb_detail
            WHERE dt >= '2025-01-01' AND dt <= '2025-01-31'
            GROUP BY dt
        ) t
        GROUP BY month
    """
    mv_rewrite_success(query_45, "mv_day_partial",
            is_partition_statistics_ready(db, ["tb_detail", "mv_day_partial"]))
    compare_res("${query_45} order by 1")

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_day_partial"""


    // 46-47. full aggregation MV, time granularity roll-up (month -> all)
    def mv_month_full_sql = """
        SELECT
            date_trunc(dt, 'month') AS month_dt,
            APPROX_COUNT_DISTINCT(uuid) AS uv,
            SUM(amt) AS gmv,
            'BY_MONTH' AS tp
        FROM tb_detail
        GROUP BY month_dt, tp
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_month_full"""
    create_partitioned_mv("mv_month_full", mv_month_full_sql, "month_dt")

    // 46. full agg, time roll-up, additive metric -- expect chosen
    def query_46 = """
        SELECT sum(amt) AS gmv
        FROM tb_detail
        WHERE dt >= '2025-01-01' AND dt < '2025-02-01'
    """
    mv_rewrite_success(query_46, "mv_month_full",
            is_partition_statistics_ready(db, ["tb_detail", "mv_month_full"]))
    compare_res(query_46)

    // 47. full agg, time roll-up, non-additive metric -- expect fail
    def query_47 = """
        SELECT APPROX_COUNT_DISTINCT(uuid) AS gmv
        FROM tb_detail
        WHERE dt >= '2025-01-01' AND dt < '2025-02-01'
    """
    mv_rewrite_fail(query_47, "mv_month_full")
    compare_res(query_47)

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_month_full"""


    // 48-49. partial aggregation MV, time granularity roll-up (month -> all)
    def mv_month_partial_sql = """
        SELECT
            date_trunc(dt, 'month') AS month_dt,
            APPROX_COUNT_DISTINCT_UNION(APPROX_COUNT_DISTINCT_STATE(uuid)) AS uv,
            SUM(amt) AS gmv,
            'BY_MONTH' AS tp
        FROM tb_detail
        GROUP BY month_dt, tp
    """
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_month_partitial"""
    create_partitioned_mv("mv_month_partitial", mv_month_partial_sql, "month_dt")

    // 48. partial agg, time roll-up, additive metric -- expect chosen
    def query_48 = """
        SELECT sum(amt) AS gmv
        FROM tb_detail
        WHERE dt >= '2025-01-01' AND dt < '2025-02-01'
    """
    mv_rewrite_success(query_48, "mv_month_partitial",
            is_partition_statistics_ready(db, ["tb_detail", "mv_month_partitial"]))
    compare_res(query_48)

    // 49. partial agg, time roll-up, non-additive metric -- expect chosen
    def query_49 = """
        SELECT APPROX_COUNT_DISTINCT(uuid) AS gmv
        FROM tb_detail
        WHERE dt >= '2025-01-01' AND dt < '2025-02-01'
    """
    mv_rewrite_success(query_49, "mv_month_partitial",
            is_partition_statistics_ready(db, ["tb_detail", "mv_month_partitial"]))
    compare_res(query_49)

    sql """DROP MATERIALIZED VIEW IF EXISTS mv_month_partitial"""

    sql """DROP TABLE IF EXISTS tb_detail"""
}
