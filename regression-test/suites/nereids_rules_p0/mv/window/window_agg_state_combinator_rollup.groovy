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

package mv.window

/**
 * Window MV rewrite with agg-state combinator rollup.
 *
 * Scenario:
 * - MV column cum_uv_state = approx_count_distinct_union(uv_state) OVER (same spec)
 * - Query         cum_uv     = approx_count_distinct_merge(uv_state) OVER (same spec)
 *
 * Rewrite expectation:
 * 1) Hit MV and chose (async_mv_rewrite_success / EXPLAIN MEMO PLAN).
 * 2) Results match query on base table (order_qt before vs after).
 * 3) Mismatched window spec or missing MV slots must not rewrite (negative cases).
 */
suite("window_agg_state_combinator_rollup") {
    String db = context.config.getDbNameByFile(context.file)
    String mvName = "mv_window_ndv_cum_state"
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "set disable_nereids_rules='ELIMINATE_CONST_JOIN_CONDITION,CONSTANT_PROPAGATION'"
    sql "SET enable_agg_state = true"

    sql """DROP TABLE IF EXISTS tb_detail_window"""

    sql """
    CREATE TABLE IF NOT EXISTS tb_detail_window (
        dt DATE NOT NULL,
        uuid VARCHAR(32) NOT NULL
    )
    DUPLICATE KEY(dt, uuid)
    DISTRIBUTED BY HASH(dt) BUCKETS 3
    PROPERTIES (
        "replication_num" = "1"
    )
    """

    sql """
    INSERT INTO tb_detail_window VALUES
    ('2025-01-01', 'u1'),
    ('2025-01-01', 'u2'),
    ('2025-01-02', 'u2'),
    ('2025-01-02', 'u3')
    """

    sql """ANALYZE TABLE tb_detail_window WITH SYNC"""

    // MV: per-day union state, then union OVER (materialized as cum_uv_state)
    def mv_sql = """
        SELECT dt, cum_uv_state
        FROM (
            SELECT dt,
                approx_count_distinct_union(uv_state) OVER (
                    PARTITION BY 1 ORDER BY dt ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_uv_state
            FROM (
                SELECT dt,
                    approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
                FROM tb_detail_window
                GROUP BY dt
            ) t
        ) t2
    """

    // Query: per-day union state, then merge OVER (same window spec as MV)
    def query_sql = """
        SELECT dt,
            approx_count_distinct_merge(uv_state) OVER (
                PARTITION BY 1 ORDER BY dt ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_uv
        FROM (
            SELECT dt,
                approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
            FROM tb_detail_window
            GROUP BY dt
        ) t
    """

    // Baseline on base table (MV not created yet)
    order_qt_query_before "${query_sql}"

    async_mv_rewrite_success(db, mv_sql, query_sql, mvName)

    // Same SQL after MV is available; rewrite should keep results
    order_qt_query_after "${query_sql}"
    // MV hit + chose is checked inside async_mv_rewrite_success via EXPLAIN MEMO PLAN

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""

    // Negative: mismatched ORDER BY should not rewrite
    def mv_bad_order = """
        SELECT dt, cum_uv_state
        FROM (
            SELECT dt,
                approx_count_distinct_union(uv_state) OVER (
                    PARTITION BY 1 ORDER BY dt DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_uv_state
            FROM (
                SELECT dt,
                    approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
                FROM tb_detail_window
                GROUP BY dt
            ) t
        ) t2
    """
    def query_good_order = """
        SELECT dt,
            approx_count_distinct_merge(uv_state) OVER (
                PARTITION BY 1 ORDER BY dt ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_uv
        FROM (
            SELECT dt,
                approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
            FROM tb_detail_window
            GROUP BY dt
        ) t
    """
    async_mv_rewrite_fail(db, mv_bad_order, query_good_order, "mv_window_ndv_bad_order")
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_window_ndv_bad_order"""

    // Negative: mismatched PARTITION BY should not rewrite
    def mv_bad_partition = """
        SELECT dt, cum_uv_state
        FROM (
            SELECT dt,
                approx_count_distinct_union(uv_state) OVER (
                    PARTITION BY dt ORDER BY dt ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_uv_state
            FROM (
                SELECT dt,
                    approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
                FROM tb_detail_window
                GROUP BY dt
            ) t
        ) t2
    """
    def query_partition_one = """
        SELECT dt,
            approx_count_distinct_merge(uv_state) OVER (
                PARTITION BY 1 ORDER BY dt ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_uv
        FROM (
            SELECT dt,
                approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
            FROM tb_detail_window
            GROUP BY dt
        ) t
    """
    async_mv_rewrite_fail(db, mv_bad_partition, query_partition_one, "mv_window_ndv_bad_partition")
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_window_ndv_bad_partition"""

    // Negative: mismatched window frame should not rewrite
    def mv_bad_frame = """
        SELECT dt, cum_uv_state
        FROM (
            SELECT dt,
                approx_count_distinct_union(uv_state) OVER (
                    PARTITION BY 1 ORDER BY dt ASC
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS cum_uv_state
            FROM (
                SELECT dt,
                    approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
                FROM tb_detail_window
                GROUP BY dt
            ) t
        ) t2
    """
    async_mv_rewrite_fail(db, mv_bad_frame, query_good_order, "mv_window_ndv_bad_frame")
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_window_ndv_bad_frame"""

    // Negative: query needs per-day uv_state, which is not in MV scan output
    def mv_only_dt = """
        SELECT dt
        FROM (
            SELECT dt,
                approx_count_distinct_union(uv_state) OVER (
                    PARTITION BY 1 ORDER BY dt ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_uv_state
            FROM (
                SELECT dt,
                    approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
                FROM tb_detail_window
                GROUP BY dt
            ) t
        ) t2
    """
    def query_need_uv_state = """
        SELECT dt, uv_state
        FROM (
            SELECT dt,
                approx_count_distinct_union(approx_count_distinct_state(uuid)) AS uv_state
            FROM tb_detail_window
            GROUP BY dt
        ) t
    """
    async_mv_rewrite_fail(db, mv_only_dt, query_need_uv_state, "mv_window_ndv_missing_slot")
    sql """DROP MATERIALIZED VIEW IF EXISTS mv_window_ndv_missing_slot"""
}
