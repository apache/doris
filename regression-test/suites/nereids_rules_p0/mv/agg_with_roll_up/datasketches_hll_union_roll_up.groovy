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

suite("datasketches_hll_union_roll_up") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    // sk1: lgK=8, {0..6}; sk2: lgK=8, {20..29}
    def sk1Base64 = "AgEHCAMIBwjL18IEK/L7BoYv+Q11gWYHgbxdBntl5gj8LUIK"
    def sk2Base64 = "AwEHCAUIAAkKAAAAIjvrBcS1nwfGGWoEyHokBO8t9wc1qTEENkcJB7hWqQxZf9QNnuSbGA=="

    sql "DROP TABLE IF EXISTS ds_hll_mv_events"
    sql """
        CREATE TABLE ds_hll_mv_events (
            event_ts DATETIME NOT NULL,
            k INT NOT NULL,
            sk STRING
        )
        DUPLICATE KEY(event_ts, k)
        DISTRIBUTED BY HASH(k) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO ds_hll_mv_events VALUES
            ('2026-11-26 10:01:00', 1, from_base64('${sk1Base64}')),
            ('2026-11-26 10:02:00', 1, from_base64('${sk2Base64}')),
            ('2026-11-26 10:15:00', 1, NULL),
            ('2026-11-26 10:03:00', 2, from_base64('${sk1Base64}')),
            ('2026-11-26 11:05:00', 1, from_base64('${sk2Base64}')),
            ('2026-11-26 11:06:00', 3, NULL)
    """
    // Replicate rows so base-table scan is much larger than the minute-level MV (HLL union is idempotent).
    sql """
        INSERT INTO ds_hll_mv_events
        SELECT event_ts, k, sk FROM ds_hll_mv_events CROSS JOIN numbers("number" = "50")
    """
    sql "analyze table ds_hll_mv_events with sync"

    def createMvAndAnalyze = { mvName, mvSql ->
        sql "DROP MATERIALIZED VIEW IF EXISTS ${mvName}"
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS ${mvSql}
        """
        waitingMTMVTaskFinished(getJobName(db, mvName))
        sql "analyze table ${mvName} with sync"
        sql "sync"
    }

    // 1) Hourly datasketches_hll_union_agg rolled up from minute-level datasketches_hll_union MV
    def mvNoArg = """
        SELECT
            date_trunc(event_ts, 'hour') AS hour_ts,
            date_trunc(event_ts, 'minute') AS min_ts,
            k,
            datasketches_hll_union(sk) AS sk_union
        FROM ds_hll_mv_events
        GROUP BY date_trunc(event_ts, 'hour'), date_trunc(event_ts, 'minute'), k
    """
    def hourlyAggQuery = """
        SELECT
            date_trunc(event_ts, 'hour') AS hour_ts,
            k,
            CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT) AS uv
        FROM ds_hll_mv_events
        GROUP BY date_trunc(event_ts, 'hour'), k
        ORDER BY hour_ts, k
    """
    sql "SET enable_materialized_view_rewrite = false"
    order_qt_hourly_agg_before "${hourlyAggQuery}"
    sql "SET enable_materialized_view_rewrite = true"
    createMvAndAnalyze("mv_ds_hll_min_no_arg", mvNoArg)
    mv_rewrite_success(hourlyAggQuery, "mv_ds_hll_min_no_arg")
    order_qt_hourly_agg_after "${hourlyAggQuery}"
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_ds_hll_min_no_arg"

    // 2) Cascading datasketches_hll_union -> datasketches_hll_union
    def hourlyUnionQuery = """
        SELECT
            hour_ts,
            k,
            CAST(ROUND(datasketches_hll_union_agg(u)) AS BIGINT) AS uv
        FROM (
            SELECT
                date_trunc(event_ts, 'hour') AS hour_ts,
                k,
                datasketches_hll_union(sk) AS u
            FROM ds_hll_mv_events
            GROUP BY date_trunc(event_ts, 'hour'), k
        ) t
        GROUP BY hour_ts, k
        ORDER BY hour_ts, k
    """
    sql "SET enable_materialized_view_rewrite = false"
    order_qt_hourly_cascade_before "${hourlyUnionQuery}"
    sql "SET enable_materialized_view_rewrite = true"
    createMvAndAnalyze("mv_ds_hll_min_cascade", mvNoArg)
    mv_rewrite_success(hourlyUnionQuery, "mv_ds_hll_min_cascade")
    order_qt_hourly_cascade_after "${hourlyUnionQuery}"
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_ds_hll_min_cascade"

    // 3) Kq <= Kmv succeeds; Kq > Kmv and no-arg query vs Kmv < 21 fail
    def mvK12 = """
        SELECT
            date_trunc(event_ts, 'hour') AS hour_ts,
            date_trunc(event_ts, 'minute') AS min_ts,
            k,
            datasketches_hll_union(sk, 12, 'HLL_8') AS sk_union
        FROM ds_hll_mv_events
        GROUP BY date_trunc(event_ts, 'hour'), date_trunc(event_ts, 'minute'), k
    """
    def queryK8 = """
        SELECT
            date_trunc(event_ts, 'hour') AS hour_ts,
            k,
            CAST(ROUND(datasketches_hll_union_agg(sk, 8)) AS BIGINT) AS uv
        FROM ds_hll_mv_events
        GROUP BY date_trunc(event_ts, 'hour'), k
        ORDER BY hour_ts, k
    """
    sql "SET enable_materialized_view_rewrite = false"
    order_qt_kq_le_kmv_before "${queryK8}"
    sql "SET enable_materialized_view_rewrite = true"
    createMvAndAnalyze("mv_ds_hll_k12_ok", mvK12)
    mv_rewrite_success(queryK8, "mv_ds_hll_k12_ok")
    order_qt_kq_le_kmv_after "${queryK8}"
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_ds_hll_k12_ok"

    sql "DROP MATERIALIZED VIEW IF EXISTS mv_ds_hll_k12_reject"
    sql """
        CREATE MATERIALIZED VIEW mv_ds_hll_k12_reject
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS ${mvK12}
    """
    waitingMTMVTaskFinishedWithoutAnalyze(getJobName(db, "mv_ds_hll_k12_reject"))
    sql "sync"

    def queryK16 = """
        SELECT
            date_trunc(event_ts, 'hour') AS hour_ts,
            k,
            CAST(ROUND(datasketches_hll_union_agg(sk, 16)) AS BIGINT) AS uv
        FROM ds_hll_mv_events
        GROUP BY date_trunc(event_ts, 'hour'), k
    """
    mv_rewrite_fail(queryK16, "mv_ds_hll_k12_reject")
    mv_rewrite_fail(hourlyAggQuery, "mv_ds_hll_k12_reject")
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_ds_hll_k12_reject"

    // 4) MV using datasketches_hll_union_agg cannot roll up
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_ds_hll_union_agg_reject"
    sql """
        CREATE MATERIALIZED VIEW mv_ds_hll_union_agg_reject
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT
            date_trunc(event_ts, 'hour') AS hour_ts,
            date_trunc(event_ts, 'minute') AS min_ts,
            k,
            datasketches_hll_union_agg(sk) AS uv
        FROM ds_hll_mv_events
        GROUP BY date_trunc(event_ts, 'hour'), date_trunc(event_ts, 'minute'), k
    """
    waitingMTMVTaskFinishedWithoutAnalyze(getJobName(db, "mv_ds_hll_union_agg_reject"))
    sql "sync"
    mv_rewrite_fail(hourlyAggQuery, "mv_ds_hll_union_agg_reject")
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_ds_hll_union_agg_reject"

    // 5) VARCHAR base column derives STRING MTMV output column and rolls up without truncation
    sql "DROP TABLE IF EXISTS ds_hll_mv_varchar_events"
    sql """
        CREATE TABLE ds_hll_mv_varchar_events (
            event_ts DATETIME NOT NULL,
            k INT NOT NULL,
            sk VARCHAR(65533)
        )
        DUPLICATE KEY(event_ts, k)
        DISTRIBUTED BY HASH(k) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO ds_hll_mv_varchar_events
        SELECT event_ts, k, CAST(sk AS VARCHAR(65533)) FROM ds_hll_mv_events
    """
    sql "analyze table ds_hll_mv_varchar_events with sync"

    def mvVarcharSql = """
        SELECT
            date_trunc(event_ts, 'hour') AS hour_ts,
            date_trunc(event_ts, 'minute') AS min_ts,
            k,
            datasketches_hll_union(sk) AS sk_union
        FROM ds_hll_mv_varchar_events
        GROUP BY date_trunc(event_ts, 'hour'), date_trunc(event_ts, 'minute'), k
    """
    def varcharAggQuery = """
        SELECT
            date_trunc(event_ts, 'hour') AS hour_ts,
            k,
            CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT) AS uv
        FROM ds_hll_mv_varchar_events
        GROUP BY date_trunc(event_ts, 'hour'), k
        ORDER BY hour_ts, k
    """
    sql "SET enable_materialized_view_rewrite = false"
    order_qt_varchar_agg_before "${varcharAggQuery}"
    sql "SET enable_materialized_view_rewrite = true"
    createMvAndAnalyze("mv_ds_hll_varchar_min", mvVarcharSql)
    order_qt_desc_mv_varchar "DESC mv_ds_hll_varchar_min"
    mv_rewrite_success(varcharAggQuery, "mv_ds_hll_varchar_min")
    order_qt_varchar_agg_after "${varcharAggQuery}"
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_ds_hll_varchar_min"
}
