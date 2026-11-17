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

import org.apache.doris.regression.util.Http
import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

/**
 * How a refresh task of an IVM MV says the way it refreshed the MV, and how the Master FE counts these tasks.
 *
 * <p>tasks("type"="mv") reports IvmRefreshMethod, the kinds of work that committed data: INCREMENTAL, PARTITION_REBUILD,
 * COMPLETE, MIXED when more than one of them did, NONE when nothing did. The Master counts each task as it ends in
 * doris_fe_ivm_refresh_tasks_total{result,method}, adds how long it ran to doris_fe_ivm_refresh_duration_seconds_total,
 * counts a task that fell back from the incremental refresh -- and committed data in the refresh that replaced it -- in
 * doris_fe_ivm_fallbacks_total{reason}, and a failed one in doris_fe_ivm_refresh_failures_total{reason}.
 *
 * <p>Cases, in order, on one MV:
 * <ol>
 *   <li>the first refresh finds every partition still to be filled and builds the MV with COMPLETE, which is not a
 *       fallback;</li>
 *   <li>an incremental refresh of new rows is INCREMENTAL, and one with nothing new commits nothing (no_change);</li>
 *   <li>a strict INCREMENTAL refresh that meets a MIN/MAX boundary fails for that reason and commits nothing;</li>
 *   <li>the same refresh allowed to fall back rebuilds the MV, which is a fallback for that reason;</li>
 *   <li>an explicit COMPLETE refresh is COMPLETE, and not a fallback;</li>
 *   <li>a truncated base partition makes its MV partition need a rebuild, which an incremental refresh does before the
 *       delta: MIXED, and not a fallback either.</li>
 * </ol>
 *
 * <p>nonConcurrent: the counters are the Master's, for every IVM MV, so the exact moves asserted here hold only while no
 * other suite refreshes one.
 */
suite("test_ivm_refresh_method_metrics", "nonConcurrent") {
    // The MV is checked against its base table below, so a query must not be answered from it; and a base-table write
    // must not be redirected into it, or the incremental path under test goes unused.
    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_dml_materialized_view_rewrite = false"

    String mvName = "ivm_refresh_method_metrics_mv"
    String masterMetricsUrl = "http://${getMasterIp()}:${getMasterPort()}/metrics"

    // The value of every doris_fe_ivm_ series the Master exports, by the series.
    def ivmSeries = {
        Map<String, Double> values = [:]
        for (String line : Http.GET(masterMetricsUrl, false, false).readLines()) {
            if (line.startsWith("doris_fe_ivm_")) {
                int split = line.lastIndexOf(' ')
                values.put(line.substring(0, split), Double.parseDouble(line.substring(split + 1)))
            }
        }
        return values
    }

    def latestTaskId = {
        def tasks = sql """
            SELECT TaskId FROM tasks('type'='mv')
            WHERE MvDatabaseName = '${context.dbName}' AND MvName = '${mvName}'
            ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
        """
        return tasks.isEmpty() ? null : tasks[0][0].toString()
    }

    def waitForNewTask = { previousTaskId ->
        def taskResult
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT TaskId, Status FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}' AND MvName = '${mvName}'
                ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
            """)
            return !taskResult.isEmpty()
                    && taskResult[0].TaskId.toString() != previousTaskId
                    && taskResult[0].Status.toString() != 'PENDING'
                    && taskResult[0].Status.toString() != 'RUNNING'
        })
        return taskResult[0].TaskId.toString()
    }

    // Runs a refresh and waits for its task to end. Returns the task, the counters that moved meanwhile and by how much,
    // and the run times added, which vary from run to run and are returned apart, by method.
    def refresh = { String statement ->
        String previousTaskId = latestTaskId()
        Map<String, Double> before = ivmSeries()
        sql statement
        String taskId = waitForNewTask(previousTaskId)
        Map<String, Double> after = ivmSeries()
        Map<String, Double> counted = [:]
        Map<String, Double> seconds = [:]
        String durationPrefix = 'doris_fe_ivm_refresh_duration_seconds_total{method="'
        after.each { series, value ->
            double moved = value - before[series]
            if (moved == 0) {
                return
            }
            if (series.startsWith(durationPrefix)) {
                seconds.put(series.substring(durationPrefix.length(), series.length() - 2), moved)
            } else {
                counted.put(series, moved)
            }
        }
        return [taskId, counted, seconds]
    }

    // How the task ended and refreshed the MV, in one row. An unset column comes back as the literal two-character
    // string "\N", which does not survive the .out round trip, so it is folded into a printable token: a method or a
    // reason is an enum name, which "\N" is not.
    def taskQuery = { String taskId ->
        """
            SELECT Status,
                   CASE WHEN IvmRefreshMethod REGEXP '^[A-Z_]+\$' THEN IvmRefreshMethod ELSE 'UNSET' END,
                   CASE WHEN IvmFallbackReason REGEXP '^[A-Z_]+\$' THEN IvmFallbackReason ELSE 'NONE' END
            FROM tasks('type'='mv')
            WHERE TaskId = '${taskId}'
        """
    }

    def tasks = { String result, String method ->
        "doris_fe_ivm_refresh_tasks_total{result=\"${result}\", method=\"${method}\"}".toString()
    }

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
    sql """DROP TABLE IF EXISTS ivm_refresh_method_metrics_t"""
    sql """
        CREATE TABLE ivm_refresh_method_metrics_t (
            dt DATE NOT NULL,
            id BIGINT NOT NULL,
            v INT
        )
        UNIQUE KEY(dt, id)
        PARTITION BY RANGE(dt) (
            PARTITION p1 VALUES [('2026-01-01'), ('2026-02-01')),
            PARTITION p2 VALUES [('2026-02-01'), ('2026-03-01')),
            PARTITION p3 VALUES [('2026-03-01'), ('2026-04-01'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql """INSERT INTO ivm_refresh_method_metrics_t VALUES
            ('2026-01-10', 1, 10), ('2026-01-10', 2, 20),
            ('2026-02-10', 3, 30),
            ('2026-03-10', 4, 40)"""
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT dt, MIN(v) AS min_v, MAX(v) AS max_v, COUNT(*) AS cnt
        FROM ivm_refresh_method_metrics_t
        GROUP BY dt
    """

    // Every series is there from 0, before a task has counted in it.
    Map<String, Double> initial = ivmSeries()
    assertTrue(initial.containsKey(tasks("canceled", "mixed")), initial.toString())
    assertTrue(initial.containsKey('doris_fe_ivm_fallbacks_total{reason="incomplete_refresh_snapshot"}'))
    assertTrue(initial.containsKey('doris_fe_ivm_refresh_failures_total{reason="partition_sync_failed"}'))
    assertTrue(initial.containsKey('doris_fe_ivm_schedule_rejected_total'))

    // 1. No partition of a new MV has been filled, so the first refresh goes straight to COMPLETE: it builds the first
    //    baseline, and the incremental refresh never gave up on anything.
    def (baselineTask, baselineCounted, baselineSeconds) = refresh("REFRESH MATERIALIZED VIEW ${mvName} AUTO")
    qt_first_baseline_task taskQuery(baselineTask)
    assertEquals([(tasks("success", "complete")): 1.0d], baselineCounted)
    assertEquals(["complete"], baselineSeconds.keySet().toList())

    // 2. New rows are caught up incrementally, and a refresh with nothing new to catch up commits nothing.
    sql """INSERT INTO ivm_refresh_method_metrics_t VALUES ('2026-03-10', 5, 50)"""
    def (deltaTask, deltaCounted, deltaSeconds) = refresh("REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL")
    qt_incremental_task taskQuery(deltaTask)
    assertEquals([(tasks("success", "incremental")): 1.0d], deltaCounted)
    assertEquals(["incremental"], deltaSeconds.keySet().toList())

    def (idleTask, idleCounted, idleSeconds) = refresh("REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL")
    qt_no_change_task taskQuery(idleTask)
    assertEquals([(tasks("no_change", "none")): 1.0d], idleCounted)

    // 3. Deleting the row that holds a group's minimum leaves the delta unable to say what the new minimum is. A strict
    //    INCREMENTAL request fails for that reason, and has committed nothing.
    sql """DELETE FROM ivm_refresh_method_metrics_t WHERE dt = '2026-01-10' AND id = 1"""
    def (strictTask, strictCounted, strictSeconds) = refresh("REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL")
    qt_strict_failure_task taskQuery(strictTask)
    assertEquals([(tasks("failed", "none")): 1.0d,
                  'doris_fe_ivm_refresh_failures_total{reason="min_max_boundary_hit"}': 1.0d], strictCounted)

    // 4. Allowed to fall back, the same refresh rebuilds the MV: a fallback, for the reason the delta gave up.
    def (fallbackTask, fallbackCounted, fallbackSeconds) =
            refresh("REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL FALLBACK")
    qt_fallback_task taskQuery(fallbackTask)
    assertEquals([(tasks("success", "complete")): 1.0d,
                  'doris_fe_ivm_fallbacks_total{reason="min_max_boundary_hit"}': 1.0d], fallbackCounted)
    order_qt_mv_after_fallback """SELECT dt, min_v, max_v, cnt FROM ${mvName}"""
    order_qt_base_after_fallback """
        SELECT dt, MIN(v), MAX(v), COUNT(*) FROM ivm_refresh_method_metrics_t GROUP BY dt
    """

    // 5. A COMPLETE request rebuilds the MV because it was asked to, which is not a fallback.
    def (completeTask, completeCounted, completeSeconds) = refresh("REFRESH MATERIALIZED VIEW ${mvName} COMPLETE")
    qt_complete_task taskQuery(completeTask)
    assertEquals([(tasks("success", "complete")): 1.0d], completeCounted)

    // 6. Truncating a base partition leaves its MV partition needing a rebuild, which no delta can make: an incremental
    //    refresh rebuilds it first and then catches up the other partitions, refreshing the MV in two ways at once.
    sql """TRUNCATE TABLE ivm_refresh_method_metrics_t PARTITION(p1)"""
    sql """INSERT INTO ivm_refresh_method_metrics_t VALUES ('2026-02-10', 6, 60)"""
    def (mixedTask, mixedCounted, mixedSeconds) = refresh("REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL")
    qt_mixed_task taskQuery(mixedTask)
    assertEquals([(tasks("success", "mixed")): 1.0d], mixedCounted)
    assertEquals(["mixed"], mixedSeconds.keySet().toList())
    order_qt_mv_after_mixed """SELECT dt, min_v, max_v, cnt FROM ${mvName}"""
    order_qt_base_after_mixed """
        SELECT dt, MIN(v), MAX(v), COUNT(*) FROM ivm_refresh_method_metrics_t GROUP BY dt
    """
}
