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

import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

/**
 * What a fallback out of the incremental attempt owes the partition the attempt rebuilt.
 *
 * <p>A partition the criterion says must be rebuilt is rebuilt before the delta runs, and that rebuild is a
 * partial read: the tables the MV does not partition by are read through their stream at the offset -- the
 * image as of the last consumption -- because only a read that covers the whole MV may advance that offset.
 * The delta that follows is what brings them up to date.
 *
 * <p>When the delta falls back instead, the partition plan answers the snapshot question again, and the
 * partition the rebuild just replaced still looks unsynced to it. Taking it out of the plan leaves a partial
 * one, whose read of those same tables is the same historical image -- so the refresh replaces the remaining
 * partitions with rows built from the old image, records the current state for all of them, and reports
 * success. The partition it skipped keeps what the rebuild gave it, from an image the refresh has now
 * advanced the offset past, and nothing will repair either: they all look up to date.
 *
 * <p>Cases pinned here, over an MV partitioned by `dt` that joins a dimension table:
 * <ol>
 *   <li>the fallback after the rebuild keeps the whole MV in its scope, so the dimension is read as it is
 *       now and both partitions end up matching the base query;</li>
 *   <li>the refresh after it has nothing left to do, which is the offset having been consumed and the
 *       requirement cleared rather than deferred.</li>
 * </ol>
 *
 * <p>All dates are literals: no current_date(), so the expectation does not depend on the run date.
 */
suite("test_ivm_fallback_keeps_the_whole_scope", "nonConcurrent") {
    def mvName = "ivm_fallback_scope_mv"
    def forcedFallbackDebugPoint = "IvmIncrRefreshManager.doRefresh.force_fallback_reason"

    def waitForNewTask = { previousTaskId ->
        def taskResult
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT TaskId, Status
                FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}'
                  AND MvName = '${mvName}'
                ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
            """)
            return !taskResult.isEmpty()
                    && taskResult[0].TaskId.toString() != previousTaskId
                    && taskResult[0].Status.toString() != 'PENDING'
                    && taskResult[0].Status.toString() != 'RUNNING'
        })
        return taskResult[0]
    }

    // The route a refresh took, in one row: which scope refreshed, and how many partitions it rebuilt
    // although the request did not ask for them. A refresh that only ran the incremental rewrite leaves
    // RefreshMode unset, and an unset column comes back as the literal two-character string "\N", which does
    // not survive the .out round trip, so fold every value that is not a scope into a printable token.
    def taskQuery = { String taskId ->
        """
            SELECT CASE WHEN RefreshMode IN ('COMPLETE', 'PARTIAL', 'NOT_REFRESH')
                        THEN RefreshMode ELSE 'NONE' END,
                   NeedRefreshPartitions,
                   IvmRebuiltPartitions
            FROM tasks('type'='mv')
            WHERE TaskId = '${taskId}'
        """
    }

    GetDebugPoint().disableDebugPointForAllFEs(forcedFallbackDebugPoint)
    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_fallback_scope_mv"""
    sql """DROP TABLE IF EXISTS ivm_fallback_scope_f"""
    sql """DROP TABLE IF EXISTS ivm_fallback_scope_d"""

    sql """
        CREATE TABLE ivm_fallback_scope_f (
            k BIGINT NOT NULL,
            dt DATE NOT NULL,
            amount INT
        )
        UNIQUE KEY(k, dt)
        PARTITION BY RANGE(dt) ()
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """ALTER TABLE ivm_fallback_scope_f ADD PARTITION p202601 VALUES [('2026-01-01'), ('2026-02-01'))"""
    sql """ALTER TABLE ivm_fallback_scope_f ADD PARTITION p202602 VALUES [('2026-02-01'), ('2026-03-01'))"""

    sql """
        CREATE TABLE ivm_fallback_scope_d (
            k BIGINT NOT NULL,
            v INT
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """INSERT INTO ivm_fallback_scope_f VALUES
            (1, '2026-01-10', 100),
            (2, '2026-02-10', 200)"""
    sql """INSERT INTO ivm_fallback_scope_d VALUES (1, 10), (2, 10)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_fallback_scope_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(k, dt)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT f.k, f.dt, f.amount, d.v
           FROM ivm_fallback_scope_f f JOIN ivm_fallback_scope_d d ON f.k = d.k
    """

    sql """REFRESH MATERIALIZED VIEW ivm_fallback_scope_mv COMPLETE"""
    def task = waitForNewTask(null)
    order_qt_baseline_mv """SELECT k, dt, amount, v FROM ivm_fallback_scope_mv ORDER BY k"""

    // A partition that needs a rebuild -- the truncate is one of the changes no delta can repair -- and a
    // dimension whose change the streams have not consumed: exactly the state the partial read is behind on.
    sql """TRUNCATE TABLE ivm_fallback_scope_f PARTITION(p202601)"""
    sql """INSERT INTO ivm_fallback_scope_f VALUES (1, '2026-01-10', 100)"""
    sql """INSERT INTO ivm_fallback_scope_d VALUES (1, 20), (2, 20)"""

    // The incremental attempt rebuilds the invalidated partition, and the forced reason makes the delta that
    // would bring the dimension up to date fall back to the partition plan.
    try {
        GetDebugPoint().enableDebugPointForAllFEs(forcedFallbackDebugPoint,
                [reason: "PLAN_PATTERN_UNSUPPORTED"])
        sql """REFRESH MATERIALIZED VIEW ivm_fallback_scope_mv INCREMENTAL FALLBACK"""
        task = waitForNewTask(task.TaskId.toString())
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(forcedFallbackDebugPoint)
    }
    qt_fallback_task taskQuery(task.TaskId.toString())
    order_qt_mv_after_the_fallback """SELECT k, dt, amount, v FROM ivm_fallback_scope_mv ORDER BY k"""

    // Nothing is left to do: the fallback's read covered the whole MV, so the dimension's change is in the
    // view and its offset is consumed, and the requirement the rebuild raised is met.
    sql """REFRESH MATERIALIZED VIEW ivm_fallback_scope_mv INCREMENTAL"""
    task = waitForNewTask(task.TaskId.toString())
    qt_task_after_the_fallback taskQuery(task.TaskId.toString())
    order_qt_mv_after_a_further_refresh """SELECT k, dt, amount, v FROM ivm_fallback_scope_mv ORDER BY k"""
}
