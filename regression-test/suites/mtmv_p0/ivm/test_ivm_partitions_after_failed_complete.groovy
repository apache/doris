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
 * What a partition refresh does after a whole-MV refresh failed partway through.
 *
 * <p>A whole-MV refresh raises every partition's requirement before it reconciles the streams and before it
 * reads anything, so a failure in between leaves the one state where the two criteria a refresh can plan by
 * disagree: every partition needs a rebuild, and every snapshot still says the MV is in sync with its base
 * tables. Planning by snapshots alone would report NOT_REFRESH and repair nothing, and only the requests
 * that reach an incremental attempt would ever recover the MV.
 *
 * <p>The failure is injected (a debug point on the IVM insert), so it lands on a real refresh rather than on
 * a crash window, and it is removed before the refreshes that have to recover from it.
 *
 * <p>Cases pinned here, over an MV with two partitions:
 * <ol>
 *   <li>a strict `PARTITIONS` refresh after the failed `COMPLETE`: it refreshes what the requirement names
 *       -- both partitions, not only the one the snapshots call unsynced -- and the increment the failed
 *       refresh left behind is applied;</li>
 *   <li>the incremental refresh after it rebuilds nothing, which is the requirement having been cleared
 *       rather than merely deferred to whichever request comes next;</li>
 *   <li>the same recovery for `PARTITIONS FALLBACK`, which does not have to reach the whole-MV attempt
 *       behind it because a partition refresh is what the requirement asks for.</li>
 * </ol>
 *
 * <p>All dates are literals: no current_date(), so the expectation does not depend on the run date.
 */
suite("test_ivm_partitions_after_failed_complete", "nonConcurrent") {
    // Cloud mode: the injection targets the local insert path, which the cloud transaction manager never
    // takes, so the refresh would succeed and there would be nothing to recover from.
    if (isCloudMode()) {
        logger.info("skip test_ivm_partitions_after_failed_complete on cloud mode: " +
                "the insert failure injection only fires on the local txn path")
        return
    }
    def mvName = "ivm_failed_complete_mv"
    def rpcFailureDebugPoint = "AbstractInsertExecutor.executeSingleInsert.ivm_rpc_failure"
    def rpcFailureFilterDebugPoint = "AbstractInsertExecutor.executeSingleInsert.ivm_rpc_failure.filter"

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
        return taskResult[0].TaskId.toString()
    }

    // The route a refresh took, in one row: whether it succeeded, which scope refreshed, and how many
    // partitions it rebuilt although the request did not ask for them. A refresh that only ran the
    // incremental rewrite leaves RefreshMode unset, and an unset column comes back as the literal
    // two-character string "\N", which does not survive the .out round trip, so fold every value that is
    // not a scope into a printable token.
    def taskQuery = { String taskId ->
        """
            SELECT Status,
                   CASE WHEN RefreshMode IN ('COMPLETE', 'PARTIAL', 'NOT_REFRESH')
                        THEN RefreshMode ELSE 'NONE' END,
                   IvmRebuiltPartitions
            FROM tasks('type'='mv')
            WHERE TaskId = '${taskId}'
        """
    }

    // A whole-MV refresh that fails on its first batch: the requirement is raised and journaled before the
    // reconciliation below it, and the snapshots it would have replaced are never written, which is the
    // state the partition refreshes have to plan by the requirement to get out of. What the failure wrote
    // is pinned by the task queries below, which report the status of the task they ran as.
    def failCompleteRefresh = { previousTaskId ->
        try {
            GetDebugPoint().enableDebugPointForAllFEs(rpcFailureFilterDebugPoint, [mv_name: mvName])
            GetDebugPoint().enableDebugPointForAllFEs(rpcFailureDebugPoint)
            sql """REFRESH MATERIALIZED VIEW ivm_failed_complete_mv COMPLETE"""
            return waitForNewTask(previousTaskId)
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs(rpcFailureFilterDebugPoint)
            GetDebugPoint().disableDebugPointForAllFEs(rpcFailureDebugPoint)
        }
    }

    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_failed_complete_mv"""
    sql """DROP TABLE IF EXISTS ivm_failed_complete_f"""

    sql """
        CREATE TABLE ivm_failed_complete_f (
            order_id BIGINT NOT NULL,
            dt DATE NOT NULL,
            amount INT
        )
        UNIQUE KEY(order_id, dt)
        PARTITION BY RANGE(dt) ()
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """ALTER TABLE ivm_failed_complete_f ADD PARTITION p202601 VALUES [('2026-01-01'), ('2026-02-01'))"""
    sql """ALTER TABLE ivm_failed_complete_f ADD PARTITION p202602 VALUES [('2026-02-01'), ('2026-03-01'))"""

    sql """INSERT INTO ivm_failed_complete_f VALUES
            (1, '2026-01-10', 100),
            (2, '2026-02-10', 200)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_failed_complete_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(order_id, dt)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT order_id, dt, amount FROM ivm_failed_complete_f
    """

    sql """REFRESH MATERIALIZED VIEW ivm_failed_complete_mv COMPLETE"""
    def taskId = waitForNewTask(null)
    order_qt_baseline_mv """SELECT order_id, dt, amount FROM ivm_failed_complete_mv"""

    // The row the failed whole-MV refresh never got to read: it is what makes the recovery visible rather
    // than a refresh that happens to report a different scope.
    sql """INSERT INTO ivm_failed_complete_f VALUES (3, '2026-01-20', 300)"""
    taskId = failCompleteRefresh(taskId)
    qt_failed_complete_task taskQuery(taskId)

    // Both partitions were marked and neither snapshot moved, so the strict form plans the requirement on
    // top of what the snapshots say: the whole MV is its scope, and the row above lands.
    sql """REFRESH MATERIALIZED VIEW ivm_failed_complete_mv PARTITIONS"""
    taskId = waitForNewTask(taskId)
    qt_partitions_task taskQuery(taskId)
    order_qt_partitions_mv """SELECT order_id, dt, amount FROM ivm_failed_complete_mv"""

    // Nothing is left to rebuild: the requirement the failed COMPLETE raised was cleared by the partition
    // refresh that honoured it, not deferred to this one, which reports no scope of its own.
    sql """REFRESH MATERIALIZED VIEW ivm_failed_complete_mv INCREMENTAL"""
    taskId = waitForNewTask(taskId)
    qt_incremental_task taskQuery(taskId)

    // And the fallback form recovers the same state without reaching the whole-MV attempt behind it.
    sql """INSERT INTO ivm_failed_complete_f VALUES (4, '2026-02-20', 400)"""
    taskId = failCompleteRefresh(taskId)
    sql """REFRESH MATERIALIZED VIEW ivm_failed_complete_mv PARTITIONS FALLBACK"""
    taskId = waitForNewTask(taskId)
    qt_partitions_fallback_task taskQuery(taskId)
    order_qt_partitions_fallback_mv """SELECT order_id, dt, amount FROM ivm_failed_complete_mv"""
}
