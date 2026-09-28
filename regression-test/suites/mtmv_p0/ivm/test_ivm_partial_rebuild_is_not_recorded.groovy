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
 * What a partition rebuilt by a partial read is allowed to be recorded as.
 *
 * <p>The rebuild a refresh does before its delta is a partial read: the tables the MV does not partition by
 * are read through their stream at the offset, the image as of the last consumption, and the delta that
 * follows is what brings them up to date. When the delta does not apply to the rebuilt partition -- it is
 * left out of the delta's scope on purpose, so a delta computed against the rows the rebuild replaced cannot
 * be applied twice -- that partition holds an image the refresh is not entitled to record as the current
 * state. Recorded anyway, it reads as caught up, as holding the table's current state, and nothing plans it
 * again: the view keeps rows the base query does not return.
 *
 * <p>So the rebuild leaves it needing one, and the passes that follow are what a partition in that state
 * takes to become current: the delta consumes the table's changes, and the next rebuild reads the table as
 * it is. Cases pinned here:
 * <ol>
 *   <li>the strict refresh whose delta fails: the task fails, and the partition it rebuilt is not recorded;</li>
 *   <li>the strict refresh after it: the delta consumes the dimension's change, the rebuild still reads the
 *       old image and is still not recorded -- which is the requirement having survived the first pass;</li>
 *   <li>the strict refresh after that: with nothing left behind the offset, the rebuild reads the dimension
 *       as it is, the rows land, and the partition is recorded;</li>
 *   <li>and one more refresh has nothing left to do.</li>
 * </ol>
 *
 * <p>All dates are literals: no current_date(), so the expectation does not depend on the run date.
 */
suite("test_ivm_partial_rebuild_is_not_recorded", "nonConcurrent") {
    def mvName = "ivm_partial_rebuild_mv"
    def forcedFallbackDebugPoint = "IvmIncrRefreshManager.doRefresh.force_fallback_reason"
    // The failure the strict path stops on: a reason strict mode does not fall back for.
    def forcedReason = "BINLOG_BROKEN"

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
    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_partial_rebuild_mv"""
    sql """DROP TABLE IF EXISTS ivm_partial_rebuild_f"""
    sql """DROP TABLE IF EXISTS ivm_partial_rebuild_d"""

    sql """
        CREATE TABLE ivm_partial_rebuild_f (
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
    sql """ALTER TABLE ivm_partial_rebuild_f ADD PARTITION p202601 VALUES [('2026-01-01'), ('2026-02-01'))"""
    sql """ALTER TABLE ivm_partial_rebuild_f ADD PARTITION p202602 VALUES [('2026-02-01'), ('2026-03-01'))"""

    sql """
        CREATE TABLE ivm_partial_rebuild_d (
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

    sql """INSERT INTO ivm_partial_rebuild_f VALUES
            (1, '2026-01-10', 100),
            (2, '2026-02-10', 200)"""
    sql """INSERT INTO ivm_partial_rebuild_d VALUES (1, 10), (2, 10)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_partial_rebuild_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(k, dt)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT f.k, f.dt, f.amount, d.v
           FROM ivm_partial_rebuild_f f JOIN ivm_partial_rebuild_d d ON f.k = d.k
    """

    sql """REFRESH MATERIALIZED VIEW ivm_partial_rebuild_mv COMPLETE"""
    def task = waitForNewTask(null)
    order_qt_baseline_mv """SELECT k, dt, amount, v FROM ivm_partial_rebuild_mv ORDER BY k"""

    // One partition that needs a rebuild, and a dimension whose change is still behind the offset: the
    // rebuild of that partition reads the dimension as it was, and it is the delta that would bring it up.
    sql """TRUNCATE TABLE ivm_partial_rebuild_f PARTITION(p202601)"""
    sql """INSERT INTO ivm_partial_rebuild_f VALUES (1, '2026-01-10', 100)"""
    sql """INSERT INTO ivm_partial_rebuild_d VALUES (1, 20), (2, 20)"""

    // The strict refresh the delta fails on. The rebuild has happened by then, and the partition it rebuilt
    // must not be recorded: what it read of the dimension is older than the dimension is.
    try {
        GetDebugPoint().enableDebugPointForAllFEs(forcedFallbackDebugPoint, [reason: forcedReason])
        sql """REFRESH MATERIALIZED VIEW ivm_partial_rebuild_mv INCREMENTAL"""
        task = waitForNewTask(task.TaskId.toString())
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(forcedFallbackDebugPoint)
    }
    qt_failed_task taskQuery(task.TaskId.toString())
    order_qt_mv_after_the_failed_refresh """SELECT k, dt, amount, v FROM ivm_partial_rebuild_mv ORDER BY k"""

    // This one's delta runs: the dimension's change is consumed, and the partition is rebuilt again -- from
    // the same old image, because the offset it is read at has not moved for it yet. It is still not
    // recorded, which is what the count says.
    sql """REFRESH MATERIALIZED VIEW ivm_partial_rebuild_mv INCREMENTAL"""
    task = waitForNewTask(task.TaskId.toString())
    qt_task_after_the_consumed_delta taskQuery(task.TaskId.toString())
    order_qt_mv_after_the_consumed_delta """SELECT k, dt, amount, v FROM ivm_partial_rebuild_mv ORDER BY k"""

    // Nothing is behind the offset now, so the rebuild reads the dimension as it is: the rows land and the
    // partition is recorded.
    sql """REFRESH MATERIALIZED VIEW ivm_partial_rebuild_mv INCREMENTAL"""
    task = waitForNewTask(task.TaskId.toString())
    qt_task_after_the_current_rebuild taskQuery(task.TaskId.toString())
    order_qt_mv_after_the_current_rebuild """SELECT k, dt, amount, v FROM ivm_partial_rebuild_mv ORDER BY k"""

    sql """REFRESH MATERIALIZED VIEW ivm_partial_rebuild_mv INCREMENTAL"""
    task = waitForNewTask(task.TaskId.toString())
    qt_task_after_the_recovery taskQuery(task.TaskId.toString())
    order_qt_mv_after_the_recovery """SELECT k, dt, amount, v FROM ivm_partial_rebuild_mv ORDER BY k"""
}
