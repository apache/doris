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
 * What a partial rebuild may record when a source table it joins is one the MV has never consumed.
 *
 * <p>The rebuild a refresh does before its delta is a partial read: the tables the MV does not partition by
 * are read through their stream at the offset, and a partition with no consumption baseline -- no offset,
 * because the table was still empty when the MV's baseline was built -- is left out of that read entirely,
 * since there is no offset to read it from. Rows such a partition holds by now are therefore rows the answer
 * does not have, so the read is not the table as it is now and the partition it rebuilt may not be recorded
 * as caught up: the snapshot that would be published describes a state those rows are not in, nothing plans
 * the partition again, and the joined rows never arrive.
 *
 * <p>What repairs it is the delta, which reads that table without the snapshot's offset and applies what it
 * finds. The rebuild's records are held until it has run, and published then. Cases pinned here:
 * <ol>
 *   <li>the strict refresh whose delta fails: the task fails, and the partition it rebuilt is not recorded,
 *       because nothing has read the dimension's rows yet;</li>
 *   <li>the refresh after it: the partition is rebuilt again -- it still owes one -- and the delta that
 *       follows reads the dimension and joins its rows in, which is also what publishes the held records;</li>
 *   <li>the refresh after that: nothing left to do, with the joined rows in place.</li>
 * </ol>
 *
 * <p>The count in the second case is what the first case is observed by: recording the rebuilt partition
 * would leave the refresh with nothing to rebuild and its delta with nothing to read, which is a task row
 * reporting no scope at all rather than one partition.
 *
 * <p>All dates are literals: no current_date(), so the expectation does not depend on the run date.
 */
suite("test_ivm_unconsumed_dimension_is_not_current", "nonConcurrent") {
    def mvName = "ivm_unconsumed_dim_mv"
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
    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_unconsumed_dim_mv"""
    sql """DROP TABLE IF EXISTS ivm_unconsumed_dim_f"""
    sql """DROP TABLE IF EXISTS ivm_unconsumed_dim_d"""

    sql """
        CREATE TABLE ivm_unconsumed_dim_f (
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
    sql """ALTER TABLE ivm_unconsumed_dim_f ADD PARTITION p202601 VALUES [('2026-01-01'), ('2026-02-01'))"""
    sql """ALTER TABLE ivm_unconsumed_dim_f ADD PARTITION p202602 VALUES [('2026-02-01'), ('2026-03-01'))"""

    sql """
        CREATE TABLE ivm_unconsumed_dim_d (
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

    sql """INSERT INTO ivm_unconsumed_dim_f VALUES
            (1, '2026-01-10', 100),
            (2, '2026-02-10', 200)"""

    // Built while the dimension is empty: its partition is never written, so the MV's baseline leaves it
    // with no consumption offset -- there is nothing of it to read through the stream afterwards.
    sql """
        CREATE MATERIALIZED VIEW ivm_unconsumed_dim_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(k, dt)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT f.k, f.dt, f.amount, d.v
           FROM ivm_unconsumed_dim_f f JOIN ivm_unconsumed_dim_d d ON f.k = d.k
    """

    sql """REFRESH MATERIALIZED VIEW ivm_unconsumed_dim_mv COMPLETE"""
    def task = waitForNewTask(null)
    order_qt_baseline_mv """SELECT k, dt, amount, v FROM ivm_unconsumed_dim_mv ORDER BY k"""

    // The dimension gets rows, one partition of the fact table is invalidated: the rebuild of that partition
    // reads the dimension through the stream, which has no offset for it to read from.
    sql """INSERT INTO ivm_unconsumed_dim_d VALUES (1, 10), (2, 10)"""
    sql """TRUNCATE TABLE ivm_unconsumed_dim_f PARTITION(p202601)"""
    sql """INSERT INTO ivm_unconsumed_dim_f VALUES (1, '2026-01-10', 100)"""

    // The strict refresh the delta fails on. The rebuild has happened by then, and what it read of the
    // dimension omitted the rows the dimension holds -- so the partition may not be recorded as caught up.
    try {
        GetDebugPoint().enableDebugPointForAllFEs(forcedFallbackDebugPoint, [reason: forcedReason])
        sql """REFRESH MATERIALIZED VIEW ivm_unconsumed_dim_mv INCREMENTAL"""
        task = waitForNewTask(task.TaskId.toString())
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(forcedFallbackDebugPoint)
    }
    qt_failed_task taskQuery(task.TaskId.toString())
    order_qt_mv_after_the_failed_refresh """SELECT k, dt, amount, v FROM ivm_unconsumed_dim_mv ORDER BY k"""

    // This one's delta runs: it reads the dimension -- no offset is not no data to it -- joins its rows in, and
    // publishes the rebuild's records with them. The partition it rebuilt is rebuilt again first, which is the
    // count below, because nothing recorded it as caught up.
    sql """REFRESH MATERIALIZED VIEW ivm_unconsumed_dim_mv INCREMENTAL"""
    task = waitForNewTask(task.TaskId.toString())
    qt_task_after_the_consumed_delta taskQuery(task.TaskId.toString())
    order_qt_mv_after_the_consumed_delta """SELECT k, dt, amount, v FROM ivm_unconsumed_dim_mv ORDER BY k"""

    // Nothing left behind the offset and nothing left owing a rebuild.
    sql """REFRESH MATERIALIZED VIEW ivm_unconsumed_dim_mv INCREMENTAL"""
    task = waitForNewTask(task.TaskId.toString())
    qt_task_after_the_recovery taskQuery(task.TaskId.toString())
    order_qt_mv_after_the_recovery """SELECT k, dt, amount, v FROM ivm_unconsumed_dim_mv ORDER BY k"""
}
