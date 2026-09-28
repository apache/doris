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
 * What a partition refresh owes the partitions it replaces, when it dies between the two halves of the
 * overwrite that replaces them.
 *
 * <p>An overwrite commits the rows into temporary partitions and publishes them with a swap afterwards, and
 * a partition refresh reads its partitions in RESET mode, whose stream offsets are committed with that first
 * half. A refresh that dies in between -- a failure, a cancellation, a master switch -- therefore leaves the
 * live partition holding the rows it had, the change it read consumed from the stream, and no epochs saying
 * so: a task that never returns publishes none. Only a requirement raised before the read gets those
 * partitions rebuilt afterwards, which is what this pins: without it the incremental refresh that follows
 * reads the delta from an offset past the change, keeps the old rows, and reports success.
 *
 * <p>The failure is injected at that boundary (a debug point in the overwrite, filtered by MV name), so it
 * lands on a real refresh rather than on a crash window, and it is removed before the refresh that has to
 * recover from it.
 *
 * <p>All dates are literals: no current_date(), so the expectation does not depend on the run date.
 */
suite("test_ivm_overwrite_failure_between_the_halves", "nonConcurrent") {
    def mvName = "ivm_overwrite_halves_mv"
    def failPoint = "InsertOverwriteTableCommand.failBetweenTheTwoHalvesOfAnOverwrite"

    def waitForNewTask = { previousTaskId ->
        def taskResult
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT TaskId, Status, ErrorMsg
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

    GetDebugPoint().disableDebugPointForAllFEs(failPoint)
    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_overwrite_halves_mv"""
    sql """DROP TABLE IF EXISTS ivm_overwrite_halves_f"""

    sql """
        CREATE TABLE ivm_overwrite_halves_f (
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
    sql """ALTER TABLE ivm_overwrite_halves_f ADD PARTITION p202601 VALUES [('2026-01-01'), ('2026-02-01'))"""
    sql """ALTER TABLE ivm_overwrite_halves_f ADD PARTITION p202602 VALUES [('2026-02-01'), ('2026-03-01'))"""

    sql """INSERT INTO ivm_overwrite_halves_f VALUES
            (1, '2026-01-10', 100),
            (2, '2026-02-10', 200)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_overwrite_halves_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(order_id, dt)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT order_id, dt, amount FROM ivm_overwrite_halves_f
    """

    sql """REFRESH MATERIALIZED VIEW ivm_overwrite_halves_mv COMPLETE"""
    def task = waitForNewTask(null)
    order_qt_baseline_mv """SELECT order_id, dt, amount FROM ivm_overwrite_halves_mv ORDER BY order_id"""

    // The change the failed refresh reads and never publishes: it is what makes the recovery visible.
    sql """INSERT INTO ivm_overwrite_halves_f VALUES (3, '2026-01-20', 300)"""

    // The partition refresh of p202601 dies after its rows are committed into the temporary partitions and
    // before the swap that would publish them. What the read consumed -- the stream offset of that base
    // partition, reset by this refresh -- is committed by then.
    def failedTask
    try {
        GetDebugPoint().enableDebugPointForAllFEs(failPoint, [mv_name: mvName])
        sql """REFRESH MATERIALIZED VIEW ivm_overwrite_halves_mv PARTITIONS"""
        failedTask = waitForNewTask(task.TaskId.toString())
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(failPoint)
    }
    assertEquals("FAILED", failedTask.Status.toString())
    // The failure has to be the one injected here, or the state below is not the state it made.
    assertTrue(failedTask.ErrorMsg.toString().contains(failPoint))
    qt_failed_task taskQuery(failedTask.TaskId.toString())
    order_qt_mv_after_the_failed_overwrite """
        SELECT order_id, dt, amount FROM ivm_overwrite_halves_mv ORDER BY order_id
    """

    // The partition the failed refresh was replacing still owes that rebuild, so the next refresh rebuilds it
    // instead of catching it up from a stream whose offset has already moved past the change. The order the
    // engine reads is the request's, which asks for the incremental path: the rebuild is what the requirement
    // adds to it, and the count of partitions it rebuilt is how the request reports that it was more.
    sql """REFRESH MATERIALIZED VIEW ivm_overwrite_halves_mv INCREMENTAL"""
    task = waitForNewTask(task.TaskId.toString())
    qt_recovery_task taskQuery(task.TaskId.toString())
    order_qt_mv_after_the_recovery """SELECT order_id, dt, amount FROM ivm_overwrite_halves_mv ORDER BY order_id"""
}
