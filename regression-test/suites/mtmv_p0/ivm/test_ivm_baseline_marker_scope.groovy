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
 * Which MV partitions a base-table partition change invalidates.
 *
 * <p>A partition drop / truncate / replace / recover changes the table through metadata and emits no row
 * binlog, so any MV partition that read the dropped rows keeps them forever unless it is rebuilt. Which
 * partitions those are is decided by the MV's partition mapping, and the three cases the mapping cannot
 * answer -- a joined base table that is not a PCT table, a partition that is not in the metadata yet
 * (RECOVER), a SELF_MANAGE MV -- fall back to rebuilding the whole MV.
 *
 * <p>Cases pinned here:
 * <ol>
 *   <li>a partition dropped from a joined table that the MV's partition column does not reach: the whole
 *       MV is rebuilt, so the rows that joined through the dropped partition are recomputed (and lose
 *       their dimension value) instead of keeping the stale one;</li>
 *   <li>a base partition that no MV partition reads: nothing is invalidated, so a strict INCREMENTAL
 *       refresh still starts (under a marker that cannot tell "not read" from "not in the snapshot",
 *       this left a complete-rebuild barrier behind and the refresh failed);</li>
 *   <li>the ordinary case: dropping a base partition that one MV partition reads removes its rows.</li>
 * </ol>
 *
 * <p>All dates are literals and every partition is created by hand: no current_date() and no dynamic
 * partition scheduler, so the expectation does not depend on the run date.
 */
suite("test_ivm_baseline_marker_scope") {
    def factTable = "ivm_marker_f"
    def dimTable = "ivm_marker_d"
    def mvName = "ivm_marker_mv"

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

    // Unset RefreshMode / IvmFallbackReason come back as the literal two-character string "\N",
    // which does not survive the .out round trip, so fold the unset value into a printable token.
    def taskQuery = { String taskId ->
        """
            SELECT Status,
                   CASE WHEN RefreshMode IN ('COMPLETE', 'PARTIAL', 'NOT_REFRESH')
                        THEN RefreshMode ELSE 'NONE' END,
                   CASE WHEN IvmFallbackReason = 'BINLOG_BROKEN'
                        THEN IvmFallbackReason ELSE 'NONE' END
            FROM tasks('type'='mv')
            WHERE TaskId = '${taskId}'
        """
    }

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
    sql """DROP TABLE IF EXISTS ${factTable}"""
    sql """DROP TABLE IF EXISTS ${dimTable}"""

    sql """
        CREATE TABLE ${factTable} (
            order_id BIGINT NOT NULL,
            dt DATE NOT NULL,
            dimension_id INT,
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
    sql """ALTER TABLE ${factTable} ADD PARTITION p202601 VALUES [('2026-01-01'), ('2026-02-01'))"""
    sql """ALTER TABLE ${factTable} ADD PARTITION p202602 VALUES [('2026-02-01'), ('2026-03-01'))"""

    // Partitioned as well, but joined on a non-partition column, so it is a base table of the MV without
    // being one of its PCT tables.
    sql """
        CREATE TABLE ${dimTable} (
            dimension_id INT NOT NULL,
            dt DATE NOT NULL,
            dimension_name VARCHAR(32)
        )
        UNIQUE KEY(dimension_id, dt)
        PARTITION BY RANGE(dt) ()
        DISTRIBUTED BY HASH(dimension_id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """ALTER TABLE ${dimTable} ADD PARTITION d202601 VALUES [('2026-01-01'), ('2026-02-01'))"""
    sql """ALTER TABLE ${dimTable} ADD PARTITION d202602 VALUES [('2026-02-01'), ('2026-03-01'))"""

    sql """INSERT INTO ${dimTable} VALUES (10, '2026-01-15', 'dim-a'), (20, '2026-02-15', 'dim-b')"""
    sql """INSERT INTO ${factTable} VALUES
            (1, '2026-01-10', 10, 100),
            (2, '2026-02-10', 20, 200)"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(order_id, dt)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT f.order_id, f.dt, f.amount, d.dimension_name
           FROM ${factTable} f
           LEFT JOIN ${dimTable} d ON f.dimension_id = d.dimension_id
    """

    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    def taskId = waitForNewTask(null)
    qt_baseline_task taskQuery(taskId)
    order_qt_baseline_mv """SELECT order_id, dt, amount, dimension_name
        FROM ${mvName}"""

    // A partition dropped from the joined table: the MV's partition column does not reach it, so which
    // MV partitions read it cannot be determined and all of them are rebuilt. The row that joined
    // through d202601 must be recomputed without its dimension value, not left as it was.
    sql """ALTER TABLE ${dimTable} DROP PARTITION d202601"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    taskId = waitForNewTask(taskId)
    qt_non_pct_drop_task taskQuery(taskId)
    order_qt_non_pct_drop_mv """SELECT order_id, dt, amount, dimension_name
        FROM ${mvName}"""

    // Added after the last refresh and dropped before the next one, so no MV partition reads it. Nothing
    // is invalidated, and a strict INCREMENTAL refresh still starts: with a complete-rebuild barrier left
    // behind it would be rejected with a baseline-rebuild error instead.
    sql """ALTER TABLE ${factTable} ADD PARTITION p202603 VALUES [('2026-03-01'), ('2026-04-01'))"""
    sql """ALTER TABLE ${factTable} DROP PARTITION p202603"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    taskId = waitForNewTask(taskId)
    qt_unread_partition_task """
        SELECT Status FROM tasks('type'='mv') WHERE TaskId = '${taskId}'
    """

    // The ordinary narrowing: one MV partition reads the dropped base partition, and its rows go away.
    sql """ALTER TABLE ${factTable} DROP PARTITION p202601"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    taskId = waitForNewTask(taskId)
    order_qt_narrowed_mv """SELECT order_id, dt, amount, dimension_name
        FROM ${mvName}"""
}
