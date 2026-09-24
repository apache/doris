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
 * What a refresh does with the partitions a metadata-only base-table change left behind.
 *
 * <p>A partition drop / truncate / replace / recover emits no row binlog, so an MV partition that read those
 * rows keeps them forever unless it is rebuilt. Such a partition carries a raised requirement, which sends
 * it to a rebuild, while the partitions nothing changed for keep catching up incrementally -- and the
 * request does not have to ask for it: a strict INCREMENTAL refresh rebuilds what the requirement names and
 * reports how many partitions that was in IvmRebuiltPartitions, rather than reporting the stale rows as
 * current.
 *
 * <p>Cases pinned here:
 * <ol>
 *   <li>the baseline: both MV partitions filled by a COMPLETE refresh, then one INCREMENTAL refresh that
 *       rebuilds nothing (IvmRebuiltPartitions 0) and still applies its delta;</li>
 *   <li>a truncated base partition: its rows go away, the other partition's delta is applied, and the
 *       refresh reports the partition it had to rebuild;</li>
 *   <li>a rename of the base table: it changes no column, so it raises no partition requirement -- an epoch
 *       is not where a rename belongs -- but it does put the MV into SCHEMA_CHANGE, and that state is what
 *       makes the strict INCREMENTAL refresh after it run as a whole-MV COMPLETE;</li>
 *   <li>a second truncated partition: the requirement keeps naming the partitions it belongs to.</li>
 * </ol>
 *
 * <p>All dates are literals: no current_date(), so the expectation does not depend on the run date.
 */
suite("test_ivm_partition_epoch_rebuild") {
    def baseTable = "ivm_epoch_f"
    def mvName = "ivm_epoch_mv"

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
    // partitions it rebuilt although the request did not ask for them.
    //
    // A refresh that only ran the incremental rewrite leaves RefreshMode unset, and an unset column comes
    // back as the literal two-character string "\N", which does not survive the .out round trip, so fold
    // every value that is not a scope into a printable token.
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

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
    sql """DROP TABLE IF EXISTS ${baseTable}"""

    sql """
        CREATE TABLE ${baseTable} (
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
    sql """ALTER TABLE ${baseTable} ADD PARTITION p202601 VALUES [('2026-01-01'), ('2026-02-01'))"""
    sql """ALTER TABLE ${baseTable} ADD PARTITION p202602 VALUES [('2026-02-01'), ('2026-03-01'))"""

    sql """INSERT INTO ${baseTable} VALUES
            (1, '2026-01-10', 100),
            (2, '2026-02-10', 200)"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(order_id, dt)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT order_id, dt, amount FROM ${baseTable}
    """

    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    def taskId = waitForNewTask(null)
    qt_baseline_task taskQuery(taskId)
    order_qt_baseline_mv """SELECT order_id, dt, amount FROM ${mvName}"""

    // The ordinary incremental refresh: nothing was invalidated, so no partition is rebuilt -- and the
    // delta still lands, which is what makes the count meaningful rather than a constant.
    sql """INSERT INTO ${baseTable} VALUES (3, '2026-01-20', 300)"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    taskId = waitForNewTask(taskId)
    qt_incremental_task taskQuery(taskId)
    order_qt_incremental_mv """SELECT order_id, dt, amount FROM ${mvName}"""

    // A truncated base partition emits no binlog, so the MV partition that read it keeps rows that no
    // longer exist anywhere. It is rebuilt; the row inserted into the other partition in the same window
    // arrives incrementally, and the refresh says it rebuilt one partition the request did not ask for.
    sql """TRUNCATE TABLE ${baseTable} PARTITION(p202601)"""
    sql """INSERT INTO ${baseTable} VALUES (4, '2026-02-20', 400)"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    taskId = waitForNewTask(taskId)
    qt_truncate_task taskQuery(taskId)
    order_qt_truncate_mv """SELECT order_id, dt, amount FROM ${mvName}"""

    // A rename leaves every column alone and names nothing new to read, so it raises no partition
    // requirement: an epoch is not where this change belongs. What it does move is the MV state. The
    // shared base-table hook puts every MV that reads the table into SCHEMA_CHANGE -- the MV query still
    // spells the old name, so it no longer analyzes -- and renaming the table back does not clear it: the
    // dependencies are registered under the name the query spells, so the rename back finds nothing to
    // update. The state is therefore what widens the strict INCREMENTAL below into a whole-MV COMPLETE.
    // The rows it leaves are the same either way; the columns this query reports are not. The count is the
    // size of the MV because the request was an INCREMENTAL that ran as a COMPLETE -- a request that was
    // itself a COMPLETE reports 0, since rebuilding everything is what it asked for.
    sql """ALTER TABLE ${baseTable} RENAME ivm_epoch_renamed"""
    sql """ALTER TABLE ivm_epoch_renamed RENAME ${baseTable}"""
    sql """INSERT INTO ${baseTable} VALUES (5, '2026-02-21', 500)"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    taskId = waitForNewTask(taskId)
    qt_rename_task taskQuery(taskId)
    order_qt_rename_mv """SELECT order_id, dt, amount FROM ${mvName}"""

    // The requirement keeps naming its own partition: truncating the other one rebuilds that one.
    sql """TRUNCATE TABLE ${baseTable} PARTITION(p202602)"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    taskId = waitForNewTask(taskId)
    qt_second_truncate_task taskQuery(taskId)
    order_qt_second_truncate_mv """SELECT order_id, dt, amount FROM ${mvName}"""
}
