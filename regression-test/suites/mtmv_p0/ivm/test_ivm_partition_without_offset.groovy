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
 * An incremental refresh that reads a base partition the stream has no offset for: one added after the MV was
 * built, and one that was still empty then.
 *
 * <p>A stream records an offset for a partition when it consumes the partition with data in it, so neither of
 * the two has one, and none of their rows are in the MV. Read through the changelog, such a partition is read
 * from the first TSO the changelog retains, and the rows older than changelog.ttl_seconds would be left out
 * while the refresh succeeds. The refresh reads it from the base table instead, as an append of all its rows.
 *
 * <p>Cases pinned here, for an MV over a unique-key table, an append-only detail MV over a duplicate-key table
 * and an MV partitioned like its unique-key base table:
 * <ol>
 *   <li>the rows written to the two partitions are older than the TTL when the MV is refreshed: a strict
 *       INCREMENTAL refresh succeeds and the MV has them;</li>
 *   <li>with the TTL back to a day, the two partitions have offsets now, and the next refresh reads their
 *       changes from the changelog;</li>
 *   <li>the partitioned MV rebuilds the MV partition of the added base partition, as before.</li>
 * </ol>
 *
 * <p>nonConcurrent: the rows age past a one-second TTL by sleeping.
 */
suite("test_ivm_partition_without_offset", "nonConcurrent") {
    def waitForNewTask = { String mvName, previousTaskId ->
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

    // How a refresh ended, in one row. Unset RefreshMode / IvmFallbackReason come back as the literal
    // two-character string "\N", which does not survive the .out round trip, so fold them into a printable
    // token; a reason is an enum name, which "\N" is not.
    def taskQuery = { String taskId ->
        """
            SELECT Status,
                   CASE WHEN RefreshMode IN ('COMPLETE', 'PARTIAL', 'NOT_REFRESH')
                        THEN RefreshMode ELSE 'NONE' END,
                   CASE WHEN IvmFallbackReason REGEXP '^[A-Z_]+\$'
                        THEN IvmFallbackReason ELSE 'NONE' END
            FROM tasks('type'='mv')
            WHERE TaskId = '${taskId}'
        """
    }

    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_no_offset_u_mv"""
    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_no_offset_d_mv"""
    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_no_offset_u_pmv"""
    sql """DROP TABLE IF EXISTS ivm_no_offset_u"""
    sql """DROP TABLE IF EXISTS ivm_no_offset_d"""

    sql """
        CREATE TABLE ivm_no_offset_u (
            k BIGINT NOT NULL,
            dt DATE NOT NULL,
            v BIGINT
        )
        UNIQUE KEY(k, dt)
        PARTITION BY RANGE(dt) (
            PARTITION p202601 VALUES [('2026-01-01'), ('2026-02-01')),
            PARTITION p202602 VALUES [('2026-02-01'), ('2026-03-01'))
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true",
            "changelog.ttl_seconds" = "86400"
        )
    """
    sql """
        CREATE TABLE ivm_no_offset_d (
            k BIGINT NOT NULL,
            dt DATE NOT NULL,
            v BIGINT
        )
        DUPLICATE KEY(k)
        PARTITION BY RANGE(dt) (
            PARTITION p202601 VALUES [('2026-01-01'), ('2026-02-01')),
            PARTITION p202602 VALUES [('2026-02-01'), ('2026-03-01'))
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true",
            "changelog.ttl_seconds" = "86400"
        )
    """
    // p202602 stays empty until after the MVs are built.
    sql """INSERT INTO ivm_no_offset_u VALUES (1, '2026-01-10', 10)"""
    sql """INSERT INTO ivm_no_offset_d VALUES (1, '2026-01-10', 10)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_no_offset_u_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT k, dt, v FROM ivm_no_offset_u
    """
    sql """
        CREATE MATERIALIZED VIEW ivm_no_offset_d_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT k, dt, v FROM ivm_no_offset_d
    """
    sql """
        CREATE MATERIALIZED VIEW ivm_no_offset_u_pmv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(k, dt)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT k, dt, v FROM ivm_no_offset_u
    """

    sql """REFRESH MATERIALIZED VIEW ivm_no_offset_u_mv INCREMENTAL"""
    def uTask = waitForNewTask('ivm_no_offset_u_mv', null)
    qt_u_baseline_task taskQuery(uTask.TaskId.toString())
    sql """REFRESH MATERIALIZED VIEW ivm_no_offset_d_mv INCREMENTAL"""
    def dTask = waitForNewTask('ivm_no_offset_d_mv', null)
    qt_d_baseline_task taskQuery(dTask.TaskId.toString())
    sql """REFRESH MATERIALIZED VIEW ivm_no_offset_u_pmv INCREMENTAL"""
    def pTask = waitForNewTask('ivm_no_offset_u_pmv', null)
    qt_p_baseline_task taskQuery(pTask.TaskId.toString())

    // 1. Rows in the partition that was empty when the MVs were built and in a partition added after that, both
    // older than the TTL by the time the MVs are refreshed. Nothing else changes, so no offset is read.
    sql """ALTER TABLE ivm_no_offset_u ADD PARTITION p202603 VALUES [('2026-03-01'), ('2026-04-01'))"""
    sql """ALTER TABLE ivm_no_offset_d ADD PARTITION p202603 VALUES [('2026-03-01'), ('2026-04-01'))"""
    sql """INSERT INTO ivm_no_offset_u VALUES (2, '2026-02-10', 20), (3, '2026-03-10', 30)"""
    sql """INSERT INTO ivm_no_offset_d VALUES (2, '2026-02-10', 20), (3, '2026-03-10', 30)"""
    sql """ALTER TABLE ivm_no_offset_u SET ("changelog.ttl_seconds" = "1")"""
    sql """ALTER TABLE ivm_no_offset_d SET ("changelog.ttl_seconds" = "1")"""
    sleep(1500)

    sql """REFRESH MATERIALIZED VIEW ivm_no_offset_u_mv INCREMENTAL"""
    uTask = waitForNewTask('ivm_no_offset_u_mv', uTask.TaskId.toString())
    qt_u_task taskQuery(uTask.TaskId.toString())
    order_qt_u_mv """SELECT k, dt, v FROM ivm_no_offset_u_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_no_offset_d_mv INCREMENTAL"""
    dTask = waitForNewTask('ivm_no_offset_d_mv', dTask.TaskId.toString())
    qt_d_task taskQuery(dTask.TaskId.toString())
    order_qt_d_mv """SELECT k, dt, v FROM ivm_no_offset_d_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_no_offset_u_pmv INCREMENTAL"""
    pTask = waitForNewTask('ivm_no_offset_u_pmv', pTask.TaskId.toString())
    qt_p_task taskQuery(pTask.TaskId.toString())
    order_qt_p_mv """SELECT k, dt, v FROM ivm_no_offset_u_pmv"""

    // 2. With the TTL back to a day, the offsets the refreshes above recorded are well inside the retention, and
    // what changes after them is read from the changelog: an update and a new row in the unique-key table, a
    // new row in the duplicate-key one.
    sql """ALTER TABLE ivm_no_offset_u SET ("changelog.ttl_seconds" = "86400")"""
    sql """ALTER TABLE ivm_no_offset_d SET ("changelog.ttl_seconds" = "86400")"""
    sql """INSERT INTO ivm_no_offset_u VALUES (2, '2026-02-10', 21), (4, '2026-03-11', 40)"""
    sql """INSERT INTO ivm_no_offset_d VALUES (4, '2026-03-11', 40)"""

    sql """REFRESH MATERIALIZED VIEW ivm_no_offset_u_mv INCREMENTAL"""
    uTask = waitForNewTask('ivm_no_offset_u_mv', uTask.TaskId.toString())
    qt_u_task_after_offsets taskQuery(uTask.TaskId.toString())
    order_qt_u_mv_after_offsets """SELECT k, dt, v FROM ivm_no_offset_u_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_no_offset_d_mv INCREMENTAL"""
    dTask = waitForNewTask('ivm_no_offset_d_mv', dTask.TaskId.toString())
    qt_d_task_after_offsets taskQuery(dTask.TaskId.toString())
    order_qt_d_mv_after_offsets """SELECT k, dt, v FROM ivm_no_offset_d_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_no_offset_u_pmv INCREMENTAL"""
    pTask = waitForNewTask('ivm_no_offset_u_pmv', pTask.TaskId.toString())
    qt_p_task_after_offsets taskQuery(pTask.TaskId.toString())
    order_qt_p_mv_after_offsets """SELECT k, dt, v FROM ivm_no_offset_u_pmv"""

    // What the base tables answer for the MV queries. Rewrite is off: a query of the MV's shape would otherwise
    // be answered from an MV, and the comparison would compare the MV with itself.
    sql """SET enable_materialized_view_rewrite = false"""
    order_qt_base_u """SELECT k, dt, v FROM ivm_no_offset_u"""
    order_qt_base_d """SELECT k, dt, v FROM ivm_no_offset_d"""
    sql """SET enable_materialized_view_rewrite = true"""
}
