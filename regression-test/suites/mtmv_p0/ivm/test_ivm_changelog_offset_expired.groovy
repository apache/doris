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
 * An incremental refresh whose stream offset the base table's changelog.ttl_seconds has expired.
 *
 * <p>An IVM MV reads a unique-key base table through an internal stream created with MIN_DELTA, and planning
 * such a read refuses to start from an offset older than the changelog the table still retains. That refusal is
 * the stream being broken for the incremental attempt, not a failure of the refresh: the attempt reports it as
 * CHANGELOG_BROKEN, and only COMPLETE, which resets the stream baselines, can bring the MV back.
 *
 * <p>Cases pinned here:
 * <ol>
 *   <li>the delta reads the expired offset: a strict INCREMENTAL refresh fails with that reason and leaves the
 *       MV as it was, an AUTO refresh, which may fall back, rebuilds the MV with COMPLETE and records the
 *       reason, and the refresh after it is incremental again;</li>
 *   <li>the rebuild an incremental attempt runs first for an MV partition that needs one reads the expired
 *       offset of a table the MV does not partition by: the same answer, before the delta runs.</li>
 * </ol>
 *
 * <p>nonConcurrent: the offsets expire by sleeping past a one-second TTL.
 */
suite("test_ivm_changelog_offset_expired", "nonConcurrent") {
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
    // token; a reason is an enum name, which "\N" is not. The error text carries TSOs, so only whether it
    // names the expired offset and the reason is kept.
    def taskQuery = { String taskId ->
        """
            SELECT Status,
                   CASE WHEN RefreshMode IN ('COMPLETE', 'PARTIAL', 'NOT_REFRESH')
                        THEN RefreshMode ELSE 'NONE' END,
                   CASE WHEN IvmFallbackReason REGEXP '^[A-Z_]+\$'
                        THEN IvmFallbackReason ELSE 'NONE' END,
                   ErrorMsg LIKE '%Changelog offset has expired according to changelog.ttl_seconds%',
                   ErrorMsg LIKE '%reason=CHANGELOG_BROKEN%'
            FROM tasks('type'='mv')
            WHERE TaskId = '${taskId}'
        """
    }

    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_offset_expired_mv"""
    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_offset_expired_pmv"""
    sql """DROP TABLE IF EXISTS ivm_offset_expired_t"""
    sql """DROP TABLE IF EXISTS ivm_offset_expired_f"""
    sql """DROP TABLE IF EXISTS ivm_offset_expired_d"""

    // 1. The delta reads the expired offset.
    sql """
        CREATE TABLE ivm_offset_expired_t (
            id BIGINT NOT NULL,
            v1 BIGINT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true",
            "changelog.ttl_seconds" = "86400"
        )
    """
    sql """INSERT INTO ivm_offset_expired_t VALUES (1, 10), (2, 20)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_offset_expired_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT id, v1 FROM ivm_offset_expired_t
    """

    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_mv INCREMENTAL"""
    def task = waitForNewTask('ivm_offset_expired_mv', null)
    qt_baseline_task taskQuery(task.TaskId.toString())
    order_qt_baseline_mv """SELECT id, v1 FROM ivm_offset_expired_mv ORDER BY id"""

    // The offset the baseline recorded falls out of the retention once the TTL is shorter than its age. The row
    // written after that is what gives the stream something to read, so the next refresh plans the read.
    sql """ALTER TABLE ivm_offset_expired_t SET ("changelog.ttl_seconds" = "1")"""
    sleep(1500)
    sql """INSERT INTO ivm_offset_expired_t VALUES (3, 30)"""

    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_mv INCREMENTAL"""
    task = waitForNewTask('ivm_offset_expired_mv', task.TaskId.toString())
    qt_strict_task taskQuery(task.TaskId.toString())
    order_qt_mv_after_strict """SELECT id, v1 FROM ivm_offset_expired_mv ORDER BY id"""

    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_mv AUTO"""
    task = waitForNewTask('ivm_offset_expired_mv', task.TaskId.toString())
    qt_fallback_task taskQuery(task.TaskId.toString())
    order_qt_mv_after_fallback """SELECT id, v1 FROM ivm_offset_expired_mv ORDER BY id"""

    // With the TTL back to a day, the offsets the rebuild left are well inside the retention again.
    sql """ALTER TABLE ivm_offset_expired_t SET ("changelog.ttl_seconds" = "86400")"""
    sql """INSERT INTO ivm_offset_expired_t VALUES (4, 40)"""
    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_mv AUTO"""
    task = waitForNewTask('ivm_offset_expired_mv', task.TaskId.toString())
    qt_task_after_fallback taskQuery(task.TaskId.toString())
    order_qt_mv_after_a_further_refresh """SELECT id, v1 FROM ivm_offset_expired_mv ORDER BY id"""

    // 2. The rebuild before the delta reads the expired offset. The MV is partitioned by the fact table and joins a
    // dimension: rebuilding some of its partitions is a read of a subset of the MV, which reads the dimension
    // through its stream at the offset.
    sql """
        CREATE TABLE ivm_offset_expired_f (
            k BIGINT NOT NULL,
            dt DATE NOT NULL,
            amount INT
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
        CREATE TABLE ivm_offset_expired_d (
            k BIGINT NOT NULL,
            v INT
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true",
            "changelog.ttl_seconds" = "86400"
        )
    """
    sql """INSERT INTO ivm_offset_expired_f VALUES (1, '2026-01-10', 100), (2, '2026-02-10', 200)"""
    sql """INSERT INTO ivm_offset_expired_d VALUES (1, 10), (2, 10)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_offset_expired_pmv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(k, dt)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT f.k, f.dt, f.amount, d.v
           FROM ivm_offset_expired_f f JOIN ivm_offset_expired_d d ON f.k = d.k
    """

    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_pmv COMPLETE"""
    task = waitForNewTask('ivm_offset_expired_pmv', null)
    order_qt_partitioned_baseline_mv """SELECT k, dt, amount, v FROM ivm_offset_expired_pmv ORDER BY k"""

    // The dimension changes after its offset and the offset expires; the truncate is a change no delta can
    // repair, so the MV partition it reaches needs a rebuild.
    sql """INSERT INTO ivm_offset_expired_d VALUES (1, 20), (2, 20)"""
    sql """ALTER TABLE ivm_offset_expired_d SET ("changelog.ttl_seconds" = "1")"""
    sleep(1500)
    sql """TRUNCATE TABLE ivm_offset_expired_f PARTITION(p202601)"""
    sql """INSERT INTO ivm_offset_expired_f VALUES (1, '2026-01-10', 100)"""

    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_pmv INCREMENTAL"""
    task = waitForNewTask('ivm_offset_expired_pmv', task.TaskId.toString())
    qt_rebuild_strict_task taskQuery(task.TaskId.toString())
    order_qt_partitioned_mv_after_strict """SELECT k, dt, amount, v FROM ivm_offset_expired_pmv ORDER BY k"""

    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_pmv AUTO"""
    task = waitForNewTask('ivm_offset_expired_pmv', task.TaskId.toString())
    qt_rebuild_fallback_task taskQuery(task.TaskId.toString())
    order_qt_partitioned_mv_after_fallback """SELECT k, dt, amount, v FROM ivm_offset_expired_pmv ORDER BY k"""

    sql """ALTER TABLE ivm_offset_expired_d SET ("changelog.ttl_seconds" = "86400")"""
    sql """INSERT INTO ivm_offset_expired_d VALUES (1, 30)"""
    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_pmv AUTO"""
    task = waitForNewTask('ivm_offset_expired_pmv', task.TaskId.toString())
    qt_partitioned_task_after_fallback taskQuery(task.TaskId.toString())
    order_qt_partitioned_mv_after_a_further_refresh """
        SELECT k, dt, amount, v FROM ivm_offset_expired_pmv ORDER BY k
    """
}
