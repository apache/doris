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
 * The keep-alive of the offsets of idle IVM stream partitions.
 *
 * <p>An IVM stream moves the offset of a partition only when a refresh consumes the partition, and a refresh only
 * consumes the partitions with new data, so the offset of a partition nobody writes stays at its last change.
 * A Master daemon keeps such an offset from expiring with changelog.ttl_seconds: every
 * ivm_stream_keepalive_interval_second, for a partition at its end whose offset has less than half the TTL left,
 * it records a keep-alive TSO up to which the partition is known to hold no change. The read of a refresh checks
 * the newer of the two against the TTL, so a partition written after being idle for longer than the TTL is still
 * read incrementally.
 *
 * <p>Cases pinned here, for an aggregate MV over a merge-on-write table and an append-only detail MV over a
 * duplicate-key table, both with a sixty-second TTL:
 * <ol>
 *   <li>idle for longer than the TTL, then written: the AUTO refresh after it is incremental, with no fallback;</li>
 *   <li>the same with the keep-alive off: the refresh falls back to COMPLETE as CHANGELOG_BROKEN, which shows the
 *       case above is the keep-alive's doing;</li>
 *   <li>a write that lands while a keep-alive round has read the offsets but recorded nothing yet: the round
 *       records nothing for the partition, and the refresh after it reads the write.</li>
 * </ol>
 *
 * <p>nonConcurrent: the partitions are left idle by sleeping past the TTL, and the keep-alive daemon is held at a
 * debug point.
 */
suite("test_ivm_stream_keepalive", "nonConcurrent") {
    // The base-table queries below check the MVs, so they must not be answered from an MV; and a base-table
    // insert must not be redirected into one, or the incremental path under test goes unused.
    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_dml_materialized_view_rewrite = false"

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
    // token; a reason is an enum name, which "\N" is not. An incremental refresh shows NONE for both.
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

    // Longer than the sixty-second TTL: an offset left alone that long has expired.
    def idleSeconds = 70

    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_keepalive_agg_mv"""
    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_keepalive_detail_mv"""
    sql """DROP TABLE IF EXISTS ivm_keepalive_mow_t"""
    sql """DROP TABLE IF EXISTS ivm_keepalive_dup_t"""

    sql """
        CREATE TABLE ivm_keepalive_mow_t (
            id BIGINT NOT NULL,
            grp INT NOT NULL,
            v1 BIGINT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true",
            "changelog.ttl_seconds" = "60"
        )
    """
    sql """
        CREATE TABLE ivm_keepalive_dup_t (
            id BIGINT NOT NULL,
            grp INT NOT NULL,
            v1 BIGINT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true",
            "changelog.ttl_seconds" = "60"
        )
    """
    sql """INSERT INTO ivm_keepalive_mow_t VALUES (1, 1, 10), (2, 2, 20)"""
    sql """INSERT INTO ivm_keepalive_dup_t VALUES (1, 1, 10), (2, 2, 20)"""

    // The aggregate keeps this MV on the unique-key model; the plain projection over the duplicate-key table makes
    // the other an append-only detail MV, whose stream is read APPEND_ONLY.
    sql """
        CREATE MATERIALIZED VIEW ivm_keepalive_agg_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(grp) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT grp, COUNT(*) AS cnt, SUM(v1) AS total FROM ivm_keepalive_mow_t GROUP BY grp
    """
    sql """
        CREATE MATERIALIZED VIEW ivm_keepalive_detail_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT id, grp, v1 FROM ivm_keepalive_dup_t
    """
    def aggStreamRows = sql """
        SELECT STREAM_ID FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND STREAM_NAME LIKE '__doris_ivm_stream_%'
          AND BASE_TABLE_NAME = 'ivm_keepalive_mow_t'
    """
    assertEquals(1, aggStreamRows.size())
    def aggStreamId = aggStreamRows[0][0]

    // A check every five seconds; the offsets of a partition at its end are renewed once less than thirty
    // seconds of the TTL is left. Both are EXPERIMENTAL configs, which SHOW FRONTEND CONFIG, and so
    // setFeConfigTemporary, knows by their experimental_ names.
    setFeConfigTemporary([experimental_ivm_stream_keepalive_interval_second: 5,
                          experimental_enable_ivm_stream_keepalive: true]) {
        sql """REFRESH MATERIALIZED VIEW ivm_keepalive_agg_mv INCREMENTAL"""
        def aggTask = waitForNewTask('ivm_keepalive_agg_mv', null)
        qt_agg_baseline_task taskQuery(aggTask.TaskId.toString())
        sql """REFRESH MATERIALIZED VIEW ivm_keepalive_detail_mv INCREMENTAL"""
        def detailTask = waitForNewTask('ivm_keepalive_detail_mv', null)
        qt_detail_baseline_task taskQuery(detailTask.TaskId.toString())

        // 1. Idle for longer than the TTL with the keep-alive on, then written: read incrementally.
        sleep(idleSeconds * 1000)
        sql """INSERT INTO ivm_keepalive_mow_t VALUES (3, 1, 30)"""
        sql """INSERT INTO ivm_keepalive_dup_t VALUES (3, 1, 30)"""
        sql """REFRESH MATERIALIZED VIEW ivm_keepalive_agg_mv AUTO"""
        aggTask = waitForNewTask('ivm_keepalive_agg_mv', aggTask.TaskId.toString())
        qt_agg_kept_alive_task taskQuery(aggTask.TaskId.toString())
        order_qt_agg_kept_alive_mv """SELECT grp, cnt, total FROM ivm_keepalive_agg_mv"""
        sql """REFRESH MATERIALIZED VIEW ivm_keepalive_detail_mv AUTO"""
        detailTask = waitForNewTask('ivm_keepalive_detail_mv', detailTask.TaskId.toString())
        qt_detail_kept_alive_task taskQuery(detailTask.TaskId.toString())
        order_qt_detail_kept_alive_mv """SELECT id, grp, v1 FROM ivm_keepalive_detail_mv"""

        // 2. The same with the keep-alive off: the offsets expire, and the refreshes fall back to COMPLETE.
        setFeConfigTemporary([experimental_enable_ivm_stream_keepalive: false]) {
            sleep(idleSeconds * 1000)
            sql """INSERT INTO ivm_keepalive_mow_t VALUES (4, 2, 40)"""
            sql """INSERT INTO ivm_keepalive_dup_t VALUES (4, 2, 40)"""
            sql """REFRESH MATERIALIZED VIEW ivm_keepalive_agg_mv AUTO"""
            aggTask = waitForNewTask('ivm_keepalive_agg_mv', aggTask.TaskId.toString())
            qt_agg_expired_task taskQuery(aggTask.TaskId.toString())
            order_qt_agg_expired_mv """SELECT grp, cnt, total FROM ivm_keepalive_agg_mv"""
            sql """REFRESH MATERIALIZED VIEW ivm_keepalive_detail_mv AUTO"""
            detailTask = waitForNewTask('ivm_keepalive_detail_mv', detailTask.TaskId.toString())
            qt_detail_expired_task taskQuery(detailTask.TaskId.toString())
            order_qt_detail_expired_mv """SELECT id, grp, v1 FROM ivm_keepalive_detail_mv"""
        }

        // 3. A round of the keep-alive holds the aggregate MV's stream where a write can land between what it read
        // and what it records: in shared-nothing mode after it has read the offsets and before it takes the base
        // table lock, in cloud mode after it has taken the committed TSO and before it reads the offsets from
        // MetaService. Held long enough for the offset to be due for a renewal, it sees the write once released,
        // and records nothing: a keep-alive TSO past the write would make the refresh below skip it.
        def holdKeepAlive = isCloudMode()
                ? 'TableStreamManager.keepAliveIdleCloudPartitions.blockBeforeReadStates'
                : 'TableStreamManager.keepAliveIdlePartitions.blockBeforeBaseTableLock'
        try {
            GetDebugPoint().enableDebugPointForAllFEs(holdKeepAlive, [value: aggStreamId])
            sleep(35 * 1000)
            sql """INSERT INTO ivm_keepalive_mow_t VALUES (5, 2, 50)"""
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs(holdKeepAlive)
        }
        // Let the released round finish before the refresh plans its read, so that what it records would count.
        sleep(2 * 1000)
        sql """REFRESH MATERIALIZED VIEW ivm_keepalive_agg_mv AUTO"""
        aggTask = waitForNewTask('ivm_keepalive_agg_mv', aggTask.TaskId.toString())
        qt_agg_raced_task taskQuery(aggTask.TaskId.toString())
        order_qt_agg_raced_mv """SELECT grp, cnt, total FROM ivm_keepalive_agg_mv"""
    }

    // What the base tables answer for the two MV queries.
    order_qt_base_agg """SELECT grp, COUNT(*), SUM(v1) FROM ivm_keepalive_mow_t GROUP BY grp"""
    order_qt_base_detail """SELECT id, grp, v1 FROM ivm_keepalive_dup_t"""
}
