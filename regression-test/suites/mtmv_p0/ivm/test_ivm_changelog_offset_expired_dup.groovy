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
 * An incremental refresh of an IVM MV over a duplicate-key base table whose stream offset the table's
 * changelog.ttl_seconds has expired.
 *
 * <p>A duplicate-key table is read through its stream APPEND_ONLY. A user stream read that way clips an expired
 * start to the changelog the table still retains and reads on from there. An IVM read may not: the rows written
 * between the offset and the cutoff would be missing from an MV whose refresh succeeded. The read refuses the
 * offset instead, the way a MIN_DELTA read does, and the refresh answers it as CHANGELOG_BROKEN, which only
 * COMPLETE can repair.
 *
 * <p>Cases pinned here, for an aggregate MV and for an append-only detail MV over the same table:
 * <ol>
 *   <li>a strict INCREMENTAL refresh fails with that reason and leaves the MV as it was;</li>
 *   <li>an AUTO refresh, which may fall back, rebuilds the MV with COMPLETE, the row written before the offset
 *       expired included, and records the reason;</li>
 *   <li>with the TTL back to a day, the refresh after it is incremental again.</li>
 * </ol>
 * A user stream on the table still reads past its expired offset, from the changelog retained.
 *
 * <p>nonConcurrent: the offsets expire by sleeping past a one-second TTL.
 */
suite("test_ivm_changelog_offset_expired_dup", "nonConcurrent") {
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

    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_offset_expired_dup_agg_mv"""
    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_offset_expired_dup_detail_mv"""
    sql """DROP STREAM IF EXISTS ivm_offset_expired_dup_stream FORCE"""
    sql """DROP TABLE IF EXISTS ivm_offset_expired_dup_t"""

    sql """
        CREATE TABLE ivm_offset_expired_dup_t (
            id BIGINT NOT NULL,
            grp INT NOT NULL,
            v1 BIGINT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true",
            "changelog.ttl_seconds" = "86400"
        )
    """
    sql """INSERT INTO ivm_offset_expired_dup_t VALUES (1, 1, 10), (2, 2, 20)"""

    // The aggregate keeps this MV on the unique-key model; the plain projection makes the other an append-only
    // detail MV. Both read the table through an internal stream of their own.
    sql """
        CREATE MATERIALIZED VIEW ivm_offset_expired_dup_agg_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(grp) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT grp, COUNT(*) AS cnt, SUM(v1) AS total FROM ivm_offset_expired_dup_t GROUP BY grp
    """
    sql """
        CREATE MATERIALIZED VIEW ivm_offset_expired_dup_detail_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT id, grp, v1 FROM ivm_offset_expired_dup_t
    """
    sql """
        CREATE STREAM ivm_offset_expired_dup_stream ON TABLE ivm_offset_expired_dup_t
        PROPERTIES ("show_initial_rows" = "false")
    """

    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_dup_agg_mv INCREMENTAL"""
    def aggTask = waitForNewTask('ivm_offset_expired_dup_agg_mv', null)
    qt_agg_baseline_task taskQuery(aggTask.TaskId.toString())
    order_qt_agg_baseline_mv """SELECT grp, cnt, total FROM ivm_offset_expired_dup_agg_mv"""
    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_dup_detail_mv INCREMENTAL"""
    def detailTask = waitForNewTask('ivm_offset_expired_dup_detail_mv', null)
    qt_detail_baseline_task taskQuery(detailTask.TaskId.toString())
    order_qt_detail_baseline_mv """SELECT id, grp, v1 FROM ivm_offset_expired_dup_detail_mv"""

    // The row written after the offsets is what a read from the retained changelog leaves out once the TTL is
    // shorter than its age.
    sql """INSERT INTO ivm_offset_expired_dup_t VALUES (3, 1, 30)"""
    order_qt_stream_before_expiry """SELECT id, grp, v1 FROM ivm_offset_expired_dup_stream"""
    sql """ALTER TABLE ivm_offset_expired_dup_t SET ("changelog.ttl_seconds" = "1")"""
    sleep(1500)

    // A user stream reads past its expired offset: the read starts at the first retained TSO, which is after
    // the row it has not consumed.
    order_qt_stream_after_expiry """SELECT id, grp, v1 FROM ivm_offset_expired_dup_stream"""

    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_dup_agg_mv INCREMENTAL"""
    aggTask = waitForNewTask('ivm_offset_expired_dup_agg_mv', aggTask.TaskId.toString())
    qt_agg_strict_task taskQuery(aggTask.TaskId.toString())
    order_qt_agg_mv_after_strict """SELECT grp, cnt, total FROM ivm_offset_expired_dup_agg_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_dup_agg_mv AUTO"""
    aggTask = waitForNewTask('ivm_offset_expired_dup_agg_mv', aggTask.TaskId.toString())
    qt_agg_fallback_task taskQuery(aggTask.TaskId.toString())
    order_qt_agg_mv_after_fallback """SELECT grp, cnt, total FROM ivm_offset_expired_dup_agg_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_dup_detail_mv INCREMENTAL"""
    detailTask = waitForNewTask('ivm_offset_expired_dup_detail_mv', detailTask.TaskId.toString())
    qt_detail_strict_task taskQuery(detailTask.TaskId.toString())
    order_qt_detail_mv_after_strict """SELECT id, grp, v1 FROM ivm_offset_expired_dup_detail_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_dup_detail_mv AUTO"""
    detailTask = waitForNewTask('ivm_offset_expired_dup_detail_mv', detailTask.TaskId.toString())
    qt_detail_fallback_task taskQuery(detailTask.TaskId.toString())
    order_qt_detail_mv_after_fallback """SELECT id, grp, v1 FROM ivm_offset_expired_dup_detail_mv"""

    // With the TTL back to a day, the offsets the rebuilds left are well inside the retention again.
    sql """ALTER TABLE ivm_offset_expired_dup_t SET ("changelog.ttl_seconds" = "86400")"""
    sql """INSERT INTO ivm_offset_expired_dup_t VALUES (4, 2, 40)"""

    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_dup_agg_mv AUTO"""
    aggTask = waitForNewTask('ivm_offset_expired_dup_agg_mv', aggTask.TaskId.toString())
    qt_agg_task_after_fallback taskQuery(aggTask.TaskId.toString())
    order_qt_agg_mv_after_a_further_refresh """SELECT grp, cnt, total FROM ivm_offset_expired_dup_agg_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_offset_expired_dup_detail_mv AUTO"""
    detailTask = waitForNewTask('ivm_offset_expired_dup_detail_mv', detailTask.TaskId.toString())
    qt_detail_task_after_fallback taskQuery(detailTask.TaskId.toString())
    order_qt_detail_mv_after_a_further_refresh """SELECT id, grp, v1 FROM ivm_offset_expired_dup_detail_mv"""

    // What the base table answers for the two MV queries.
    order_qt_base_agg """SELECT grp, COUNT(*), SUM(v1) FROM ivm_offset_expired_dup_t GROUP BY grp"""
    order_qt_base_detail """SELECT id, grp, v1 FROM ivm_offset_expired_dup_t"""
}
