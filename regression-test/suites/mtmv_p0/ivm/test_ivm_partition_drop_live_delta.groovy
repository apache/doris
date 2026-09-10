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
 * Dropping a base-table partition invalidates the IVM baseline, because the rows disappear through
 * metadata rather than through row binlog entries. The MV partition built from that base partition
 * is then removed by partition sync, which is exactly what the baseline barrier recorded.
 *
 * <p>The refresh must still consume the delta that accumulated on the *surviving* partitions: it
 * may not report SUCCESS while leaving those partitions stale. This case inserts a row into a
 * surviving partition after the drop, so an EMPTY baseline-rebuild intersection cannot be mistaken
 * for "nothing to do".
 *
 * <p>Partitions are managed by hand (no dynamic partition scheduler) and every dt is a literal, so
 * the case is fully deterministic.
 */
suite("test_ivm_partition_drop_live_delta", "nonConcurrent") {
    def tableName = "ivm_part_drop_t"
    def mvName = "ivm_part_drop_mv"

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

    // An unset IvmFallbackReason comes back as the literal two-character string "\N", which does not
    // survive the .out round trip, so fold the unset value into a printable token.
    def taskQuery = { String taskId ->
        """
            SELECT Status,
                   CASE WHEN IvmFallbackReason = 'BINLOG_BROKEN' THEN IvmFallbackReason ELSE 'NONE' END
            FROM tasks('type'='mv')
            WHERE TaskId = '${taskId}'
        """
    }

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
        CREATE TABLE ${tableName} (
            dt DATE NOT NULL,
            id INT NOT NULL,
            v INT
        )
        UNIQUE KEY(dt, id)
        PARTITION BY RANGE(dt) ()
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """ALTER TABLE ${tableName} ADD PARTITION p202601 VALUES [('2026-01-01'), ('2026-02-01'))"""
    sql """ALTER TABLE ${tableName} ADD PARTITION p202602 VALUES [('2026-02-01'), ('2026-03-01'))"""
    sql """ALTER TABLE ${tableName} ADD PARTITION p202603 VALUES [('2026-03-01'), ('2026-04-01'))"""
    sql """ALTER TABLE ${tableName} ADD PARTITION p202604 VALUES [('2026-04-01'), ('2026-05-01'))"""
    sql """INSERT INTO ${tableName} VALUES
            ('2026-01-10', 1, 10), ('2026-02-10', 2, 20), ('2026-03-10', 3, 30)"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        KEY(dt, id)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT dt, id, v FROM ${tableName}
    """

    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    def taskId = waitForNewTask(null)
    qt_baseline_task taskQuery(taskId)
    order_qt_baseline_base """SELECT dt, id, v FROM ${tableName} ORDER BY dt, id"""
    order_qt_baseline_mv """SELECT dt, id, v FROM ${mvName} ORDER BY dt, id"""

    sql """ALTER TABLE ${tableName} DROP PARTITION p202601"""
    sql """INSERT INTO ${tableName} VALUES ('2026-02-15', 4, 40)"""

    // A strict incremental refresh must refuse to run against a broken baseline.
    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    taskId = waitForNewTask(taskId)
    qt_strict_task taskQuery(taskId)

    // The fallback reports SUCCESS, so the MV has to match the base table afterwards: the expired
    // partition is gone AND the row written to the surviving partition has been consumed. An MV
    // that is missing that row means the refresh silently skipped the surviving partitions' delta.
    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL FALLBACK"""
    taskId = waitForNewTask(taskId)
    qt_fallback_task taskQuery(taskId)
    order_qt_fallback_base """SELECT dt, id, v FROM ${tableName} ORDER BY dt, id"""
    order_qt_fallback_mv """SELECT dt, id, v FROM ${mvName} ORDER BY dt, id"""

    // A following strict incremental refresh must be able to continue from the repaired baseline.
    sql """INSERT INTO ${tableName} VALUES ('2026-03-15', 5, 50)"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    taskId = waitForNewTask(taskId)
    qt_resumed_task taskQuery(taskId)
    order_qt_resumed_base """SELECT dt, id, v FROM ${tableName} ORDER BY dt, id"""
    order_qt_resumed_mv """SELECT dt, id, v FROM ${mvName} ORDER BY dt, id"""
}
