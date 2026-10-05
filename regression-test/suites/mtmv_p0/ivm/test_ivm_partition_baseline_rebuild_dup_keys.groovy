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
 * Same baseline-rebuild pre-step as test_ivm_partition_drop_live_delta, but on the branch where the
 * affected MV partition SURVIVES: TRUNCATE keeps the partition range, so partition sync leaves the
 * MV partition in place and the pre-step really has something to rebuild.
 *
 * <p>Two things are pinned here. The refreshed partition is picked up again by the IVM attempt that
 * follows, which may only apply the remaining delta -- on a duplicate-key MV a double apply shows up
 * as extra copies of the same row, not as a wrong value. And the row written to the surviving
 * partition after the truncate must still be consumed. Both are checked by comparing whole result
 * sets, so row multiplicities are part of the expectation.
 */
suite("test_ivm_partition_baseline_rebuild_dup_keys") {
    def tableName = "ivm_part_dup_t"
    def mvName = "ivm_part_dup_mv"

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
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
        CREATE TABLE ${tableName} (
            dt DATE NOT NULL,
            id INT NOT NULL,
            v INT
        )
        DUPLICATE KEY(dt, id)
        PARTITION BY RANGE(dt) ()
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql """ALTER TABLE ${tableName} ADD PARTITION p202601 VALUES [('2026-01-01'), ('2026-02-01'))"""
    sql """ALTER TABLE ${tableName} ADD PARTITION p202602 VALUES [('2026-02-01'), ('2026-03-01'))"""
    // Repeated identical rows: a double-applied delta grows the multiplicity instead of hiding in a
    // unique key.
    sql """INSERT INTO ${tableName} VALUES
            ('2026-01-10', 1, 10), ('2026-01-10', 1, 10),
            ('2026-02-10', 3, 30), ('2026-02-10', 3, 30)"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT dt, id, v FROM ${tableName}
    """

    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    def taskId = waitForNewTask(null)
    qt_baseline_task taskQuery(taskId)
    order_qt_baseline_base """SELECT dt, id, v FROM ${tableName} ORDER BY dt, id, v"""
    order_qt_baseline_mv """SELECT dt, id, v FROM ${mvName} ORDER BY dt, id, v"""

    // TRUNCATE replaces the partition, so the MV partition of that range stays alive and the
    // baseline pre-step has a real partition to rebuild.
    sql """TRUNCATE TABLE ${tableName} PARTITION(p202601)"""
    sql """INSERT INTO ${tableName} VALUES ('2026-02-15', 4, 40)"""

    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL"""
    taskId = waitForNewTask(taskId)
    qt_strict_task taskQuery(taskId)

    sql """REFRESH MATERIALIZED VIEW ${mvName} INCREMENTAL FALLBACK"""
    taskId = waitForNewTask(taskId)
    qt_fallback_task taskQuery(taskId)
    order_qt_fallback_base """SELECT dt, id, v FROM ${tableName} ORDER BY dt, id, v"""
    order_qt_fallback_mv """SELECT dt, id, v FROM ${mvName} ORDER BY dt, id, v"""
}
