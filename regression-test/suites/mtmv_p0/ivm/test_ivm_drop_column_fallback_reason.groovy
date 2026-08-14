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

suite("test_ivm_drop_column_fallback_reason", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_drop_col_reason_mv"""
    sql """DROP TABLE IF EXISTS ivm_drop_col_reason_t"""
    sql """
        CREATE TABLE ivm_drop_col_reason_t (
            id BIGINT NOT NULL,
            value INT NULL,
            spare INT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """INSERT INTO ivm_drop_col_reason_t VALUES (1, 10, 100), (2, 20, 200)"""
    sql """
        CREATE MATERIALIZED VIEW ivm_drop_col_reason_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT id, value FROM ivm_drop_col_reason_t
    """

    def waitForNewTask = { previousTaskId ->
        def taskResult
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT TaskId, Status
                FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}'
                  AND MvName = 'ivm_drop_col_reason_mv'
                ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
            """)
            return !taskResult.isEmpty()
                    && taskResult[0].TaskId.toString() != previousTaskId
                    && taskResult[0].Status.toString() != 'PENDING'
                    && taskResult[0].Status.toString() != 'RUNNING'
        })
        return taskResult[0].TaskId.toString()
    }

    sql """REFRESH MATERIALIZED VIEW ivm_drop_col_reason_mv INCREMENTAL"""
    def taskId = waitForNewTask(null)
    order_qt_initial_rows """
        SELECT id, value FROM ivm_drop_col_reason_mv ORDER BY id
    """

    sql """ALTER TABLE ivm_drop_col_reason_t DROP COLUMN spare"""
    Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
        def alterJobs = sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'ivm_drop_col_reason_t'
            ORDER BY CreateTime DESC LIMIT 1
        """
        return !alterJobs.isEmpty() && alterJobs[0][9].toString() == 'FINISHED'
    })
    sql """INSERT INTO ivm_drop_col_reason_t(id, value) VALUES (3, 30)"""

    sql """REFRESH MATERIALIZED VIEW ivm_drop_col_reason_mv INCREMENTAL"""
    taskId = waitForNewTask(taskId)
    order_qt_after_drop_column_task """
        SELECT Status
        FROM tasks('type'='mv') WHERE TaskId = '${taskId}'
        ORDER BY TaskId
    """
    order_qt_rows_after_drop_column """
        SELECT id, value FROM ivm_drop_col_reason_mv ORDER BY id
    """

    sql """INSERT INTO ivm_drop_col_reason_t(id, value) VALUES (4, 40)"""
    sql """REFRESH MATERIALIZED VIEW ivm_drop_col_reason_mv INCREMENTAL"""
    taskId = waitForNewTask(taskId)
    order_qt_following_incremental_task """
        SELECT Status
        FROM tasks('type'='mv') WHERE TaskId = '${taskId}'
        ORDER BY TaskId
    """
    order_qt_rows_after_following_incremental """
        SELECT id, value FROM ivm_drop_col_reason_mv ORDER BY id
    """
}
