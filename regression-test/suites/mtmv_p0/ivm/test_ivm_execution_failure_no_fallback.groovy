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

suite("test_ivm_execution_failure_no_fallback", "nonConcurrent") {
    def commitFailureDebugPoint = "DatabaseTransactionMgr.commitTransaction.failed"

    GetDebugPoint().disableDebugPointForAllFEs(commitFailureDebugPoint)
    sql """drop materialized view if exists ivm_exec_fail_mv"""
    sql """drop table if exists ivm_exec_fail_t"""

    sql """
        CREATE TABLE ivm_exec_fail_t (
            id BIGINT NOT NULL,
            v1 BIGINT
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
    sql """INSERT INTO ivm_exec_fail_t VALUES (1, 10)"""

    sql """
        CREATE MATERIALIZED VIEW ivm_exec_fail_mv
        BUILD DEFERRED REFRESH INCREMENTAL FALLBACK ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS SELECT id, v1 FROM ivm_exec_fail_t
    """

    def latestTask = {
        def taskResult
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            taskResult = sql_return_maparray("""
                SELECT Status, RefreshMode, IvmFallbackReason, ErrorMsg
                FROM tasks('type'='mv')
                WHERE MvDatabaseName = '${context.dbName}'
                  AND MvName = 'ivm_exec_fail_mv'
                ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
            """)
            return !taskResult.isEmpty()
                    && taskResult[0].Status.toString() != 'PENDING'
                    && taskResult[0].Status.toString() != 'RUNNING'
        })
        return taskResult[0]
    }

    sql """REFRESH MATERIALIZED VIEW ivm_exec_fail_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_exec_fail_mv")
    order_qt_execution_failure_initial """
        SELECT id, v1 FROM ivm_exec_fail_mv ORDER BY id
    """

    sql """INSERT INTO ivm_exec_fail_t VALUES (2, 20)"""
    Thread.sleep(1000)

    def failedTask
    try {
        GetDebugPoint().enableDebugPointForAllFEs(commitFailureDebugPoint)
        sql """REFRESH MATERIALIZED VIEW ivm_exec_fail_mv INCREMENTAL FALLBACK"""
        failedTask = latestTask()
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(commitFailureDebugPoint)
    }

    assertEquals("FAILED", failedTask.Status.toString())
    assertTrue(failedTask.RefreshMode == null || failedTask.RefreshMode.toString() == "\\N")
    assertTrue(failedTask.IvmFallbackReason == null || failedTask.IvmFallbackReason.toString() == "\\N")
    assertTrue(failedTask.ErrorMsg.toString().contains(commitFailureDebugPoint))
    order_qt_execution_failure_mv_unchanged """
        SELECT id, v1 FROM ivm_exec_fail_mv ORDER BY id
    """
}
