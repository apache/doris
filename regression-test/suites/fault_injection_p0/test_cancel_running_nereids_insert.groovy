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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

suite("test_cancel_running_nereids_insert", "nonConcurrent") {
    def tableName = "test_cancel_running_nereids_insert_tbl"
    def label = "test_cancel_running_nereids_insert_label"
    def debugPoint = "AbstractInsertExecutor.execImpl.blockBeforeCoordinatorExec"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id BIGINT,
            value BIGINT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    GetDebugPoint().enableDebugPointForAllFEs(debugPoint)
    def insertError = new AtomicReference<String>()
    def insertFuture = thread {
        try {
            sql """
                INSERT INTO ${tableName} WITH LABEL ${label}
                SELECT 1, number FROM numbers("number" = "1")
            """
        } catch (Throwable t) {
            insertError.set(t.getMessage())
        }
    }

    try {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).until(
            {
                def loadJobs = sql "SHOW LOAD WHERE LABEL = '${label}'"
                return loadJobs.size() == 1 && loadJobs[0][2] == "PENDING"
                        && Long.parseLong(loadJobs[0][15].toString()) > 0
            }
        )

        sql "CANCEL LOAD WHERE LABEL = '${label}'"
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(debugPoint)
    }

    insertFuture.get()
    assertNotNull(insertError.get())
    assertTrue(insertError.get().contains("cancel"))

    def loadJobs = sql "SHOW LOAD WHERE LABEL = '${label}'"
    assertEquals(1, loadJobs.size())
    assertEquals("CANCELLED", loadJobs[0][2])
    assertTrue(Long.parseLong(loadJobs[0][15].toString()) > 0)
    order_qt_result "SELECT * FROM ${tableName}"
}
