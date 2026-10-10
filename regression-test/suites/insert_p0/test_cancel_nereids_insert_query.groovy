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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

suite("test_cancel_nereids_insert_query", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_cancel_nereids_insert_query"
    sql "DROP TABLE IF EXISTS test_cancel_nereids_ctas"
    sql """
        CREATE TABLE test_cancel_nereids_insert_query (
            k BIGINT
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    def waitQueryId = { String connectionId, String statementPrefix, String marker ->
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10)
        while (System.currentTimeMillis() < deadline) {
            def process = sql_return_maparray("SHOW FULL PROCESSLIST").find {
                it.Id.toString() == connectionId
                        && it.Info != null
                        && it.Info.toString().toUpperCase().contains(statementPrefix)
                        && it.Info.toString().contains(marker)
            }
            if (process != null) {
                return process.QueryId.toString()
            }
            sleep(50)
        }
        throw new IllegalStateException("Query did not appear in processlist: ${marker}")
    }

    def runAndCancel = { String debugPoint, String labelPrefix ->
        String label = "${labelPrefix}_${System.nanoTime()}"
        GetDebugPoint().enableDebugPointForAllFEs(debugPoint, [value: 3000])
        def insertError = new AtomicReference<Throwable>()
        def insertConnectionId = new AtomicReference<String>()
        def connectionReady = new CountDownLatch(1)
        def insertFuture = thread {
            try {
                insertConnectionId.set("${sql('SELECT CONNECTION_ID()')[0][0]}")
                connectionReady.countDown()
                sql """
                    INSERT INTO test_cancel_nereids_insert_query WITH LABEL ${label}
                    SELECT number FROM numbers("number" = "1")
                """
            } catch (Throwable t) {
                insertError.set(t)
            } finally {
                connectionReady.countDown()
            }
        }

        try {
            assertTrue(connectionReady.await(10, TimeUnit.SECONDS), "INSERT connection was not ready")
            String queryId = waitQueryId.call(insertConnectionId.get(), "INSERT", label)
            sql "KILL QUERY \"${queryId}\""
            insertFuture.get(15, TimeUnit.SECONDS)

            assertNotNull(insertError.get(), "The cancelled INSERT unexpectedly succeeded")
            assertTrue(insertError.get().getMessage().contains("cancel query by user"))
            assertEquals(0L, sql("SELECT COUNT(*) FROM test_cancel_nereids_insert_query")[0][0].toLong())

            def transaction = sql_return_maparray """
                    SHOW TRANSACTION FROM ${context.dbName} WHERE LABEL = '${label}'
                """
            assertEquals(1, transaction.size())
            assertEquals("ABORTED", transaction[0].TransactionStatus.toString())
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs(debugPoint)
        }
    }

    runAndCancel.call("InsertIntoTableCommand.beforeSetCoordinator.sleep",
            "test_cancel_before_coordinator")
    runAndCancel.call("AbstractInsertExecutor.beforeOnComplete.sleep",
            "test_cancel_before_commit")

    GetDebugPoint().enableDebugPointForAllFEs(
            "InsertIntoTableCommand.beforeSetCoordinator.sleep", [value: 3000])
    def ctasError = new AtomicReference<Throwable>()
    def ctasConnectionId = new AtomicReference<String>()
    def ctasConnectionReady = new CountDownLatch(1)
    def ctasFuture = thread {
        try {
            ctasConnectionId.set("${sql('SELECT CONNECTION_ID()')[0][0]}")
            ctasConnectionReady.countDown()
            sql """
                CREATE TABLE test_cancel_nereids_ctas
                PROPERTIES ("replication_num" = "1")
                AS SELECT number AS k FROM numbers("number" = "1")
            """
        } catch (Throwable t) {
            ctasError.set(t)
        } finally {
            ctasConnectionReady.countDown()
        }
    }

    try {
        assertTrue(ctasConnectionReady.await(10, TimeUnit.SECONDS), "CTAS connection was not ready")
        String queryId = waitQueryId.call(
                ctasConnectionId.get(), "CREATE TABLE", "test_cancel_nereids_ctas")
        sql "KILL QUERY \"${queryId}\""
        ctasFuture.get(15, TimeUnit.SECONDS)

        assertNotNull(ctasError.get(), "The cancelled CTAS unexpectedly succeeded")
        assertTrue(ctasError.get().getMessage().contains("cancel query by user"))
        assertEquals(0, sql("SHOW TABLES LIKE 'test_cancel_nereids_ctas'").size())
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(
                "InsertIntoTableCommand.beforeSetCoordinator.sleep")
    }
}
