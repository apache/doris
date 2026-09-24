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

import java.util.concurrent.atomic.AtomicReference

import org.apache.doris.regression.util.Http

suite("test_cloud_mow_stale_resp_load_load_conflict", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    // A previous fault-injection suite may have failed during cleanup. Clear points before
    // table creation as well, because stale load hooks can affect the setup inserts below.
    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def customFeConfig = [
        delete_bitmap_lock_expiration_seconds : 10,
        calculate_delete_bitmap_task_timeout_seconds : 15,
    ]

    setFeConfigTemporary(customFeConfig) {

        def table1 = "test_cloud_mow_stale_resp_load_load_conflict"
        sql "DROP TABLE IF EXISTS ${table1} FORCE;"
        sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                `k1` int NOT NULL,
                `c1` int,
                `c2` int
                )UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "replication_num" = "1"); """

        sql "insert into ${table1} values(1,1,1);"
        sql "insert into ${table1} values(2,2,2);"
        sql "insert into ${table1} values(3,3,3);"
        sql "sync;"
        order_qt_sql "select * from ${table1};"

        Thread firstLoadThread = null
        def firstLoadError = new AtomicReference<Throwable>()
        try {
            GetDebugPoint().clearDebugPointsForAllFEs()
            GetDebugPoint().clearDebugPointsForAllBEs()

            def tablets = sql_return_maparray("show tablets from ${table1};")
            assertEquals(1, tablets.size())
            def tabletId = tablets[0].TabletId
            def backends = sql_return_maparray("show backends;").findAll {
                it.Alive.toString().equalsIgnoreCase("true")
            }
            assertFalse(backends.isEmpty(), "no alive backend")
            def getActiveCalcTasks = {
                long activeTasks = 0
                backends.each { be ->
                    def conn = Http.openConnection("http://${be.Host}:${be.BrpcPort}/brpc_metrics")
                    conn.connectTimeout = 5000
                    conn.readTimeout = 5000
                    def metrics = conn.inputStream.getText('UTF-8')
                    def matcher = metrics =~ /(?m)^task_calculate_delete_bitmap\s+(\d+)$/
                    assert matcher.find() : "task_calculate_delete_bitmap not found on ${be.Host}:${be.BrpcPort}"
                    activeTasks += matcher.group(1).toLong()
                }
                return activeTasks
            }
            long activeCalcTasks = getActiveCalcTasks()
            def firstLoadLabel = "stale_resp_first_${UUID.randomUUID().toString().replaceAll('-', '')}"

            // Block after calculation releases the BE tablet's rowset-update lock. Blocking
            // inside update_delete_bitmap would prevent the second load from calculating at all.
            GetDebugPoint().enableDebugPointForAllBEs(
                    "CloudTabletCalcDeleteBitmapTask.handle.block_after_calc",
                    [tablet_id: "${tabletId}", timeout: "90"])
            firstLoadThread = Thread.start {
                try {
                    sql "insert into ${table1} with label `${firstLoadLabel}` values(1,999,999),(2,888,888);"
                } catch (Throwable t) {
                    firstLoadError.set(t)
                }
            }

            // Queue the second load only after the first has started calculation under the FE
            // commit lock. Its 15-second wait exceeds the 10-second MS lock expiration.
            awaitUntil(30, 0.1) {
                if (firstLoadError.get() != null) {
                    throw firstLoadError.get()
                }
                getActiveCalcTasks() > activeCalcTasks
            }
            def firstLoadTxns = sql_return_maparray("show transaction where label = '${firstLoadLabel}';")
            assertEquals(1, firstLoadTxns.size())
            def firstLoadTxnId = firstLoadTxns[0].TransactionId
            // Keep the original task blocked, and block its retries as well, while letting
            // other transactions calculate and respond on whichever BE serves the tablet.
            GetDebugPoint().enableDebugPointForAllBEs(
                    "CloudTabletCalcDeleteBitmapTask.handle.block_after_calc",
                    [tablet_id: "${tabletId}", transaction_id: "${firstLoadTxnId}", timeout: "90"])

            sql "insert into ${table1}(k1,c1,c2) values(1,666,666),(2,555,555);"

            order_qt_sql "select * from ${table1};"


            // Keep both the stale response and a retry in flight before releasing the hook.
            // This suite is nonConcurrent; the blocked first task remains in the active count.
            awaitUntil(30, 0.1) {
                if (firstLoadError.get() != null) {
                    throw firstLoadError.get()
                }
                getActiveCalcTasks() >= activeCalcTasks + 2
            }
            assertTrue(firstLoadThread.isAlive(), "first load must remain blocked before releasing responses")
            GetDebugPoint().disableDebugPointForAllBEs(
                    "CloudTabletCalcDeleteBitmapTask.handle.block_after_calc")
            firstLoadThread.join(60000)
            assertFalse(firstLoadThread.isAlive(), "the first load did not finish after releasing the delayed responses")
            if (firstLoadError.get() != null) {
                throw firstLoadError.get()
            }

            Thread.sleep(1000)

            order_qt_sql "select * from ${table1};"
            
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
            if (firstLoadThread != null && firstLoadThread.isAlive()) {
                firstLoadThread.join(60000)
                assertFalse(firstLoadThread.isAlive(), "first load did not exit after clearing debug points")
            }
        }

        sql "DROP TABLE IF EXISTS ${table1};"
    }
}
