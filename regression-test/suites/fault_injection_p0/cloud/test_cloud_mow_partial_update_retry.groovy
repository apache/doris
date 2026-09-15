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

suite("test_cloud_mow_partial_update_retry", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def customFeConfig = [
        delete_bitmap_lock_expiration_seconds : 10,
        calculate_delete_bitmap_task_timeout_seconds : 30,
    ]

    setFeConfigTemporary(customFeConfig) {

        def table1 = "test_cloud_mow_partial_update_retry"
        sql "DROP TABLE IF EXISTS ${table1} FORCE;"
        sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                `k1` int NOT NULL,
                `c1` int,
                `c2` int,
                `c3` int
                )UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "replication_num" = "1"); """

        sql "insert into ${table1} values(1,1,1,1);"
        sql "insert into ${table1} values(2,2,2,2);"
        sql "insert into ${table1} values(3,3,3,2);"
        sql "sync;"
        qt_sql "select * from ${table1} order by k1;"

        def t1 = null
        try {
            def tablets = sql_return_maparray("show tablets from ${table1};")
            assert tablets.size() == 1
            def tabletId = tablets[0].TabletId
            def backends = (sql "show backends;").findAll {
                it[9].toString().equalsIgnoreCase("true")
            }.collect { be ->
                [ip: be[1], httpPort: be[4], brpcPort: be[5]]
            }
            assert !backends.isEmpty() : "no alive backend"
            def getActiveCalcTasks = {
                long activeTasks = 0
                backends.each { be ->
                    def metrics = new URL("http://${be.ip}:${be.brpcPort}/brpc_metrics").text
                    def matcher = metrics =~ /(?m)^task_calculate_delete_bitmap\s+(\d+)$/
                    assert matcher.find() : "task_calculate_delete_bitmap not found on ${be.ip}:${be.brpcPort}"
                    activeTasks += matcher.group(1).toLong()
                }
                return activeTasks
            }
            def activeCalcTasks = getActiveCalcTasks()
            def firstLoadLabel = "core_6136_first_${UUID.randomUUID().toString().replaceAll('-', '')}"

            // Initially block the only load on this tablet. After its transaction id is visible,
            // narrow the debug point to that transaction so the interleaved load can finish on any
            // BE while both the stale first response and retry responses remain blocked.
            GetDebugPoint().enableDebugPointForAllBEs(
                    "CloudTabletCalcDeleteBitmapTask.handle.block_after_calc",
                    [tablet_id: "${tabletId}", timeout: "90"])

            // the first load
            def firstLoadException = new AtomicReference<Throwable>()
            t1 = Thread.start {
                try {
                    sql "set enable_unique_key_partial_update=true;"
                    sql "sync;"
                    sql "insert into ${table1} with label `${firstLoadLabel}` (k1,c1) values(1,999),(2,666);"
                } catch (Throwable t) {
                    firstLoadException.set(t)
                }
            }

            // An active calc task proves that the first load has acquired both the fair per-table
            // FE commit lock and the MS delete bitmap lock. Starting the second load only after
            // this handshake makes it queue ahead of the first load's retry. The first FE wait is
            // longer than the MS lock expiration, so that lock has expired when the queue advances.
            awaitUntil(30, 0.1) {
                getActiveCalcTasks() > activeCalcTasks
            }
            def firstLoadTxns = sql_return_maparray(
                    "show transaction where label = '${firstLoadLabel}';")
            assert firstLoadTxns.size() == 1
            def firstLoadTxnId = firstLoadTxns[0].TransactionId
            GetDebugPoint().enableDebugPointForAllBEs(
                    "CloudTabletCalcDeleteBitmapTask.handle.block_after_calc",
                    [tablet_id: "${tabletId}", transaction_id: "${firstLoadTxnId}", timeout: "90"])

            sql "set enable_unique_key_partial_update=true;"
            sql "sync;"
            sql "insert into ${table1}(k1,c2) values(1,888),(2,777);"

            qt_sql "select * from ${table1} order by k1;"

            // The intermediate result has been checked while the first load's retry response is
            // still blocked. Now let both the retry and the stale first response return.
            GetDebugPoint().disableDebugPointForAllBEs(
                    "CloudTabletCalcDeleteBitmapTask.handle.block_after_calc")
            t1.join(60000)
            assert !t1.isAlive() : "the first partial update did not finish"
            if (firstLoadException.get() != null) {
                throw firstLoadException.get()
            }

            Thread.sleep(1000)

            qt_sql "select * from ${table1} order by k1;"
            
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
            if (t1 != null && t1.isAlive()) {
                t1.join(60000)
            }
        }
    }
}
