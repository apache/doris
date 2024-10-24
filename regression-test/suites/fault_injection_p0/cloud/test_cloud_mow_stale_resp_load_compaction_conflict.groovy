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

import org.junit.Assert
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_cloud_mow_stale_resp_load_compaction_conflict", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def customFeConfig = [
        delete_bitmap_lock_expiration_seconds : 10,
        calculate_delete_bitmap_task_timeout_seconds : 15,
    ]

    setFeConfigTemporary(customFeConfig) {

        def table1 = "test_cloud_mow_stale_resp_load_compaction_conflict"
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


        def beNodes = sql_return_maparray("show backends;")
        def tabletStat = sql_return_maparray("show tablets from ${table1};").get(0)
        def tabletBackendId = tabletStat.BackendId
        def tabletId = tabletStat.TabletId
        def tabletBackend;
        for (def be : beNodes) {
            if (be.BackendId == tabletBackendId) {
                tabletBackend = be
                break;
            }
        }
        logger.info("tablet ${tabletId} on backend ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}");


        try {
            GetDebugPoint().clearDebugPointsForAllFEs()
            GetDebugPoint().clearDebugPointsForAllBEs()

            // block the first load
            GetDebugPoint().enableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.enable_spin_wait", [token: "token1"])
            GetDebugPoint().enableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.block", [wait_token: "token1"])

            // the first load
            t1 = Thread.start {
                sql "insert into ${table1} values(1,999,999),(2,888,888);"
            }

            // wait util the first load's delete bitmap update lock expired
            // to ensure that the second load can take the delete bitmap update lock
            // Config.delete_bitmap_lock_expiration_seconds = 10s
            Thread.sleep(11 * 1000)

            // trigger full compaction on tablet
            logger.info("trigger compaction on another BE ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}")
            def (code, out, err) = be_run_full_compaction(tabletBackend.Host, tabletBackend.HttpPort, tabletId)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            Assert.assertEquals(code, 0)
            def compactJson = parseJson(out.trim())
            Assert.assertEquals("success", compactJson.status.toLowerCase())

            // wait for full compaction to complete
            Awaitility.await().atMost(3, TimeUnit.SECONDS).pollDelay(200, TimeUnit.MILLISECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(
                {
                    (code, out, err) = be_get_compaction_status(tabletBackend.Host, tabletBackend.HttpPort, tabletId)
                    logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                    Assert.assertEquals(code, 0)
                    def compactionStatus = parseJson(out.trim())
                    Assert.assertEquals("success", compactionStatus.status.toLowerCase())
                    return !compactionStatus.run_status
                }
            )
            order_qt_sql "select * from ${table1};"


            // keep waiting util the delete bitmap calculation timeout(Config.calculate_delete_bitmap_task_timeout_seconds = 15s)
            // and the coordinator BE will retry to commit the first load's txn
            Thread.sleep(15 * 1000)

            // let the first partial update load finish
            GetDebugPoint().enableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.block")
            t1.join()

            Thread.sleep(1000)

            order_qt_sql "select * from ${table1};"
            
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }

        sql "DROP TABLE IF EXISTS ${table1};"
    }
}
