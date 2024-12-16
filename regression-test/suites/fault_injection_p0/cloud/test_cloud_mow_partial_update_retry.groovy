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

suite("test_cloud_mow_partial_update_retry", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

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
            "enable_mow_light_delete" = "false",
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """

    sql "insert into ${table1} values(1,1,1,1);"
    sql "insert into ${table1} values(2,2,2,2);"
    sql "insert into ${table1} values(3,3,3,3);"
    sql "sync;"
    // order_qt_sql "select * from ${table1};"

    def tabletStats = sql_return_maparray("show tablets from ${table1};")
    assertTrue(tabletStats.size() == 1)
    def tabletId = tabletStats[0].TabletId

    def enable_publish_spin_wait = { tokenName -> 
        GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait", [token: "${tokenName}"])
    }

    def disable_publish_spin_wait = {
        GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
    }

    def enable_block_in_publish = { passToken -> 
        GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block", [pass_token: "${passToken}"])
    }

    def disable_block_in_publish = {
        GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
    }


    def customFeConfig = [
        delete_bitmap_lock_expiration_seconds : 200,
        calculate_delete_bitmap_task_timeout_seconds : 20,
        mow_calculate_delete_bitmap_retry_times : 5
    ]

    setFeConfigTemporary(customFeConfig) {
        try {
            // block the partial update in publish phase
            enable_publish_spin_wait("token1")
            enable_block_in_publish("-1")
            Thread.sleep(500)

            // the first partial update load
            def t1 = Thread.start {
                sql "set enable_unique_key_partial_update=true;"
                sql "set enable_insert_strict=false"
                sql "sync;"
                sql "insert into ${table1}(k1,c1) values(1,999);"
            }
            Thread.sleep(700)

            // the second partial update load that conflicts with the first one
            enable_publish_spin_wait("token2")
            Thread.sleep(500)
            def t2 = Thread.start {
                sql "set enable_unique_key_partial_update=true;"
                sql "set enable_insert_strict=false"
                sql "sync;"
                sql "insert into ${table1}(k1,c2) values(1,888);"
            }
            Thread.sleep(500)

            // let the first partial update load finish
            enable_block_in_publish("token1")
            t1.join()


            GetDebugPoint().enableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask._handle_rowset.inject_err", [tablet_id: "${tabletId}"]);

            // block partial update load 2 in publish phase so that it will time out 
            GetDebugPoint().enableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.enable_spin_wait", [token: "token3"])
            GetDebugPoint().enableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.block", [wait_token: "token3"])

            Thread.sleep(2000)
            logger.info("begin to unblock partial update load 2")
            // unblock partial update load 2
            disable_block_in_publish()
            
            Thread.sleep(3000)

            GetDebugPoint().disableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.enable_spin_wait")
            GetDebugPoint().disableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.block")

            t2.join()

            // qt_sql "select * from ${table1} order by k1;"
            
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
