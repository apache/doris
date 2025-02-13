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

suite("test_cloud_mow_lock_expired", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def table1 = "test_cloud_mow_lock_expired"
    sql "DROP TABLE IF EXISTS ${table1} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table1} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_mow_light_delete" = "false",
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """

    sql "insert into ${table1} values(1,1,1);"
    sql "insert into ${table1} values(2,2,2);"
    sql "insert into ${table1} values(3,3,3);"
    sql "sync;"
    qt_sql "select * from ${table1} order by k1;"

    def customFeConfig = [
        delete_bitmap_lock_expiration_seconds : 5,
        calculate_delete_bitmap_task_timeout_seconds : 3,
        mow_calculate_delete_bitmap_retry_times : 3
    ]

    setFeConfigTemporary(customFeConfig) {
        try {
            // simulate that the commit rpc to MS takes a lot time due to networks
            // and by the time it arrives, the delete bitmap lock has expired
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.executeCommitTxnRequest.block")

            Thread.sleep(1000)
            def t1 = Thread.start {
                sql "insert into ${table1} values(1,99,99),(4,4,4);"
            }

            Thread.sleep(5000) // wait util delete bitmap lock expired
            trigger_and_wait_compaction(table1, "full")
            qt_sql "select * from ${table1} order by k1;"

            Thread.sleep(1000)
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.executeCommitTxnRequest.block")

            t1.join() // retry to success
            qt_sql "select * from ${table1} order by k1;"
            
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllFEs()
        }
    }


    customFeConfig = [
        delete_bitmap_lock_expiration_seconds : 5,
        calculate_delete_bitmap_task_timeout_seconds : 3,
        mow_calculate_delete_bitmap_retry_times : 1 // no retry
    ]

    sql "insert into ${table1} values(10,10,10);"
    sql "insert into ${table1} values(20,20,20);"
    sql "insert into ${table1} values(30,30,30);"
    sql "sync;"
    qt_sql "select * from ${table1} order by k1;"

    setFeConfigTemporary(customFeConfig) {
        try {
            // simulate that the commit rpc to MS takes a lot time due to networks
            // and by the time it arrives, the delete bitmap lock has expired
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.executeCommitTxnRequest.block")

            Thread.sleep(1000)
            def t1 = Thread.start {
                // no retry, it will fail due to lock expire
                test {
                    sql "insert into ${table1} values(10,99,99),(40,40,40);"
                    exception "delete bitmap update lock expired"
                }
            }

            Thread.sleep(5000) // wait util delete bitmap lock expired
            trigger_and_wait_compaction(table1, "full")
            qt_sql "select * from ${table1} order by k1;"

            Thread.sleep(1000)
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.executeCommitTxnRequest.block")

            t1.join()
            qt_sql "select * from ${table1} order by k1;"
            
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllFEs()
        }
    }
}
