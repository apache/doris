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

suite("test_cloud_pending_delete_bitmaps_removed_by_other_txn", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def customFeConfig = [
        delete_bitmap_lock_expiration_seconds : 5,
        calculate_delete_bitmap_task_timeout_seconds : 20,
    ]

    setFeConfigTemporary(customFeConfig) {

        def table1 = "test_cloud_pending_delete_bitmaps_removed_by_other_txn"
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

        try {
            GetDebugPoint().clearDebugPointsForAllFEs()
            GetDebugPoint().clearDebugPointsForAllBEs()

            // let the first load fail and retry after it writes pending delete bitmaps in MS and before
            // it commit txn in MS
            GetDebugPoint().enableDebugPointForAllBEs("CloudTablet::save_delete_bitmap.enable_sleep", [sleep: 5])
            GetDebugPoint().enableDebugPointForAllBEs("CloudTablet::save_delete_bitmap.injected_error", [retry: true])

            // the first load
            def t1 = Thread.start {
                sql "insert into ${table1} values(1,999,999);"
            }

            Thread.sleep(1000)

            def t2 = Thread.start {
                try {
                    // this should fail
                    sql "insert into ${table1} values(2,888,888);"
                } catch(Exception e) {
                    logger.info(e.getMessage())
                }
            }

            // let the second load fail after it remove the pending delete bitmaps in MS written by load 1
            Thread.sleep(5000)
            GetDebugPoint().enableDebugPointForAllBEs("CloudTablet::save_delete_bitmap.injected_error", [retry: false])


            Thread.sleep(5000)
            GetDebugPoint().disableDebugPointForAllBEs("CloudTablet::save_delete_bitmap.injected_error")

            t1.join()
            t2.join()

            Thread.sleep(300)
            // force it read delete bitmaps from MS rather than BE's cache
            GetDebugPoint().enableDebugPointForAllBEs("CloudTxnDeleteBitmapCache::get_delete_bitmap.cache_miss")
            qt_sql "select * from ${table1} order by k1,c1,c2;"
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
