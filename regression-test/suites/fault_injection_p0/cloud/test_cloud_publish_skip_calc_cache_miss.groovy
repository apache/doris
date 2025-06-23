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

suite("test_cloud_publish_skip_calc_cache_miss", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def customFeConfig = [
        delete_bitmap_lock_expiration_seconds : 10,
        calculate_delete_bitmap_task_timeout_seconds : 5,
    ]

    setFeConfigTemporary(customFeConfig) {

        def table1 = "test_cloud_publish_skip_calc_cache_miss"
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

            GetDebugPoint().enableDebugPointForAllBEs("CloudTxnDeleteBitmapCache::get_delete_bitmap.cache_miss")
            // inject failure after saving its result in BE's delete bitmap cache successfully, so that later retry will
            // try to skip to calculate
            GetDebugPoint().enableDebugPointForAllBEs("CloudTablet::save_delete_bitmap.injected_error", [retry: true, sleep: 5])

            def t1 = Thread.start {
                sql "insert into ${table1} values(1,999,999);"
            }

            Thread.sleep(3000)

            GetDebugPoint().disableDebugPointForAllBEs("CloudTablet::save_delete_bitmap.injected_error")

            t1.join()

            qt_dup_key_count "select count() from (select k1,count() as cnt from ${table1} group by k1 having cnt > 1) A;"
            qt_sql "select * from ${table1} order by k1,c1,c2;"
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
            GetDebugPoint().clearDebugPointsForAllFEs()
        }
    }
}
