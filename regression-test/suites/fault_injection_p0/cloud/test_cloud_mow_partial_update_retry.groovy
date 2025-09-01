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

suite("test_cloud_mow_partial_update_retry", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def customFeConfig = [
        delete_bitmap_lock_expiration_seconds : 10,
        calculate_delete_bitmap_task_timeout_seconds : 15,
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

        try {
            // block the first load
            GetDebugPoint().enableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.enable_spin_wait", [token: "token1"])
            GetDebugPoint().enableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.block", [wait_token: "token1"])

            // the first load
            def t1 = Thread.start {
                sql "set enable_unique_key_partial_update=true;"
                sql "sync;"
                sql "insert into ${table1}(k1,c1) values(1,999),(2,666);"
            }

            // wait util the first partial update load's delete bitmap update lock expired
            // to ensure that the second load can take the delete bitmap update lock
            // Config.delete_bitmap_lock_expiration_seconds = 10s
            def timeout = getFeConfig("delete_bitmap_lock_expiration_seconds").toInteger() + 2;
            Thread.sleep(timeout * 1000)

            // the second load
            GetDebugPoint().enableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.enable_spin_wait", [token: "token2"])
            Thread.sleep(200)

            sql "set enable_unique_key_partial_update=true;"
            sql "sync;"
            sql "insert into ${table1}(k1,c2) values(1,888),(2,777);"

            qt_sql "select * from ${table1} order by k1;"


            // keep waiting util the delete bitmap calculation timeout(Config.calculate_delete_bitmap_task_timeout_seconds = 15s)
            // and the first load will retry the calculation of delete bitmap
            timeout = getFeConfig("calculate_delete_bitmap_task_timeout_seconds").toInteger() + 2;
            Thread.sleep(timeout * 1000)

            // let the first partial update load finish
            GetDebugPoint().enableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.block")
            t1.join()

            Thread.sleep(1000)

            qt_sql "select * from ${table1} order by k1;"
            
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
