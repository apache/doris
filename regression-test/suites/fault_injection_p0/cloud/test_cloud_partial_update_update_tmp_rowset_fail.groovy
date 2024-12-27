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

suite("test_cloud_partial_update_update_tmp_rowset_fail", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()


    def table1 = "test_partial_update_update_tmp_rowset_fail"
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
        // let two partial update load have conflicts
        GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
        GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")

        // this will case the older partial update fail in publish phase after it write a new segment due to conflict
        // and before it updates the num_segments field of tmp rowset's meta 
        GetDebugPoint().enableDebugPointForAllBEs("CloudTablet::save_delete_bitmap.update_tmp_rowset.error")

        def t1 = Thread.start {
            try {
                sql "set enable_unique_key_partial_update=true;"
                sql "sync;"
                sql "insert into ${table1}(k1,c1) values(1,999),(2,666);"
            } catch(Exception e) {
                logger.info(e.getMessage())
            }
        }
        Thread.sleep(800)

        def t2 = Thread.start {
            try {
                sql "set enable_unique_key_partial_update=true;"
                sql "sync;"
                sql "insert into ${table1}(k1,c2) values(1,888),(2,777);"
            } catch(Exception e) {
                logger.info(e.getMessage())
            }
        }
        Thread.sleep(800)


        GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
        GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")


        t1.join()
        t2.join()

        qt_sql "select * from ${table1} order by k1;"        
    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
