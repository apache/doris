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

suite("test_cloud_mow_txn_insert_publish_conflict", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def table1 = "test_cloud_mow_txn_insert_publish_conflict"
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


    def table2 = "test_cloud_mow_txn_insert_publish_conflict_2"
    sql "DROP TABLE IF EXISTS ${table2} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table2} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """
    sql "insert into ${table2} values(1,888,888),(4,888,888);"


    def table3 = "test_cloud_mow_txn_insert_publish_conflict_3"
    sql "DROP TABLE IF EXISTS ${table3} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table3} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """
    sql "insert into ${table3} values(1,999,999),(2,999,999);"


    sql "insert into ${table1} values(1,1,1);"
    sql "insert into ${table1} values(2,2,2);"
    sql "insert into ${table1} values(3,3,3),(4,4,4);"
    sql "sync;"
    order_qt_sql "select * from ${table1};"

    try {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()

        GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
        GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")

        def t1 = Thread.start {
            sql "insert into ${table1} values(1,777,777),(2,777,777);"
        }
        Thread.sleep(800)

        def t2 = Thread.start {
            sql "begin;"
            sql "insert into ${table1} select * from ${table3};" // (1,999,999),(2,999,999)
            sql "insert into ${table1} select * from ${table2};" // (1,888,888)
            sql "commit"
        }

        Thread.sleep(5000)

        GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
        GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")

        t1.join()
        t2.join()

        Thread.sleep(2000)

        // force it read delete bitmaps from MS rather than BE's cache(delete bitmap in BE's cache is correct)
        GetDebugPoint().enableDebugPointForAllBEs("CloudTxnDeleteBitmapCache::get_delete_bitmap.cache_miss")

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
