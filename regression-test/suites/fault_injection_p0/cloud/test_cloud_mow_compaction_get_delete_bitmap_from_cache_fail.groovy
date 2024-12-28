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

import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

suite("test_cloud_mow_compaction_get_delete_bitmap_from_cache_fail", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def tableName = "test_cloud_mow_compaction_get_delete_bitmap_from_cache_fail"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName}
            (k int, v1 int, v2 int )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH (k)
            BUCKETS 1  PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write"="true",
                "disable_auto_compaction" = "true");
        """

    sql "insert into ${tableName} values(1,1,1);"
    sql "insert into ${tableName} values(2,2,2);"
    sql "insert into ${tableName} values(3,3,3);"
    sql "insert into ${tableName} values(4,4,4);"
    sql "insert into ${tableName} values(5,5,5);"
    sql "sync;"
    order_qt_sql "select * from ${tableName};"

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    try {
        def inject_spin_wait = 'CloudCumulativeCompaction::modify_rowsets.enable_spin_wait'
        def inject_spin_block = 'CloudCumulativeCompaction::modify_rowsets.block'
        def inject_cache_miss = 'CloudTxnDeleteBitmapCache::get_delete_bitmap.cache_miss'
        def injectBe = null
        def backends = sql_return_maparray('show backends')
        def array = sql_return_maparray("SHOW TABLETS FROM ${tableName}")
        def injectBeId = array[0].BackendId
        def tabletId = array[0].TabletId
        injectBe = backends.stream().filter(be -> be.BackendId == injectBeId).findFirst().orElse(null)

        DebugPoint.enableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, inject_spin_wait)
        DebugPoint.enableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, inject_spin_block)
        DebugPoint.enableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, inject_cache_miss)
        logger.info("run compaction:" + tabletId)
        (code, out, err) = be_run_cumulative_compaction(injectBe.Host, injectBe.HttpPort, tabletId)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)

        // Concurrent inserts
        sql "insert into ${tableName} values(1,2,3);"
        sql "insert into ${tableName} values(2,3,4);"
        sql "insert into ${tableName} values(3,4,5);"
        sql "sync;"
        order_qt_sql "set use_fix_replica=0; select * from ${tableName};"

        // let compaction continue
        DebugPoint.disableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, inject_spin_block)

         do {
             Thread.sleep(100)
             (code, out, err) = be_get_compaction_status(injectBe.Host, injectBe.HttpPort, tabletId)
             logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
             assertEquals(code, 0)
             def compactionStatus = parseJson(out.trim())
             assertEquals("success", compactionStatus.status.toLowerCase())
             running = compactionStatus.run_status
         } while (running)

        Thread.sleep(200)
        order_qt_sql "set use_fix_replica=0; select * from ${tableName};"
    } catch (Exception e) {
        logger.info(e.getMessage())
        assertTrue(false)
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}

