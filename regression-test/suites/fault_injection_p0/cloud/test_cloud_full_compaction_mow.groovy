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

suite("test_cloud_full_compaction_mow","nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def tableName = "test_cloud_full_compaction_mow"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k` int ,
            `v` int ,
        ) engine=olap
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """

    sql """ INSERT INTO ${tableName} VALUES (0,00)"""
    sql """ INSERT INTO ${tableName} VALUES (1,10)"""
    sql """ INSERT INTO ${tableName} VALUES (2,20)"""
    sql """ INSERT INTO ${tableName} VALUES (3,30)"""

    def tabletStats = sql_return_maparray("show tablets from ${tableName};")
    def tabletId = tabletStats[0].TabletId
    def tabletBackendId = tabletStats[0].BackendId
    def tabletBackend
    def backends = sql_return_maparray('show backends')
    for (def be : backends) {
        if (be.BackendId == tabletBackendId) {
            tabletBackend = be
            break;
        }
    }
    logger.info("tablet ${tabletId} on backend ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}");

    GetDebugPoint().clearDebugPointsForAllBEs()
    GetDebugPoint().clearDebugPointsForAllFEs()

    try {
        // block load and compaction
        GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
        GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
        GetDebugPoint().enableDebugPointForAllBEs("CloudFullCompaction::modify_rowsets.block")

        def t1 = Thread.start {
            sql """ INSERT INTO ${tableName} VALUES (1,99),(2,99),(3,99);"""
        }

        Thread.sleep(1000)

        // trigger full compaction
        logger.info("trigger full compaction on BE ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}")
        def (code, out, err) = be_run_full_compaction(tabletBackend.Host, tabletBackend.HttpPort, tabletId)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assert  code == 0
        def compactJson = parseJson(out.trim())
        assert "success" == compactJson.status.toLowerCase()

        // let the load publish
        GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
        GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
        t1.join()
        Thread.sleep(1000)


        // let full compaction continue and wait for compaction to finish
        GetDebugPoint().disableDebugPointForAllBEs("CloudFullCompaction::modify_rowsets.block")
        def running = true
        do {
            Thread.sleep(1000)
            (code, out, err) = be_get_compaction_status(tabletBackend.Host, tabletBackend.HttpPort, tabletId)
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)

        qt_dup_key_count "select count() from (select k, count(*) from ${tableName} group by k having count(*) > 1) t"
        qt_sql "select * from ${tableName} order by k;"
    } catch (Exception e) {
        logger.info(e.getMessage())
        exception = true;
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().clearDebugPointsForAllFEs()
    }
}
