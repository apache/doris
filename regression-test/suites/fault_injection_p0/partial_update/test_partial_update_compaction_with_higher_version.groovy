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

suite("test_partial_update_compaction_with_higher_version", "nonConcurrent") {

    def table1 = "test_partial_update_compaction_with_higher_version"
    sql "DROP TABLE IF EXISTS ${table1} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table1} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int,
            `c3` int,
            `c4` int
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """

    sql "insert into ${table1} values(1,1,1,1,1);"
    sql "insert into ${table1} values(2,2,2,2,2);"
    sql "insert into ${table1} values(3,3,3,3,3);"
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

    def check_rs_metas = { expected_rs_meta_size, check_func -> 
        if (isCloudMode()) {
            return
        }

        def metaUrl = sql_return_maparray("show tablets from ${table1};").get(0).MetaUrl
        def (code, out, err) = curl("GET", metaUrl)
        Assert.assertEquals(code, 0)
        def jsonMeta = parseJson(out.trim())

        Assert.assertEquals(jsonMeta.rs_metas.size(), expected_rs_meta_size)
        for (def meta : jsonMeta.rs_metas) {
            int startVersion = meta.start_version
            int endVersion = meta.end_version
            int numSegments = meta.num_segments
            int numRows = meta.num_rows
            String overlapPb = meta.segments_overlap_pb
            logger.info("[${startVersion}-${endVersion}] ${overlapPb} ${meta.num_segments} ${numRows} ${meta.rowset_id_v2}")
            check_func(startVersion, endVersion, numSegments, numRows, overlapPb)
        }
    }

    check_rs_metas(4, {int startVersion, int endVersion, int numSegments, int numRows, String overlapPb ->
        if (startVersion == 0) {
            // [0-1]
            Assert.assertEquals(endVersion, 1)
            Assert.assertEquals(numSegments, 0)
        } else {
            // [2-2], [3-3], [4-4]
            Assert.assertEquals(startVersion, endVersion)
            Assert.assertEquals(numSegments, 1)
        }
    })

    def enable_publish_spin_wait = { tokenName -> 
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait", [token: "${tokenName}"])
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait", [token: "${tokenName}"])
        }
    }

    def disable_publish_spin_wait = {
        if (isCloudMode()) {
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
        } else {
            GetDebugPoint().disableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait")
        }
    }

    def enable_block_in_publish = { passToken -> 
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block", [pass_token: "${passToken}"])
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.block", [pass_token: "${passToken}"])
        }
    }

    def disable_block_in_publish = {
        if (isCloudMode()) {
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
        } else {
            GetDebugPoint().disableDebugPointForAllBEs("EnginePublishVersionTask::execute.block")
        }
    }

    try {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()

        // block the partial update in publish phase
        enable_publish_spin_wait("token1")
        enable_block_in_publish("-1")

        // the first partial update load
        def t1 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "sync;"
            sql "insert into ${table1}(k1,c1,c2) values(1,999,999),(2,888,888),(3,777,777);"
        }

        Thread.sleep(600)

        // the second partial update load that conflicts with the first one
        enable_publish_spin_wait("token2")
        def t2 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "sync;"
            sql "insert into ${table1}(k1,c3,c4) values(1,666,666),(3,555,555);"
        }

        Thread.sleep(400)

        // let the first partial update load finish
        enable_block_in_publish("token1")
        t1.join()
        Thread.sleep(200)
        check_rs_metas(5, {int startVersion, int endVersion, int numSegments, int numRows, String overlapPb ->
            if (startVersion == 0) {
                // [0-1]
                Assert.assertEquals(endVersion, 1)
                Assert.assertEquals(numSegments, 0)
            } else {
                // [2-2], [3-3], [4-4], [5-5]
                Assert.assertEquals(startVersion, endVersion)
                Assert.assertEquals(numSegments, 1)
            }
        })

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

        check_rs_metas(1, {int startVersion, int endVersion, int numSegments, int numRows, String overlapPb ->
            // check the rowset produced by full compaction
            // [0-5]
            Assert.assertEquals(startVersion, 0)
            Assert.assertEquals(endVersion, 5)
            Assert.assertEquals(numRows, 3)
            Assert.assertEquals(overlapPb, "NONOVERLAPPING")
        })

        // let the second partial update load publish
        disable_block_in_publish()
        t1.join()
        Thread.sleep(300)

        order_qt_sql "select * from ${table1};"

        check_rs_metas(2, {int startVersion, int endVersion, int numSegments, int numRows, String overlapPb ->
            if (startVersion == 6) {
                // [6-6]
                Assert.assertEquals(endVersion, 6)
                // checks that partial update didn't skip the alignment process of rowsets produced by compaction and
                // generate new segment in publish phase
                Assert.assertEquals(numSegments, 2)
                Assert.assertEquals(numRows, 4) // 4 = 2 + 2
            }
        })
        
    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    // sql "DROP TABLE IF EXISTS ${table1};"
}
