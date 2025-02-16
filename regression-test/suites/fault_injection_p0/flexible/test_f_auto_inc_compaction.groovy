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

suite("test_f_auto_inc_compaction", "nonConcurrent") {

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def tableName = "test_f_auto_inc_compaction"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL,
        `id` BIGINT NOT NULL AUTO_INCREMENT
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "disable_auto_compaction" = "true",
        "store_row_column" = "false"); """

    sql """insert into ${tableName}(k,v1,v2) values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6);"""
    qt_sql "select k,v1,v2,id from ${tableName} order by k;"
    sql "delete from ${tableName} where k=1;"
    sql "delete from ${tableName} where k=3;"
    sql "delete from ${tableName} where k=6;"
    qt_sql "select k,v1,v2 from ${tableName} order by k;"
    qt_check_auto_inc_dup "select count(*) from ${tableName} group by id having count(*) > 1;"

    def beNodes = sql_return_maparray("show backends;")
    def tabletStat = sql_return_maparray("show tablets from ${tableName};").get(0)
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

        def metaUrl = sql_return_maparray("show tablets from ${tableName};").get(0).MetaUrl
        def (code, out, err) = curl("GET", metaUrl)
        Assert.assertEquals(code, 0)
        def jsonMeta = parseJson(out.trim())

        logger.info("jsonMeta.rs_metas.size(): ${jsonMeta.rs_metas.size()}, expected_rs_meta_size: ${expected_rs_meta_size}")
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


        enable_publish_spin_wait("t1")
        enable_block_in_publish("-1")

        Thread.sleep(1000)
        def t1 = Thread.start {
            sql "insert into ${tableName} values(99,99,99,99);"
        }
        Thread.sleep(1000)


        enable_publish_spin_wait("t2")
        Thread.sleep(1000)
        def t2 = Thread.start {
            String load2 = 
                    """{"k":3,"v1":100}
                    {"k":6,"v2":600}"""
            streamLoad {
                table "${tableName}"
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                inputStream new ByteArrayInputStream(load2.getBytes())
                time 60000
            }
        }
        Thread.sleep(1000)


        // most have a new load after load2's max version sees in flush phase. Otherwise load2 will skip to calc delete bitmap
        // of rowsets which is produced by compaction and whose end version is lower than the max version
        // the load1 sees in flush phase, see https://github.com/apache/doris/pull/38487 for details

        // let load1 publish
        enable_block_in_publish("t1")
        t1.join()
        Thread.sleep(2000)


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
            // [0-6]
            Assert.assertEquals(startVersion, 0)
            Assert.assertEquals(endVersion, 6)
            Assert.assertEquals(numRows, 7)
            Assert.assertEquals(overlapPb, "NONOVERLAPPING")
        })

        qt_after_compaction "select k,v1,v2 from ${tableName} order by k;"
        qt_check_auto_inc_dup "select count(*) from ${tableName} group by id having count(*) > 1;"

        disable_publish_spin_wait()
        disable_block_in_publish()
        Thread.sleep(1000)
        t1.join()

        qt_sql "select k,v1,v2 from ${tableName} order by k;"
        qt_check_auto_inc_dup "select count(*) from ${tableName} group by id having count(*) > 1;"
        qt_check_auto_inc_val "select count(*) from ${tableName} where id=0;"
    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
