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

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.doris.regression.suite.ClusterOptions
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_mow_delete_unused_rowset_dm_docker", "docker") {
    logger.info("test_mow_delete_unused_rowset_dm_docker")
    def options = new ClusterOptions()
    options.cloudMode = false
    options.setFeNum(1)
    options.setBeNum(1)
    options.enableDebugPoints()
    options.feConfigs.add("enable_workload_group=false")
    // beConfigs
    options.beConfigs.add('compaction_promotion_version_count=5')
    options.beConfigs.add('tablet_rowset_stale_sweep_time_sec=0')
    options.beConfigs.add('enable_mow_verbose_log=true')
    options.beConfigs.add('enable_java_support=false')

    def testTable = "test_mow_delete_unused_rowset_dm_docker"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]

    def triggerCompaction = { tablet ->
        def compact_type = "cumulative"
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        if (compact_type == "cumulative") {
            def (code_1, out_1, err_1) = be_run_cumulative_compaction(be_host, be_http_port, tablet_id)
            logger.info("Run compaction: code=" + code_1 + ", out=" + out_1 + ", err=" + err_1)
            assertEquals(code_1, 0)
            return out_1
        } else if (compact_type == "full") {
            def (code_2, out_2, err_2) = be_run_full_compaction(be_host, be_http_port, tablet_id)
            logger.info("Run compaction: code=" + code_2 + ", out=" + out_2 + ", err=" + err_2)
            assertEquals(code_2, 0)
            return out_2
        } else {
            assertFalse(True)
        }
    }

    def getTabletStatus = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/compaction/show?tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
        def code = process.waitFor()
        def out = process.getText()
        logger.info("Get tablet status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def tabletStatus = parseJson(out.trim())
        return tabletStatus
    }

    def waitForCompaction = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        def running = true
        do {
            Thread.sleep(1000)
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X GET http://${be_host}:${be_http_port}")
            sb.append("/api/compaction/run_status?tablet_id=")
            sb.append(tablet_id)

            String command = sb.toString()
            logger.info(command)
            def process = command.execute()
            def code = process.waitFor()
            def out = process.getText()
            logger.info("Get compaction status: code=" + code + ", out=" + out)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
    }

    def getLocalDeleteBitmapStatus = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        boolean running = true
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/delete_bitmap/count_local?verbose=true&tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
        def code = process.waitFor()
        def out = process.getText()
        logger.info("Get local delete bitmap count status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def deleteBitmapStatus = parseJson(out.trim())
        return deleteBitmapStatus
    }

    docker(options) {
        sql """ DROP TABLE IF EXISTS ${testTable} """
        sql """
        create table ${testTable} (`k` int NOT NULL, `v` int NOT NULL)
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        );
        """
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        def tablets = sql_return_maparray """ show tablets from ${testTable}; """
        logger.info("tablets: " + tablets)
        assertEquals(1, tablets.size())
        def tablet = tablets[0]

        GetDebugPoint().enableDebugPointForAllBEs("CumulativeCompaction.modify_rowsets.delete_expired_stale_rowset")
        GetDebugPoint().enableDebugPointForAllBEs("DeleteBitmapAction._handle_show_local_delete_bitmap_count.start_delete_unused_rowset")

        // 1. write some data
        sql """ INSERT INTO ${testTable} VALUES (1,98); """
        sql """ INSERT INTO ${testTable} VALUES (1,99),(2,99); """
        sql """ INSERT INTO ${testTable} VALUES (3,99); """
        sql """ INSERT INTO ${testTable} VALUES (4,99); """
        sql """ INSERT INTO ${testTable} VALUES (5,99); """
        sql "sync"
        order_qt_sql1 """ select * from ${testTable}; """

        // 2. trigger compaction to generate base rowset
        getTabletStatus(tablet)
        assertTrue(triggerCompaction(tablet).contains("Success"))
        waitForCompaction(tablet)
        def tablet_status = getTabletStatus(tablet)
        assertEquals(2, tablet_status["rowsets"].size())

        // 3. wait for no delete bitmap and no stale rowsets
        def local_dm = getLocalDeleteBitmapStatus(tablet)
        assertEquals(0, local_dm["delete_bitmap_count"])
        tablet_status = getTabletStatus(tablet)
        assertEquals(0, tablet_status["stale_rowsets"].size())

        // 3. write some data
        sql """ INSERT INTO ${testTable} VALUES (1,100); """
        sql """ INSERT INTO ${testTable} VALUES (1,101),(2,100); """
        sql """ INSERT INTO ${testTable} VALUES (3,100); """
        sql """ INSERT INTO ${testTable} VALUES (4,100); """
        sql """ INSERT INTO ${testTable} VALUES (5,100); """
        sql """ sync """
        order_qt_sql2 "select * from ${testTable}"
        tablet_status = getTabletStatus(tablet)
        assertEquals(7, tablet_status["rowsets"].size())

        // 4. trigger compaction
        GetDebugPoint().enableDebugPointForAllBEs("StorageEngine::start_delete_unused_rowset.block")
        assertTrue(triggerCompaction(tablet).contains("Success"))
        waitForCompaction(tablet)
        tablet_status = getTabletStatus(tablet)
        assertEquals(3, tablet_status["rowsets"].size())

        // 5. block delete unused rowset, there are delete bitmaps; wait for no stale rowsets
        GetDebugPoint().disableDebugPointForAllBEs("DeleteBitmapAction._handle_show_local_delete_bitmap_count.start_delete_unused_rowset")
        local_dm = getLocalDeleteBitmapStatus(tablet)
        logger.info("local_dm 1: " + local_dm)
        assertEquals(6, local_dm["delete_bitmap_count"])
        tablet_status = getTabletStatus(tablet)
        assertEquals(0, tablet_status["stale_rowsets"].size())

        // 6. restart be. check delete bitmap count
        cluster.restartBackends()
        tablet_status = getTabletStatus(tablet)
        logger.info("tablet status after restart: " + tablet_status)
        for (int i = 0; i < 300; i++) {
            local_dm = getLocalDeleteBitmapStatus(tablet)
            if (local_dm["delete_bitmap_count"] == 5) {
                break
            }
            sleep(20)
        }
        local_dm = getLocalDeleteBitmapStatus(tablet)
        logger.info("local_dm 2: " + local_dm)
        assertEquals(5, local_dm["delete_bitmap_count"])
        order_qt_sql3 """ select * from ${testTable}; """

        // 7. restart be to check to the deleted delete bitmap is stored to local storage
        cluster.restartBackends()
        tablet_status = getTabletStatus(tablet)
        logger.info("tablet status after restart2: " + tablet_status)
        for (int i = 0; i < 300; i++) {
            local_dm = getLocalDeleteBitmapStatus(tablet)
            if (local_dm["delete_bitmap_count"] == 5) {
                break
            }
            sleep(20)
        }
        local_dm = getLocalDeleteBitmapStatus(tablet)
        logger.info("local_dm 3: " + local_dm)
        assertEquals(5, local_dm["delete_bitmap_count"])
    }
}
