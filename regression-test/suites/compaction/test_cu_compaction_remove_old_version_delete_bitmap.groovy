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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_cu_compaction_remove_old_version_delete_bitmap", "nonConcurrent") {
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_params = [string: [:]]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_param = { paramName, paramValue ->
        // for eache be node, set paramName=paramValue
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
            assertTrue(out.contains("OK"))
        }
    }

    def reset_be_param = { paramName ->
        // for eache be node, reset paramName to default
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def original_value = backendId_to_params.get(id).get(paramName)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, original_value))
            assertTrue(out.contains("OK"))
        }
    }

    def get_be_param = { paramName ->
        // for eache be node, get param value by default
        def paramValue = ""
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            // get the config value from be
            def (code, out, err) = curl("GET", String.format("http://%s:%s/api/show_config?conf_item=%s", beIp, bePort, paramName))
            assertTrue(code == 0)
            assertTrue(out.contains(paramName))
            // parsing
            def resultList = parseJson(out)[0]
            assertTrue(resultList.size() == 4)
            // get original value
            paramValue = resultList[2]
            backendId_to_params.get(id, [:]).put(paramName, paramValue)
        }
    }

    def triggerCompaction = { be_host, be_http_port, compact_type, tablet_id ->
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

    def getTabletStatus = { be_host, be_http_port, tablet_id ->
        boolean running = true
        Thread.sleep(1000)
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/compaction/show?tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        process = command.execute()
        code = process.waitFor()
        out = process.getText()
        logger.info("Get tablet status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def tabletStatus = parseJson(out.trim())
        return tabletStatus
    }

    def waitForCompaction = { be_host, be_http_port, tablet_id ->
        boolean running = true
        do {
            Thread.sleep(1000)
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X GET http://${be_host}:${be_http_port}")
            sb.append("/api/compaction/run_status?tablet_id=")
            sb.append(tablet_id)

            String command = sb.toString()
            logger.info(command)
            process = command.execute()
            code = process.waitFor()
            out = process.getText()
            logger.info("Get compaction status: code=" + code + ", out=" + out)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
    }

    def getLocalDeleteBitmapStatus = { be_host, be_http_port, tablet_id ->
        boolean running = true
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/delete_bitmap/count_local?tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        process = command.execute()
        code = process.waitFor()
        out = process.getText()
        logger.info("Get local delete bitmap count status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def deleteBitmapStatus = parseJson(out.trim())
        return deleteBitmapStatus
    }

    def getMSDeleteBitmapStatus = { be_host, be_http_port, tablet_id ->
        boolean running = true
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/delete_bitmap/count_ms?tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        process = command.execute()
        code = process.waitFor()
        out = process.getText()
        logger.info("Get ms delete bitmap count status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def deleteBitmapStatus = parseJson(out.trim())
        return deleteBitmapStatus
    }

    def testTable = "test_cu_compaction_remove_old_version_delete_bitmap"
    def timeout = 10000
    sql """ DROP TABLE IF EXISTS ${testTable}"""
    def testTableDDL = """
        create table ${testTable}
            (
            `plan_id` bigint(20) NOT NULL,
            `target_id` int(20) NOT NULL,
            `target_name` varchar(255) NOT NULL
            )
            ENGINE=OLAP
            UNIQUE KEY(`plan_id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`plan_id`) BUCKETS 1
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "true",
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true"
            );
    """
    sql testTableDDL
    sql "sync"

    // store the original value
    get_be_param("compaction_promotion_version_count")
    get_be_param("tablet_rowset_stale_sweep_time_sec")
    set_be_param("compaction_promotion_version_count", "5")
    set_be_param("tablet_rowset_stale_sweep_time_sec", "0")

    try {
        GetDebugPoint().enableDebugPointForAllBEs("CumulativeCompaction.modify_rowsets.delete_expired_stale_rowsets")
        // 1. test normal
        sql "sync"
        sql """ INSERT INTO ${testTable} VALUES (0,0,'1'),(1,1,'1'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'2'),(2,2,'2'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'3'),(3,3,'3'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'4'),(4,4,'4'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'5'),(5,5,'5'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'6'),(6,6,'6'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'7'),(7,7,'7'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'8'),(8,8,'8'); """

        qt_sql "select * from ${testTable} order by plan_id"

        // trigger compaction to generate base rowset
        def tablets = sql_return_maparray """ show tablets from ${testTable}; """
        logger.info("tablets: " + tablets)
        def local_delete_bitmap_count = 0
        def ms_delete_bitmap_count = 0
        def local_delete_bitmap_cardinality = 0;
        def ms_delete_bitmap_cardinality = 0;
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
            logger.info("tablet: " + tablet_info)
            String trigger_backend_id = tablet.BackendId
            getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id);

            // before compaction, delete_bitmap_count is (rowsets num - 1)
            local_delete_bitmap_count = getLocalDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).delete_bitmap_count
            local_delete_bitmap_cardinality = getLocalDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).cardinality
            logger.info("local_delete_bitmap_count:" + local_delete_bitmap_count)
            logger.info("local_delete_bitmap_cardinality:" + local_delete_bitmap_cardinality)
            assertTrue(local_delete_bitmap_count == 7)
            assertTrue(local_delete_bitmap_cardinality == 7)

            if (isCloudMode()) {
                ms_delete_bitmap_count = getMSDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).delete_bitmap_count
                ms_delete_bitmap_cardinality = getMSDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).cardinality
                logger.info("ms_delete_bitmap_count:" + ms_delete_bitmap_count)
                logger.info("ms_delete_bitmap_cardinality:" + ms_delete_bitmap_cardinality)
                assertTrue(ms_delete_bitmap_count == 7)
                assertTrue(ms_delete_bitmap_cardinality == 7)
            }


            assertTrue(triggerCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id],
                    "cumulative", tablet_id).contains("Success"));
            waitForCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
            getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id);
        }

        qt_sql "select * from ${testTable} order by plan_id"

        def now = System.currentTimeMillis()

        sql """ INSERT INTO ${testTable} VALUES (0,0,'9'),(1,9,'9'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'10'),(1,10,'10'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'11'),(1,11,'11'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'12'),(1,12,'12'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'13'),(1,13,'13'); """

        def time_diff = System.currentTimeMillis() - now
        logger.info("time_diff:" + time_diff)
        assertTrue(time_diff <= timeout, "wait_for_insert_into_values timeout")

        qt_sql "select * from ${testTable} order by plan_id"

        // trigger cu compaction to remove old version delete bitmap

        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
            logger.info("tablet: " + tablet_info)

            // before compaction, local delete_bitmap_count is (total rowsets num - 1), ms delete_bitmap_count is new rowset num
            String trigger_backend_id = tablet.BackendId
            local_delete_bitmap_count = getLocalDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).delete_bitmap_count
            local_delete_bitmap_cardinality = getLocalDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).cardinality
            logger.info("local_delete_bitmap_count:" + local_delete_bitmap_count)
            logger.info("local_delete_bitmap_cardinality:" + local_delete_bitmap_cardinality)
            assertTrue(local_delete_bitmap_count == 12)
            assertTrue(local_delete_bitmap_cardinality == 17)

            if (isCloudMode()) {
                ms_delete_bitmap_count = getMSDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).delete_bitmap_count
                ms_delete_bitmap_cardinality = getMSDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).cardinality
                logger.info("ms_delete_bitmap_count:" + ms_delete_bitmap_count)
                logger.info("ms_delete_bitmap_cardinality:" + ms_delete_bitmap_cardinality)
                assertTrue(ms_delete_bitmap_count == 5)
                assertTrue(ms_delete_bitmap_cardinality == 10)
            }

            getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id);
            assertTrue(triggerCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id],
                    "cumulative", tablet_id).contains("Success"));
            waitForCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
            getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id);

            Thread.sleep(1000)
            // after compaction, delete_bitmap_count is 1, cardinality is 2, check it
            local_delete_bitmap_count = getLocalDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).delete_bitmap_count
            local_delete_bitmap_cardinality = getLocalDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).cardinality
            logger.info("local_delete_bitmap_count:" + local_delete_bitmap_count)
            logger.info("local_delete_bitmap_cardinality:" + local_delete_bitmap_cardinality)
            assertTrue(local_delete_bitmap_count == 1)
            assertTrue(local_delete_bitmap_cardinality == 2)
            if (isCloudMode()) {
                ms_delete_bitmap_count = getMSDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).delete_bitmap_count
                ms_delete_bitmap_cardinality = getMSDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).cardinality
                logger.info("ms_delete_bitmap_count:" + ms_delete_bitmap_count)
                logger.info("ms_delete_bitmap_cardinality:" + ms_delete_bitmap_cardinality)
                assertTrue(ms_delete_bitmap_count == 1)
                assertTrue(ms_delete_bitmap_cardinality == 2)
            }
        }

        qt_sql "select * from ${testTable} order by plan_id"

        // 2. test update delete bitmap failed

        now = System.currentTimeMillis()

        sql """ INSERT INTO ${testTable} VALUES (0,0,'14'),(1,19,'19'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'15'),(1,20,'20'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'16'),(1,21,'21'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'17'),(1,22,'22'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'18'),(1,23,'23'); """

        time_diff = System.currentTimeMillis() - now
        logger.info("time_diff:" + time_diff)
        assertTrue(time_diff <= timeout, "wait_for_insert_into_values timeout")

        qt_sql "select * from ${testTable} order by plan_id"
        GetDebugPoint().enableDebugPointForAllBEs("CloudCumulativeCompaction.modify_rowsets.update_delete_bitmap_failed")
        if (isCloudMode()) {
            for (def tablet in tablets) {
                String tablet_id = tablet.TabletId
                def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
                logger.info("tablet: " + tablet_info)
                String trigger_backend_id = tablet.BackendId

                local_delete_bitmap_count = getLocalDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).delete_bitmap_count
                ms_delete_bitmap_count = getMSDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).delete_bitmap_count
                logger.info("local_delete_bitmap_count:" + local_delete_bitmap_count)
                logger.info("ms_delete_bitmap_count:" + ms_delete_bitmap_count)
                assertTrue(local_delete_bitmap_count == 6)
                assertTrue(local_delete_bitmap_count == ms_delete_bitmap_count)

                local_delete_bitmap_cardinality = getLocalDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).cardinality
                ms_delete_bitmap_cardinality = getMSDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).cardinality
                logger.info("local_delete_bitmap_cardinality:" + local_delete_bitmap_cardinality)
                logger.info("ms_delete_bitmap_cardinality:" + ms_delete_bitmap_cardinality)
                assertTrue(local_delete_bitmap_cardinality == 12)
                assertTrue(ms_delete_bitmap_cardinality == 12)

                getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id);
                assertTrue(triggerCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id],
                        "cumulative", tablet_id).contains("Success"));
                waitForCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
                getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id);

                // update fail, local delete_bitmap_count will not change
                Thread.sleep(1000)
                local_delete_bitmap_count = getLocalDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).delete_bitmap_count
                logger.info("local_delete_bitmap_count:" + local_delete_bitmap_count)
                assertTrue(local_delete_bitmap_count == 6)
            }
        }

        qt_sql "select * from ${testTable} order by plan_id"

        now = System.currentTimeMillis()

        sql """ INSERT INTO ${testTable} VALUES (0,1,'1'),(1,24,'24'); """
        sql """ INSERT INTO ${testTable} VALUES (0,3,'2'),(1,25,'25'); """
        sql """ INSERT INTO ${testTable} VALUES (0,3,'3'),(1,26,'26'); """
        sql """ INSERT INTO ${testTable} VALUES (0,4,'4'),(1,27,'27'); """
        sql """ INSERT INTO ${testTable} VALUES (0,5,'5'),(1,28,'28'); """

        time_diff = System.currentTimeMillis() - now
        logger.info("time_diff:" + time_diff)
        assertTrue(time_diff <= timeout, "wait_for_insert_into_values timeout")

        qt_sql "select * from ${testTable} order by plan_id"

        GetDebugPoint().disableDebugPointForAllBEs("CloudCumulativeCompaction.modify_rowsets.update_delete_bitmap_failed")
    } finally {
        reset_be_param("compaction_promotion_version_count")
        reset_be_param("tablet_rowset_stale_sweep_time_sec")
        GetDebugPoint().disableDebugPointForAllBEs("CumulativeCompaction.modify_rowsets.delete_expired_stale_rowsets")
        GetDebugPoint().disableDebugPointForAllBEs("CloudCumulativeCompaction.modify_rowsets.update_delete_bitmap_failed")
    }

}