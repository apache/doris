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

suite("test_single_compaction_fault_injection", "p2, nonConcurrent") {
    def tableName = "test_single_compaction"

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

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

    def triggerSingleCompaction = { be_host, be_http_port, tablet_id ->
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X POST http://${be_host}:${be_http_port}")
        sb.append("/api/compaction/run?tablet_id=")
        sb.append(tablet_id)
        sb.append("&compact_type=cumulative&remote=true")

        Integer maxRetries = 10; // Maximum number of retries
        Integer retryCount = 0; // Current retry count
        Integer sleepTime = 5000; // Sleep time in milliseconds
        String cmd = sb.toString()
        def process
        int code_3
        String err_3
        String out_3

        while (retryCount < maxRetries) {
            process = cmd.execute()
            code_3 = process.waitFor()
            err_3 = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())))
            out_3 = process.getText()

            // If the command was successful, break the loop
            if (code_3 == 0) {
                break
            }

            // If the command was not successful, increment the retry count, sleep for a while and try again
            retryCount++
            sleep(sleepTime)
        }
        assertEquals(code_3, 0)
        logger.info("Get compaction status: code=" + code_3 + ", out=" + out_3)
        return out_3
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

    // find the master be for single compaction
    Boolean found = false
    String master_backend_id
    List<String> follower_backend_id = new ArrayList<>()
    String tablet_id
    def tablets
    try {
        GetDebugPoint().enableDebugPointForAllFEs('getTabletReplicaInfos.returnEmpty')
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NULL,
                `name` varchar(255) NULL,
                `score` int(11) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ( "replication_num" = "2", "enable_single_replica_compaction" = "true", "enable_unique_key_merge_on_write" = "false", "compaction_policy" = "time_series");
        """

        tablets = sql_return_maparray """ show tablets from ${tableName}; """
        // wait for update replica infos
        Thread.sleep(70000)
        // The test table only has one bucket with 2 replicas,
        // and `show tablets` will return 2 different replicas with the same tablet.
        // So we can use the same tablet_id to get tablet/trigger compaction with different backends.
        tablet_id = tablets[0].TabletId
        def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
        logger.info("tablet: " + tablet_info)
        for (def tablet in tablets) {
            String trigger_backend_id = tablet.BackendId
            def tablet_status = getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
            if (!tablet_status.containsKey("single replica compaction status")) {
                if (found) {
                    found = false
                    logger.warn("multipe master");
                    break;
                }
                found = true
                master_backend_id = trigger_backend_id
            } else {
                follower_backend_id.add(trigger_backend_id)
            }
        }
        assertFalse(found)
        assertFalse(master_backend_id.isEmpty())
        assertTrue(follower_backend_id.isEmpty())
        master_backend_id = ""
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs('getTabletReplicaInfos.returnEmpty')
        // wait for update replica infos
        Thread.sleep(70000)
        // The test table only has one bucket with 2 replicas,
        // and `show tablets` will return 2 different replicas with the same tablet.
        // So we can use the same tablet_id to get tablet/trigger compaction with different backends.
        tablet_id = tablets[0].TabletId
        def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
        for (def tablet in tablets) {
            String trigger_backend_id = tablet.BackendId
            def tablet_status = getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id);
            if (!tablet_status.containsKey("single replica compaction status")) {
                if (found) {
                    logger.warn("multipe master")
                    assertTrue(false)
                }
                found = true
                master_backend_id = trigger_backend_id
            } else {
                follower_backend_id.add(trigger_backend_id)
            }
        }
        assertTrue(found)
        assertFalse(master_backend_id.isEmpty())
        assertFalse(follower_backend_id.isEmpty())
    }
    

    def checkSucceedCompactionResult = {
        def master_tablet_status = getTabletStatus(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id], tablet_id);
        def master_rowsets = master_tablet_status."rowsets"
        assert master_rowsets instanceof List
        logger.info("rowset size: " + master_rowsets.size())

        for (String backend: follower_backend_id) {
            def tablet_status = getTabletStatus(backendId_to_backendIP[backend], backendId_to_backendHttpPort[backend], tablet_id);
            def rowsets = tablet_status."rowsets"
            assert rowsets instanceof List
            assertEquals(master_rowsets.size(), rowsets.size())
        }
    }

    def checkFailedCompactionResult = {
        def master_tablet_status = getTabletStatus(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id], tablet_id);
        def master_rowsets = master_tablet_status."rowsets"
        assert master_rowsets instanceof List
        logger.info("rowset size: " + master_rowsets.size())

        for (String backend: follower_backend_id) {
            def tablet_status = getTabletStatus(backendId_to_backendIP[backend], backendId_to_backendHttpPort[backend], tablet_id);
            def rowsets = tablet_status."rowsets"
            assert rowsets instanceof List
            assertFalse(master_rowsets.size() == rowsets.size())
        }
    }

    // return ok
    try {
        GetDebugPoint().enableDebugPointForAllBEs("do_single_compaction_return_ok");
        for (String id in follower_backend_id) {
            assertTrue(triggerSingleCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id).contains("Success"));
            waitForCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id);
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("do_single_compaction_return_ok");
    }
    sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "b", 100); """
    sql """ INSERT INTO ${tableName} VALUES (2, "a", 100); """
    sql """ INSERT INTO ${tableName} VALUES (2, "b", 100); """
    sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
    sql """ INSERT INTO ${tableName} VALUES (3, "b", 100); """

    // trigger master be to do compaction
    assertTrue(triggerCompaction(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id],
                "full", tablet_id).contains("Success")); 
    waitForCompaction(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id], tablet_id)

    try {
        GetDebugPoint().enableDebugPointForAllBEs("single_compaction_failed_get_peer");
        for (String id in follower_backend_id) {
            out = triggerSingleCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id)
            assertTrue(out.contains("compaction task is successfully triggered") || out.contains("tablet don't have peer replica"));
        }
        checkFailedCompactionResult.call()
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("single_compaction_failed_get_peer")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("single_compaction_failed_get_peer_versions");
        for (String id in follower_backend_id) {
            out = triggerSingleCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id)
            assertTrue(out.contains("compaction task is successfully triggered") || out.contains("tablet failed get peer versions"));
        }
        checkFailedCompactionResult.call()
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("single_compaction_failed_get_peer_versions")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("single_compaction_failed_make_snapshot");
        for (String id in follower_backend_id) {
            out = triggerSingleCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id)
            assertTrue(out.contains("compaction task is successfully triggered") || out.contains("failed snapshot"));
        }
        checkFailedCompactionResult.call()
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("single_compaction_failed_make_snapshot")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("single_compaction_failed_download_file");
        for (String id in follower_backend_id) {
            out = triggerSingleCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id)
            assertTrue(out.contains("compaction task is successfully triggered") || out.contains("failed to download file"));
        }
        checkFailedCompactionResult.call()
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("single_compaction_failed_download_file")
    }

    // trigger follower be to fetch compaction result
    for (String id in follower_backend_id) {
        assertTrue(triggerSingleCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id).contains("Success")); 
        waitForCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id)
    }

    // check rowsets
    checkSucceedCompactionResult.call()

    sql """ INSERT INTO ${tableName} VALUES (4, "a", 100); """
    sql """ INSERT INTO ${tableName} VALUES (5, "a", 100); """
    sql """ INSERT INTO ${tableName} VALUES (6, "a", 100); """
    sql """ DELETE FROM ${tableName} WHERE id = 4; """
    sql """ INSERT INTO ${tableName} VALUES (7, "a", 100); """
    sql """ INSERT INTO ${tableName} VALUES (8, "a", 100); """

    // trigger master be to do compaction with delete
    assertTrue(triggerCompaction(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id],
                "full", tablet_id).contains("Success")); 
    waitForCompaction(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id], tablet_id)

    // trigger follower be to fetch compaction result
    for (String id in follower_backend_id) {
        assertTrue(triggerSingleCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id).contains("Success")); 
        waitForCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id)
    }

    // check rowsets
    checkSucceedCompactionResult.call()

    qt_sql """
    select * from  ${tableName} order by id
    """
}