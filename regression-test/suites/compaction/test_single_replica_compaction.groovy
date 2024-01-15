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

suite("test_single_replica_compaction", "p2") {
    def tableName = "test_single_replica_compaction"
  
    def set_be_config = { key, value ->
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    boolean disableAutoCompaction = true
    boolean has_update_be_config = false
    try {
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
        
        logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List

        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "disable_auto_compaction") {
                disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
            }
        }
        set_be_config.call("disable_auto_compaction", "true")
        has_update_be_config = true

        def triggerCompaction = { be_host, be_http_port, compact_type, tablet_id ->
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X POST http://${be_host}:${be_http_port}")
            sb.append("/api/compaction/run?tablet_id=")
            sb.append(tablet_id)
            sb.append("&compact_type=${compact_type}")

            String command = sb.toString()
            logger.info(command)
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("Run compaction: code=" + code + ", out=" + out + ", disableAutoCompaction " + disableAutoCompaction + ", err=" + err)
            if (!disableAutoCompaction) {
                return "Success, " + out
            }
            assertEquals(code, 0)
            return out
        } 

        def triggerFullCompaction = { be_host, be_http_port, table_id ->
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X POST http://${be_host}:${be_http_port}")
            sb.append("/api/compaction/run?table_id=")
            sb.append(table_id)
            sb.append("&compact_type=full")

            String command = sb.toString()
            logger.info(command)
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("Run compaction: code=" + code + ", out=" + out + ", disableAutoCompaction " + disableAutoCompaction + ", err=" + err)
            if (!disableAutoCompaction) {
                return "Success, " + out
            }
            assertEquals(code, 0)
            return out
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
            logger.info("Get tablet status: code=" + code + ", out=" + out)
            assertEquals(code, 0)
            def tabletStatus = parseJson(out.trim())
            return tabletStatus
        }


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
            PROPERTIES ( "replication_num" = "3", "enable_single_replica_compaction" = "true", "enable_unique_key_merge_on_write" = "false" );
        """

        String[][] tablets = sql """ show tablets from ${tableName}; """

        // wait for update replica infos
        // be.conf: update_replica_infos_interval_seconds
        Thread.sleep(20000)
        
        // find the master be for single replica compaction
        Boolean found = false
        String master_backend_id;
        List<String> follower_backend_id = new ArrayList<>()
        // The test table only has one bucket with 3 replicas,
        // and `show tablets` will return 3 different replicas with the same tablet.
        // So we can use the same tablet_id to get tablet/trigger compaction with different backends.
        String tablet_id = tablets[0][0]
        String[][] tablet_info = sql """ show tablet ${tablet_id}; """
        logger.info("tablet: " + tablet_info)
        def table_id = tablet_info[0][5]
        for (String[] tablet in tablets) {
            String trigger_backend_id = tablet[2]
            def tablet_status = getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id);
            def fetchFromPeerValue = tablet_status."fetch from peer"

            if (found && fetchFromPeerValue.contains("-1")) {
                logger.warn("multipe master");
                assertTrue(false)
            }
            if (fetchFromPeerValue.contains("-1")) {
                found = true
                master_backend_id = trigger_backend_id
            } else {
                follower_backend_id.add(trigger_backend_id)
            }
        }

        def checkCompactionResult = {
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

        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "b", 100); """
        sql """ INSERT INTO ${tableName} VALUES (2, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (2, "b", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "b", 100); """

        // trigger master be to do cum compaction
        assertTrue(triggerCompaction(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id],
                    "cumulative", tablet_id).contains("Success")); 
        waitForCompaction(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id], tablet_id)

        // trigger follower be to fetch compaction result
        for (String id in follower_backend_id) {
            assertTrue(triggerCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id],
                    "cumulative", tablet_id).contains("Success")); 
            waitForCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id)
        }

        // check rowsets
        checkCompactionResult.call()

        sql """ INSERT INTO ${tableName} VALUES (4, "a", 100); """
        sql """ DELETE FROM ${tableName} WHERE id = 4; """
        sql """ INSERT INTO ${tableName} VALUES (5, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (6, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (7, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (8, "a", 100); """

        // trigger master be to do cum compaction with delete
        assertTrue(triggerCompaction(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id],
                    "cumulative", tablet_id).contains("Success")); 
        waitForCompaction(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id], tablet_id)

        // trigger follower be to fetch compaction result
        for (String id in follower_backend_id) {
            assertTrue(triggerFullCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], 
                        table_id).contains("Success")); 
            waitForCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id)
        }

        // check rowsets
        checkCompactionResult.call()

        // trigger master be to do base compaction
        assertTrue(triggerCompaction(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id],
                    "base", tablet_id).contains("Success")); 
        waitForCompaction(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id], tablet_id)

        // // trigger follower be to fetch compaction result
        for (String id in follower_backend_id) {
            assertTrue(triggerFullCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id],
                        table_id).contains("Success")); 
            waitForCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id)
        }

        // check rowsets
        checkCompactionResult.call()

        qt_sql """
        select * from  ${tableName} order by id
        """
  
    } finally {
        if (has_update_be_config) {
            set_be_config.call("disable_auto_compaction", disableAutoCompaction.toString())
        }
    }
}