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

suite("test_single_compaction_with_variant_inverted", "p2, nonConcurrent") {
    def tableName = "test_single_compaction_with_variant_inverted"

    def calc_file_crc_on_tablet = { ip, port, tablet ->
        return curl("GET", String.format("http://%s:%s/api/calc_crc?tablet_id=%s", ip, port, tablet))
    }

    boolean disableAutoCompaction = true
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

    def triggerSingleCompaction = { be_host, be_http_port, tablet_id ->
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X POST http://${be_host}:${be_http_port}")
        sb.append("/api/compaction/run?tablet_id=")
        sb.append(tablet_id)
        sb.append("&compact_type=cumulative&remote=true")

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
    sql """ set disable_inverted_index_v1_for_variant = false """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `score` int(11) NULL,
            `properties` variant,
            INDEX idx_props (`properties`) USING INVERTED PROPERTIES("parser" = "none") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "2",
            "enable_single_replica_compaction" = "true",
            "inverted_index_storage_format" = "V1",
            "compaction_policy" = "time_series"
        );
    """
    sql """ set disable_inverted_index_v1_for_variant = true """

    def tablets = sql_return_maparray """ show tablets from ${tableName}; """

    // wait for update replica infos
    Thread.sleep(72000)
    
    // find the master be for single replica compaction
    Boolean found = false
    String master_backend_id;
    List<String> follower_backend_id = new ArrayList<>()
    // The test table only has one bucket with 2 replicas,
    // and `show tablets` will return 2 different replicas with the same tablet.
    // So we can use the same tablet_id to get tablet/trigger compaction with different backends.
    String tablet_id = tablets[0].TabletId
    def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
    logger.info("tablet: " + tablet_info)
    for (def tablet in tablets) {
        String trigger_backend_id = tablet.BackendId
        def tablet_status = getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id);
        if (!tablet_status.containsKey("single replica compaction status")) {
            if (found) {
                logger.warn("multipe master");
                assertTrue(false)
            }
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

    def checkTabletFileCrc = {
        def (master_code, master_out, master_err) = calc_file_crc_on_tablet(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id], tablet_id)
        logger.info("Run calc_file_crc_on_tablet: ip=" + backendId_to_backendIP[master_backend_id] + " code=" + master_code + ", out=" + master_out + ", err=" + master_err)

        for (String backend: follower_backend_id) {
            def (follower_code, follower_out, follower_err) = calc_file_crc_on_tablet(backendId_to_backendIP[backend], backendId_to_backendHttpPort[backend], tablet_id)
            logger.info("Run calc_file_crc_on_tablet: ip=" + backendId_to_backendIP[backend] + " code=" + follower_code + ", out=" + follower_out + ", err=" + follower_err)
            assertTrue(parseJson(follower_out.trim()).crc_value == parseJson(master_out.trim()).crc_value)
            assertTrue(parseJson(follower_out.trim()).start_version == parseJson(master_out.trim()).start_version)
            assertTrue(parseJson(follower_out.trim()).end_version == parseJson(master_out.trim()).end_version)
            assertTrue(parseJson(follower_out.trim()).file_count == parseJson(master_out.trim()).file_count)
            assertTrue(parseJson(follower_out.trim()).rowset_count == parseJson(master_out.trim()).rowset_count)
        }
    }

    sql """ INSERT INTO ${tableName} VALUES (1, "a", 100, '{"a" : 1234, "point" : 1, "xxxx" : "ddddd"}'); """
    sql """ INSERT INTO ${tableName} VALUES (1, "b", 100, '{"%a" : 1234, "@point" : 1, "xxxx" : "ddddd"}'); """
    sql """ INSERT INTO ${tableName} VALUES (2, "a", 100, '{"@a" : 1234, "%point" : 1, "xxxx" : "ddddd"}'); """
    sql """ INSERT INTO ${tableName} VALUES (2, "b", 100, '{"%a" : 1234, "%point" : 1, "xxxx" : "ddddd"}'); """
    sql """ INSERT INTO ${tableName} VALUES (3, "a", 100, '{"@a" : 1234, "@point" : 1, "xxxx" : "ddddd"}'); """
    sql """ INSERT INTO ${tableName} VALUES (3, "b", 100, '{"a" : 1234, "point" : 1, "xxxx" : "ddddd"}'); """

    // trigger master be to do full compaction
    assertTrue(triggerCompaction(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id],
                "full", tablet_id).contains("Success")); 
    waitForCompaction(backendId_to_backendIP[master_backend_id], backendId_to_backendHttpPort[master_backend_id], tablet_id)

    // trigger follower be to fetch compaction result
    for (String id in follower_backend_id) {
        assertTrue(triggerSingleCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id).contains("Success")); 
        waitForCompaction(backendId_to_backendIP[id], backendId_to_backendHttpPort[id], tablet_id)
    }

    // check rowsets
    checkCompactionResult.call()
    checkTabletFileCrc.call()

    qt_sql """
    select count() from  ${tableName} where cast(properties['xxxx'] as string) MATCH_ANY 'ddddd';
    """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
}
