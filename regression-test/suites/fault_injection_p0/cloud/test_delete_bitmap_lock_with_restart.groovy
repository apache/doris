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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_delete_bitmap_lock_with_restart", "docker") {
    if (!isCloudMode()) {
        return
    }
    def options = new ClusterOptions()
    options.feConfigs += [
            'cloud_cluster_check_interval_second=1',
            'sys_log_verbose_modules=org',
            'heartbeat_interval_second=1'
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.enableDebugPoints()
    options.cloudMode = true

    def customFeConfig1 = [meta_service_rpc_retry_times: 5]
    def tableName = "tbl_basic"
    def do_stream_load = {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'columns', 'id, name, score'
            file "test_stream_load.csv"

            check { result, exception, startTime, endTime ->
                log.info("Stream load result: ${result}")
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
    }
    //1. load
    docker(options) {
        sql """ drop table if exists ${tableName}; """

        sql """
        CREATE TABLE `${tableName}` (
            `id` int(11) NOT NULL,
            `name` varchar(10) NULL,
            `score` int(11) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "disable_auto_compaction" = "true",
            "enable_unique_key_merge_on_write" = "true",
            "replication_num" = "1"
        );
        """
        do_stream_load()
        GetDebugPoint().enableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.handle.inject_sleep", [percent: "1.0", sleep: "15"])
        Thread.startDaemon {
            do_stream_load()
        }
        // 1. load + restart fe
        cluster.restartFrontends()
        def now = System.currentTimeMillis()
        do_stream_load()
        def time_cost = System.currentTimeMillis() - now
        log.info("time_cost(ms): ${time_cost}")
        assertTrue(time_cost > 30000, "wait time should bigger than 30s")

        // 2. load + restart be

        Thread.startDaemon {
            do_stream_load()
        }
        cluster.restartBackends()
        now = System.currentTimeMillis()
        do_stream_load()
        time_cost = System.currentTimeMillis() - now
        log.info("time_cost(ms): ${time_cost}")
        assertTrue(time_cost < 10000, "wait time should bigger than 10s")
    }
    //2. compaction
    options.beConfigs += [
            'delete_bitmap_lock_expiration_seconds=60',
    ]
    docker(options) {
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

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
        def waitForCompaction = { be_host, be_http_port, tablet_id ->
            boolean running = true
            do {
                Thread.sleep(100)
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
                if (code == 0) {
                    def compactionStatus = parseJson(out.trim())
                    assertEquals("success", compactionStatus.status.toLowerCase())
                    running = compactionStatus.run_status
                } else {
                    break
                }
            } while (running)
        }

        sql """ drop table if exists ${tableName}; """

        sql """
        CREATE TABLE `${tableName}` (
            `id` int(11) NOT NULL,
            `name` varchar(10) NULL,
            `score` int(11) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "disable_auto_compaction" = "true",
            "enable_unique_key_merge_on_write" = "true",
            "replication_num" = "1"
        );
        """
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (1, "AAA", 15);"""
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (2, "BBB", 25);"""
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (3, "CCC", 35);"""
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (4, "DDD", 45);"""
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (5, "EEE", 55);"""

        GetDebugPoint().enableDebugPointForAllBEs("CloudMetaMgr.get_delete_bitmap_update_lock.inject_sleep", [percent: "1.0", sleep: "10"])
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
        logger.info("tablets: " + tablets)
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
            logger.info("tablet: " + tablet_info)
            String trigger_backend_id = tablet.BackendId
            getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
            assertTrue(triggerCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id],
                    "cumulative", tablet_id).contains("Success"))

        }
        // 1. compaction + restart fe
        cluster.restartFrontends()
        def now = System.currentTimeMillis()
        do_stream_load()
        def time_cost = System.currentTimeMillis() - now
        log.info("time_cost(ms): ${time_cost}")
        assertTrue(time_cost < 10000, "wait time should less than 10s")
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            String trigger_backend_id = tablet.BackendId
            waitForCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
            getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
        }
        sleep(30000)
        context.reconnectFe()
        // 2. compaction + restart be
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (1, "AAA", 15);"""
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (2, "BBB", 25);"""
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (3, "CCC", 35);"""
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (4, "DDD", 45);"""
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (5, "EEE", 55);"""
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
            logger.info("tablet: " + tablet_info)
            String trigger_backend_id = tablet.BackendId
            getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
            assertTrue(triggerCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id],
                    "cumulative", tablet_id).contains("Success"))

        }
        cluster.restartBackends()
        now = System.currentTimeMillis()
        do_stream_load()
        time_cost = System.currentTimeMillis() - now
        log.info("time_cost(ms): ${time_cost}")
        assertTrue(time_cost > 10000, "wait time should less than 10s")
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            String trigger_backend_id = tablet.BackendId
            waitForCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
            getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
        }
    }
    //3. sc
    docker(options) {
        def getJobState = {
            def res = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1"
            assert res.size() == 1
            log.info("res:" + res[0].State)
            return res[0].State
        }
        sql """ drop table if exists ${tableName}; """

        sql """
        CREATE TABLE `${tableName}` (
            `id` int(11) NOT NULL,
            `name` varchar(10) NULL,
            `score` int(11) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "disable_auto_compaction" = "true",
            "enable_unique_key_merge_on_write" = "true",
            "replication_num" = "1"
        );
        """
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (1, "AAA", 15);"""
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (2, "BBB", 25);"""
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (3, "CCC", 35);"""
        GetDebugPoint().enableDebugPointForAllBEs("CloudMetaMgr.get_delete_bitmap_update_lock.inject_sleep", [percent: "1.0", sleep: "10"])
        sql "alter table ${tableName} modify column score varchar(100);"
        // 1. sc + restart fe
        cluster.restartFrontends()
        context.reconnectFe()
        for (int i = 0; i < 30; i++) {
            log.info("i: ${i}")
            try {
                def now = System.currentTimeMillis()
                sql """ INSERT INTO ${tableName} (id, name, score) VALUES (1, "AAA", 15);"""
                def time_cost = System.currentTimeMillis() - now
                log.info("time_cost(ms): ${time_cost}")
                assertTrue(time_cost < 10000, "wait time should less than 10s")
                break
            } catch (Exception e) {
                log.info("Exception:" + e)
                Thread.sleep(2000)
            }
        }
        int max_try_time = 30
        while (max_try_time--) {
            def result = getJobState(tableName)
            if (result == "FINISHED" || result == "CANCELLED") {
                break
            } else {
                Thread.sleep(1000)
            }
        }
        // 2. sc + restart be
        sql "alter table ${tableName} modify column score varchar(200);"
        cluster.restartBackends()
        def now = System.currentTimeMillis()
        do_stream_load()
        def time_cost = System.currentTimeMillis() - now
        log.info("time_cost(ms): ${time_cost}")
        assertTrue(time_cost > 10000, "wait time should less than 10s")
        max_try_time = 30
        while (max_try_time--) {
            def result = getJobState(tableName)
            if (result == "FINISHED" || result == "CANCELLED") {
                break
            } else {
                Thread.sleep(1000)
            }
        }
    }
}