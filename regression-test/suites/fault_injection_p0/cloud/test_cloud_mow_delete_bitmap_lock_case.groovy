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

import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_cloud_mow_delete_bitmap_lock_case", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }
    GetDebugPoint().clearDebugPointsForAllFEs()

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
            def savedParams = backendId_to_params.get(id)
            if (savedParams == null || !savedParams.containsKey(paramName)) {
                logger.info("skip resetting BE config {} on {}, original value was not captured",
                        paramName, id)
                continue
            }
            def original_value = savedParams.get(paramName)
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

    def customFeConfig1 = [calculate_delete_bitmap_task_timeout_seconds: 2, meta_service_rpc_retry_times: 5]
    def customFeConfig2 = [delete_bitmap_lock_expiration_seconds: 2, meta_service_rpc_retry_times: 5]
    def customFeConfig3 = [mow_calculate_delete_bitmap_retry_times: 1]
    def customFeConfig4 = [calculate_delete_bitmap_task_timeout_seconds: 2, mow_calculate_delete_bitmap_retry_times: 1]
    def customFeConfig5 = [meta_service_rpc_retry_times: 5]
    def tableName = "tbl_basic"
    String[][] backends = sql """ show backends """
    assertTrue(backends.size() > 0)
    def backendIdToBackendIP = [:]
    def backendIdToBackendBrpcPort = [:]
    for (String[] backend in backends) {
        if (backend[9].equals("true")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendBrpcPort.put(backend[0], backend[5])
        }
    }
    // Send every stream load in this suite to one known BE. The retry metric is process-local,
    // so using the same coordinator lets each assertion measure only this suite's request path.
    String streamLoadBackendId = backendIdToBackendIP.keySet().toList().sort()[0]
    String streamLoadBackendHost = backendIdToBackendIP.get(streamLoadBackendId)
    int streamLoadBackendHttpPort = backendId_to_backendHttpPort.get(streamLoadBackendId).toInteger()

    def getMetricsMethod = { currentBackendId, check_func ->
        httpTest {
            endpoint backendIdToBackendIP.get(currentBackendId) + ":" + backendIdToBackendBrpcPort.get(currentBackendId)
            uri "/brpc_metrics"
            op "get"
            check check_func
        }
    }

    def getStreamLoadRetryCount = {
        int retryCount = -1
        getMetricsMethod.call(streamLoadBackendId) { respCode, body ->
            logger.info("get stream load retry count from backend {} resp Code {}",
                    streamLoadBackendId, "${respCode}".toString())
            assertEquals("200", "${respCode}".toString())
            String metrics = "${body}".toString()
            for (String line in metrics.split('\n')) {
                if (line.startsWith("stream_load_commit_retry_counter")) {
                    logger.info("find on backend {}: {}", streamLoadBackendId, line)
                    retryCount = line.replaceAll(
                            "stream_load_commit_retry_counter ", "").toInteger()
                    break
                }
            }
        }
        assertTrue(retryCount >= 0,
                "stream_load_commit_retry_counter is missing on backend ${streamLoadBackendId}")
        return retryCount
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
        Boolean enableTls = (context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false
        def protocol = enableTls ? "https" : "http"
        sb.append("curl -X GET ${protocol}://${be_host}:${be_http_port}")
        sb.append("/api/compaction/show?tablet_id=")
        sb.append(tablet_id)
        if (enableTls) {
            sb.append(" --cert ${context.config.otherConfigs.get("trustCert")}")
            sb.append(" --key ${context.config.otherConfigs.get("trustCAKey")}")
            sb.append(" --cacert ${context.config.otherConfigs.get("trustCACert")}")
        }

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

    def waitForCompaction = { be_host, be_http_port, tablet_id ->
        boolean running = true
        do {
            Thread.sleep(100)
            StringBuilder sb = new StringBuilder();
        Boolean enableTls = (context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false
        def protocol = enableTls ? "https" : "http"
        sb.append("curl -X GET ${protocol}://${be_host}:${be_http_port}")
        sb.append("/api/compaction/run_status?tablet_id=")
        sb.append(tablet_id)
        if (enableTls) {
            sb.append(" --cert ${context.config.otherConfigs.get("trustCert")}")
            sb.append(" --key ${context.config.otherConfigs.get("trustCAKey")}")
            sb.append(" --cacert ${context.config.otherConfigs.get("trustCACert")}")
        }

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

    def do_stream_load = {
        streamLoad {
            directToBe(streamLoadBackendHost, streamLoadBackendHttpPort)
            table "${tableName}"

            set 'column_separator', ','
            set 'columns', 'id, name, score'
            file "test_stream_load.csv"

            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                log.info("Stream load result: ${result}")
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
    }

    def do_insert_into = {
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (1, "Emily", 25),(2, "Benjamin", 35);"""
    }

    def getAlterTableState = { table_name ->
        waitForSchemaChangeDone {
            sql """ SHOW ALTER TABLE COLUMN WHERE tablename='${table_name}' ORDER BY createtime DESC LIMIT 1 """
            time 600
        }
        return true
    }

    def waitForSC = {
        Awaitility.await().atMost(60, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> {
            def res = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1"
            assert res.size() == 1
            if (res[0].State == "FINISHED" || res[0].State == "CANCELLED") {
                return true;
            }
            return false;
        });
    }

    try {
        GetDebugPoint().enableDebugPointForAllFEs('FE.mow.check.lock.release', null)
        // store the original value
        get_be_param("mow_stream_load_commit_retry_times")
        set_be_param("mow_stream_load_commit_retry_times", "2")
        // create table
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
        // 1.test normal load, lock is released normally, retry times is 0
        // 1.1 first load success
        int retryCountBeforeLoad = getStreamLoadRetryCount()
        try {
            GetDebugPoint().enableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")
            streamLoad {
                directToBe(streamLoadBackendHost, streamLoadBackendHttpPort)
                table "${tableName}"

                set 'column_separator', ','
                set 'columns', 'id, name, score'
                file "test_stream_load0.csv"

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    log.info("Stream load result: ${result}")
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                }
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")
        }
        qt_sql1 """ select * from ${tableName} order by id"""

        assertEquals(retryCountBeforeLoad, getStreamLoadRetryCount())
        // 1.2 second load success
        retryCountBeforeLoad = getStreamLoadRetryCount()
        streamLoad {
            directToBe(streamLoadBackendHost, streamLoadBackendHttpPort)
            table "${tableName}"

            set 'column_separator', ','
            set 'columns', 'id, name, score'
            file "test_stream_load1.csv"

            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                log.info("Stream load result: ${result}")
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
        qt_sql2 """ select * from ${tableName} order by id"""

        assertEquals(retryCountBeforeLoad, getStreamLoadRetryCount())


        //2. test commit fail, lock is released normally, will not retry
        // 2.1 first load will fail on fe commit phase
        GetDebugPoint().enableDebugPointForAllFEs('FE.mow.commit.exception', null)
        retryCountBeforeLoad = getStreamLoadRetryCount()
        streamLoad {
            directToBe(streamLoadBackendHost, streamLoadBackendHttpPort)
            table "${tableName}"

            set 'column_separator', ','
            set 'columns', 'id, name, score'
            file "test_stream_load2.csv"

            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                log.info("Stream load result: ${result}")
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("FE.mow.commit.exception"))
            }
        }
        qt_sql3 """ select * from ${tableName} order by id"""

        // commit fail is not DELETE_BITMAP_LOCK_ERR will not retry
        assertEquals(retryCountBeforeLoad, getStreamLoadRetryCount())

        // 2.2 second load will success because of removing exception injection
        GetDebugPoint().disableDebugPointForAllFEs('FE.mow.commit.exception')
        retryCountBeforeLoad = getStreamLoadRetryCount()
        streamLoad {
            directToBe(streamLoadBackendHost, streamLoadBackendHttpPort)
            table "${tableName}"

            set 'column_separator', ','
            set 'columns', 'id, name, score'
            file "test_stream_load2.csv"

            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                log.info("Stream load result: ${result}")
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
        qt_sql4 """ select * from ${tableName} order by id"""
        assertEquals(retryCountBeforeLoad, getStreamLoadRetryCount())

        // 3. test update delete bitmap fail, lock is released normally, will retry
        setFeConfigTemporary(customFeConfig2) {
            // 3.1 first load will fail on calculate delete bitmap timeout
            GetDebugPoint().enableDebugPointForAllBEs("CloudMetaMgr::test_update_delete_bitmap_fail")

            def now = System.currentTimeMillis()
            retryCountBeforeLoad = getStreamLoadRetryCount()
            streamLoad {
                directToBe(streamLoadBackendHost, streamLoadBackendHttpPort)
                table "${tableName}"

                set 'column_separator', ','
                set 'columns', 'id, name, score'
                file "test_stream_load3.csv"

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    log.info("Stream load result: ${result}")
                    def json = parseJson(result)
                    assertEquals("fail", json.Status.toLowerCase())
                    assertTrue(json.Message.contains("update delete bitmap failed"))
                }
            }
            def time_cost = System.currentTimeMillis() - now
            assertEquals(retryCountBeforeLoad + 2, getStreamLoadRetryCount())
            qt_sql5 """ select * from ${tableName} order by id"""

            // 3.2 second load will success because of removing timeout simulation
            GetDebugPoint().disableDebugPointForAllBEs("CloudMetaMgr::test_update_delete_bitmap_fail")
            retryCountBeforeLoad = getStreamLoadRetryCount()
            streamLoad {
                directToBe(streamLoadBackendHost, streamLoadBackendHttpPort)
                table "${tableName}"

                set 'column_separator', ','
                set 'columns', 'id, name, score'
                file "test_stream_load3.csv"

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    log.info("Stream load result: ${result}")
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                }
            }
            assertEquals(retryCountBeforeLoad, getStreamLoadRetryCount())
            qt_sql6 """ select * from ${tableName} order by id"""
        }

        //4. test wait fe lock timeout, will retry
        setFeConfigTemporary(customFeConfig1) {
            get_be_param("txn_commit_rpc_timeout_ms")
            set_be_param("txn_commit_rpc_timeout_ms", "10000")
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.tryCommitLock.timeout", [sleep_time: 5])
            // 4.1 first load will fail, because of waiting for fe lock timeout
            def now = System.currentTimeMillis()
            retryCountBeforeLoad = getStreamLoadRetryCount()
            streamLoad {
                directToBe(streamLoadBackendHost, streamLoadBackendHttpPort)
                table "${tableName}"

                set 'column_separator', ','
                set 'columns', 'id, name, score'
                file "test_stream_load4.csv"

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    log.info("Stream load result: ${result}")
                    def json = parseJson(result)
                    assertEquals("fail", json.Status.toLowerCase())
                    assertTrue(json.Message.contains("get table cloud commit lock timeout"))
                }
            }
            def time_cost = System.currentTimeMillis() - now
            assertEquals(retryCountBeforeLoad + 2, getStreamLoadRetryCount())
            assertTrue(time_cost > 10000, "wait time should bigger than total retry interval")
            qt_sql7 """ select * from ${tableName} order by id"""

            // 4.2 second load will success because of removing timeout simulation
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.tryCommitLock.timeout")
            retryCountBeforeLoad = getStreamLoadRetryCount()
            streamLoad {
                directToBe(streamLoadBackendHost, streamLoadBackendHttpPort)
                table "${tableName}"

                set 'column_separator', ','
                set 'columns', 'id, name, score'
                file "test_stream_load4.csv"

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    log.info("Stream load result: ${result}")
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                }
            }
            assertEquals(retryCountBeforeLoad, getStreamLoadRetryCount())
            qt_sql8 """ select * from ${tableName} order by id"""
            reset_be_param("txn_commit_rpc_timeout_ms")
        }
        //5. test wait delete bitmap lock timeout, lock is released normally, will retry
        GetDebugPoint().enableDebugPointForAllFEs("FE.mow.get_delete_bitmap_lock.fail")
        // 5.1 first load will fail, because of waiting for delete bitmap lock timeout
        setFeConfigTemporary(customFeConfig1) {
            def now = System.currentTimeMillis()
            retryCountBeforeLoad = getStreamLoadRetryCount()
            streamLoad {
                directToBe(streamLoadBackendHost, streamLoadBackendHttpPort)
                table "${tableName}"

                set 'column_separator', ','
                set 'columns', 'id, name, score'
                file "test_stream_load5.csv"

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    log.info("Stream load result: ${result}")
                    def json = parseJson(result)
                    assertEquals("fail", json.Status.toLowerCase())
                    assertTrue(json.Message.contains("test get_delete_bitmap_lock fail"))
                }
            }
            def time_cost = System.currentTimeMillis() - now
            assertEquals(retryCountBeforeLoad + 2, getStreamLoadRetryCount())
            qt_sql9 """ select * from ${tableName} order by id"""

            // 5.2 second load will success because of removing timeout simulation
            GetDebugPoint().disableDebugPointForAllFEs("FE.mow.get_delete_bitmap_lock.fail")
            retryCountBeforeLoad = getStreamLoadRetryCount()
            streamLoad {
                directToBe(streamLoadBackendHost, streamLoadBackendHttpPort)
                table "${tableName}"

                set 'column_separator', ','
                set 'columns', 'id, name, score'
                file "test_stream_load5.csv"

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    log.info("Stream load result: ${result}")
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                }
            }
            assertEquals(retryCountBeforeLoad, getStreamLoadRetryCount())
            qt_sql10 """ select * from ${tableName} order by id"""
        }

        //6.test calculate delete bitmap task timeout, after retry, will succeed
        setFeConfigTemporary(customFeConfig1) {
            // 6.1 first load will retry because of calculating delete bitmap timeout, and finally succeed
            GetDebugPoint().enableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")

            def now = System.currentTimeMillis()
            retryCountBeforeLoad = getStreamLoadRetryCount()
            streamLoad {
                directToBe(streamLoadBackendHost, streamLoadBackendHttpPort)
                table "${tableName}"

                set 'column_separator', ','
                set 'columns', 'id, name, score'
                file "test_stream_load6.csv"

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    log.info("Stream load result: ${result}")
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                }
            }
            def time_cost = System.currentTimeMillis() - now
            assertEquals(retryCountBeforeLoad + 1, getStreamLoadRetryCount())
            assertTrue(time_cost > 2000, "wait time should bigger than total retry interval")
            qt_sql11 """ select * from ${tableName} order by id"""

            // 6.2 second load will success and no need retry because of removing timeout simulation
            GetDebugPoint().disableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")
            retryCountBeforeLoad = getStreamLoadRetryCount()
            streamLoad {
                directToBe(streamLoadBackendHost, streamLoadBackendHttpPort)
                table "${tableName}"

                set 'column_separator', ','
                set 'columns', 'id, name, score'
                file "test_stream_load.csv"

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    log.info("Stream load result: ${result}")
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                }
            }
            assertEquals(retryCountBeforeLoad, getStreamLoadRetryCount())
            qt_sql12 """ select * from ${tableName} order by id"""
        }

        //7. test parallel load
        GetDebugPoint().disableDebugPointForAllFEs('FE.mow.check.lock.release')
        setFeConfigTemporary(customFeConfig2) {
            GetDebugPoint().enableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")
            def threads = []
            def now = System.currentTimeMillis()
            for (int k = 0; k <= 1; k++) {
                logger.info("start load thread:" + k)
                threads.add(Thread.startDaemon {
                    do_stream_load()
                })
            }
            for (Thread th in threads) {
                th.join()
            }
            def time_cost = System.currentTimeMillis() - now
            log.info("time_cost(ms): ${time_cost}")
            assertTrue(time_cost > 6000, "wait time should bigger than 6s")

            threads = []
            now = System.currentTimeMillis()
            for (int k = 0; k <= 1; k++) {
                logger.info("start insert into thread:" + k)
                threads.add(Thread.startDaemon {
                    do_insert_into()
                })
            }
            for (Thread th in threads) {
                th.join()
            }
            time_cost = System.currentTimeMillis() - now
            log.info("time_cost(ms): ${time_cost}")
            assertTrue(time_cost > 6000, "wait time should bigger than 6s")
            GetDebugPoint().disableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")

        }
        //8. test insert into timeout config
        setFeConfigTemporary(customFeConfig3) {
            try {
                GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.tryCommitLock.timeout", [sleep_time: 15])
                sql """ set global insert_visible_timeout_ms=15000; """
                sql """ INSERT INTO ${tableName} (id, name, score) VALUES (1, "Emily", 25),(2, "Benjamin", 35);"""
            } catch (Exception e) {
                logger.info("failed: " + e.getMessage())
                assertTrue(e.getMessage().contains("test get table cloud commit lock timeout"))
            } finally {
                GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.tryCommitLock.timeout")
                sql """ set global insert_visible_timeout_ms=60000; """
            }
        }
        setFeConfigTemporary(customFeConfig4) {
            try {
                GetDebugPoint().enableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")
                sql """ INSERT INTO ${tableName} (id, name, score) VALUES (1, "Emily", 25),(2, "Benjamin", 35);"""
            } catch (Exception e) {
                logger.info("failed: " + e.getMessage())
                assertTrue(e.getMessage().contains("Failed to calculate delete bitmap. Timeout"))
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")
            }
        }
        setFeConfigTemporary(customFeConfig5) {
            try {
                GetDebugPoint().enableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")
                sql """ INSERT INTO ${tableName} (id, name, score) VALUES (1, "Emily", 25),(2, "Benjamin", 35);"""
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")
            }
        }
        streamLoad {
            directToBe(streamLoadBackendHost, streamLoadBackendHttpPort)
            table "${tableName}"

            set 'column_separator', ','
            set 'columns', 'id, name, score'
            file "test_stream_load.csv"

            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                log.info("Stream load result: ${result}")
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }

        // 9. Observe a real MS lock conflict, then release the load and verify recovery.
        // Keep the original lazy-commit and packed-file settings. Neither thread start nor
        // elapsed time proves that a load has reached the commit/lock phase.
        def lockTestFeConfig = [meta_service_rpc_retry_times: 5,
                delete_bitmap_lock_expiration_seconds: 180,
                calculate_delete_bitmap_task_timeout_seconds: 180,
                enable_schema_change_retry: true, schema_change_max_retry_time: 20]
        setFeConfigTemporary(lockTestFeConfig) {
            setBeConfigTemporary([get_delete_bitmap_lock_max_retry_times: 1,
                                  disable_auto_compaction: true]) {
                final String loadBarrier = "CloudTabletCalcDeleteBitmapTask.handle.block_after_calc"
                final String alterBarrier = "CloudSchemaChangeJob::_process_delete_bitmap.before_new_inc.block"
                def tablet = sql_return_maparray("SHOW TABLETS FROM ${tableName}")[0]
                long tabletId = tablet.TabletId as long
                long tableId = getTableId(tableName)
                long dbId = getDbId()
                def backendById = sql_return_maparray("SHOW BACKENDS").collectEntries {
                    [(it.BackendId.toString()): it]
                }
                def instanceId = context.config.otherConfigs.get("multiClusterInstanceId") ?:
                        context.config.multiClusterInstance
                assertTrue(instanceId != null && !instanceId.toString().trim().isEmpty(),
                        "the deployed MS instance must be configured")
                def endpoint = context.config.metaServiceHttpAddress
                def token = context.config.metaServiceToken
                assertTrue(endpoint && token, "MS HTTP address and token must be configured")
                def lockParams = [token: token, key_type: "MetaDeleteBitmapUpdateLock",
                        instance_id: instanceId, table_id: tableId, partition_id: -1]
                String lockQuery = lockParams.collect { key, value ->
                    "${key}=${java.net.URLEncoder.encode(value.toString(), 'UTF-8')}"
                }.join('&')
                String msBase = endpoint.contains('://') ? endpoint : "http://${endpoint}"
                def readLock = {
                    // Do not use the logging HTTP helpers with a credential-bearing URL.
                    HttpURLConnection conn = new URL("${msBase}/MetaService/http/get_value?${lockQuery}").openConnection()
                    conn.connectTimeout = 5000
                    conn.readTimeout = 5000
                    conn.instanceFollowRedirects = false
                    try {
                        int status = conn.responseCode
                        def stream = status == 200 ? conn.inputStream : conn.errorStream
                        String body = stream == null ? '' : stream.withCloseable { it.getText('UTF-8') }
                        if (status == 500 && body.contains('kv not found')) {
                            return null // Before the load commits, the lock key can be absent.
                        }
                        assertEquals(200, status, "MS lock read failed for table ${tableId}")
                        return parseJson(body)
                    } catch (IOException e) {
                        throw new IOException("MS lock read failed for table ${tableId}: ${e.class.simpleName}")
                    } finally {
                        conn.disconnect()
                    }
                }
                def readMetrics = { be ->
                    def conn = new URL("http://${be.Host}:${be.BrpcPort}/brpc_metrics").openConnection()
                    conn.connectTimeout = 5000
                    conn.readTimeout = 5000
                    try {
                        def metrics = [:]
                        conn.inputStream.withCloseable { stream ->
                            stream.getText('UTF-8').eachLine { line ->
                                def fields = line.trim().split(/\s+/)
                                if (fields.size() == 2 && !line.startsWith('#')) {
                                    metrics[fields[0]] = fields[1]
                                }
                            }
                        }
                        return metrics
                    } finally {
                        conn.disconnect()
                    }
                }
                def waitForLoadTasks = {
                    // Cleanup uses absolute quiescence, never a baseline polluted by another
                    // suite's still-running task. This suite is nonConcurrent.
                    awaitUntil(60, 0.2) {
                        backendById.values().findAll { it.Alive.toString() == 'true' }.every { be ->
                            def count = readMetrics(be).task_calculate_delete_bitmap
                            assertNotNull(count, "missing calculate-delete-bitmap metric on ${be.Host}")
                            count.toLong() == 0
                        }
                    }
                }
                def readTablet = {
                    def (code, out, err) = curl('GET', tablet.CompactionStatus.toString(), null, 5, '', '', 1)
                    assertEquals(0, code, "cannot read tablet ${tabletId}")
                    parseJson(out)
                }
                def loadError = new java.util.concurrent.atomic.AtomicReference<Throwable>()
                Thread loadThread = null
                Long loadTxnId = null
                Long alterJobId = null
                boolean alterSubmitted = false
                def checkLoad = {
                    if (loadError.get() != null) {
                        throw new AssertionError("lock-holder INSERT failed", loadError.get())
                    }
                }
                def ownsLock = {
                    checkLoad()
                    def lock = readLock()
                    loadThread != null && loadThread.isAlive() && lock != null &&
                            lock.lock_id.toString().toLong() == loadTxnId &&
                            lock.expiration.toString().toLong() > System.currentTimeMillis().intdiv(1000) + 5
                }
                def startLockHolder = {
                    loadError.set(null)
                    loadTxnId = null
                    String label = "bitmap_lock_${UUID.randomUUID().toString().replace('-', '')}"
                    GetDebugPoint().enableDebugPointForAllBEs(loadBarrier,
                            [tablet_id: tabletId, timeout: 180])
                    loadThread = Thread.startDaemon {
                        try {
                            sql """INSERT INTO ${tableName} WITH LABEL `${label}` (id, name, score)
                                   VALUES (1, "Emily", 25),(2, "Benjamin", 35)"""
                        } catch (Throwable t) {
                            loadError.set(t)
                        }
                    }
                    awaitUntil(60, 0.2) {
                        checkLoad()
                        def txns = sql_return_maparray("SHOW TRANSACTION WHERE LABEL='${label}'")
                        if (txns.size() != 1) {
                            return false
                        }
                        loadTxnId = txns[0].TransactionId as long
                        ownsLock()
                    }
                    logger.info("INSERT {} owns the MS lock for table {}", loadTxnId, tableId)
                }
                def releaseLoad = {
                    GetDebugPoint().disableDebugPointForAllBEs(loadBarrier)
                    if (loadThread != null) {
                        loadThread.join(60000)
                        assertFalse(loadThread.isAlive(), "lock-holder INSERT did not exit")
                    }
                    waitForLoadTasks()
                    checkLoad()
                }
                Throwable primaryError = null
                try {
                    waitForLoadTasks()
                    // Cumulative compaction has no automatic scheduler for this table. Require
                    // a failed attempt on this tablet, then explicitly retry after releasing load.
                    long visibleVersion = sql_return_maparray("SHOW PARTITIONS FROM ${tableName}")[0].VisibleVersion as long
                    syncAndWaitTabletVersion([tablet], visibleVersion)
                    def before = readTablet()
                    startLockHolder()
                    def (code, out, err) = be_run_cumulative_compaction(
                            backendId_to_backendIP[tablet.BackendId.toString()],
                            backendId_to_backendHttpPort[tablet.BackendId.toString()], tabletId.toString())
                    assertEquals(0, code)
                    assertEquals('success', parseJson(out).status.toString().toLowerCase())
                    awaitUntil(30, 0.2) {
                        assertTrue(ownsLock(), "INSERT lost its lock before the compaction conflict")
                        def state = readTablet()
                        assertEquals(before['last cumulative success time'], state['last cumulative success time'],
                                "compaction succeeded while INSERT owns the MS lock")
                        state['last cumulative failure time'] != before['last cumulative failure time'] &&
                                state['last cumulative status'].toString().contains('DELETE_BITMAP_LOCK_ERROR')
                    }
                    releaseLoad()
                    def expectedRows = sql("SELECT id, name, score FROM ${tableName} ORDER BY id")
                    def beforeRetry = readTablet()
                    trigger_and_wait_compaction(tableName, "cumulative")
                    def afterRetry = readTablet()
                    assertEquals('[OK]', afterRetry['last cumulative status'])
                    assertTrue(afterRetry['last cumulative success time'] != beforeRetry['last cumulative success time'])
                    assertTrue(afterRetry.rowsets != beforeRetry.rowsets, "compaction did not replace its input rowsets")
                    assertTrue(afterRetry.missing_rowsets.isEmpty(), "compaction left a version gap")
                    assertEquals(expectedRows, sql("SELECT id, name, score FROM ${tableName} ORDER BY id"))

                    // Start ALTER before the load, otherwise it can wait at WAITING_TXN and
                    // never reach the MS lock. Pause the BE immediately before taking that lock.
                    GetDebugPoint().enableDebugPointForAllBEs(alterBarrier, [timeout: 180])
                    sql "ALTER TABLE ${tableName} ORDER BY (id,score,name)"
                    alterSubmitted = true
                    def alterJob = {
                        def jobs = sql_return_maparray("SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1")
                        assertEquals(1, jobs.size())
                        if (alterJobId != null) {
                            assertEquals(alterJobId, jobs[0].JobId as long, "ALTER job changed")
                        }
                        jobs[0]
                    }
                    awaitUntil(60, 0.2) {
                        def job = alterJob()
                        alterJobId = job.JobId as long
                        assertFalse(job.State in ['CANCELLED', 'FINISHED'], "ALTER did not stop before MS lock: ${job}")
                        job.State == 'RUNNING'
                    }
                    def tasks = sql_return_maparray("SHOW PROC '/jobs/${dbId}/schema_change/${alterJobId}'")
                    def task = tasks.find { (it.BaseTabletId as long) == tabletId }
                    assertNotNull(task, "no ALTER task for base tablet ${tabletId}")
                    def alterBe = backendById[task.BackendId.toString()]
                    final String backoffMetric = 'cloud_be_mow_get_dbm_lock_backoff_sleep_time'
                    def initialMetrics = readMetrics(alterBe)
                    assertNotNull(initialMetrics[backoffMetric + '_count'], "missing MS-lock backoff metric")
                    long backoffCount = initialMetrics[backoffMetric + '_count'].toLong()
                    startLockHolder()
                    GetDebugPoint().disableDebugPointForAllBEs(alterBarrier)
                    awaitUntil(60, 0.2) {
                        assertTrue(ownsLock(), "INSERT lost its lock before the ALTER conflict")
                        assertEquals('RUNNING', alterJob().State, "ALTER must retry the same job")
                        def failures = sql_return_maparray("SHOW PROC '/tasks/ALTER/${task.BackendId}'")
                        def failure = failures.find { (it.TaskSignature as long) == (task.RollupTabletId as long) }
                        def metrics = readMetrics(alterBe)
                        // PROC identifies this exact ALTER task. The BE-local metric is only
                        // corroboration that the failure went through MS LOCK_CONFLICT backoff;
                        // neither a cluster-wide counter nor WAITING_TXN is sufficient.
                        failure != null && (failure.FailedTimes as int) > 0 &&
                                metrics[backoffMetric + '_count'].toLong() > backoffCount &&
                                metrics[backoffMetric + '_latency'].toLong() > 0
                    }
                    releaseLoad()
                    awaitUntil(120, 0.5) {
                        def job = alterJob()
                        assertFalse(job.State == 'CANCELLED', "ALTER failed instead of retrying: ${job}")
                        job.State == 'FINISHED'
                    }
                    assertEquals(expectedRows, sql("SELECT id, name, score FROM ${tableName} ORDER BY id"))
                } catch (Throwable t) {
                    primaryError = t
                    throw t
                } finally {
                    // Release both barriers even if the handshake/assertion fails. Drain the
                    // actual load/ALTER before restoring timeouts or running the next suite.
                    def cleanupErrors = []
                    [loadBarrier, alterBarrier].each { point ->
                        try { GetDebugPoint().disableDebugPointForAllBEs(point) }
                        catch (Throwable t) { cleanupErrors.add(t) }
                    }
                    try { releaseLoad() } catch (Throwable t) { cleanupErrors.add(t) }
                    if (alterSubmitted) {
                        try {
                            awaitUntil(120, 0.5) {
                                def jobs = sql_return_maparray("SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1")
                                if (jobs.size() != 1) { return false }
                                if (alterJobId == null) { alterJobId = jobs[0].JobId as long }
                                (jobs[0].JobId as long) == alterJobId &&
                                        jobs[0].State in ['FINISHED', 'CANCELLED']
                            }
                        } catch (Throwable t) { cleanupErrors.add(t) }
                    }
                    if (!cleanupErrors.isEmpty()) {
                        def failure = primaryError ?: new AssertionError('lock test cleanup failed')
                        cleanupErrors.each { failure.addSuppressed(it) }
                        if (primaryError == null) { throw failure }
                    }
                }
            }
        }
        //10.test stream load will fail when not found delete bitmap cache
        setFeConfigTemporary(customFeConfig5) {
            GetDebugPoint().enableDebugPointForAllBEs("CloudTxnDeleteBitmapCache.get_tablet_txn_info.not_found")
            try {
                do_insert_into()
            } catch (Exception e) {
                logger.info("failed: " + e.getMessage())
                assertTrue(e.getMessage().contains("NOT_FOUND"))
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs("CloudTxnDeleteBitmapCache.get_tablet_txn_info.not_found")
            }
        }
        //11. test rpc timeout
        setFeConfigTemporary(customFeConfig5) {
            get_be_param("txn_commit_rpc_timeout_ms")
            set_be_param("txn_commit_rpc_timeout_ms", "5000")
            GetDebugPoint().enableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.handle.inject_sleep", [percent: "1.0", sleep: "15"])
            def threads = []
            for (int k = 0; k < 5; k++) {
                logger.info("start load thread:" + k)
                threads.add(Thread.startDaemon {
                    do_stream_load()
                })
            }
            for (Thread th in threads) {
                th.join()
            }
            GetDebugPoint().disableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.handle.inject_sleep")
            reset_be_param("txn_commit_rpc_timeout_ms")
        }
        //12. test compaction or schema change fail will release lock
        setFeConfigTemporary(customFeConfig5) {
            get_be_param("delete_bitmap_lock_expiration_seconds")
            set_be_param("delete_bitmap_lock_expiration_seconds", "60")
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (1, "AAA", 15);"""
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (2, "BBB", 25);"""
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (3, "CCC", 35);"""
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (4, "DDD", 45);"""
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (5, "EEE", 55);"""
            def tablets = sql_return_maparray """ show tablets from ${tableName}; """
            logger.info("tablets: " + tablets)
            GetDebugPoint().enableDebugPointForAllBEs("CumulativeCompaction.modify_rowsets.trigger_abort_job_failed")
            for (def tablet in tablets) {
                String tablet_id = tablet.TabletId
                def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
                logger.info("tablet: " + tablet_info)
                String trigger_backend_id = tablet.BackendId
                assertTrue(triggerCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id],
                        "cumulative", tablet_id).contains("Success"));
                waitForCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
                getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id);
            }
            def now = System.currentTimeMillis()
            do_insert_into()
            def time_cost = System.currentTimeMillis() - now
            log.info("time_cost(ms): ${time_cost}")
            assertTrue(time_cost < 10000, "wait time should less than 10s")
            GetDebugPoint().disableDebugPointForAllBEs("CumulativeCompaction.modify_rowsets.trigger_abort_job_failed")

//            GetDebugPoint().enableDebugPointForAllBEs("CloudMetaMgr::test_update_delete_bitmap_fail")
//            sql "alter table ${tableName} modify column score varchar(100);"
//            waitForSC()
//            def res = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1"
//            assert res[0].State == "CANCELLED"
//            assert res[0].Msg.contains("[DELETE_BITMAP_LOCK_ERROR]test update delete bitmap failed")
//            now = System.currentTimeMillis()
//            do_insert_into()
//            time_cost = System.currentTimeMillis() - now
//            log.info("time_cost(ms): ${time_cost}")
//            assertTrue(time_cost < 10000, "wait time should less than 10s")
            reset_be_param("delete_bitmap_lock_expiration_seconds")
        }
        //13. when get delete bitmap lock failed, compaction and sc retry times will not exceed max retry times
        setFeConfigTemporary(customFeConfig3) {
            get_be_param("get_delete_bitmap_lock_max_retry_times")
            set_be_param("get_delete_bitmap_lock_max_retry_times", "2")
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (1, "A1", 15);"""
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (2, "B2", 25);"""
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (3, "C3", 35);"""
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (4, "D4", 45);"""
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (5, "E5", 55);"""
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (6, "E6", 66);"""
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (7, "E7", 77);"""
            def tablets = sql_return_maparray """ show tablets from ${tableName}; """
            logger.info("tablets: " + tablets)
            GetDebugPoint().enableDebugPointForAllBEs("CloudMetaMgr::test_get_delete_bitmap_update_lock_conflict")
            for (def tablet in tablets) {
                String tablet_id = tablet.TabletId
                def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
                logger.info("tablet: " + tablet_info)
                String trigger_backend_id = tablet.BackendId
                getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
                int index = 0;
                while (!triggerCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id],
                        "cumulative", tablet_id).contains("Success")) {
                    if (index > 60) {
                        break;
                    }
                    Thread.sleep(2000)
                    logger.info("index: " + index)
                    index++;
                }
                assertTrue(index <= 60, "index should less than 60")
                def now = System.currentTimeMillis()
                waitForCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
                getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
                def time_cost = System.currentTimeMillis() - now
                log.info("time_cost(ms): ${time_cost}")
                assertTrue(time_cost > 3 * 500, "wait time should bigger than 1.5s")
                assertTrue(time_cost < 10 * 2000, "wait time should less than 20s")

                now = System.currentTimeMillis()
                sql "alter table ${tableName} modify column score varchar(200);"
                waitForSC()
                def res = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1"
                assert res[0].State == "FINISHED"
                time_cost = System.currentTimeMillis() - now
                log.info("time_cost(ms): ${time_cost}")
                assertTrue(time_cost > 3 * 500, "wait time should bigger than 1.5s")
                assertTrue(time_cost < 10 * 2000, "wait time should less than 20s")
            }
        }
    } finally {
        // Release fault hooks first so cleanup RPCs and subsequent suites cannot be affected.
        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().clearDebugPointsForAllFEs()
        reset_be_param("mow_stream_load_commit_retry_times")
        reset_be_param("txn_commit_rpc_timeout_ms")
        reset_be_param("delete_bitmap_lock_expiration_seconds")
        reset_be_param("get_delete_bitmap_lock_max_retry_times")
    }

}
