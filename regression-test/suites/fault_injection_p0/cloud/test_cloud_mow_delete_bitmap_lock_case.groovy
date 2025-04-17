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

    def customFeConfig1 = [calculate_delete_bitmap_task_timeout_seconds: 2, meta_service_rpc_retry_times: 5]
    def customFeConfig2 = [delete_bitmap_lock_expiration_seconds: 2, meta_service_rpc_retry_times: 5]
    def customFeConfig3 = [mow_calculate_delete_bitmap_retry_times: 1]
    def customFeConfig4 = [calculate_delete_bitmap_task_timeout_seconds: 2, mow_calculate_delete_bitmap_retry_times: 1]
    def customFeConfig5 = [meta_service_rpc_retry_times: 5]
    def tableName = "tbl_basic"
    String[][] backends = sql """ show backends """
    assertTrue(backends.size() > 0)
    String backendId;
    def backendIdToBackendIP = [:]
    def backendIdToBackendBrpcPort = [:]
    for (String[] backend in backends) {
        if (backend[9].equals("true")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendBrpcPort.put(backend[0], backend[5])
        }
    }

    backendId = backendIdToBackendIP.keySet()[0]
    def getMetricsMethod = { check_func ->
        httpTest {
            endpoint backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendBrpcPort.get(backendId)
            uri "/brpc_metrics"
            op "get"
            check check_func
        }
    }

    int total_retry = 0;
    int last_total_retry = -1;

    def getTotalRetry = {
        getMetricsMethod.call() { respCode, body ->
            logger.info("get total retry resp Code {}", "${respCode}".toString())
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            for (String line in strs) {
                if (line.startsWith("stream_load_commit_retry_counter")) {
                    logger.info("find: {}", line)
                    total_retry = line.replaceAll("stream_load_commit_retry_counter ", "").toInteger()
                    if (last_total_retry < 0) {
                        last_total_retry = total_retry
                    }
                    break
                }
            }
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
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
    }

    def do_stream_load = {
        streamLoad {
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
        getTotalRetry.call()
        log.info("last_total_retry:" + last_total_retry)
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
        try {
            GetDebugPoint().enableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")
            streamLoad {
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

        getTotalRetry.call()
        assertEquals(last_total_retry, total_retry)
        // 1.2 second load success
        streamLoad {
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

        getTotalRetry.call()
        assertEquals(last_total_retry, total_retry)


        //2. test commit fail, lock is released normally, will not retry
        // 2.1 first load will fail on fe commit phase
        GetDebugPoint().enableDebugPointForAllFEs('FE.mow.commit.exception', null)
        streamLoad {
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
        getTotalRetry.call()
        assertEquals(last_total_retry, total_retry)

        // 2.2 second load will success because of removing exception injection
        GetDebugPoint().disableDebugPointForAllFEs('FE.mow.commit.exception')
        streamLoad {
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
        getTotalRetry.call()
        assertEquals(last_total_retry, total_retry)

        // 3. test update delete bitmap fail, lock is released normally, will retry
        setFeConfigTemporary(customFeConfig2) {
            // 3.1 first load will fail on calculate delete bitmap timeout
            GetDebugPoint().enableDebugPointForAllBEs("CloudMetaMgr::test_update_delete_bitmap_fail")

            def now = System.currentTimeMillis()
            streamLoad {
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
            getTotalRetry.call()
            assertEquals(last_total_retry + 2, total_retry)
            qt_sql5 """ select * from ${tableName} order by id"""

            // 3.2 second load will success because of removing timeout simulation
            GetDebugPoint().disableDebugPointForAllBEs("CloudMetaMgr::test_update_delete_bitmap_fail")
            streamLoad {
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
            getTotalRetry.call()
            assertEquals(last_total_retry + 2, total_retry)
            qt_sql6 """ select * from ${tableName} order by id"""
        }

        //4. test wait fe lock timeout, will retry
        setFeConfigTemporary(customFeConfig1) {
            get_be_param("txn_commit_rpc_timeout_ms")
            set_be_param("txn_commit_rpc_timeout_ms", "10000")
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.tryCommitLock.timeout", [sleep_time: 5])
            // 4.1 first load will fail, because of waiting for fe lock timeout
            def now = System.currentTimeMillis()
            streamLoad {
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
            getTotalRetry.call()
            assertEquals(last_total_retry + 4, total_retry)
            assertTrue(time_cost > 10000, "wait time should bigger than total retry interval")
            qt_sql7 """ select * from ${tableName} order by id"""

            // 4.2 second load will success because of removing timeout simulation
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.tryCommitLock.timeout")
            streamLoad {
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
            getTotalRetry.call()
            assertEquals(last_total_retry + 4, total_retry)
            qt_sql8 """ select * from ${tableName} order by id"""
            reset_be_param("txn_commit_rpc_timeout_ms")
        }
        //5. test wait delete bitmap lock timeout, lock is released normally, will retry
        GetDebugPoint().enableDebugPointForAllFEs("FE.mow.get_delete_bitmap_lock.fail")
        // 5.1 first load will fail, because of waiting for delete bitmap lock timeout
        setFeConfigTemporary(customFeConfig1) {
            def now = System.currentTimeMillis()
            streamLoad {
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
            getTotalRetry.call()
            assertEquals(last_total_retry + 6, total_retry)
            qt_sql9 """ select * from ${tableName} order by id"""

            // 5.2 second load will success because of removing timeout simulation
            GetDebugPoint().disableDebugPointForAllFEs("FE.mow.get_delete_bitmap_lock.fail")
            streamLoad {
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
            getTotalRetry.call()
            assertEquals(last_total_retry + 6, total_retry)
            qt_sql10 """ select * from ${tableName} order by id"""
        }

        //6.test calculate delete bitmap task timeout, after retry, will succeed
        setFeConfigTemporary(customFeConfig1) {
            // 6.1 first load will retry because of calculating delete bitmap timeout, and finally succeed
            GetDebugPoint().enableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")

            def now = System.currentTimeMillis()
            streamLoad {
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
            getTotalRetry.call()
            assertEquals(last_total_retry + 7, total_retry)
            assertTrue(time_cost > 2000, "wait time should bigger than total retry interval")
            qt_sql11 """ select * from ${tableName} order by id"""

            // 6.2 second load will success and no need retry because of removing timeout simulation
            GetDebugPoint().disableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")
            streamLoad {
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
            getTotalRetry.call()
            assertEquals(last_total_retry + 7, total_retry)
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

        //9. when load hold delete bitmap lock, compaction and schema change will fail and retry
        setFeConfigTemporary(customFeConfig5) {
            GetDebugPoint().enableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.handle.inject_sleep", [percent: "1.0", sleep: "10"])
            Thread.startDaemon {
                do_insert_into()
            }
            def tablets = sql_return_maparray """ show tablets from ${tableName}; """
            logger.info("tablets: " + tablets)
            for (def tablet in tablets) {
                String tablet_id = tablet.TabletId
                def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
                logger.info("tablet: " + tablet_info)
                String trigger_backend_id = tablet.BackendId
                def now = System.currentTimeMillis()
                assertTrue(triggerCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id],
                        "cumulative", tablet_id).contains("Success"));
                waitForCompaction(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
                getTabletStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id);
                def time_cost = System.currentTimeMillis() - now
                log.info("time_cost(ms): ${time_cost}")
                assertTrue(time_cost > 10000, "wait time should bigger than 10s")
            }
            Thread.startDaemon {
                do_insert_into()
            }
            def now = System.currentTimeMillis()
            sql """ alter table ${tableName} order by (id,score,name); """
            assertTrue(getAlterTableState(tableName), "schema change should success")
            def time_cost = System.currentTimeMillis() - now
            log.info("time_cost(ms): ${time_cost}")
            assertTrue(time_cost > 10000, "wait time should bigger than 10s")
            GetDebugPoint().disableDebugPointForAllFEs("CloudEngineCalcDeleteBitmapTask.handle.inject_sleep")
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
        reset_be_param("mow_stream_load_commit_retry_times")
        reset_be_param("txn_commit_rpc_timeout_ms")
        reset_be_param("delete_bitmap_lock_expiration_seconds")
        reset_be_param("get_delete_bitmap_lock_max_retry_times")
        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().clearDebugPointsForAllFEs()
    }

}
