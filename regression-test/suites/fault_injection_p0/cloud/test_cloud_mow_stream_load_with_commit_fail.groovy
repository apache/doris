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

suite("test_cloud_mow_stream_load_with_commit_fail", "nonConcurrent") {
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

    try {
        GetDebugPoint().enableDebugPointForAllFEs('FE.mow.check.lock.release', null)
        getTotalRetry.call()
        log.info("last_total_retry:" + last_total_retry)
        // store the original value
        get_be_param("mow_stream_load_commit_retry_times")
        set_be_param("mow_stream_load_commit_retry_times", "2")
        def tableName = "tbl_basic"
        // create table
        sql """ drop table if exists ${tableName}; """

        sql """
        CREATE TABLE `${tableName}` (
            `id` int(11) NOT NULL,
            `name` varchar(1100) NULL,
            `score` int(11) NULL default "-1"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "replication_num" = "1"
        );
        """
        // 1.test normal load, lock is released normally, retry times is 0
        // 1.1 first load success
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'columns', 'id, name, score'
            file "test_stream_load0.csv"

            time 10000 // limit inflight 10s
            directToBe backendIdToBackendIP.get(backendId), backendId_to_backendHttpPort.get(backendId).toInteger()

            check { result, exception, startTime, endTime ->
                log.info("Stream load result: ${result}")
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
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

            directToBe backendIdToBackendIP.get(backendId), backendId_to_backendHttpPort.get(backendId).toInteger()

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
            directToBe backendIdToBackendIP.get(backendId), backendId_to_backendHttpPort.get(backendId).toInteger()

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
            directToBe backendIdToBackendIP.get(backendId), backendId_to_backendHttpPort.get(backendId).toInteger()

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
                directToBe backendIdToBackendIP.get(backendId), backendId_to_backendHttpPort.get(backendId).toInteger()

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
                directToBe backendIdToBackendIP.get(backendId), backendId_to_backendHttpPort.get(backendId).toInteger()

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
                directToBe backendIdToBackendIP.get(backendId), backendId_to_backendHttpPort.get(backendId).toInteger()

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
                directToBe backendIdToBackendIP.get(backendId), backendId_to_backendHttpPort.get(backendId).toInteger()

                check { result, exception, startTime, endTime ->
                    log.info("Stream load result: ${result}")
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                }
            }
            getTotalRetry.call()
            assertEquals(last_total_retry + 4, total_retry)
            qt_sql8 """ select * from ${tableName} order by id"""
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
                directToBe backendIdToBackendIP.get(backendId), backendId_to_backendHttpPort.get(backendId).toInteger()

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
                directToBe backendIdToBackendIP.get(backendId), backendId_to_backendHttpPort.get(backendId).toInteger()

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
                directToBe backendIdToBackendIP.get(backendId), backendId_to_backendHttpPort.get(backendId).toInteger()

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
                directToBe backendIdToBackendIP.get(backendId), backendId_to_backendHttpPort.get(backendId).toInteger()

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
    } finally {
        reset_be_param("mow_stream_load_commit_retry_times")
        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().clearDebugPointsForAllFEs()
    }

}
