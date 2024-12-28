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

suite("test_cloud_mow_stream_load_with_timeout", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }
    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

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

    def customFeConfig = [
            calculate_delete_bitmap_task_timeout_seconds: 2,
            meta_service_rpc_retry_times                : 5
    ]

    // store the original value
    get_be_param("mow_stream_load_commit_retry_times")

    def tableName = "tbl_basic"
    // test fe release lock when calculating delete bitmap timeout
    setFeConfigTemporary(customFeConfig) {
        try {
            // disable retry to make this problem more clear
            set_be_param("mow_stream_load_commit_retry_times", "1")
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
            // this streamLoad will fail on calculate delete bitmap timeout
            GetDebugPoint().enableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'columns', 'id, name, score'
                file "test_stream_load.csv"

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    log.info("Stream load result: ${result}")
                    def json = parseJson(result)
                    assertEquals("fail", json.Status.toLowerCase())
                    assertTrue(json.Message.contains("Timeout"))
                }
            }
            qt_sql """ select * from ${tableName} order by id"""

            // this streamLoad will success because of removing timeout simulation
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
            qt_sql """ select * from ${tableName} order by id"""
        } finally {
            reset_be_param("mow_stream_load_commit_retry_times")
            GetDebugPoint().disableDebugPointForAllBEs("CloudEngineCalcDeleteBitmapTask.execute.enable_wait")
            sql "DROP TABLE IF EXISTS ${tableName};"
            GetDebugPoint().clearDebugPointsForAllBEs()
        }

    }

    //test fe don't send calculating delete bitmap task to be twice when txn is committed or visible
    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.commitTransaction.timeout")
    try {
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
        qt_sql """ select * from ${tableName} order by id"""
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.commitTransaction.timeout")
        sql "DROP TABLE IF EXISTS ${tableName};"
        GetDebugPoint().clearDebugPointsForAllFEs()
    }

}