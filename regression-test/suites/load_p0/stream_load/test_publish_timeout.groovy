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

suite("test_publish_timeout", "nonConcurrent") {

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def get_be_param = { paramName ->
        // assuming paramName on all BEs have save value
        String backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List
        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == paramName) {
                return ((List<String>) ele)[2]
            }
        }
    }

    def set_be_param = { paramName, paramValue ->
        // for eache BE node, set paramName=paramValue
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
            assertTrue(out.contains("OK"))
        }
    }

    String saved_txn_commit_rpc_timeout_ms = get_be_param.call("txn_commit_rpc_timeout_ms")

    log.info("Old txn_commit_rpc_timeout_ms value is : ${saved_txn_commit_rpc_timeout_ms}".toString())
    
    // Setting txn_commit_rpc_timeout_ms =< 0 will trigger publish timeout
    set_be_param.call("txn_commit_rpc_timeout_ms", "0")

    log.info("New txn_commit_rpc_timeout_ms value is : 0".toString())

    def testTable = "all_types"
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` int(11) NULL,
              `k2` tinyint(4) NULL,
              `k3` smallint(6) NULL,
              `k4` bigint(20) NULL,
              `k5` largeint(40) NULL,
              `k6` float NULL,
              `k7` double NULL,
              `k8` decimal(9, 0) NULL,
              `k9` char(10) NULL,
              `k10` varchar(1024) NULL,
              `k11` text NULL,
              `k12` date NULL,
              `k13` datetime NULL
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """

        streamLoad {
            table "${testTable}"
            // set http request header params
            set 'label', "test_publish_timeout_" + UUID.randomUUID().toString()
            set 'format', 'csv'
            set 'column_separator', ','
            file 'all_types.csv'
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertTrue(json.Message.contains("PUBLISH_TIMEOUT"))
                assertEquals(2500, json.NumberTotalRows)
                assertEquals(2500, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql "SELECT COUNT(*) FROM ${testTable}"
    } finally {
        sql "DROP TABLE IF EXISTS ${testTable}"
    }
    set_be_param.call("txn_commit_rpc_timeout_ms", saved_txn_commit_rpc_timeout_ms)
    log.info("Set txn_commit_rpc_timeout_ms value to : ${saved_txn_commit_rpc_timeout_ms}".toString())
}
