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

suite("test_stream_load_err_log_limit", "p0, nonConcurrent") {
    sql "show tables"

    def tableName = "test_stream_load_err_log_limit_table"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int NOT NULL,
            `k2` varchar(20) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_param = { paramName, paramValue ->
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
            assertTrue(out.contains("OK"))
        }
    }

    try {
        set_be_param.call("load_error_log_limit_bytes", "100")

        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            set 'columns', 'k1, k2, k3'
            file 'test_stream_load_err_log_limit.csv'

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                def (code, out, err) = curl("GET", json.ErrorURL)
                log.info("error result: " + out)
                def checkError = out.contains("error log is too long")
                assertTrue(checkError)
                log.info("url: " + json.ErrorURL)
            }
        }
    } finally {
        set_be_param.call("load_error_log_limit_bytes", "209715200")
    }

    sql "sync"
    qt_sql "select count(*) from ${tableName}"
    sql """ DROP TABLE IF EXISTS ${tableName} """
}