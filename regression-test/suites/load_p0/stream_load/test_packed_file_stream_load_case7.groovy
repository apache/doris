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

import java.net.URLEncoder

suite("test_packed_file_stream_load_case7", "p0,nonConcurrent") {
    if (!isCloudMode()) {
        log.info("skip packed_file cases in non cloud mode")
        return
    }

    final String tableName = "packed_file_case7"
    final String dataFile = "cloud_p0/packed_file/merge_file_stream_load.csv"
    final int rowsPerLoad = 200
    final int rowsInP1 = 100
    final int rowsInP2 = rowsPerLoad - rowsInP1

    def createTable = {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `id` INT,
                `name` VARCHAR(50),
                INDEX idx_name(`name`) USING INVERTED PROPERTIES("parser" = "english")
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            PARTITION BY RANGE(`id`) (
                PARTITION p1 VALUES LESS THAN (101),
                PARTITION p2 VALUES LESS THAN (201)
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
    }

    def assertRowCount = { long expected ->
        def count = sql """ select count(*) from ${tableName} """
        assertEquals(expected, (count[0][0] as long))
    }

    def runLoads = { int iterations, boolean allowFailure ->
        int success = 0
        for (int i = 0; i < iterations; i++) {
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'format', 'csv'
                file dataFile
                time 120000

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    if (json.Status?.toLowerCase() == "success") {
                        assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                        success++
                    } else {
                        // 50% injection probability: expect partial successes (not all pass, not all fail)
                        assertTrue(allowFailure, "unexpected load failure: ${result}")
                    }
                }
            }
        }
        sql "sync"
        return success
    }

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

    def triggerInject = { String name, String op, String behavior, Integer code, Integer probability ->
        backendId_to_backendIP.each { String backendId, String beHost ->
            def beHttpPort = backendId_to_backendHttpPort.get(backendId)
            def queryParams = [:]
            if (op == "set") {
                queryParams.put("name", name)
                queryParams.put("behavior", behavior)
                if (behavior == "return_error") {
                    queryParams.put("code", code.toString())
                    if (probability != null) {
                        queryParams.put("probability", probability.toString())
                    }
                }
            } else if (op == "clear") {
                if (name != null && !name.isEmpty()) {
                    queryParams.put("name", name)
                }
            }

            StringBuilder urlBuilder = new StringBuilder()
            urlBuilder.append("http://${beHost}:${beHttpPort}")
            urlBuilder.append("/api/injection_point/${op}")
            if (!queryParams.isEmpty()) {
                urlBuilder.append("?")
                urlBuilder.append(queryParams.collect { k, v ->
                    "${k}=${URLEncoder.encode(v.toString(), 'UTF-8')}"
                }.join("&"))
            }
            String url = urlBuilder.toString()
            log.info("execute inject cmd: curl -sS ${url}")
            def process = ["curl", "-sS", url].execute()
            def exit = process.waitFor()
            def err = process.getErrorStream().getText()
            def out = process.getText()
            log.info("inject output: ${out}")
            assertEquals(0, exit, "failed to execute injection command, err: ${err}")
        }
    }

    def withInjection = { int probability, Closure action ->
        if (backendId_to_backendIP.isEmpty()) {
            log.info("skip injection cases because backend http info is missing")
            return
        }
        triggerInject(null, "enable", null, null, null)
        triggerInject("CloudMetaMgr::commit_rowset", "set", "return_error", -1, probability)
        try {
            action()
        } finally {
            triggerInject("CloudMetaMgr::commit_rowset", "clear", null, null, null)
            triggerInject(null, "disable", null, null, null)
        }
    }

    withInjection(50) {
        createTable()
        def successLoads = runLoads(100, true)
        // 50% injection probability: expect partial successes (not all pass, not all fail)
        assertTrue(successLoads > 0 && successLoads < 100,
                   "expected partial successes, got=${successLoads}")
        assertRowCount(successLoads * rowsPerLoad)
        sql """ ALTER TABLE ${tableName} DROP PARTITION p1 """
        sql "sync"
        assertRowCount(successLoads * rowsInP2)
        sql """ DROP TABLE IF EXISTS ${tableName} """
    }
}
