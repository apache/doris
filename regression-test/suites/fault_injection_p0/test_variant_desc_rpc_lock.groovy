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

suite("test_variant_desc_rpc_lock", "nonConcurrent") {
    if (isCloudMode() || !getFeConfig("enable_debug_points").toBoolean()) {
        logger.info("This suite requires a shared-nothing cluster with FE debug points enabled")
        return
    }

    sql "DROP TABLE IF EXISTS variant_desc_rpc_lock"
    sql """
        CREATE TABLE variant_desc_rpc_lock (id INT, payload VARIANT<PROPERTIES(
                "variant_enable_doc_mode" = "false")>)
        DUPLICATE KEY(id)
        PARTITION BY RANGE(id) (PARTITION p1 VALUES LESS THAN (100))
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO variant_desc_rpc_lock VALUES (0, PARSE_TO_VARIANT('{"detail":1}'))"""
    sql "SELECT * FROM variant_desc_rpc_lock LIMIT 1"

    def waitForResponse = "FetchRemoteTabletSchemaUtil.fetch.waitForResponse"
    def loadResults = [:]
    setBeConfigTemporary([txn_commit_rpc_timeout_ms: "20000"]) {
        def descStatements = ["DESC variant_desc_rpc_lock", "DESC variant_desc_rpc_lock PARTITION (p1)"]
        descStatements.eachWithIndex { descSql, index ->
            GetDebugPoint().enableDebugPointForAllFEs(waitForResponse, [timeout: "60"])
            def desc = extraThread("extended-desc-${index}") {
                sql "SET describe_extend_variant_column = true"
                sql descSql
            }
            try {
                awaitUntil(10, 0.1) {
                    sql_return_maparray("SHOW FULL PROCESSLIST").any {
                        it.Command.equalsIgnoreCase("Query") && it.Info.equalsIgnoreCase(descSql)
                                && it.Time.toInteger() >= 1
                    }
                }
                streamLoad {
                    table "variant_desc_rpc_lock"
                    set "group_commit", "off_mode"
                    set "format", "json"
                    set "read_json_by_line", "true"
                    inputText """{"id":${index + 1},"payload":{"detail":2}}"""
                    timeout 30000
                    check { result, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        logger.info("Stream Load during ${descSql}: ${result}")
                        loadResults[descSql] = parseJson(result)
                    }
                }
                assertFalse(desc.isDone(), "Extended DESC must still be waiting for its schema response")
            } finally {
                GetDebugPoint().disableDebugPointForAllFEs(waitForResponse)
            }
            def columns = desc.get(30, TimeUnit.SECONDS).collect { it[0] }
            assertTrue(columns.contains("payload.detail"), "${descSql} returned ${columns}".toString())
        }
    }
    loadResults.each { descSql, result ->
        assertEquals("Success", result.Status, "Stream Load during ${descSql}: ${result}".toString())
        assertEquals(1, result.NumberLoadedRows)
    }
}
