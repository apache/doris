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

import java.util.Collections

suite("ex08_clear_cache_query_concurrency") {
    if (!isCloudMode()) {
        return
    }

    def customBeConfig = [
        enable_evict_file_cache_in_advance : false,
        file_cache_enter_disk_resource_limit_mode_percent : 99
    ]

    setBeConfigTemporary(customBeConfig) {
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """use @${validCluster};"""

        String tableName = "ex08_clear_cache_qry_ccy"
        def ddl = new File("""${context.file.parent}/../ddl/ex08_clear_cache_query_concurrency.sql""").text
                .replace("\${TABLE_NAME}", tableName)
        sql ddl

        String[][] backends = sql """show backends"""
        def backendIdToBackendIP = [:]
        def backendIdToBackendHttpPort = [:]
        for (String[] backend in backends) {
            if (backend[9].equals("true") && backend[19].contains("${validCluster}")) {
                backendIdToBackendIP.put(backend[0], backend[1])
                backendIdToBackendHttpPort.put(backend[0], backend[4])
            }
        }
        assertEquals(backendIdToBackendIP.size(), 1)

        def backendId = backendIdToBackendIP.keySet()[0]
        def clearUrl = backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendHttpPort.get(backendId) + "/api/file_cache?op=clear&sync=false"

        try {
            def values = (0..<1000).collect { i -> "(${i}, 'ccy_${i}')" }.join(",")
            sql """insert into ${tableName} values ${values}"""
            qt_ex08_preheat """select count(*) from ${tableName} where c1 like 'ccy_%'"""

            def queryErrors = Collections.synchronizedList(new ArrayList<String>())

            def queryThread = Thread.start {
                for (int i = 0; i < 60; i++) {
                    try {
                        def rows = sql """select count(*) from ${tableName} where c1 like 'ccy_%'"""
                        assertEquals(rows[0][0] as Long, 1000L)
                    } catch (Throwable t) {
                        queryErrors.add("${t.getMessage()}")
                    }
                    sleep(150)
                }
            }

            def clearThread = Thread.start {
                for (int i = 0; i < 15; i++) {
                    try {
                        httpTest {
                            endpoint ""
                            uri clearUrl
                            op "get"
                            body ""
                            check { respCode, body ->
                                assertEquals("${respCode}".toString(), "200")
                            }
                        }
                    } catch (Throwable t) {
                        queryErrors.add("clear api failed: ${t.getMessage()}")
                    }
                    sleep(300)
                }
            }

            queryThread.join(60000)
            clearThread.join(60000)

            assertTrue(queryErrors.isEmpty(), "query/clear concurrency has errors: ${queryErrors}")
            qt_ex08_final """select count(*) from ${tableName} where c1 like 'ccy_%'"""
        } finally {
            sql """drop table if exists ${tableName}"""
        }
    }
}

