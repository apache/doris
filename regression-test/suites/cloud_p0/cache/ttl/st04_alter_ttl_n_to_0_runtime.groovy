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

suite("st04_alter_ttl_n_to_0_runtime") {
    def customBeConfig = [
        enable_evict_file_cache_in_advance : false,
        file_cache_enter_disk_resource_limit_mode_percent : 99,
        file_cache_background_ttl_gc_interval_ms : 1000,
        file_cache_background_ttl_info_update_interval_ms : 1000,
        file_cache_background_tablet_id_flush_interval_ms : 1000
    ]

    setBeConfigTemporary(customBeConfig) {
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """use @${validCluster};"""

        String tableName = "st04_ttl_n_to_0_tpl"
        def ddl = new File("""${context.file.parent}/../ddl/st04_alter_ttl_n_to_0_runtime.sql""").text
                .replace("\${TABLE_NAME}", tableName)
        sql ddl

        String[][] backends = sql """show backends"""
        def backendIdToBackendIP = [:]
        def backendIdToBackendHttpPort = [:]
        def backendIdToBackendBrpcPort = [:]
        for (String[] backend in backends) {
            if (backend[9].equals("true") && backend[19].contains("${validCluster}")) {
                backendIdToBackendIP.put(backend[0], backend[1])
                backendIdToBackendHttpPort.put(backend[0], backend[4])
                backendIdToBackendBrpcPort.put(backend[0], backend[5])
            }
        }
        assertEquals(backendIdToBackendIP.size(), 1)

        def backendId = backendIdToBackendIP.keySet()[0]
        def clearUrl = backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendHttpPort.get(backendId) + "/api/file_cache?op=clear&sync=true"
        httpTest {
            endpoint ""
            uri clearUrl
            op "get"
            body ""
            check { respCode, body ->
                assertEquals("${respCode}".toString(), "200")
            }
        }

        def getTabletIds = { String tbl ->
            def tablets = sql """show tablets from ${tbl}"""
            assertTrue(tablets.size() > 0, "No tablets found for table ${tbl}")
            tablets.collect { it[0] as Long }
        }

        def waitForFileCacheType = { List<Long> tabletIds, String expectedType, long timeoutMs = 600000L, long intervalMs = 2000L ->
            long start = System.currentTimeMillis()
            while (System.currentTimeMillis() - start < timeoutMs) {
                boolean allMatch = true
                for (Long tabletId in tabletIds) {
                    def rows = sql """select type from information_schema.file_cache_info where tablet_id = ${tabletId}"""
                    if (rows.isEmpty()) {
                        allMatch = false
                        break
                    }
                    def mismatch = rows.find { row -> !row[0]?.toString()?.equalsIgnoreCase(expectedType) }
                    if (mismatch) {
                        allMatch = false
                        break
                    }
                }
                if (allMatch) {
                    return
                }
                sleep(intervalMs)
            }
            assertTrue(false, "Timeout waiting for ${expectedType}, tablets=${tabletIds}")
        }

        def waitTtlCacheSizeZero = { long timeoutMs = 120000L, long intervalMs = 2000L ->
            long start = System.currentTimeMillis()
            while (System.currentTimeMillis() - start < timeoutMs) {
                long ttlCacheSize = -1L
                httpTest {
                    endpoint backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendBrpcPort.get(backendId)
                    uri "/brpc_metrics"
                    op "get"
                    check { respCode, body ->
                        assertEquals("${respCode}".toString(), "200")
                        String out = "${body}".toString()
                        def lines = out.split('\n')
                        for (String line in lines) {
                            if (line.startsWith("#")) {
                                continue
                            }
                            if (line.contains("ttl_cache_size")) {
                                def idx = line.indexOf(' ')
                                ttlCacheSize = line.substring(idx).trim().toLong()
                                break
                            }
                        }
                    }
                }
                if (ttlCacheSize == 0L) {
                    return
                }
                sleep(intervalMs)
            }
            assertTrue(false, "Timeout waiting ttl_cache_size = 0")
        }

        def insertBatch = { int start, int end ->
            def values = (start..<end).collect { i -> "(${i}, 'value_${i}')" }.join(",")
            sql """insert into ${tableName} values ${values}"""
        }
        insertBatch(0, 200)
        insertBatch(200, 400)

        qt_sql """select count(*) from ${tableName} where v1 like 'value_%'"""
        sleep(5000)

        def tabletIds = getTabletIds.call(tableName)
        waitForFileCacheType.call(tabletIds, "ttl")

        // ST-04 未覆盖点模板：运行期 ALTER N->0 后，缓存类型应从 ttl 转为 normal
        sql """alter table ${tableName} set ("file_cache_ttl_seconds"="0")"""
        waitForFileCacheType.call(tabletIds, "normal")
        waitTtlCacheSizeZero.call()

        sql """drop table if exists ${tableName}"""
    }
}
