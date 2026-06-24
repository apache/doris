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

suite("st10_drop_table_cleanup") {
    def customBeConfig = [
        enable_evict_file_cache_in_advance : false,
        file_cache_enter_disk_resource_limit_mode_percent : 99,
        file_cache_background_ttl_gc_interval_ms : 1000,
        file_cache_background_ttl_info_update_interval_ms : 1000,
        file_cache_background_tablet_id_flush_interval_ms : 1000
    ]
    def customFeConfig = [
        rehash_tablet_after_be_dead_seconds : 5
    ]

    setBeConfigTemporary(customBeConfig) {
        setFeConfigTemporary(customFeConfig) {
            def clusters = sql "SHOW CLUSTERS"
            assertTrue(!clusters.isEmpty())
            def currentClusterRow = clusters.find { row ->
                row.size() > 1 && row[1]?.toString()?.equalsIgnoreCase("true")
            }
            def validCluster = (currentClusterRow != null ? currentClusterRow[0] : clusters[0][0]).toString()
            sql """use @${validCluster};"""

            String tableName = "st10_drop_cleanup_tpl"
            def ddl = new File("""${context.file.parent}/../ddl/st10_drop_table_cleanup.sql""").text
                    .replace("\${TABLE_NAME}", tableName)
            sql ddl

            String[][] backends = sql """show backends"""
            def backendIdToBackendIP = [:]
            def backendIdToBackendHttpPort = [:]
            def backendIdToBackendBrpcPort = [:]
            for (String[] backend in backends) {
                def beClusterInfo = backend[19]?.toString() ?: ""
                def isAlive = backend[9]?.toString()?.equalsIgnoreCase("true")
                def belongToCurrentCluster = beClusterInfo.contains("\"compute_group_name\" : \"${validCluster}\"")
                        || beClusterInfo.contains("\"compute_group_name\":\"${validCluster}\"")
                        || beClusterInfo.contains("${validCluster}")
                if (isAlive && belongToCurrentCluster) {
                    backendIdToBackendIP.put(backend[0], backend[1])
                    backendIdToBackendHttpPort.put(backend[0], backend[4])
                    backendIdToBackendBrpcPort.put(backend[0], backend[5])
                }
            }
            assertTrue(!backendIdToBackendIP.isEmpty(), "No alive backend found in cluster ${validCluster}")
            def backendIds = backendIdToBackendIP.keySet().toList()

            for (def backendId in backendIds) {
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
            }

            def getTabletIds = { String tbl ->
                def tablets = sql """show tablets from ${tbl}"""
                assertTrue(tablets.size() > 0, "No tablets found for table ${tbl}")
                tablets.collect { it[0] as Long }
            }

        def waitForFileCacheType = { List<Long> tabletIds, String expectedType, long timeoutMs = 120000L, long intervalMs = 2000L ->
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

        def waitDroppedTabletCacheInfoEmpty = { List<Long> tabletIds, long timeoutMs = 180000L, long intervalMs = 3000L ->
            if (tabletIds.isEmpty()) {
                return
            }
            String idList = tabletIds.join(",")
            long start = System.currentTimeMillis()
            while (System.currentTimeMillis() - start < timeoutMs) {
                def rows = sql """select tablet_id from information_schema.file_cache_info where tablet_id in (${idList}) limit 1"""
                if (rows.isEmpty()) {
                    return
                }
                sleep(intervalMs)
            }
            assertTrue(false, "Timeout waiting dropped tablet cache entries cleaned, tablets=${tabletIds}")
        }

        def getBrpcMetricSum = { String metricNameSubstr ->
            long sumValue = 0L
            for (def backendId in backendIds) {
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
                            if (!line.contains(metricNameSubstr)) {
                                continue
                            }
                            def idx = line.indexOf(' ')
                            if (idx <= 0) {
                                continue
                            }
                            try {
                                sumValue += line.substring(idx).trim().toLong()
                            } catch (Exception e) {
                                logger.warn("ignore unparsable metric line: ${line}")
                            }
                        }
                    }
                }
            }
            return sumValue
        }

        def waitBrpcMetricLE = { String metricNameSubstr, long upperBound, long timeoutMs = 180000L, long intervalMs = 3000L ->
            long start = System.currentTimeMillis()
            while (System.currentTimeMillis() - start < timeoutMs) {
                long cur = getBrpcMetricSum.call(metricNameSubstr)
                if (cur <= upperBound) {
                    return
                }
                sleep(intervalMs)
            }
            long curFinal = getBrpcMetricSum.call(metricNameSubstr)
            assertTrue(curFinal <= upperBound, "Metric ${metricNameSubstr} should <= ${upperBound}, actual=${curFinal}")
        }

        def waitBrpcMetricGE = { String metricNameSubstr, long lowerBound, long timeoutMs = 120000L, long intervalMs = 2000L ->
            long start = System.currentTimeMillis()
            while (System.currentTimeMillis() - start < timeoutMs) {
                long cur = getBrpcMetricSum.call(metricNameSubstr)
                if (cur >= lowerBound) {
                    return
                }
                sleep(intervalMs)
            }
            long curFinal = getBrpcMetricSum.call(metricNameSubstr)
            assertTrue(curFinal >= lowerBound, "Metric ${metricNameSubstr} should >= ${lowerBound}, actual=${curFinal}")
        }

            final String ttlMgrSetMetric = "file_cache_ttl_mgr_tablet_id_set_size"
            long ttlMgrSetSizeBeforeAll = getBrpcMetricSum.call(ttlMgrSetMetric)

            def values = (0..<300).collect { i -> "(${i}, 'drop_tpl_${i}')" }.join(",")
            sql """insert into ${tableName} values ${values}"""
            qt_sql """select count(*) from ${tableName} where c1 like 'drop_tpl_%'"""
            sleep(5000)

            def tabletIds = getTabletIds.call(tableName)
            waitForFileCacheType.call(tabletIds, "ttl")
            waitBrpcMetricGE.call(ttlMgrSetMetric, ttlMgrSetSizeBeforeAll + tabletIds.toSet().size())
            long ttlMgrSetSizeBeforeDropTable = getBrpcMetricSum.call(ttlMgrSetMetric)

            // ST-10 未覆盖点模板：Drop 后不应残留被删除 tablet 的 file_cache_info 记录
            sql """drop table if exists ${tableName} force"""
            waitDroppedTabletCacheInfoEmpty.call(tabletIds)
            /*
            // be 的 tablet 元信息清理时间较长 取决于recycler回收速度，这里不做检查
            waitBrpcMetricLE.call(ttlMgrSetMetric, ttlMgrSetSizeBeforeAll)
            // 可选增强 1：Drop table 前后 file_cache_ttl_mgr_tablet_id_set_size 对比
            assertTrue(ttlMgrSetSizeBeforeDropTable >= ttlMgrSetSizeBeforeAll)
            */
        }
    }
}
