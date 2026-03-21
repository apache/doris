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

suite("st10_drop_partition_cleanup") {
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
            def validCluster = clusters[0][0]
            sql """use @${validCluster};"""

            String tableName = "st10_drop_part_cleanup_tpl"
            def ddl = new File("""${context.file.parent}/../ddl/st10_drop_partition_cleanup.sql""").text
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
                printResponse false
                check { respCode, body ->
                    assertEquals("${respCode}".toString(), "200")
                }
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

        def waitDroppedTabletCacheInfoEmpty = { List<Long> tabletIds, long timeoutMs = 300000L, long intervalMs = 3000L ->
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

        def waitTabletCacheInfoNonEmpty = { List<Long> tabletIds, long timeoutMs = 120000L, long intervalMs = 2000L ->
            if (tabletIds.isEmpty()) {
                assertTrue(false, "tabletIds is empty")
            }
            String idList = tabletIds.join(",")
            long start = System.currentTimeMillis()
            while (System.currentTimeMillis() - start < timeoutMs) {
                def rows = sql """select tablet_id from information_schema.file_cache_info where tablet_id in (${idList}) limit 1"""
                if (!rows.isEmpty()) {
                    return
                }
                sleep(intervalMs)
            }
            assertTrue(false, "Timeout waiting tablet cache entries exist, tablets=${tabletIds}")
        }

        def getBrpcMetricSum = { String metricNameSubstr ->
            long sumValue = 0L
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
                        logger.info("metric line: ${line}")
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

        def getPartitionTabletIds = { String tbl, String partitionName ->
            def tablets = sql """show tablets from ${tbl} partition ${partitionName}"""
            assertTrue(!tablets.isEmpty(), "No tablets found for partition ${partitionName}")
            tablets.collect { it[0] as Long }
        }

            def p1Values = (0..<120).collect { i -> "(${i}, 'p1_${i}')" }.join(",")
            def p2Values = (1000..<1120).collect { i -> "(${i}, 'p2_${i}')" }.join(",")
            sql """insert into ${tableName} values ${p1Values}"""
            sql """insert into ${tableName} values ${p2Values}"""
            qt_part_preheat """select count(*) from ${tableName}"""
            sleep(5000)

            def p1Tablets = getPartitionTabletIds.call(tableName, "p1")
            def p2Tablets = getPartitionTabletIds.call(tableName, "p2")
            waitForFileCacheType.call((p1Tablets + p2Tablets).unique(), "ttl")

            final String ttlMgrSetMetric = "file_cache_ttl_mgr_tablet_id_set_size"
            long ttlMgrSetSizeBeforeDropPartition = getBrpcMetricSum.call(ttlMgrSetMetric)

            sql """alter table ${tableName} drop partition p1 force"""
            waitDroppedTabletCacheInfoEmpty.call(p1Tablets)
            waitTabletCacheInfoNonEmpty.call(p2Tablets)
            waitBrpcMetricLE.call(ttlMgrSetMetric, ttlMgrSetSizeBeforeDropPartition)

            qt_part_survivor """select count(*) from ${tableName} where k1 >= 1000"""
            sql """drop table if exists ${tableName}"""
        }
    }
}
