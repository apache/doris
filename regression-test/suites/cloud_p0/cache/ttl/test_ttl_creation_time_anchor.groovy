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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_ttl_creation_time_anchor", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.beNum = 1
    options.feConfigs += [
        "cloud_cluster_check_interval_second=1",
    ]
    options.beConfigs += [
        "file_cache_enter_disk_resource_limit_mode_percent=99",
        "enable_evict_file_cache_in_advance=false",
        "file_cache_background_monitor_interval_ms=1000",
    ]

    docker(options) {
        String[][] backends = sql """ show backends """
        String backendId
        def backendIdToBackendIP = [:]
        def backendIdToBackendHttpPort = [:]
        def backendIdToBackendBrpcPort = [:]
        for (String[] backend in backends) {
            if (backend[9].equals("true")) {
                backendIdToBackendIP.put(backend[0], backend[1])
                backendIdToBackendHttpPort.put(backend[0], backend[4])
                backendIdToBackendBrpcPort.put(backend[0], backend[5])
            }
        }
        assertEquals(backendIdToBackendIP.size(), 1)

        backendId = backendIdToBackendIP.keySet()[0]
        def clearUrl = backendIdToBackendIP.get(backendId) + ":" +
                backendIdToBackendHttpPort.get(backendId) +
                """/api/file_cache?op=clear&sync=true"""

        def clearFileCache = { check_func ->
            httpTest {
                endpoint ""
                uri clearUrl
                op "get"
                body ""
                check check_func
            }
        }

        def getCacheMetrics = {
            long ttlCacheSize = 0L
            long normalQueueCacheSize = 0L
            httpTest {
                endpoint backendIdToBackendIP.get(backendId) + ":" +
                        backendIdToBackendBrpcPort.get(backendId)
                uri "/brpc_metrics"
                op "get"
                check { respCode, body ->
                    assertEquals("${respCode}".toString(), "200")
                    String out = "${body}".toString()
                    def strs = out.split('\n')
                    for (String line in strs) {
                        if (line.startsWith("#")) {
                            continue
                        }
                        if (line.contains("file_cache_ttl_cache_size")) {
                            def idx = line.indexOf(' ')
                            ttlCacheSize = line.substring(idx + 1).trim().toLong()
                        }
                        if (line.contains("file_cache_normal_queue_cache_size")) {
                            def idx = line.indexOf(' ')
                            normalQueueCacheSize = line.substring(idx + 1).trim().toLong()
                        }
                    }
                }
            }
            return [ttlCacheSize, normalQueueCacheSize]
        }

        def waitForCacheEmpty = {
            int maxTry = 15
            while (maxTry-- > 0) {
                def metrics = getCacheMetrics()
                if (metrics[0] == 0L && metrics[1] == 0L) {
                    return
                }
                sleep(2000)
            }
            def metrics = getCacheMetrics()
            assertEquals(metrics[0], 0L)
            assertEquals(metrics[1], 0L)
        }

        def waitForFreshTabletMetrics = {
            int maxTry = 30
            while (maxTry-- > 0) {
                def metrics = getCacheMetrics()
                if (metrics[0] > 0L && metrics[1] == 0L) {
                    return
                }
                sleep(2000)
            }
            def metrics = getCacheMetrics()
            assertTrue(metrics[0] > 0L)
            assertEquals(metrics[1], 0L)
        }

        def waitForOldTabletMetrics = {
            int maxTry = 30
            while (maxTry-- > 0) {
                def metrics = getCacheMetrics()
                if (metrics[0] == 0L && metrics[1] > 0L) {
                    return
                }
                sleep(2000)
            }
            def metrics = getCacheMetrics()
            assertEquals(metrics[0], 0L)
            assertTrue(metrics[1] > 0L)
        }

        def createTable = { String tableName, int ttlSeconds ->
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """
                CREATE TABLE ${tableName} (
                    id BIGINT,
                    payload STRING
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "file_cache_ttl_seconds" = "${ttlSeconds}",
                    "disable_auto_compaction" = "true"
                )
            """
        }

        def insertRows = { String tableName ->
            sql """
                INSERT INTO ${tableName}
                SELECT
                    number,
                    concat(cast(number as string), '-', repeat(md5(cast(number as string)), 16))
                FROM numbers("number" = "100000")
            """
            sql """ sync """
        }

        def scanTable = { String tableName ->
            def result = sql """ select sum(length(payload)) from ${tableName} """
            assertTrue(result[0][0].toString().toLong() > 0L)
        }

        sql """ set enable_file_cache = true """
        sql """ set disable_file_cache = false """

        def freshTableName = "ttl_creation_time_fresh_" + Math.abs(UUID.randomUUID().hashCode())
        try {
            clearFileCache.call() {
                respCode, body -> {}
            }
            waitForCacheEmpty()
            createTable(freshTableName, 180)
            insertRows(freshTableName)
            clearFileCache.call() {
                respCode, body -> {}
            }
            waitForCacheEmpty()
            scanTable(freshTableName)
            waitForFreshTabletMetrics()
        } finally {
            sql """ drop table if exists ${freshTableName} """
        }

        def oldTableName = "ttl_creation_time_old_" + Math.abs(UUID.randomUUID().hashCode())
        try {
            clearFileCache.call() {
                respCode, body -> {}
            }
            waitForCacheEmpty()
            createTable(oldTableName, 5)
            sleep(7000)
            insertRows(oldTableName)
            clearFileCache.call() {
                respCode, body -> {}
            }
            waitForCacheEmpty()
            scanTable(oldTableName)
            waitForOldTabletMetrics()
        } finally {
            sql """ drop table if exists ${oldTableName} """
        }
    }
}
