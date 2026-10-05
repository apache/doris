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

suite("st06_warmup_ttl_type_assert", "nonConcurrent") {
    // This setting is static in FE; isolate it and restore it even when a cache assertion fails.
    String originalSyncLoad = sql("select @@enable_multi_cluster_sync_load")[0][0].toString()
    assertTrue(originalSyncLoad.toLowerCase() in ["true", "false", "1", "0"],
            "Unexpected sync-load setting: ${originalSyncLoad}")
    onFinish {
        sql "set enable_multi_cluster_sync_load = ${originalSyncLoad}"
        logger.info("Restored enable_multi_cluster_sync_load=${originalSyncLoad}")
    }
    // Only explicit warm-up should populate the target cache in this case.
    sql "set enable_multi_cluster_sync_load = false"
    logger.info("Explicit warmup: disabled automatic sync load, previous=${originalSyncLoad}")

    def customBeConfig = [
        enable_evict_file_cache_in_advance : false,
        file_cache_enter_disk_resource_limit_mode_percent : 99
    ]

    setBeConfigTemporary(customBeConfig) {
        def clusters = sql "SHOW CLUSTERS"
        if (clusters.size() < 2) {
            logger.info("skip st06_warmup_ttl_type_assert, need at least 2 clusters")
            return
        }

        def sourceCluster = clusters[0][0]
        def targetCluster = clusters[1][0]
        String tableName = "st06_warmup_ttl_tpl"

        // Read both data columns on BE; count(*) may be evaluated entirely in FE.
        sql "set enable_sql_cache = false"
        sql "set enable_query_cache = false"
        String dataQuery = "select sum(k1), sum(length(v1)) from ${tableName}"

        def getClusterBackends = { String clusterName ->
            sql_return_maparray("SHOW BACKENDS").findAll { be ->
                "${be.Alive}".equalsIgnoreCase("true") &&
                        parseJson(be.Tag.toString()).compute_group_name == clusterName
            }.collectEntries { be -> [(be.BackendId as Long): be] }
        }

        def waitForTargetTablets = { List<Long> tabletIds ->
            sql "use @${targetCluster}"
            long deadline = System.currentTimeMillis() + 120000L
            def missing = tabletIds
            while (System.currentTimeMillis() < deadline) {
                def backends = getClusterBackends.call(targetCluster)
                def tablets = sql_return_maparray("show tablets from ${tableName}")
                def readyIds = []
                for (def tablet : tablets) {
                    long tabletId = tablet.TabletId as Long
                    long primaryBeId = tablet.PrimaryBackendId as Long
                    // BackendId alone can be computed on demand before the primary route exists.
                    if (!tabletIds.contains(tabletId) || !backends.containsKey(primaryBeId) ||
                            (tablet.BackendId as Long) != primaryBeId) {
                        continue
                    }
                    def be = backends[primaryBeId]
                    boolean ready = false
                    try {
                        // Fetch only tablet metadata, leaving the target data cache cold for warm-up.
                        httpTest {
                            endpoint "${be.Host}:${be.HttpPort}"
                            uri "/api/meta/header/${tabletId}"
                            op "get"
                            check { respCode, body ->
                                if ((respCode as int) == 200) {
                                    def meta = parseJson(body.toString())
                                    // Cloud header JSON omits rs_metas. Check tablet identity/state here;
                                    // the source scan and post-warm-up cache checks establish data readiness.
                                    ready = (meta.tablet_id as Long) == tabletId &&
                                            (meta.ttl_seconds as Long) == 3600L &&
                                            meta.tablet_state == "PB_RUNNING" && meta.schema != null
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.info("Waiting for tablet ${tabletId} on ${targetCluster}, " +
                                "BE ${primaryBeId}: ${e.message}")
                    }
                    if (ready) {
                        readyIds.add(tabletId)
                    }
                }
                missing = tabletIds - readyIds
                if (missing.isEmpty()) {
                    logger.info("Target tablet metadata is ready: cluster=${targetCluster}, tablets=${tabletIds}")
                    return
                }
                logger.info("Waiting for target tablet metadata: cluster=${targetCluster}, missing=${missing}")
                sleep(1000)
            }
            assertTrue(false, "Timeout waiting for target tablet metadata: cluster=${targetCluster}, missing=${missing}")
        }

        def waitForFileCacheType = { String clusterName, List<Long> tabletIds ->
            sql "use @${clusterName}"
            def backendIds = getClusterBackends.call(clusterName).keySet()
            assertTrue(!backendIds.isEmpty(), "No alive backends in ${clusterName}")
            long deadline = System.currentTimeMillis() + 600000L
            def rows = []
            def missing = tabletIds
            while (System.currentTimeMillis() < deadline) {
                rows = sql """select tablet_id, type, be_id from information_schema.file_cache_info
                              where tablet_id in (${tabletIds.join(',')})"""
                assertTrue(rows.every { backendIds.contains(it[2] as Long) },
                        "Cache query ran outside ${clusterName}: expected BEs=${backendIds}, rows=${rows}")
                missing = tabletIds.findAll { tabletId ->
                    def tabletRows = rows.findAll { (it[0] as Long) == tabletId }
                    tabletRows.isEmpty() || tabletRows.any { !it[1]?.toString()?.equalsIgnoreCase("ttl") }
                }
                if (missing.isEmpty()) {
                    return
                }
                logger.info("Waiting for TTL cache: cluster=${clusterName}, missing=${missing}, rows=${rows}")
                sleep(2000)
            }
            assertTrue(false, "Timeout waiting for TTL cache: cluster=${clusterName}, missing=${missing}, rows=${rows}")
        }

        try {
            sql """use @${sourceCluster};"""
            def ddl = new File("""${context.file.parent}/../ddl/st06_warmup_ttl_type_assert.sql""").text
                    .replace("\${TABLE_NAME}", tableName)
            sql ddl

            def values = (0..<200).collect { i -> "(${i}, 'warmup_tpl_${i}')" }.join(",")
            sql """insert into ${tableName} values ${values}"""
            qt_source_preheat dataQuery

            def sourceTablets = sql """show tablets from ${tableName}"""
            assertTrue(sourceTablets.size() > 0, "No tablets found for table ${tableName} in source cluster ${sourceCluster}")
            def sourceTabletIds = sourceTablets.collect { it[0] as Long }

            waitForTargetTablets.call(sourceTabletIds)

            // ST-06 部分覆盖点模板：显式断言 warmup 后目标集群缓存类型为 ttl
            def jobIdRows = sql """warm up cluster ${targetCluster} with table ${tableName};"""
            assertTrue(!jobIdRows.isEmpty())
            def jobId = jobIdRows[0][0]

            def waitWarmUpJobFinished = { Object id, long timeoutMs = 600000L, long intervalMs = 5000L ->
                long start = System.currentTimeMillis()
                while (System.currentTimeMillis() - start < timeoutMs) {
                    def stateRows = sql """SHOW WARM UP JOB WHERE ID = ${id}"""
                    if (stateRows.isEmpty()) {
                        sleep(intervalMs)
                        continue
                    }
                    def state = stateRows[0][3].toString()
                    if ("FINISHED".equalsIgnoreCase(state)) {
                        return
                    }
                    if ("CANCELLED".equalsIgnoreCase(state) || "FAILED".equalsIgnoreCase(state)) {
                        assertTrue(false, "Warm up job failed, id=${id}, state=${state}")
                    }
                    sleep(intervalMs)
                }
                assertTrue(false, "Timeout waiting warm up job finished, id=${id}")
            }
            waitWarmUpJobFinished.call(jobId)

            sql """use @${targetCluster};"""
            def targetTablets = sql """show tablets from ${tableName}"""
            assertTrue(targetTablets.size() > 0, "No tablets found for table ${tableName} in target cluster ${targetCluster}")
            def targetTabletIds = targetTablets.collect { it[0] as Long }
            assertTrue(sourceTabletIds.toSet() == targetTabletIds.toSet(),
                    "Tablet IDs differ between source and target, source=${sourceTabletIds}, target=${targetTabletIds}")

            // Check warm-up before reading target data, so a query cannot populate a missing cache.
            waitForFileCacheType.call(sourceCluster, sourceTabletIds)
            waitForFileCacheType.call(targetCluster, targetTabletIds)
            sql """use @${targetCluster};"""
            qt_target_query dataQuery
        } finally {
            sql """use @${sourceCluster};"""
            sql """drop table if exists ${tableName}"""
        }
    }
}
