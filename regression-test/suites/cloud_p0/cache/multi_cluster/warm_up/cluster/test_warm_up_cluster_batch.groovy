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

import org.apache.doris.regression.util.WarmupMetricsUtils

suite("test_warm_up_cluster_batch", "nonConcurrent") {
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

    String sourceCluster = "regression_cluster_name0"
    String targetCluster = "regression_cluster_name1"
    def tableNames = ["customer", "supplier"]
    def dataQueries = [
        customer: "select sum(C_CUSTKEY), sum(length(C_COMMENT)) from customer",
        supplier: "select sum(s_suppkey), sum(length(s_address)) from supplier"
    ]
    def backends = sql_return_maparray("SHOW BACKENDS")
    def clusterBackends = [sourceCluster, targetCluster].collectEntries { clusterName ->
        def members = backends.findAll { be ->
            "${be.Alive}".equalsIgnoreCase("true") &&
                    parseJson(be.Tag.toString()).compute_group_name == clusterName
        }.collect { be ->
            [id: be.BackendId as Long, ip: be.Host.toString(),
             httpPort: be.HttpPort.toString(), brpcPort: be.BrpcPort.toString()]
        }
        assertTrue(!members.isEmpty(), "No alive backends in ${clusterName}")
        [(clusterName): members]
    }
    logger.info("Batch warmup backends: ${clusterBackends}")
    assertTrue(getFeConfig("enable_fetch_cluster_cache_hotspot").equalsIgnoreCase("true"),
            "Cluster warmup requires the FE cache hotspot collector to be enabled at startup")
    def hotspotInterval = getFeConfig("fetch_cluster_cache_hotspot_interval_ms")

    def waitUntil = { String description, long timeoutMs, Closure condition ->
        long deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (condition()) {
                return
            }
            sleep(1000)
        }
        assertTrue(false, "Timeout after ${timeoutMs}ms: ${description}")
    }
    def pendingLoads = [] as Set
    def warmupJobs = [] as Set
    def tableInfo = [:]
    def getGlobalTtlBytes = {
        [source: WarmupMetricsUtils.getBackendMetricSum(clusterBackends[sourceCluster], "ttl_cache_size"),
         target: WarmupMetricsUtils.getBackendMetricSum(clusterBackends[targetCluster], "ttl_cache_size")]
    }
    def getNormalCache = { String clusterName ->
        sql "use @${clusterName}"
        def beIds = clusterBackends[clusterName].collect { it.id }.join(',')
        tableInfo.collectEntries { tableName, info ->
            def rows = sql """select tablet_id, sum(size)
                from information_schema.file_cache_info
                where tablet_id in (${info.tabletIds.join(',')})
                  and be_id in (${beIds}) and lower(type) = 'normal'
                group by tablet_id"""
            def bytesByTablet = rows.collectEntries { [(it[0] as Long): it[1] as Long] }
            [(tableName): [bytes: bytesByTablet.values().sum(0L) as Long,
                          complete: info.tabletIds.every { (bytesByTablet[it] ?: 0L) > 0L }]]
        }
    }
    def getHotspotCounts = {
        sql "use @${sourceCluster}"
        def rows = sql """select table_id, partition_id, sum(query_per_day)
            from __internal_schema.cloud_cache_hotspot
            where cluster_name = '${sourceCluster}'
              and table_id in (${tableInfo.values().collect { it.id }.join(',')})
              and insert_day = current_date()
            group by table_id, partition_id"""
        rows.collectEntries { [("${it[0]}:${it[1]}".toString()): it[2] as Long] }
    }
    def waitForTargetTablets = {
        sql "use @${targetCluster}"
        def targetBes = clusterBackends[targetCluster].collectEntries { [(it.id): it] }
        def missing = []
        waitUntil("target primary routing and tablet metadata; see missing tablet log", 120000L) {
            missing = []
            tableInfo.each { tableName, info ->
                def ready = [] as Set
                def tablets = sql_return_maparray("show tablets from ${tableName}")
                tablets.each { tablet ->
                    long id = tablet.TabletId as Long
                    long primary = tablet.PrimaryBackendId as Long
                    if (info.tabletIds.contains(id) && targetBes.containsKey(primary) &&
                            (tablet.BackendId as Long) == primary) {
                        def be = targetBes[primary]
                        try {
                            // Load metadata only; querying the target table could mask a failed warmup.
                            httpTest {
                                endpoint "${be.ip}:${be.httpPort}"
                                uri "/api/meta/header/${id}"
                                op "get"
                                check { code, body ->
                                    if ((code as int) == 200) {
                                        def meta = parseJson(body.toString())
                                        // Cloud header JSON omits rowsets; data readiness is
                                        // established by the source scan and cache baseline.
                                        if ((meta.tablet_id as Long) == id &&
                                                (meta.ttl_seconds as Long) == 0L &&
                                                meta.tablet_state == "PB_RUNNING" && meta.schema != null) {
                                            ready.add(id)
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                            logger.info("Waiting for target tablet ${id} on BE ${primary}: ${e.message}")
                        }
                    }
                }
                missing.addAll(info.tabletIds - ready)
            }
            logger.info("Target tablet readiness: missing=${missing}")
            missing.isEmpty()
        }
    }

    // This case must exercise hotspot-based selection, not the all-partitions bypass.
    setFeConfigTemporary([cloud_warm_up_force_all_partitions: false]) {
        try {
            sql "use @${sourceCluster}"
            sql "set enable_sql_cache = false"
            sql "set enable_query_cache = false"
            // Only disabling SQL/query cache does not disable COUNT(*) constant rewriting.
            // The SUM expressions above force scans of actual data columns on the source BE.
            tableNames.each { tableName ->
                sql "DROP TABLE IF EXISTS ${tableName} FORCE"
                sql (new File("${context.file.parent}/../ddl/${tableName}.sql").text + """
                    PROPERTIES("disable_auto_compaction"="true", "file_cache_ttl_seconds"="0")""")
                def tabletIds = sql("show tablets from ${tableName}").collect { it[0] as Long }.unique()
                def partitionIds = sql("show partitions from ${tableName}").collect { it[0] as Long }.unique()
                long tableId = getTableId(tableName)
                assertTrue(tableId > 0L && !tabletIds.isEmpty() && !partitionIds.isEmpty(),
                        "Missing table/partition/tablet IDs for ${tableName}")
                tableInfo[tableName] = [id: tableId, tabletIds: tabletIds, partitionIds: partitionIds]
            }
            logger.info("Batch warmup tables: ${tableInfo}")
            sql "TRUNCATE TABLE __internal_schema.cloud_cache_hotspot"

            def ttlBeforeClear = getGlobalTtlBytes()
            clusterBackends.values().flatten().each { be ->
                WarmupMetricsUtils.clearFileCache(be.ip, be.httpPort)
            }
            // Held blocks can survive sync=true. Keep their baseline for diagnosis only.
            def ttlAfterClear = getGlobalTtlBytes()
            logger.info("TTL cache clear: before=${ttlBeforeClear}, after=${ttlAfterClear}, " +
                    "source_minus_target_after_clear=${ttlAfterClear.source - ttlAfterClear.target}, " +
                    "source_change_after_clear=${ttlAfterClear.source - ttlBeforeClear.source}, " +
                    "target_change_after_clear=${ttlAfterClear.target - ttlBeforeClear.target}")

            def s3BucketName = getS3BucketName()
            def s3Properties = """WITH S3 (
                "AWS_ACCESS_KEY" = "${getS3AK()}",
                "AWS_SECRET_KEY" = "${getS3SK()}",
                "AWS_ENDPOINT" = "${getS3Endpoint()}",
                "AWS_REGION" = "${getS3Region()}",
                "provider" = "${getS3Provider()}")
                PROPERTIES("exec_mem_limit"="8589934592", "load_parallelism"="3")"""
            def loadOnce = { String tableName ->
                String label = "${tableName}_${UUID.randomUUID().toString().replace('-', '')}"
                String loadSql = new File("${context.file.parent}/../ddl/${tableName}_load.sql").text
                        .replace('${s3BucketName}', s3BucketName).replace('${loadLabel}', label)
                sql (loadSql + s3Properties)
                pendingLoads.add(label)
                waitUntil("load ${label} to finish", 300000L) {
                    def rows = sql "show load where Label = '${label}'"
                    if (rows.isEmpty()) {
                        return false
                    }
                    String state = rows.last()[2].toString()
                    assertTrue(!state.equalsIgnoreCase("CANCELLED"), "Load ${label} cancelled: ${rows.last()}")
                    if (state.equalsIgnoreCase("FINISHED")) {
                        pendingLoads.remove(label)
                        return true
                    }
                    false
                }
            }
            def previousBytes = tableNames.collectEntries { [(it): 0L] }
            for (int round = 0; round < 10; round++) {
                sql "use @${sourceCluster}"
                tableNames.each { loadOnce(it) }
                if (round % 2 == 0) {
                    continue
                }
                int batch = (round + 1) / 2
                def hotspotBeforeQueries = getHotspotCounts()
                // One real scan per table is enough to advance the hotspot counters for this batch.
                tableNames.each { tableName ->
                    def result = sql(dataQueries[tableName])
                    assertTrue(!result.isEmpty() && result[0].every { it != null },
                            "Data query should read nonempty ${tableName}")
                }
                // Require hotspot counters for these new table/partition IDs to advance beyond
                // the pre-query snapshot, without assuming identical FE and BE timestamps.
                waitUntil("batch ${batch} hotspot refresh (configured interval=${hotspotInterval}ms); " +
                        "run with a short FE startup hotspot interval", 180000L) {
                    def counts = getHotspotCounts()
                    logger.info("Batch ${batch} hotspot counts: before=${hotspotBeforeQueries}, current=${counts}")
                    tableInfo.values().every { info ->
                        info.partitionIds.every { partitionId ->
                            String key = "${info.id}:${partitionId}"
                            (counts[key] ?: 0L) > (hotspotBeforeQueries[key] ?: 0L)
                        }
                    }
                }
                waitForTargetTablets()
                def sourceCache = [:]
                waitUntil("batch ${batch} source NORMAL cache to include new data", 120000L) {
                    sourceCache = getNormalCache(sourceCluster)
                    logger.info("Batch ${batch} source NORMAL cache=${sourceCache}, previous=${previousBytes}")
                    tableNames.every { sourceCache[it].complete && sourceCache[it].bytes > previousBytes[it] }
                }
                def job = sql "WARM UP CLUSTER ${targetCluster} WITH CLUSTER ${sourceCluster}"
                def jobId = job[0][0]
                warmupJobs.add(jobId)
                waitUntil("warmup job ${jobId} to finish", 300000L) {
                    def rows = sql "SHOW WARM UP JOB WHERE ID = ${jobId}"
                    if (rows.isEmpty()) {
                        return false
                    }
                    def states = rows[0].collect { it?.toString() }
                    assertTrue(!states.contains("CANCELLED"), "Warmup ${jobId} cancelled: ${rows[0]}")
                    states.contains("FINISHED")
                }
                def targetCache = [:]
                waitUntil("batch ${batch}: per-table NORMAL cache bytes must be positive and equal", 180000L) {
                    sourceCache = getNormalCache(sourceCluster)
                    targetCache = getNormalCache(targetCluster)
                    logger.info("Batch ${batch} NORMAL cache: source=${sourceCache}, target=${targetCache}")
                    tableNames.every { tableName ->
                        sourceCache[tableName].complete && targetCache[tableName].complete &&
                                sourceCache[tableName].bytes > previousBytes[tableName] &&
                                sourceCache[tableName].bytes == targetCache[tableName].bytes
                    }
                }
                previousBytes = sourceCache.collectEntries { name, cache -> [(name): cache.bytes] }
                def currentTtl = getGlobalTtlBytes()
                logger.info("Batch ${batch} global TTL=${currentTtl}, baseline_after_clear=${ttlAfterClear}, " +
                        "source_minus_target=${currentTtl.source - currentTtl.target}")
            }
        } finally {
            warmupJobs.each { jobId ->
                try {
                    def rows = sql "SHOW WARM UP JOB WHERE ID = ${jobId}"
                    if (!rows.isEmpty() && rows[0].any { it?.toString() in ["PENDING", "RUNNING"] }) {
                        sql "CANCEL WARM UP JOB WHERE ID = ${jobId}"
                    }
                } catch (Exception e) {
                    logger.warn("Failed to clean up warmup ${jobId}: ${e.message}")
                }
            }
            sql "use @${sourceCluster}"
            pendingLoads.each { label ->
                try {
                    sql "CANCEL LOAD WHERE LABEL = '${label}'"
                } catch (Exception e) {
                    logger.warn("Failed to clean up load ${label}: ${e.message}")
                }
            }
            try {
                sql "DROP TABLE IF EXISTS customer FORCE"
            } finally {
                sql "DROP TABLE IF EXISTS supplier FORCE"
            }
        }
    }
}
