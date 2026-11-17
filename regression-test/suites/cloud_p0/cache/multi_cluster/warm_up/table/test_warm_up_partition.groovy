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

suite("test_warm_up_partition", "nonConcurrent") {
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
    String tableName = "customer"
    def partitionNames = ["p1", "p2", "p3"]
    long ttlSeconds = 12000L
    def partitionInfo = [:]
    def warmupJobs = [] as Set
    def pendingLoads = [] as Set
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
    logger.info("Partition warmup backends: ${clusterBackends}")

    def waitUntil = { String description, long timeoutMs, Closure condition ->
        long deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (condition()) {
                return
            }
            sleep(1000)
        }
        assertTrue(false, "Timeout after ${timeoutMs}ms: ${description}; see last state in log")
    }
    def getGlobalTtlBytes = {
        [source: WarmupMetricsUtils.getBackendMetricSum(clusterBackends[sourceCluster], "ttl_cache_size"),
         target: WarmupMetricsUtils.getBackendMetricSum(clusterBackends[targetCluster], "ttl_cache_size")]
    }
    def getPartitionCache = { String clusterName ->
        sql "use @${clusterName}"
        def tabletIds = partitionInfo.values().collectMany { it.tabletIds }
        def beIds = clusterBackends[clusterName].collect { it.id }
        // Read all cache types: a NORMAL block in an untouched partition is also unexpected.
        def rows = sql """select tablet_id, lower(type), sum(size), count(*)
            from information_schema.file_cache_info
            where tablet_id in (${tabletIds.join(',')}) and be_id in (${beIds.join(',')})
            group by tablet_id, lower(type)"""
        partitionInfo.collectEntries { name, info ->
            def partitionRows = rows.findAll { (it[0] as Long) in info.tabletIds }
            def ttlRows = partitionRows.findAll { it[1]?.toString() == "ttl" }
            def ttlByTablet = ttlRows.collectEntries { [(it[0] as Long): it[2] as Long] }
            [(name): [ttlBytes: ttlByTablet.values().sum(0L) as Long, ttlByTablet: ttlByTablet,
                      complete: info.tabletIds.every { (ttlByTablet[it] ?: 0L) > 0L },
                      blocks: partitionRows.sum(0L) { it[3] as Long },
                      nonTtlBlocks: partitionRows.findAll { it[1]?.toString() != "ttl" }
                              .sum(0L) { it[3] as Long }]]
        }
    }
    def waitForTargetTablets = {
        sql "use @${targetCluster}"
        def targetBes = clusterBackends[targetCluster].collectEntries { [(it.id): it] }
        waitUntil("target primary routing and tablet metadata", 120000L) {
            def missing = [:]
            partitionInfo.each { name, info ->
                def ready = [] as Set
                def tablets = sql_return_maparray("show tablets from ${tableName} partition ${name}")
                tablets.each { tablet ->
                    long id = tablet.TabletId as Long
                    long primary = tablet.PrimaryBackendId as Long
                    if (info.tabletIds.contains(id) && targetBes.containsKey(primary) &&
                            (tablet.BackendId as Long) == primary) {
                        def be = targetBes[primary]
                        try {
                            // Fetch metadata only; a target data query could populate the cache itself.
                            httpTest {
                                endpoint "${be.ip}:${be.httpPort}"
                                uri "/api/meta/header/${id}"
                                op "get"
                                check { code, body ->
                                    if ((code as int) == 200) {
                                        def meta = parseJson(body.toString())
                                        // Cloud header JSON omits rowsets. The source baseline and
                                        // per-partition warm-up assertions verify the loaded data.
                                        if ((meta.tablet_id as Long) == id &&
                                                (meta.ttl_seconds as Long) == ttlSeconds &&
                                                meta.tablet_state == "PB_RUNNING" && meta.schema != null) {
                                            ready.add(id)
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                            logger.info("Waiting for ${name} tablet ${id} on BE ${primary}: ${e.message}")
                        }
                    }
                }
                missing[name] = [tablets: info.tabletIds - ready, loadedSourceVersion: info.version]
            }
            logger.info("Target tablet readiness: ${missing}")
            missing.values().every { it.tablets.isEmpty() }
        }
    }

    setBeConfigTemporary([
        enable_evict_file_cache_in_advance: false,
        file_cache_enter_disk_resource_limit_mode_percent: 99
    ]) {
        try {
            sql "use @${sourceCluster}"
            sql "DROP TABLE IF EXISTS ${tableName} FORCE"
            sql (new File("${context.file.parent}/../ddl/customer_with_partition.sql").text + """
                PROPERTIES("file_cache_ttl_seconds"="${ttlSeconds}", "disable_auto_compaction"="true")""")
            partitionNames.each { name ->
                def ids = sql("show tablets from ${tableName} partition ${name}")
                        .collect { it[0] as Long }.unique()
                assertTrue(!ids.isEmpty(), "Missing tablets for ${name}")
                partitionInfo[name] = [tabletIds: ids]
            }

            def ttlBeforeClear = getGlobalTtlBytes()
            clusterBackends.values().flatten().each { be ->
                WarmupMetricsUtils.clearFileCache(be.ip, be.httpPort)
            }
            // sync=true can leave held blocks from other tables. Keep global totals for diagnosis only.
            def ttlAfterClear = getGlobalTtlBytes()
            logger.info("TTL cache clear: before=${ttlBeforeClear}, after=${ttlAfterClear}, " +
                    "source_minus_target_after_clear=${ttlAfterClear.source - ttlAfterClear.target}, " +
                    "source_change_after_clear=${ttlAfterClear.source - ttlBeforeClear.source}, " +
                    "target_change_after_clear=${ttlAfterClear.target - ttlBeforeClear.target}")

            String loadLabel = "customer_${UUID.randomUUID().toString().replace('-', '')}"
            String loadSql = new File("${context.file.parent}/../ddl/customer_load.sql").text
                    .replace('${s3BucketName}', getS3BucketName()).replace('${loadLabel}', loadLabel)
            sql (loadSql + """WITH S3 (
                "AWS_ACCESS_KEY" = "${getS3AK()}", "AWS_SECRET_KEY" = "${getS3SK()}",
                "AWS_ENDPOINT" = "${getS3Endpoint()}", "AWS_REGION" = "${getS3Region()}",
                "provider" = "${getS3Provider()}")
                PROPERTIES("exec_mem_limit"="8589934592", "load_parallelism"="3")""")
            pendingLoads.add(loadLabel)
            waitUntil("load ${loadLabel} to finish", 300000L) {
                def rows = sql "show load where Label = '${loadLabel}'"
                logger.info("Partition warmup load state: ${rows}")
                if (rows.isEmpty()) {
                    return false
                }
                String state = rows.last()[2].toString()
                assertTrue(!state.equalsIgnoreCase("CANCELLED"), "Load ${loadLabel} cancelled: ${rows.last()}")
                if (state.equalsIgnoreCase("FINISHED")) {
                    pendingLoads.remove(loadLabel)
                    return true
                }
                false
            }
            def partitions = sql_return_maparray("show partitions from ${tableName}")
            partitionNames.each { name ->
                def partition = partitions.find { it.PartitionName.toString() == name }
                assertTrue(partition != null && (partition.VisibleVersion as Long) > 1L,
                        "Missing loaded version for ${name}: ${partitions}")
                partitionInfo[name].version = partition.VisibleVersion as Long
            }
            logger.info("Loaded partitions: ${partitionInfo}")
            waitForTargetTablets()

            def sourceBaseline = [:]
            long stableSince = System.currentTimeMillis()
            waitUntil("source partitions to have stable nonempty TTL cache", 120000L) {
                def current = getPartitionCache(sourceCluster)
                logger.info("Source partition cache before warmup: ${current}")
                if (current != sourceBaseline) {
                    sourceBaseline = current
                    stableSince = System.currentTimeMillis()
                }
                current.values().every { it.complete && it.nonTtlBlocks == 0L } &&
                        System.currentTimeMillis() - stableSince >= 3000L
            }
            def targetBefore = getPartitionCache(targetCluster)
            assertTrue(targetBefore.values().every { it.blocks == 0L },
                    "Target partitions must be cold before warmup: ${targetBefore}")

            def warmedPartitions = [] as Set
            partitionNames.each { name ->
                // Explicit partition warmup does not require hotspot queries or hotspot collection.
                def job = sql "WARM UP CLUSTER ${targetCluster} WITH TABLE ${tableName} PARTITION ${name}"
                assertTrue(!job.isEmpty(), "No warmup job returned for ${name}")
                def jobId = job[0][0]
                warmupJobs.add(jobId)
                waitUntil("warmup ${name} job ${jobId} to finish", 120000L) {
                    def rows = sql "SHOW WARM UP JOB WHERE ID = ${jobId}"
                    logger.info("Partition ${name} warmup job ${jobId}: ${rows}")
                    if (rows.isEmpty()) {
                        return false
                    }
                    def states = rows[0].collect { it?.toString() }
                    assertTrue(!states.contains("CANCELLED"), "Warmup ${name} job ${jobId} cancelled: ${rows[0]}")
                    states.contains("FINISHED")
                }
                warmedPartitions.add(name)
                waitUntil("after ${name}: partition TTL cache must match source and untouched partitions stay cold",
                        120000L) {
                    def source = getPartitionCache(sourceCluster)
                    def target = getPartitionCache(targetCluster)
                    logger.info("After ${name} job ${jobId}: warmed=${warmedPartitions}, " +
                            "source=${source}, target=${target}, baseline=${sourceBaseline}")
                    // Fail immediately on scope/type violations; waiting must not hide unintended warmup.
                    partitionNames.each { partitionName ->
                        if (!warmedPartitions.contains(partitionName)) {
                            assertTrue(target[partitionName].blocks == 0L,
                                    "Untouched partition ${partitionName} has cache after ${name}: ${target}")
                        } else {
                            assertTrue(target[partitionName].nonTtlBlocks == 0L,
                                    "Expected only TTL cache for ${partitionName}: ${target[partitionName]}")
                        }
                    }
                    partitionNames.every { partitionName ->
                        def src = source[partitionName]
                        def dst = target[partitionName]
                        src.complete && src.nonTtlBlocks == 0L &&
                                src.ttlByTablet == sourceBaseline[partitionName].ttlByTablet &&
                                (!warmedPartitions.contains(partitionName) ||
                                 (dst.complete && dst.ttlBytes == src.ttlBytes && dst.ttlByTablet == src.ttlByTablet))
                    }
                }
            }
            sql "use @${targetCluster}"
            test {
                sql "WARM UP CLUSTER ${targetCluster} WITH TABLE ${tableName} PARTITION p5"
                exception "The partition p5 doesn't exist"
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
            // Keep DROP in finally so even a cleanup SQL failure does not leave the test table behind.
            try {
                sql "use @${sourceCluster}"
                pendingLoads.each { label ->
                    try {
                        sql "CANCEL LOAD WHERE LABEL = '${label}'"
                    } catch (Exception e) {
                        logger.warn("Failed to clean up load ${label}: ${e.message}")
                    }
                }
            } finally {
                sql "DROP TABLE IF EXISTS ${tableName} FORCE"
            }
        }
    }
}
