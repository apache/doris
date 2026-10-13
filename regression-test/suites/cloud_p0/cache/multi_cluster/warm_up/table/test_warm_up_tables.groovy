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

suite("test_warm_up_tables", "nonConcurrent") {
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
    def tableDefinitions = [customer: "customer_with_partition", supplier: "supplier"]
    long ttlSeconds = 12000L
    def partitionInfo = [
        "customer.p1": [table: "customer", partition: "p1"],
        "customer.p2": [table: "customer", partition: "p2"],
        "customer.p3": [table: "customer", partition: "p3"],
        "supplier": [table: "supplier", partition: null]
    ]
    def showTabletsSql = { info ->
        "show tablets from ${info.table}" + (info.partition ? " partition ${info.partition}" : "")
    }
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
    logger.info("Multi-table warmup backends: ${clusterBackends}")

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
                      bytesByType: partitionRows.groupBy { it[1]?.toString() }.collectEntries { type, entries ->
                          [(type): entries.sum(0L) { it[2] as Long }]
                      },
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
                def tablets = sql_return_maparray(showTabletsSql(info))
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
                                        // Cloud header JSON omits rowsets. Source data and cache
                                        // baselines are checked separately from target readiness.
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

    def clearCaches = { String phase, List members ->
        def before = getGlobalTtlBytes()
        members.each { be ->
            httpTest {
                endpoint "${be.ip}:${be.httpPort}"
                uri "/api/file_cache?op=clear&sync=true"
                op "get"
                check { code, body ->
                    assertTrue((code as int) == 200, "Clear cache failed on BE ${be.id}: ${code}, ${body}")
                    def response = parseJson(body.toString())
                    assertTrue(response.status == "OK", "Clear cache failed on BE ${be.id}: ${body}")
                    logger.info("${phase} clear BE ${be.id}: ${body}")
                }
            }
        }
        def after = getGlobalTtlBytes()
        // Busy blocks belonging to other tables must not determine whether this case passes.
        logger.info("${phase} global TTL: before=${before}, after=${after}, " +
                "source_minus_target_after_clear=${after.source - after.target}, " +
                "source_change_after_clear=${after.source - before.source}, " +
                "target_change_after_clear=${after.target - before.target}")
    }
    def waitForTargetCold = { String phase ->
        waitUntil("${phase}: target test-table cache cleanup incomplete", 120000L) {
            def cache = getPartitionCache(targetCluster)
            logger.info("${phase} target cache: ${cache}")
            cache.values().every { it.blocks == 0L }
        }
    }

    setBeConfigTemporary([
        enable_evict_file_cache_in_advance: false,
        file_cache_enter_disk_resource_limit_mode_percent: 99
    ]) {
        try {
            sql "use @${sourceCluster}"
            String dbName = sql("select database()")[0][0].toString()
            assertTrue(!dbName.isEmpty(), "Missing database for warmup Tables assertions")
            tableDefinitions.each { tableName, ddlName ->
                sql "DROP TABLE IF EXISTS ${tableName} FORCE"
                sql (new File("${context.file.parent}/../ddl/${ddlName}.sql").text + """
                    PROPERTIES("file_cache_ttl_seconds"="${ttlSeconds}", "disable_auto_compaction"="true")""")
            }
            partitionInfo.each { name, info ->
                def ids = sql(showTabletsSql(info)).collect { it[0] as Long }.unique()
                assertTrue(!ids.isEmpty(), "Missing tablets for ${name}")
                info.tabletIds = ids
            }
            clearCaches("initial", clusterBackends.values().flatten())

            tableDefinitions.keySet().each { tableName ->
                String label = "${tableName}_${UUID.randomUUID().toString().replace('-', '')}"
                String loadSql = new File("${context.file.parent}/../ddl/${tableName}_load.sql").text
                        .replace('${s3BucketName}', getS3BucketName()).replace('${loadLabel}', label)
                sql (loadSql + """WITH S3 (
                    "AWS_ACCESS_KEY" = "${getS3AK()}", "AWS_SECRET_KEY" = "${getS3SK()}",
                    "AWS_ENDPOINT" = "${getS3Endpoint()}", "AWS_REGION" = "${getS3Region()}",
                    "provider" = "${getS3Provider()}")
                    PROPERTIES("exec_mem_limit"="8589934592", "load_parallelism"="3")""")
                pendingLoads.add(label)
                waitUntil("load ${label} to finish", 300000L) {
                    def rows = sql "show load where Label = '${label}'"
                    logger.info("Multi-table warmup load state: ${rows}")
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
                def partitions = sql_return_maparray("show partitions from ${tableName}")
                partitionInfo.findAll { name, info -> info.table == tableName }.each { name, info ->
                    def matching = partitions.findAll {
                        info.partition == null || it.PartitionName.toString() == info.partition
                    }
                    assertTrue(matching.size() == 1 && (matching[0].VisibleVersion as Long) > 1L,
                            "Missing loaded version for ${name}: ${partitions}")
                    info.version = matching[0].VisibleVersion as Long
                }
            }
            logger.info("Loaded table/partition tablets: ${partitionInfo}")
            waitForTargetTablets()

            def sourceBaseline = [:]
            long stableSince = System.currentTimeMillis()
            waitUntil("source tables to have stable nonempty TTL cache", 120000L) {
                def current = getPartitionCache(sourceCluster)
                logger.info("Source cache before warmup: ${current}")
                if (current != sourceBaseline) {
                    sourceBaseline = current
                    stableSince = System.currentTimeMillis()
                }
                current.values().every { it.complete && it.nonTtlBlocks == 0L } &&
                        System.currentTimeMillis() - stableSince >= 3000L
            }
            waitForTargetCold("before first warmup")

            def stages = [
                [name: "customer_p1_p2",
                 clause: "TABLE customer PARTITION p1 AND TABLE customer PARTITION p2",
                 tables: ["customer.p1", "customer.p2"], warmed: ["customer.p1", "customer.p2"]],
                [name: "customer_p3_and_supplier",
                 clause: "TABLE customer PARTITION p3 AND TABLE supplier",
                 tables: ["customer.p3", "supplier"], warmed: partitionInfo.keySet().toList()],
                [name: "whole_tables", clause: "TABLE customer AND TABLE supplier",
                 tables: ["customer", "supplier"], warmed: partitionInfo.keySet().toList()]
            ]
            stages.each { stage ->
                if (stage.name == "whole_tables") {
                    // Keep the source baseline, but require both target tables to become cold again.
                    clearCaches("before whole_tables", clusterBackends[targetCluster])
                    waitForTargetCold("before whole_tables")
                }
                sql "use @${targetCluster}"
                // These explicit table/partition jobs do not depend on hotspot collection.
                def job = sql "WARM UP CLUSTER ${targetCluster} WITH ${stage.clause}"
                assertTrue(!job.isEmpty(), "No warmup job returned for ${stage.name}")
                def jobId = job[0][0]
                warmupJobs.add(jobId)
                def expectedTables = stage.tables.collect { "${dbName}.${it}".toString() }.sort()
                waitUntil("warmup ${stage.name} job ${jobId} to finish", 120000L) {
                    def rows = sql_return_maparray("SHOW WARM UP JOB WHERE ID = ${jobId}")
                    logger.info("${stage.name} warmup job ${jobId}: ${rows}")
                    if (rows.isEmpty()) {
                        return false
                    }
                    assertTrue(rows.size() == 1, "Unexpected warmup rows for ${jobId}: ${rows}")
                    def state = rows[0]
                    assertTrue(state.JobId.toString() == jobId.toString(), "Wrong warmup job: ${state}")
                    assertTrue(state.DstComputeGroup == targetCluster, "Wrong warmup target: ${state}")
                    assertTrue(state.Type == "TABLE", "Wrong warmup type: ${state}")
                    def actualTables = (state.Tables ?: "").toString().split(',').collect { it.trim() }.sort()
                    assertTrue(actualTables == expectedTables,
                            "Wrong Tables for ${stage.name}: expected=${expectedTables}, actual=${actualTables}")
                    assertTrue(state.Status != "CANCELLED", "Warmup ${stage.name} cancelled: ${state}")
                    state.Status == "FINISHED"
                }
                waitUntil("after ${stage.name}: per-tablet TTL must match source and untouched tables stay cold",
                        120000L) {
                    def source = getPartitionCache(sourceCluster)
                    def target = getPartitionCache(targetCluster)
                    logger.info("After ${stage.name} job ${jobId}: source=${source}, target=${target}, " +
                            "baseline=${sourceBaseline}")
                    partitionInfo.keySet().each { name ->
                        if (!stage.warmed.contains(name)) {
                            assertTrue(target[name].blocks == 0L,
                                    "Untouched ${name} has cache after ${stage.name}: ${target[name]}")
                        } else {
                            assertTrue(target[name].nonTtlBlocks == 0L,
                                    "Expected only TTL cache for ${name}: ${target[name]}")
                        }
                    }
                    partitionInfo.keySet().every { name ->
                        def src = source[name]
                        def dst = target[name]
                        src.complete && src.nonTtlBlocks == 0L &&
                                src.ttlByTablet == sourceBaseline[name].ttlByTablet &&
                                (!stage.warmed.contains(name) ||
                                 (dst.complete && dst.ttlBytes == src.ttlBytes && dst.ttlByTablet == src.ttlByTablet))
                    }
                }
            }
        } finally {
            warmupJobs.each { jobId ->
                try {
                    def rows = sql_return_maparray("SHOW WARM UP JOB WHERE ID = ${jobId}")
                    if (!rows.isEmpty() && rows[0].Status in ["PENDING", "RUNNING"]) {
                        sql "CANCEL WARM UP JOB WHERE ID = ${jobId}"
                    }
                } catch (Exception e) {
                    logger.warn("Failed to clean up warmup ${jobId}: ${e.message}")
                }
            }
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
                try {
                    sql "DROP TABLE IF EXISTS customer FORCE"
                } finally {
                    sql "DROP TABLE IF EXISTS supplier FORCE"
                }
            }
        }
    }
}
