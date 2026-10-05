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

suite("test_warm_up_compute_group", "nonConcurrent") {
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

    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="12000") """
    def getJobState = { jobId ->
         def jobStateResult = sql """  SHOW WARM UP JOB WHERE ID = ${jobId} """
         return jobStateResult[0]
    }
    def table = "customer"

    String sourceCluster = "regression_cluster_name0"
    String targetCluster = "regression_cluster_name1"
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
    logger.info("Warmup source=${sourceCluster}, target=${targetCluster}, backends=${clusterBackends}")
    sql "use @${sourceCluster}"
    sql "set enable_sql_cache = false"
    sql "set enable_query_cache = false"

    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
    sql new File("""${context.file.parent}/../ddl/supplier_delete.sql""").text
    // create table if not exists
    sql (new File("""${context.file.parent}/../ddl/${table}.sql""").text + ttlProperties)
    sql (new File("""${context.file.parent}/../ddl/supplier.sql""").text + ttlProperties)

    sql """ TRUNCATE TABLE __internal_schema.cloud_cache_hotspot; """
    sleep(30000)

    def s3BucketName = getS3BucketName()
    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}",
        |"provider" = "${getS3Provider()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()
    
    

    // Capture the IDs before DROP so the final check can still address this case's tablets.
    def tableTabletIds = [table, "supplier"].collectEntries { tableName ->
        def ids = sql("show tablets from ${tableName}").collect { it[0] as Long }.unique()
        assertTrue(!ids.isEmpty(), "No tablets found for ${tableName}")
        [(tableName): ids]
    }
    logger.info("Case table tablets: ${tableTabletIds}")

    def getGlobalTtlBytes = {
        [source: WarmupMetricsUtils.getBackendMetricSum(clusterBackends[sourceCluster], "ttl_cache_size"),
         target: WarmupMetricsUtils.getBackendMetricSum(clusterBackends[targetCluster], "ttl_cache_size")]
    }
    def clearCacheAndLogTtl = { List<String> clustersToClear, String phase ->
        def before = getGlobalTtlBytes()
        clustersToClear.each { clusterName ->
            clusterBackends[clusterName].each { be ->
                WarmupMetricsUtils.clearFileCache(be.ip, be.httpPort)
            }
        }
        // sync=true can leave referenced blocks pending recycle. Record the residual baseline;
        // it is diagnostic data, not an assertion that either BE's entire cache must be empty.
        def after = getGlobalTtlBytes()
        logger.info("TTL cache clear phase=${phase}, bytes_before=${before}, bytes_after=${after}, " +
                "source_minus_target_after_clear=${after.source - after.target}, " +
                "source_change_after_clear=${after.source - before.source}, " +
                "target_change_after_clear=${after.target - before.target}")
        return after
    }
    def getTableTtlBytes = { String clusterName ->
        sql "use @${clusterName}"
        def backendIds = clusterBackends[clusterName].collect { it.id }.join(',')
        tableTabletIds.collectEntries { tableName, ids ->
            def rows = sql """select coalesce(sum(size), 0)
                from information_schema.file_cache_info
                where tablet_id in (${ids.join(',')})
                  and be_id in (${backendIds}) and lower(type) = 'ttl'"""
            [(tableName): rows[0][0] as Long]
        }
    }
    def initialTtlAfterClear = clearCacheAndLogTtl([sourceCluster, targetCluster], "before_load")

    def load_customer_once =  { 
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        def loadLabel = table + "_" + uniqueID
        // load data from cos
        def loadSql = new File("""${context.file.parent}/../ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
        loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
        sql loadSql

        // check load state
        while (true) {
            def stateResult = sql "show load where Label = '${loadLabel}'"
            def loadState = stateResult[stateResult.size() - 1][2].toString()
            if ("CANCELLED".equalsIgnoreCase(loadState)) {
                throw new IllegalStateException("load ${loadLabel} failed.")
            } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                break
            }
            sleep(5000)
        }
    }

    def load_supplier_once =  { 
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        def loadLabel = "supplier_" + uniqueID
        // load data from cos
        def loadSql = new File("""${context.file.parent}/../ddl/supplier_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
        loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
        sql loadSql

        // check load state
        while (true) {
            def stateResult = sql "show load where Label = '${loadLabel}'"
            def loadState = stateResult[stateResult.size() - 1][2].toString()
            if ("CANCELLED".equalsIgnoreCase(loadState)) {
                throw new IllegalStateException("load ${loadLabel} failed.")
            } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                break
            }
            sleep(5000)
        }
    }
    
    sql "use @regression_cluster_name0"
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_supplier_once()
    load_supplier_once()
    load_supplier_once()

    for (int i = 0; i < 1000; i++) {
        sql "select count(*) from customer"
        sql "select count(*) from supplier"
    }
    sleep(40000)
    def jobId_ = sql "WARM UP COMPUTE GROUP regression_cluster_name1 WITH COMPUTE GROUP regression_cluster_name0"
    def waitJobDone = { jobId ->
        int retryTime = 120
        int i = 0
        for (; i < retryTime; i++) {
            sleep(1000)
            def statuses = getJobState(jobId[0][0])
            if (statuses.any { it != null && it.equals("CANCELLED") }) {
                assertTrue(false);
            }
            if (statuses.any { it != null && it.equals("FINISHED") }) {
                break;
            }
        }
        if (i == retryTime) {
            sql "cancel warm up job where id = ${jobId[0][0]}"
            assertTrue(false);
        }
    }
    waitJobDone(jobId_)
    
    // Cache metadata is published asynchronously. Compare each table independently so that
    // missing bytes in one table cannot be hidden by extra bytes in the other table.
    long cacheDeadline = System.currentTimeMillis() + 120000L
    def sourceTableBytes = [:]
    def targetTableBytes = [:]
    boolean cacheMatches = false
    while (System.currentTimeMillis() < cacheDeadline) {
        sourceTableBytes = getTableTtlBytes(sourceCluster)
        targetTableBytes = getTableTtlBytes(targetCluster)
        logger.info("Case table TTL cache bytes: source=${sourceTableBytes}, target=${targetTableBytes}")
        cacheMatches = tableTabletIds.keySet().every { tableName ->
            sourceTableBytes[tableName] > 0L &&
                    sourceTableBytes[tableName] == targetTableBytes[tableName]
        }
        if (cacheMatches) {
            break
        }
        sleep(1000)
    }
    def ttlAfterWarmup = getGlobalTtlBytes()
    logger.info("Global TTL cache bytes after warmup=${ttlAfterWarmup}, " +
            "source_minus_target=${ttlAfterWarmup.source - ttlAfterWarmup.target}, " +
            "baseline_after_clear=${initialTtlAfterClear}, " +
            "source_change_since_clear=${ttlAfterWarmup.source - initialTtlAfterClear.source}, " +
            "target_change_since_clear=${ttlAfterWarmup.target - initialTtlAfterClear.target}")
    assertTrue(cacheMatches, "Case table TTL cache bytes must be positive and match on both clusters: " +
            "source=${sourceTableBytes}, target=${targetTableBytes}, tablets=${tableTabletIds}")

    try {
        sql "WARM UP COMPUTE GROUP regression_cluster_name1 WITH COMPUTE GROUP regression_cluster_name2"
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }
    
    try {
        sql "WARM UP COMPUTE GROUP regression_cluster_name2 WITH COMPUTE GROUP regression_cluster_name0"
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }

    try {
        sql "WARM UP COMPUTE GROUP regression_cluster_name0 WITH COMPUTE GROUP regression_cluster_name0"
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }

    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
    sql new File("""${context.file.parent}/../ddl/supplier_delete.sql""").text

    clearCacheAndLogTtl([targetCluster], "after_drop")
    jobId_ = sql "WARM UP COMPUTE GROUP regression_cluster_name1 WITH COMPUTE GROUP regression_cluster_name0"
    waitJobDone(jobId_)
    long droppedCacheDeadline = System.currentTimeMillis() + 120000L
    def droppedTableBytes = [:]
    boolean droppedCacheCleared = false
    while (System.currentTimeMillis() < droppedCacheDeadline) {
        droppedTableBytes = getTableTtlBytes(targetCluster)
        droppedCacheCleared = droppedTableBytes.values().every { it == 0L }
        if (droppedCacheCleared) {
            break
        }
        logger.info("Waiting for dropped table TTL cache to clear on ${targetCluster}: ${droppedTableBytes}")
        sleep(1000)
    }
    assertTrue(droppedCacheCleared,
            "Dropped tables must have no TTL cache on ${targetCluster}: ${droppedTableBytes}")
}
