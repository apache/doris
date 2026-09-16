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

suite("test_warm_up_table", "nonConcurrent") {
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

    def custoBeConfig = [
        enable_evict_file_cache_in_advance : false,
        file_cache_enter_disk_resource_limit_mode_percent : 99
    ]

    setBeConfigTemporary(custoBeConfig) {
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="12000") """
    def getJobState = { jobId ->
         def jobStateResult = sql """  SHOW WARM UP JOB WHERE ID = ${jobId} """
         return jobStateResult[0]
    }
    def getTablesFromShowCommand = { jobId ->
         def jobStateResult = sql """  SHOW WARM UP JOB WHERE ID = ${jobId} """
         return jobStateResult[0]
    }

    List<String> ipList = new ArrayList<>();
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> brpcPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    println("the value is " + context.config.multiClusterBes);
    int num = 0
    for(String values : bes) {
        if (num++ == 2) break;
        println("the value is " + values);
        String[] beInfo = values.split(':');
        ipList.add(beInfo[0]);
        hbPortList.add(beInfo[1]);
        httpPortList.add(beInfo[2]);
        beUniqueIdList.add(beInfo[3]);
        brpcPortList.add(beInfo[4]);
    }

    println("the ip is " + ipList);
    println("the heartbeat port is " + hbPortList);
    println("the http port is " + httpPortList);
    println("the be unique id is " + beUniqueIdList);
    println("the brpc port is " + brpcPortList);

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



    sql "use @regression_cluster_name0"

    def table = "customer"
    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
    // create table if not exists
    sql (new File("""${context.file.parent}/../ddl/${table}.sql""").text + ttlProperties)
    sql """ alter table ${table} set ("disable_auto_compaction" = "true") """ // no influence from compaction

    sleep(10000)

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

    def clearFileCache = { ip, port ->
        httpTest {
            endpoint ""
            uri ip + ":" + port + """/api/file_cache?op=clear&sync=true"""
            op "get"
            body ""
        }
    }

    def clusterBackends = ["regression_cluster_name0", "regression_cluster_name1"].collectEntries { name ->
        def members = sql_return_maparray("SHOW BACKENDS").findAll { be ->
            "${be.Alive}".equalsIgnoreCase("true") &&
                    parseJson(be.Tag.toString()).compute_group_name == name
        }.collect { be ->
            [id: be.BackendId as Long, ip: be.Host.toString(),
             httpPort: be.HttpPort.toString(), brpcPort: be.BrpcPort.toString()]
        }
        assertTrue(!members.isEmpty(), "No alive backends in ${name}")
        [(name): members]
    }
    def tabletIds = []
    def waitUntil = { String stage, long timeoutMs, Closure ready ->
        long deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (ready()) { return }
            sleep(1000)
        }
        assertTrue(false, "Timeout waiting for ${stage}; see last cache state in log")
    }
    def getGlobalTtl = {
        clusterBackends.collectEntries { name, members ->
            [(name): WarmupMetricsUtils.getBackendMetricSum(members, "ttl_cache_size")]
        }
    }
    def getTableCache = { String clusterName ->
        sql "use @${clusterName}"
        def beIds = clusterBackends[clusterName].collect { it.id }
        def rows = sql """select tablet_id, lower(type), sum(size)
            from information_schema.file_cache_info
            where tablet_id in (${tabletIds.join(',')}) and be_id in (${beIds.join(',')})
            group by tablet_id, lower(type)"""
        def ttlByTablet = rows.findAll { it[1]?.toString() == "ttl" }
                .collectEntries { [(it[0] as Long): it[2] as Long] }
        [ttlByTablet: ttlByTablet, complete: tabletIds.every { (ttlByTablet[it] ?: 0L) > 0L },
         nonTtlBytes: rows.findAll { it[1]?.toString() != "ttl" }.sum(0L) { it[2] as Long }]
    }
    def waitForTargetTablets = {
        sql "use @regression_cluster_name1"
        def targetBes = clusterBackends.regression_cluster_name1.collectEntries { [(it.id): it] }
        waitUntil("target primary tablet metadata", 120000L) {
            def ready = [] as Set
            sql_return_maparray("show tablets from ${table}").each { tablet ->
                long id = tablet.TabletId as Long
                long primary = tablet.PrimaryBackendId as Long
                if (id in tabletIds && targetBes.containsKey(primary) && (tablet.BackendId as Long) == primary) {
                    def be = targetBes[primary]
                    try {
                        // Metadata only: a business read here would populate the target cache.
                        def meta = parseJson(new URL("http://${be.ip}:${be.httpPort}/api/meta/header/${id}")
                                .getText(connectTimeout: 5000, readTimeout: 5000))
                        if ((meta.tablet_id as Long) == id && (meta.ttl_seconds as Long) == 12000L &&
                                meta.tablet_state == "PB_RUNNING" && meta.schema != null) {
                            ready.add(id)
                        }
                    } catch (Exception e) {
                        logger.info("Waiting for target tablet ${id} on BE ${primary}: ${e.message}")
                    }
                }
            }
            logger.info("Target tablet metadata: missing=${tabletIds - ready}")
            ready.size() == tabletIds.size()
        }
    }

    def createdWarmUpJobIds = [] as Set
    def recordWarmUpJob = { jobId ->
        if (jobId != null && !jobId.isEmpty()) {
            createdWarmUpJobIds.add(jobId[0][0])
        }
    }
    def cleanupWarmUpJobs = {
        createdWarmUpJobIds.each { jobId ->
            try {
                def statuses = getJobState(jobId)
                if (statuses.any { it != null && (it.equals("PENDING") || it.equals("RUNNING")) }) {
                    sql "cancel warm up job where id = ${jobId}"
                }
            } catch (Exception e) {
                logger.info("ignore warm up job cleanup failure, job id: ${jobId}, error: ${e.getMessage()}")
            }
        }
    }

    try {
    def ttlBeforeClear = getGlobalTtl()
    clusterBackends.values().flatten().each { be -> clearFileCache.call(be.ip, be.httpPort) }
    def ttlAfterClear = getGlobalTtl()
    logger.info("Single-table global TTL clear: before=${ttlBeforeClear}, after=${ttlAfterClear}")
    sleep(30000)

    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()

    tabletIds = sql_return_maparray("show tablets from ${table}")
            .collect { it.TabletId as Long }.unique().sort()
    assertTrue(!tabletIds.isEmpty(), "No tablets for ${table}")
    logger.info("Single-table warmup tablets=${tabletIds}, backends=${clusterBackends}")
    def sourceCache = null
    def candidate = null
    long stableSince = 0L
    waitUntil("stable nonempty source TTL cache", 120000L) {
        sourceCache = getTableCache("regression_cluster_name0")
        boolean complete = sourceCache.complete && sourceCache.nonTtlBytes == 0L
        if (!complete || sourceCache != candidate) { stableSince = System.currentTimeMillis() }
        candidate = sourceCache
        logger.info("Single-table source TTL baseline=${sourceCache}")
        complete && System.currentTimeMillis() - stableSince >= 3000L
    }
    waitForTargetTablets()

    def jobId = sql "warm up cluster regression_cluster_name1 with table customer;"
    recordWarmUpJob(jobId)
    try {
        sql "warm up cluster regression_cluster_name1 with table customer;"
        assertTrue(false)
    } catch (Exception e) {
        assertTrue("${e.getMessage()}".contains("already has a pending job"))
    }
    int retryTime = 120
    int j = 0
    for (; j < retryTime; j++) {
        sleep(1000)
        def statuses = getJobState(jobId[0][0])
        if (statuses.any { it != null && it.equals("CANCELLED") }) {
            assertTrue(false);
        }
        if (statuses.any { it != null && it.equals("FINISHED") }) {
            break;
        }
    }
    if (j == retryTime) {
        sql "cancel warm up job where id = ${jobId[0][0]}"
        assertTrue(false);
    }
    def tablesString = getTablesFromShowCommand(jobId[0][0])

    assertTrue(tablesString.any { it != null && it.contains("customer") })
    // Global TTL includes residual blocks from other tables; assert this table on the target BEs.
    try {
        waitUntil("target per-tablet TTL cache to equal the nonzero source baseline", 180000L) {
            def currentSource = getTableCache("regression_cluster_name0")
            def targetCache = getTableCache("regression_cluster_name1")
            logger.info("Single-table TTL cache: source=${currentSource}, target=${targetCache}")
            currentSource == sourceCache && targetCache.complete && targetCache.nonTtlBytes == 0L &&
                    targetCache.ttlByTablet == sourceCache.ttlByTablet
        }
    } finally {
        def ttlAfterWarmup = getGlobalTtl()
        def delta = ttlAfterWarmup.collectEntries { name, bytes -> [(name): bytes - ttlAfterClear[name]] }
        logger.info("Single-table global TTL after warmup=${ttlAfterWarmup}, " +
                "baseline_after_clear=${ttlAfterClear}, delta_since_clear=${delta}")
    }

    try {
        sql "warm up cluster regression_cluster_name2 with table customer;"
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }

    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
    try {
        sql "warm up cluster regression_cluster_name1 with table customer;"
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }
    } finally {
        cleanupWarmUpJobs()
        sql "DROP TABLE IF EXISTS ${table} FORCE"
    }
    }
}
