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

suite("alter_ttl_4", "nonConcurrent") {
    def customBeConfig = [
        enable_evict_file_cache_in_advance: false,
        file_cache_enter_disk_resource_limit_mode_percent: 99,
        file_cache_background_ttl_gc_interval_ms: 1000,
        file_cache_background_ttl_info_update_interval_ms: 1000,
        file_cache_background_tablet_id_flush_interval_ms: 1000
    ]
    setBeConfigTemporary(customBeConfig) {
        String targetTable = "alter_ttl_4_target"
        String pressureTable = "alter_ttl_4_pressure"
        long initialTtl = 3600L
        long rowsPerLoad = 15000000L // TPCH SF100 customer.tbl
        long maxPressureRows = 64L * rowsPerLoad
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        String clusterName = clusters[0][0].toString()
        def backends = sql_return_maparray("SHOW BACKENDS").findAll { be ->
            "${be.Alive}".equalsIgnoreCase("true") &&
                    parseJson(be.Tag.toString()).compute_group_name == clusterName
        }.collectEntries { be -> [(be.BackendId as Long): be] }
        assertTrue(!backends.isEmpty(), "No alive backends in ${clusterName}")
        def backendIds = backends.keySet()
        def tabletIds = []
        def pendingLoads = [] as Set
        def lastState = [:]
        def cacheDifference = [:]
        def originalQueryCache = sql("select @@enable_sql_cache, @@enable_query_cache")[0].collect { value ->
            String setting = value.toString().toLowerCase(Locale.ROOT)
            assertTrue(setting in ["true", "false", "0", "1"], "Unexpected cache setting: ${value}")
            setting
        }
        def waitUntil = { String description, long timeoutMs, Closure ready ->
            long deadline = System.currentTimeMillis() + timeoutMs
            while (System.currentTimeMillis() < deadline) {
                if (ready()) {
                    return
                }
                sleep(1000)
            }
            assertTrue(false, "Timeout after ${timeoutMs}ms: ${description}; last_state=${lastState}")
        }
        def scanTarget = {
            sql "use @${clusterName}"
            // Read every data column, including strings; neither COUNT metadata nor LIMIT can satisfy this scan.
            def rows = sql """select count(*), sum(C_CUSTKEY), sum(length(C_NAME)), sum(length(C_ADDRESS)),
                sum(C_NATIONKEY), sum(length(C_PHONE)), sum(C_ACCTBAL),
                sum(length(C_MKTSEGMENT)), sum(length(C_COMMENT)) from ${targetTable}"""
            assertTrue(rows.size() == 1 && rows[0].size() == 9 && rows[0].every { it != null },
                    "Unexpected data result: ${rows}")
            assertEquals(rowsPerLoad, rows[0][0] as Long)
            rows[0].collect { new BigDecimal(it.toString()).stripTrailingZeros() }
        }
        def getCache = {
            sql "use @${clusterName}"
            def rows = sql """select be_id, cache_path, tablet_id, `hash`, `offset`, size, lower(type)
                from information_schema.file_cache_info
                where tablet_id in (${tabletIds.join(',')}) and be_id in (${backendIds.join(',')})"""
            def blocks = [:]
            rows.each { row ->
                def key = [row[0] as Long, row[1].toString(), row[2] as Long,
                           row[3].toString(), row[4] as Long]
                assertTrue(backendIds.contains(key[0]) && tabletIds.contains(key[2]), "Unexpected cache owner: ${row}")
                assertTrue(key[4] >= 0L && (row[5] as Long) > 0L, "Invalid cache range: ${row}")
                assertTrue(!blocks.containsKey(key), "Duplicate cache block: ${key}")
                blocks[key] = [size: row[5] as Long, type: row[6]?.toString()]
            }
            blocks
        }
        def blockSizes = { Map blocks -> blocks.collectEntries { key, value -> [(key): value.size] } }
        def sameBlocks = { Map expected, Map actual ->
            cacheDifference = [missing: (expected.keySet() - actual.keySet()).take(10),
                               unexpected: (actual.keySet() - expected.keySet()).take(10),
                               changed_sizes: actual.findAll { key, value -> value.size != expected[key]?.size }.take(10)]
            blockSizes(expected) == blockSizes(actual)
        }
        def cacheSummary = { Map blocks ->
            blocks.groupBy { key, value -> [key[0], key[2], value.type] }
                    .collectEntries { key, entries -> [(key): entries.values().sum(0L) { it.size }] }
        }
        def coverage = { Map blocks ->
            blocks.groupBy { key, value -> key[0..3] }.collectEntries { file, entries ->
                def ranges = []
                entries.collect { key, value -> [key[4], key[4] + value.size] }.sort { a, b -> a[0] <=> b[0] }
                        .each { range ->
                            assertTrue(ranges.isEmpty() || range[0] >= ranges.last()[1],
                                    "Overlapping cache ranges: file=${file}, ranges=${ranges}, next=${range}")
                            if (!ranges.isEmpty() && range[0] == ranges.last()[1]) {
                                ranges.last()[1] = range[1]
                            } else {
                                ranges.add(range)
                            }
                        }
                [(file): ranges]
            }
        }
        def missingCoverage = { Map expected, Map actual ->
            expected.findAll { file, ranges ->
                !ranges.every { range ->
                    actual[file]?.any { present -> present[0] <= range[0] && present[1] >= range[1] }
                }
            }
        }
        def waitForCache = { String stage, String type, Closure matches ->
            def previous = null
            def result = [:]
            long stableSince = 0L
            waitUntil(stage, 600000L) {
                result = getCache()
                def missingTablets = tabletIds.findAll { id -> !result.keySet().any { it[2] == id } }
                boolean matching = matches(result)
                boolean complete = missingTablets.isEmpty() &&
                        result.values().every { it.type == type } && matching
                lastState = [stage: stage, cache_by_be_tablet_type: cacheSummary(result),
                             missing_tablets: missingTablets, differences: cacheDifference]
                if (result != previous) {
                    logger.info("${lastState}")
                }
                if (!complete || result != previous) {
                    stableSince = System.currentTimeMillis()
                }
                previous = result
                complete && System.currentTimeMillis() - stableSince >= 3000L
            }
            result
        }
        def metricNames = ["file_cache_ttl_cache_size", "file_cache_capacity", "file_cache_evict_by_self_lru_normal"]
        def getMetrics = {
            // Per-BE diagnostic snapshots; residual cache from other tables is not a failure.
            backends.collectEntries { id, be ->
                def values = metricNames.collectEntries { [(it): null] }
                try {
                    String text = new URL("http://${be.Host}:${be.BrpcPort}/brpc_metrics")
                            .getText(connectTimeout: 5000, readTimeout: 5000)
                    text.readLines().each { line ->
                        def fields = line.trim().split(/\s+/)
                        if (fields.size() == 2 && !fields[0].startsWith("#")) {
                            metricNames.each { name ->
                                if (fields[0] == name || fields[0].endsWith("_${name}")) {
                                    values[name] = (values[name] ?: 0L) + (fields[1] as Long)
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Cannot read diagnostic metrics for BE ${id}: ${e.message}")
                }
                [(id): values]
            }
        }
        def logMetricDelta = { String stage, Map before, Map after ->
            def delta = backendIds.collectEntries { id ->
                [(id): metricNames.collectEntries { name ->
                    [(name): before[id][name] != null && after[id][name] != null ?
                            after[id][name] - before[id][name] : null]
                }]
            }
            logger.info("${stage} metrics: before=${before}, after=${after}, delta_by_be=${delta}")
        }
        def loadTable = { String table, long timeoutMs ->
            long deadline = System.currentTimeMillis() + timeoutMs
            String label = "${table}_${UUID.randomUUID().toString().replace('-', '')}"
            pendingLoads.add(label)
            String statement = new File("${context.file.parent}/../ddl/customer_ttl_load.sql").text
                    .replace('${loadLabel}', label).replace('${s3BucketName}', getS3BucketName())
                    .replace('INTO TABLE customer_ttl', "INTO TABLE ${table}")
            sql(statement + """WITH S3 (
                "AWS_ACCESS_KEY"="${getS3AK()}", "AWS_SECRET_KEY"="${getS3SK()}",
                "AWS_ENDPOINT"="${getS3Endpoint()}", "AWS_REGION"="${getS3Region()}",
                "provider"="${getS3Provider()}")
                PROPERTIES("exec_mem_limit"="8589934592", "load_parallelism"="3")""")
            waitUntil("load ${label}", Math.max(0L, deadline - System.currentTimeMillis())) {
                def rows = sql "show load where Label = '${label}'"
                lastState = [label: label, state: rows.isEmpty() ? "NOT_VISIBLE" : rows.last()[2]]
                if (rows.isEmpty()) {
                    return false
                }
                String state = rows.last()[2].toString()
                assertTrue(!state.equalsIgnoreCase("CANCELLED"), "Load cancelled: ${label}, details=${rows.last()}")
                if (state.equalsIgnoreCase("FINISHED")) {
                    pendingLoads.remove(label)
                    return true
                }
                false
            }
        }
        Throwable failure = null
        try {
            sql "use @${clusterName}"
            sql "set enable_sql_cache = false"
            sql "set enable_query_cache = false"
            [(targetTable): initialTtl, (pressureTable): 0L].each { table, ttl ->
                sql "drop table if exists ${table} force"
                String ddl = new File("${context.file.parent}/../ddl/customer_ttl.sql").text
                        .replace('customer_ttl', table)
                sql(ddl + """ PROPERTIES("file_cache_ttl_seconds"="${ttl}", "disable_auto_compaction"="true")""")
            }
            tabletIds = sql_return_maparray("show tablets from ${targetTable}")
                    .collect { it.TabletId as Long }.unique().sort()
            assertTrue(tabletIds.size() == 32, "Expected 32 target tablets: ${tabletIds}")
            loadTable(targetTable, 300000L)
            def dataBaseline = scanTarget()

            def creationTimes = [:]
            // The full scan above verifies data; cloud header JSON does not include rowsets.
            waitUntil("target tablet TTL metadata", 600000L) {
                creationTimes = [:]
                sql_return_maparray("show tablets from ${targetTable}").each { tablet ->
                    long id = tablet.TabletId as Long
                    long primary = tablet.PrimaryBackendId as Long
                    if (!(id in tabletIds) || !backends.containsKey(primary) || (tablet.BackendId as Long) != primary) {
                        return
                    }
                    def be = backends[primary]
                    try {
                        httpTest {
                            endpoint "${be.Host}:${be.HttpPort}"
                            uri "/api/meta/header/${id}"
                            op "get"
                            check { code, body ->
                                if ((code as int) == 200) {
                                    def meta = parseJson(body.toString())
                                    if ((meta.tablet_id as Long) == id && (meta.creation_time as Long) > 0L &&
                                            (meta.ttl_seconds as Long) == initialTtl &&
                                            meta.tablet_state == "PB_RUNNING" && meta.schema != null) {
                                        creationTimes[id] = meta.creation_time as Long
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.info("Waiting for metadata: tablet=${id}, BE=${primary}, ${e.message}")
                    }
                }
                lastState = [missing_tablets: tabletIds - creationTimes.keySet()]
                creationTimes.size() == tabletIds.size()
            }
            def ttlBaseline = waitForCache("initial TTL cache", "ttl") { true }
            def baselineCoverage = coverage(ttlBaseline)
            waitUntil("tablet creation time plus one second to have passed", 60000L) {
                long now = sql("select unix_timestamp()")[0][0] as Long
                assertTrue(now < creationTimes.values().min() + initialTtl, "Initial TTL already expired")
                now > creationTimes.values().max() + 1L
            }
            logger.info("TTL baseline: creation_times=${creationTimes}, metrics=${getMetrics()}")

            sql "alter table ${targetTable} set (\"file_cache_ttl_seconds\"=\"1\")"
            waitForCache("expired TTL blocks to become NORMAL", "normal") { blocks ->
                sameBlocks(ttlBaseline, blocks)
            }

            long pressureDeadline = System.currentTimeMillis() + 1200000L
            long pressureRows = 0L
            def metricsBeforePressure = getMetrics()
            def evictedFiles = [:]
            while (evictedFiles.isEmpty() && pressureRows < maxPressureRows &&
                    System.currentTimeMillis() < pressureDeadline) {
                loadTable(pressureTable, Math.min(300000L, pressureDeadline - System.currentTimeMillis()))
                pressureRows += rowsPerLoad
                // Poll cache metadata between loads without touching target data or clearing caches.
                long checkDeadline = Math.min(pressureDeadline, System.currentTimeMillis() + 5000L)
                while (evictedFiles.isEmpty() && System.currentTimeMillis() < checkDeadline) {
                    def current = getCache()
                    assertTrue(current.values().every { it.type == "normal" },
                            "Expired target cache changed type during pressure: ${cacheSummary(current)}")
                    evictedFiles = missingCoverage(baselineCoverage, coverage(current))
                    if (evictedFiles.isEmpty()) {
                        sleep(1000)
                    }
                }
                logger.info("Cache pressure: imported_rows=${pressureRows}, missing_files=${evictedFiles.keySet()}")
            }
            def metricsAfterPressure = getMetrics()
            logMetricDelta("Pressure", metricsBeforePressure, metricsAfterPressure)
            assertTrue(!evictedFiles.isEmpty(), "No target cache eviction before pressure limit: " +
                    "imported_rows=${pressureRows}/${maxPressureRows}, metrics=${metricsAfterPressure}")

            assertEquals(dataBaseline, scanTarget())
            // Re-reading an evicted file may split blocks differently. Compare merged byte ranges.
            def reloaded = waitForCache("reloaded NORMAL cache coverage", "normal") { blocks ->
                def current = coverage(blocks)
                cacheDifference = [missing: missingCoverage(baselineCoverage, current).take(10),
                                   extra: missingCoverage(current, baselineCoverage).take(10)]
                current == baselineCoverage
            }
            def metricsBeforeExtension = getMetrics()
            long now = sql("select unix_timestamp()")[0][0] as Long
            long extendedTtl = Math.max(initialTtl, now - (creationTimes.values().min() as Long) + 1200L)
            sql "alter table ${targetTable} set (\"file_cache_ttl_seconds\"=\"${extendedTtl}\")"
            // Keep positive -> positive: routing through TTL=0 would hide expired-TTL renewal bugs.
            // No target reads after ALTER until the background conversion has been verified.
            waitForCache("renewed TTL on existing NORMAL blocks (1 -> ${extendedTtl})", "ttl") { blocks ->
                sameBlocks(reloaded, blocks)
            }
            logMetricDelta("Extended TTL=${extendedTtl}", metricsBeforeExtension, getMetrics())
            assertEquals(dataBaseline, scanTarget())
        } catch (Throwable t) {
            failure = t
            throw t
        } finally {
            def cleanupErrors = []
            pendingLoads.each { label ->
                try { sql "cancel load where label = '${label}'" } catch (Throwable t) { cleanupErrors.add(t) }
            }
            [targetTable, pressureTable].each { table ->
                try { sql "drop table if exists ${table} force" } catch (Throwable t) { cleanupErrors.add(t) }
            }
            [enable_sql_cache: originalQueryCache[0], enable_query_cache: originalQueryCache[1]].each { name, value ->
                try { sql "set ${name} = ${value}" } catch (Throwable t) { cleanupErrors.add(t) }
            }
            if (!cleanupErrors.isEmpty()) {
                if (failure != null) {
                    cleanupErrors.each { failure.addSuppressed(it) }
                } else {
                    cleanupErrors.tail().each { cleanupErrors[0].addSuppressed(it) }
                    throw cleanupErrors[0]
                }
            }
        }
    }
}
