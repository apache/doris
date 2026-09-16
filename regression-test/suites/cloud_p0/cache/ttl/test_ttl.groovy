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

suite("test_ttl", "nonConcurrent") {
    def customBeConfig = [
        enable_evict_file_cache_in_advance: false,
        file_cache_enter_disk_resource_limit_mode_percent: 99,
        file_cache_background_ttl_gc_interval_ms: 1000,
        file_cache_background_ttl_info_update_interval_ms: 1000,
        file_cache_background_tablet_id_flush_interval_ms: 1000
    ]
    setBeConfigTemporary(customBeConfig) {
        String tableName = "test_ttl_natural_expiration"
        long ttlSeconds = 300L
        int bucketCount = 4
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
        def metadata = [:]
        def lastState = [:]
        def metricsBaseline = null
        def originalQueryCache = sql("select @@enable_sql_cache, @@enable_query_cache")[0].collect { value ->
            String setting = value.toString().toLowerCase(Locale.ROOT)
            assertTrue(setting in ["true", "false", "0", "1"], "Unexpected cache setting: ${value}")
            setting
        }
        def serverTime = { sql("select unix_timestamp()")[0][0] as Long }
        def waitUntil = { String stage, long deadlineMs, Closure ready ->
            while (System.currentTimeMillis() < deadlineMs) {
                if (ready()) {
                    return
                }
                sleep(1000)
            }
            assertTrue(false, "Timeout waiting for ${stage}: cluster=${clusterName}, " +
                    "tablets=${tabletIds}, BEs=${backendIds}, last_state=${lastState}")
        }
        def getCache = {
            sql "use @${clusterName}"
            def blocks = [:]
            def rows = sql """select be_id, cache_path, tablet_id, `hash`, `offset`, size, lower(type)
                from information_schema.file_cache_info
                where tablet_id in (${tabletIds.join(',')}) and be_id in (${backendIds.join(',')})"""
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
        def summarizeCache = { Map blocks ->
            blocks.groupBy { key, value -> [key[0], key[2]] }.collectEntries { owner, entries ->
                [(owner): [block_count: entries.size(), bytes: entries.values().sum(0L) { it.size },
                           by_type: (["ttl", "normal"] + entries.values().collect { it.type }).unique().collectEntries { type ->
                               def values = entries.values().findAll { it.type == type }
                               [(type): [block_count: values.size(), bytes: values.sum(0L) { it.size }]]
                           }]]
            }
        }
        def metricNames = ["file_cache_ttl_cache_size", "file_cache_normal_queue_cache_size",
                           "file_cache_ttl_cache_lru_queue_size", "file_cache_ttl_cache_lru_queue_element_count",
                           "file_cache_normal_queue_element_count", "file_cache_ttl_cache_evict_size",
                           "file_cache_normal_queue_evict_size", "file_cache_ttl_mgr_tablet_id_set_size"]
        def getMetrics = {
            // These include other tables and update asynchronously. Only scoped cache data is asserted.
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
                def missing = values.findAll { name, value -> value == null }.keySet()
                if (!missing.isEmpty()) {
                    logger.warn("Unavailable diagnostic metrics: BE=${id}, names=${missing}")
                }
                [(id): values]
            }
        }
        def logMetrics = { String stage ->
            def current = getMetrics()
            def delta = backendIds.collectEntries { id ->
                [(id): metricNames.collectEntries { name ->
                    [(name): metricsBaseline != null && metricsBaseline[id][name] != null && current[id][name] != null ?
                            current[id][name] - metricsBaseline[id][name] : null]
                }]
            }
            logger.info("TTL metrics: stage=${stage}, cluster=${clusterName}, by_be=${current}, " +
                    "delta_from_ttl_baseline=${delta}")
            current
        }
        String address = "Address Line 1"
        String phone = "123-456-7890"
        String segment = "AUTOMOBILE"
        String comment = "This is a test comment for the customer. " + ("X" * 50)
        def customerIds = (10001..10200).toList()
        def customerNames = customerIds.collect { String.format('Customer#%09d', it) }
        def normalize = { List values -> values.collect { new BigDecimal(it.toString()).stripTrailingZeros() } }
        def expectedData = normalize([customerIds.size(), customerIds.sum(0L), customerNames.sum(0L) { it.length() },
                200L * address.length(), 200L * 15L, 200L * phone.length(),
                200G * 12345.67G, 200L * segment.length(), 200L * comment.length()])
        def scanTable = {
            // Read every column; result caches and metadata-only COUNT cannot satisfy this query.
            def rows = sql """select count(*), sum(C_CUSTKEY), sum(length(C_NAME)), sum(length(C_ADDRESS)),
                sum(C_NATIONKEY), sum(length(rtrim(C_PHONE))), sum(C_ACCTBAL),
                sum(length(rtrim(C_MKTSEGMENT))), sum(length(C_COMMENT)) from ${tableName}"""
            assertTrue(rows.size() == 1 && rows[0].size() == 9 && rows[0].every { it != null },
                    "Unexpected data result: ${rows}")
            assertEquals(expectedData, normalize(rows[0]))
        }
        Throwable failure = null
        try {
            sql "use @${clusterName}"
            sql "set enable_sql_cache = false"
            sql "set enable_query_cache = false"
            logMetrics("before_create")
            sql "drop table if exists ${tableName} force"
            long prepareStartedMs = System.currentTimeMillis()
            // Leave a minute for observing TTL before expiry, even on ASAN and with delayed metadata flushes.
            long prepareDeadlineMs = prepareStartedMs + (ttlSeconds - 60L) * 1000L
            String ddl = new File("${context.file.parent}/../ddl/customer_ttl.sql").text
                    .replace('customer_ttl', tableName).replace('BUCKETS 32', "BUCKETS ${bucketCount}")
            sql(ddl + """ PROPERTIES("file_cache_ttl_seconds"="${ttlSeconds}", "disable_auto_compaction"="true")""")
            tabletIds = sql_return_maparray("show tablets from ${tableName}")
                    .collect { it.TabletId as Long }.unique().sort()
            assertTrue(tabletIds.size() == bucketCount, "Expected ${bucketCount} tablets: ${tabletIds}")
            String values = customerIds.withIndex().collect { id, i ->
                "(${id}, '${customerNames[i]}', '${address}', 15, '${phone}', 12345.67, '${segment}', '${comment}')"
            }.join(",")
            long insertStartedMs = System.currentTimeMillis()
            sql "insert into ${tableName} values ${values}"
            logger.info("TTL fixture: rows=200, tablets=${tabletIds}, insert_elapsed_ms=${System.currentTimeMillis() - insertStartedMs}")
            scanTable()

            // The full scan above verifies data; cloud header JSON does not include rowsets.
            waitUntil("tablet TTL metadata", prepareDeadlineMs) {
                metadata = [:]
                sql_return_maparray("show tablets from ${tableName}").each { tablet ->
                    long id = tablet.TabletId as Long
                    long primary = tablet.PrimaryBackendId as Long
                    if (!(id in tabletIds) || !backends.containsKey(primary) || (tablet.BackendId as Long) != primary) {
                        return
                    }
                    def be = backends[primary]
                    try {
                        def meta = parseJson(new URL("http://${be.Host}:${be.HttpPort}/api/meta/header/${id}")
                                .getText(connectTimeout: 5000, readTimeout: 5000))
                        if ((meta.tablet_id as Long) == id && (meta.creation_time as Long) > 0L &&
                                (meta.ttl_seconds as Long) == ttlSeconds &&
                                meta.tablet_state == "PB_RUNNING" && meta.schema != null) {
                            metadata[id] = [be_id: primary, creation_time: meta.creation_time as Long,
                                            expires_at: (meta.creation_time as Long) + ttlSeconds]
                        }
                    } catch (Exception e) {
                        logger.info("Waiting for loaded metadata: tablet=${id}, BE=${primary}, ${e.message}")
                    }
                }
                lastState = [missing_tablets: tabletIds - metadata.keySet(), metadata: metadata]
                metadata.size() == tabletIds.size()
            }
            long now = serverTime()
            assertTrue(metadata.values().every { it.creation_time <= now }, "Future tablet creation time: ${metadata}, now=${now}")
            long earliestExpiry = metadata.values().min { it.expires_at }.expires_at
            long latestExpiry = metadata.values().max { it.expires_at }.expires_at
            logger.info("TTL deadlines: ttl_seconds=${ttlSeconds}, server_now=${now}, by_tablet=${metadata}")
            def baseline = [:]
            def candidate = null
            long stableSince = 0L
            waitUntil("stable TTL baseline before expiry", prepareDeadlineMs) {
                baseline = getCache()
                now = serverTime()
                assertTrue(now + 30L < earliestExpiry, "Preparation exhausted TTL window: now=${now}, metadata=${metadata}")
                def missing = tabletIds.findAll { id -> !baseline.keySet().any { it[2] == id } }
                boolean allTtl = missing.isEmpty() && baseline.values().every { it.type == "ttl" }
                lastState = [missing_tablets: missing, cache_by_be_tablet: summarizeCache(baseline), seconds_to_expiry: earliestExpiry - now]
                if (baseline != candidate) {
                    logger.info("TTL baseline progress: ${lastState}")
                }
                if (!allTtl || baseline != candidate) {
                    stableSince = System.currentTimeMillis()
                }
                candidate = baseline
                allTtl && System.currentTimeMillis() - stableSince >= 3000L
            }
            logger.info("TTL baseline: preparation_elapsed_ms=${System.currentTimeMillis() - prepareStartedMs}, " +
                    "cache_by_be_tablet=${summarizeCache(baseline)}, blocks=${baseline}")
            metricsBaseline = logMetrics("ttl_baseline")

            // Keep TTL unchanged: this case covers natural expiry, separately from ALTER TTL cases.
            // A new interval does not wake a thread already sleeping for the old 180-second interval.
            now = serverTime()
            long conversionDeadlineMs = System.currentTimeMillis() + Math.max(0L, latestExpiry + 1L - now) * 1000L + 600000L
            def firstNormalAt = [:]
            def converted = [:]
            def previous = null
            long lastLogMs = 0L
            stableSince = 0L
            waitUntil("natural TTL expiration", conversionDeadlineMs) {
                // Do not read business data until the original blocks have converted completely.
                converted = getCache()
                now = serverTime() // Read time after the snapshot to avoid a boundary crossing during the query.
                def differences = [missing: (baseline.keySet() - converted.keySet()).take(10),
                                   unexpected: (converted.keySet() - baseline.keySet()).take(10),
                                   changed_sizes: converted.findAll { key, value -> value.size != baseline[key]?.size }.take(10)]
                converted.each { key, value ->
                    // FE sets creation_time; BE checks its own clock. Allow two seconds of clock skew.
                    assertTrue(value.type != "normal" || now + 2L >= metadata[key[2]].expires_at,
                            "Cache converted before expiry: block=${key}, now=${now}, metadata=${metadata[key[2]]}")
                }
                tabletIds.each { id ->
                    def expected = baseline.findAll { key, value -> key[2] == id }
                    def actual = converted.findAll { key, value -> key[2] == id }
                    if (!firstNormalAt.containsKey(id) && blockSizes(expected) == blockSizes(actual) &&
                            actual.values().every { it.type == "normal" }) {
                        firstNormalAt[id] = now
                    }
                }
                boolean complete = blockSizes(baseline) == blockSizes(converted) &&
                        converted.values().every { it.type == "normal" } && now > latestExpiry
                lastState = [server_now: now, seconds_to_last_expiry: latestExpiry - now,
                             cache_by_be_tablet: summarizeCache(converted), differences: differences,
                             tablets_not_yet_normal: tabletIds - firstNormalAt.keySet()]
                if (converted != previous || System.currentTimeMillis() - lastLogMs >= 30000L) {
                    logger.info("TTL expiration progress: ${lastState}")
                    lastLogMs = System.currentTimeMillis()
                }
                if (!complete || converted != previous) {
                    stableSince = System.currentTimeMillis()
                }
                previous = converted
                complete && System.currentTimeMillis() - stableSince >= 3000L
            }
            def timing = metadata.collectEntries { id, meta ->
                [(id): meta + [first_normal_observed_at: firstNormalAt[id],
                               observed_conversion_lag_seconds: firstNormalAt[id] - meta.expires_at]]
            }
            logger.info("TTL natural expiration verified: by_tablet=${timing}, " +
                    "before=${summarizeCache(baseline)}, after=${summarizeCache(converted)}")
            logMetrics("normal_after_expiry")
            scanTable()
        } catch (Throwable t) {
            failure = t
            logger.warn("TTL case failed: cluster=${clusterName}, metadata=${metadata}, last_state=${lastState}")
            try { logMetrics("failure") } catch (Throwable diagnosticError) { t.addSuppressed(diagnosticError) }
            throw t
        } finally {
            def cleanupErrors = []
            try { sql "drop table if exists ${tableName} force" } catch (Throwable t) { cleanupErrors.add(t) }
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
