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

suite("test_file_cache_info", "nonConcurrent") {
    def customBeConfig = [
        enable_evict_file_cache_in_advance: false,
        file_cache_enter_disk_resource_limit_mode_percent: 99
    ]
    setBeConfigTemporary(customBeConfig) {
        String tableName = "test_file_cache_info_lifecycle"
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty(), "No compute group found")
        String clusterName = clusters[0][0].toString()
        String dbName = sql("SELECT DATABASE()")[0][0].toString()
        def backends = sql_return_maparray("SHOW BACKENDS").findAll { be ->
            "${be.Alive}".equalsIgnoreCase("true") &&
                    parseJson(be.Tag.toString()).compute_group_name == clusterName
        }.collectEntries { be -> [(be.BackendId as Long): be] }
        assertTrue(!backends.isEmpty(), "No alive backends in ${clusterName}")
        def backendIds = backends.keySet()
        Long tabletId = null
        def lastState = [:]
        def clearResponses = [:]
        def originalQueryCache = sql("select @@enable_sql_cache, @@enable_query_cache")[0].collect { value ->
            String setting = value.toString().toLowerCase(Locale.ROOT)
            assertTrue(setting in ["true", "false", "0", "1"], "Unexpected cache setting: ${value}")
            setting
        }
        def expectedRows = (1..5).collect { i ->
            [i as Long, String.format('Customer#%09d', i), "address${i}".toString(),
             "city${i}".toString(), "nation${i}".toString(), "region${i}".toString(),
             "phone${i}".toString(), "segment${i}".toString()]
        }
        def readTable = {
            sql "use @${clusterName}"
            // Read and validate every column; result caches are disabled for both reads.
            def rows = sql "select * from ${tableName} order by c_custkey"
            assertTrue(rows.every { it.size() == 8 && it.every { value -> value != null } },
                    "Unexpected data result: ${rows}")
            def normalized = rows.collect { row -> [row[0] as Long] + row.drop(1).collect { it.toString() } }
            assertEquals(expectedRows, normalized)
        }
        def getCache = {
            sql "use @${clusterName}"
            def rows = sql """select be_id, cache_path, tablet_id, `hash`, `offset`, size, lower(type)
                from information_schema.file_cache_info
                where tablet_id = ${tabletId} and be_id in (${backendIds.join(',')})"""
            def blocks = [:]
            rows.each { row ->
                def key = [row[0] as Long, row[1].toString(), row[2] as Long,
                           row[3].toString(), row[4] as Long]
                assertTrue(backendIds.contains(key[0]) && key[2] == tabletId, "Unexpected cache owner: ${row}")
                assertTrue(key[4] >= 0L && (row[5] as Long) > 0L, "Invalid cache range: ${row}")
                assertTrue(!blocks.containsKey(key), "Duplicate cache block: ${key}")
                blocks[key] = [size: row[5] as Long, type: row[6]?.toString()]
            }
            blocks
        }
        def summarizeCache = { Map blocks ->
            blocks.groupBy { key, value -> [key[0], key[2]] }.collectEntries { owner, entries ->
                [(owner): [block_count: entries.size(), bytes: entries.values().sum(0L) { it.size },
                           bytes_by_type: entries.values().groupBy { it.type }.collectEntries { type, values ->
                               [(type): values.sum(0L) { it.size }]
                           }]]
            }
        }
        def waitForCache = { String phase, boolean expectPresent, long timeoutMs ->
            long startedMs = System.currentTimeMillis()
            long deadlineMs = startedMs + timeoutMs
            long stableSince = 0L
            long lastLogMs = 0L
            def previous = null
            while (System.currentTimeMillis() < deadlineMs) {
                def blocks = getCache()
                boolean matches = expectPresent ? !blocks.isEmpty() : blocks.isEmpty()
                lastState = [phase: phase, elapsed_ms: System.currentTimeMillis() - startedMs,
                             cache_by_be_tablet: summarizeCache(blocks), blocks: blocks]
                if (blocks != previous || System.currentTimeMillis() - lastLogMs >= 30000L) {
                    logger.info("file_cache_info lifecycle: cluster=${clusterName}, ${lastState}")
                    lastLogMs = System.currentTimeMillis()
                }
                if (!matches || blocks != previous) {
                    stableSince = System.currentTimeMillis()
                }
                previous = blocks
                // A transient empty metadata snapshot is not a completed clear.
                if (matches && System.currentTimeMillis() - stableSince >= 3000L) {
                    logger.info("file_cache_info phase complete: ${lastState}")
                    return blocks
                }
                sleep(1000)
            }
            assertTrue(false, "Timeout waiting for ${phase}: cluster=${clusterName}, tablet=${tabletId}, " +
                    "BEs=${backendIds}, clear_responses=${clearResponses}, last_state=${lastState}")
        }
        def waitForReaders = {
            long deadlineMs = System.currentTimeMillis() + 60000L
            long quietSince = System.currentTimeMillis()
            while (System.currentTimeMillis() < deadlineMs) {
                def active = sql_return_maparray("SHOW PROCESSLIST").findAll { row ->
                    row.Db?.toString() == dbName && row.Info?.toString()?.contains(tableName) &&
                            !"${row.Command}".equalsIgnoreCase("Sleep")
                }
                lastState = [phase: "reader_drain", active_queries: active]
                if (!active.isEmpty()) {
                    quietSince = System.currentTimeMillis()
                } else if (System.currentTimeMillis() - quietSince >= 3000L) {
                    return
                }
                sleep(1000)
            }
            assertTrue(false, "Timeout waiting for table queries to finish: ${lastState}")
        }
        def clearCache = { long beId ->
            def be = backends[beId]
            String url = "http://${be.Host}:${be.HttpPort}/api/file_cache?op=clear&sync=true"
            long startedMs = System.currentTimeMillis()
            clearResponses[beId] = [url: url]
            logger.info("file_cache_info clear start: BE=${beId}, url=${url}")
            def connection = new URL(url).openConnection()
            connection.setConnectTimeout(5000)
            connection.setReadTimeout(60000)
            try {
                int status = connection.getResponseCode()
                String body = (status == 200 ? connection.getInputStream() : connection.getErrorStream())
                        ?.withCloseable { it.getText("UTF-8") } ?: ""
                clearResponses[beId] += [http_status: status, elapsed_ms: System.currentTimeMillis() - startedMs,
                                         response: body]
                logger.info("file_cache_info clear response: BE=${beId}, ${clearResponses[beId]}")
                assertEquals(200, status, "Clear failed on BE ${beId}: ${body}")
                def response = parseJson(body)
                assertTrue("${response.status}".equalsIgnoreCase("OK"), "Clear failed on BE ${beId}: ${body}")
                def counters = ["num_files_all", "num_cells_all", "num_cells_to_delete", "num_cells_wait_recycle"]
                        .collectEntries { name ->
                            def matcher = response.msg?.toString() =~ /\b${name}=(\d+)/
                            def values = []
                            while (matcher.find()) { values.add(matcher.group(1) as Long) }
                            [(name): values.isEmpty() ? null : values.sum(0L)]
                        }
                clearResponses[beId].counters = counters
                logger.info("file_cache_info clear counters: BE=${beId}, ${counters}")
            } catch (Throwable t) {
                clearResponses[beId].error = t.toString()
                clearResponses[beId].elapsed_ms = System.currentTimeMillis() - startedMs
                throw t
            } finally {
                connection.disconnect()
            }
        }
        Throwable failure = null
        try {
            sql "use @${clusterName}"
            sql "set enable_sql_cache = false"
            sql "set enable_query_cache = false"
            sql "drop table if exists ${tableName} force"
            sql """CREATE TABLE ${tableName} (
                c_custkey INT, c_name STRING, c_address STRING, c_city STRING,
                c_nation STRING, c_region STRING, c_phone STRING, c_mktsegment STRING
            ) DUPLICATE KEY(c_custkey)
            DISTRIBUTED BY HASH(c_custkey) BUCKETS 1
            PROPERTIES("file_cache_ttl_seconds"="3600", "disable_auto_compaction"="true")"""
            def tablets = sql_return_maparray("show tablets from ${tableName}")
                    .collect { it.TabletId as Long }.unique()
            assertEquals(1, tablets.size(), "Expected one tablet: ${tablets}")
            tabletId = tablets[0]
            String values = expectedRows.collect { row ->
                "(${row[0]}, " + row.drop(1).collect { "'${it}'" }.join(", ") + ")"
            }.join(", ")
            sql "insert into ${tableName} values ${values}"
            readTable()
            def baseline = waitForCache("populated", true, 120000L)
            waitForReaders()
            // SQL quiescence does not prove that every BE holder has been released. The scoped
            // empty-state check below remains the barrier for clear and async Meta Store deletion.
            def cacheOwners = baseline.keySet().collect { it[0] }.unique().sort()
            logger.info("file_cache_info before clear: cluster=${clusterName}, tablet=${tabletId}, " +
                    "cache_owners=${cacheOwners}, cache_by_be_tablet=${summarizeCache(baseline)}, blocks=${baseline}")
            long clearStartedMs = System.currentTimeMillis()
            cacheOwners.each { clearCache(it as Long) }
            waitForCache("cleared", false, 600000L)
            logger.info("file_cache_info clear verified: tablet=${tabletId}, " +
                    "clear_to_empty_ms=${System.currentTimeMillis() - clearStartedMs}, responses=${clearResponses}")

            // Only now read the business table again. No repeated clear or forced release can
            // bypass a stuck holder; persistent remnants must fail with the block details above.
            long reloadStartedMs = System.currentTimeMillis()
            readTable()
            def reloaded = waitForCache("reloaded", true, 120000L)
            // Readers may split a file into different blocks than the INSERT writer did.
            logger.info("file_cache_info reload verified: tablet=${tabletId}, " +
                    "read_to_visible_ms=${System.currentTimeMillis() - reloadStartedMs}, " +
                    "cache_by_be_tablet=${summarizeCache(reloaded)}, blocks=${reloaded}")
        } catch (Throwable t) {
            failure = t
            logger.warn("file_cache_info lifecycle failed: cluster=${clusterName}, tablet=${tabletId}, " +
                    "clear_responses=${clearResponses}, last_state=${lastState}, error=${t}")
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
