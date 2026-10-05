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

suite("st04_alter_ttl_n_to_0_runtime", "nonConcurrent") {
    def customBeConfig = [
        enable_evict_file_cache_in_advance : false,
        file_cache_enter_disk_resource_limit_mode_percent : 99,
        file_cache_background_ttl_gc_interval_ms : 1000,
        file_cache_background_ttl_info_update_interval_ms : 1000,
        file_cache_background_tablet_id_flush_interval_ms : 1000
    ]

    setBeConfigTemporary(customBeConfig) {
        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        String clusterName = clusters[0][0].toString()
        def backends = sql_return_maparray("SHOW BACKENDS").findAll { be ->
            "${be.Alive}".equalsIgnoreCase("true") &&
                    parseJson(be.Tag.toString()).compute_group_name == clusterName
        }.collectEntries { be -> [(be.BackendId as Long): be] }
        assertTrue(!backends.isEmpty(), "No alive backends in ${clusterName}")
        def backendIds = backends.keySet()
        String tableName = "st04_ttl_n_to_0_tpl"
        String dataQuery = "select count(*), sum(k1), sum(length(v1)) from ${tableName}"
        def tabletIds = []
        def originalQueryCache = sql("select @@enable_sql_cache, @@enable_query_cache")[0].collect { value ->
            String setting = value.toString().toLowerCase(Locale.ROOT)
            assertTrue(setting in ["true", "false", "0", "1"], "Unexpected cache setting: ${value}")
            setting
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
                assertTrue(backendIds.contains(key[0]) && tabletIds.contains(key[2]),
                        "Cache outside ${clusterName}/${tabletIds}: ${row}")
                assertTrue(!blocks.containsKey(key), "Duplicate cache block: ${key}")
                blocks[key] = [size: row[5] as Long, type: row[6]?.toString()]
            }
            blocks
        }
        def summarizeCache = { Map blocks ->
            blocks.groupBy { key, value -> [key[0], key[2]] }.collectEntries { key, entries ->
                [(key): [blocks: entries.size(),
                         bytesByType: entries.values().groupBy { it.type }.collectEntries { type, values ->
                             [(type): values.sum(0L) { it.size }]
                         }]]
            }
        }
        def waitForCache = { String description, Closure ready ->
            // Updating the interval cannot interrupt an already sleeping background thread.
            long deadline = System.currentTimeMillis() + 600000L
            def blocks = [:]
            def previous = null
            while (System.currentTimeMillis() < deadline) {
                blocks = getCache()
                if (blocks != previous) {
                    logger.info("${description}: cluster=${clusterName}, " +
                            "cache_by_be_tablet=${summarizeCache(blocks)}, blocks=${blocks}")
                    previous = blocks
                }
                if (ready(blocks)) {
                    return blocks
                }
                sleep(2000)
            }
            assertTrue(false, "Timeout after 600000ms waiting for ${description}: " +
                    "cluster=${clusterName}, tablets=${tabletIds}, BEs=${backendIds}, blocks=${blocks}")
        }
        def getGlobalTtlBytes = {
            // Diagnostic only: other tables and busy blocks may keep the BE's TTL queue nonempty.
            backends.collectEntries { id, be ->
                Long bytes = null
                try {
                    String metrics = new URL("http://${be.Host}:${be.BrpcPort}/brpc_metrics")
                            .getText(connectTimeout: 5000, readTimeout: 5000)
                    def values = metrics.readLines().findResults { line ->
                        def fields = line.trim().split(/\s+/)
                        if (fields.size() == 2 && (fields[0] == "file_cache_ttl_cache_size" ||
                                fields[0].endsWith("_file_cache_ttl_cache_size"))) {
                            return fields[1] as Long
                        }
                        null
                    }
                    if (!values.isEmpty()) {
                        bytes = values.sum(0L)
                    }
                } catch (Exception e) {
                    logger.warn("Cannot read diagnostic TTL metric for BE ${id}: ${e.message}")
                }
                [(id): bytes]
            }
        }

        try {
            sql "use @${clusterName}"
            sql "set enable_sql_cache = false"
            sql "set enable_query_cache = false"
            def ddl = new File("${context.file.parent}/../ddl/st04_alter_ttl_n_to_0_runtime.sql").text
                    .replace('${TABLE_NAME}', tableName)
            sql ddl
            tabletIds = sql_return_maparray("show tablets from ${tableName}")
                    .collect { it.TabletId as Long }.unique().sort()
            assertTrue(tabletIds.size() == 8, "Expected 8 tablets: ${tabletIds}")

            // Fresh tablet IDs isolate this case without clearing other tables' caches.
            [0, 200].each { start ->
                def values = (start..<(start + 200)).collect { i -> "(${i}, 'value_${i}')" }.join(",")
                sql "insert into ${tableName} values ${values}"
            }
            qt_before_alter dataQuery

            def candidate = null
            long stableSince = 0L
            def baseline = waitForCache("stable TTL baseline") { blocks ->
                boolean allTtl = tabletIds.every { id -> blocks.keySet().any { it[2] == id } } &&
                        blocks.values().every { it.size > 0L && it.type == "ttl" }
                if (!allTtl || blocks != candidate) {
                    candidate = allTtl ? blocks : null
                    stableSince = System.currentTimeMillis()
                    return false
                }
                System.currentTimeMillis() - stableSince >= 3000L
            }
            def globalBefore = getGlobalTtlBytes()
            logger.info("TTL before ALTER: global_by_be=${globalBefore}, " +
                    "case_by_be_tablet=${summarizeCache(baseline)}")
            sql "alter table ${tableName} set (\"file_cache_ttl_seconds\"=\"0\")"

            // Do not read table data here: verify the background conversion of existing blocks.
            def lastDifference = null
            def converted = waitForCache("existing TTL blocks to become NORMAL") { blocks ->
                def difference = [missing: baseline.keySet() - blocks.keySet(),
                                  unexpected: blocks.keySet() - baseline.keySet(),
                                  changed: blocks.findAll { key, value ->
                                      value.type != "normal" || value.size != baseline[key]?.size
                                  }]
                if (difference != lastDifference) {
                    logger.info("TTL conversion differences: ${difference}")
                    lastDifference = difference
                }
                difference.values().every { it.isEmpty() }
            }
            // Exact block equality also ensures every tablet retains its nonzero baseline bytes.
            def globalAfter = getGlobalTtlBytes()
            def globalDelta = backendIds.collectEntries { id ->
                [(id): globalBefore[id] != null && globalAfter[id] != null ?
                        globalAfter[id] - globalBefore[id] : null]
            }
            logger.info("TTL after ALTER: global_by_be=${globalAfter}, global_delta_by_be=${globalDelta}, " +
                    "case_by_be_tablet=${summarizeCache(converted)}")
            qt_after_alter dataQuery
        } finally {
            try {
                sql "use @${clusterName}"
                sql "drop table if exists ${tableName} force"
            } finally {
                try {
                    sql "set enable_sql_cache = ${originalQueryCache[0]}"
                } finally {
                    sql "set enable_query_cache = ${originalQueryCache[1]}"
                }
            }
        }
    }
}
