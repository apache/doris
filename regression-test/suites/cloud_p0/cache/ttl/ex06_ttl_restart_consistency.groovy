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

import org.apache.doris.regression.suite.ClusterOptions

suite("ex06_ttl_restart_consistency", "docker") {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 1
    options.msNum = 1
    options.cloudMode = true
    options.beConfigs += [
        "enable_file_cache=true",
        "file_cache_enter_disk_resource_limit_mode_percent=99"
    ]

    docker(options) {
        if (!isCloudMode()) {
            return
        }

        def clusters = sql "SHOW CLUSTERS"
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """use @${validCluster};"""

        String tableName = "ex06_ttl_restart_consistency"
        // The other direction: a table that only gets its TTL by ALTER.
        String gainTableName = "ex06_ttl_restart_consistency_gain"
        def ddl = new File("""${context.file.parent}/../ddl/ex06_ttl_restart_consistency.sql""").text
        sql ddl.replace("\${TABLE_NAME}", tableName)
        def noTtlDdl = ddl.replace("\${TABLE_NAME}", gainTableName)
                .replace('"file_cache_ttl_seconds" = "900"', '"file_cache_ttl_seconds" = "0"')
        assertTrue(noTtlDdl.contains('"file_cache_ttl_seconds" = "0"'))
        sql noTtlDdl

        try {
            def getTabletIds = { String table ->
                def tablets = sql """show tablets from ${table}"""
                assertTrue(!tablets.isEmpty(), "No tablets found for table ${table}")
                tablets.collect { it[0] as Long }
            }

            def waitForFileCacheType = { List<Long> tabletIds, String expectedType,
                                         long timeoutMs = 1800000L, long intervalMs = 2000L ->
                long start = System.currentTimeMillis()
                logger.info("tablets collection is ${tabletIds.toString()}")
                while (System.currentTimeMillis() - start < timeoutMs) {
                    boolean allMatch = true
                    for (Long tabletId in tabletIds) {
                        def rows = sql """select type from information_schema.file_cache_info where tablet_id = ${tabletId}"""
                        logger.info("Tablet ${tabletId} file cache info: ${rows.toString()}")
                        if (rows.isEmpty()) {
                            allMatch = false
                            break
                        }
                        // Without TTL a block shows its class: NORMAL, or INDEX when a query cached
                        // it first (a segment footer), which a later fill does not change.
                        def mismatch = rows.find { row ->
                            def type = row[0]?.toString() ?: ""
                            expectedType == "normal" ? type.equalsIgnoreCase("ttl") : !type.equalsIgnoreCase(expectedType)
                        }
                        if (mismatch) {
                            allMatch = false
                            break
                        }
                    }
                    if (allMatch) {
                        return
                    }
                    sleep(intervalMs)
                }
                assertTrue(false, "Timeout waiting type=${expectedType}, tablets=${tabletIds}")
            }

            // TYPE above is computed from the tablet's current TTL. Where the blocks actually are
            // shows in the size of the TTL queue, which must come to hold exactly the blocks of
            // the given tablets.
            def ttlQueueBytes = {
                long total = 0
                sql_return_maparray("SHOW BACKENDS").each { be ->
                    def metrics = new URL("http://${be.Host}:${be.BrpcPort}/brpc_metrics").text
                    (metrics =~ /file_cache_ttl_cache_size\s+(\d+)/).each { m -> total += m[1] as long }
                }
                total
            }
            def cachedBytes = { List<Long> tabletIds ->
                def rows = sql """select sum(size) from information_schema.file_cache_info
                        where tablet_id in (${tabletIds.join(',')})"""
                rows[0][0] == null ? 0L : rows[0][0] as long
            }
            def waitForTtlQueueToHold = { List<Long> tabletIds, long timeoutMs = 600000L ->
                long start = System.currentTimeMillis()
                while (System.currentTimeMillis() - start < timeoutMs) {
                    long expected = cachedBytes(tabletIds)
                    long actual = ttlQueueBytes()
                    logger.info("TTL queue holds ${actual} bytes, tablets ${tabletIds} have ${expected}")
                    if (expected > 0 && actual == expected) {
                        return
                    }
                    sleep(2000)
                }
                assertTrue(false, "TTL queue does not hold exactly the blocks of tablets ${tabletIds}")
            }

            def assertTtlInShowCreate = { String table, long ttlSeconds ->
                def rows = sql """show create table ${table};"""
                assertTrue(!rows.isEmpty(), "No show create table result for ${table}")
                def createTableSql = rows[0][1].toString()
                logger.info("show create table ${table}: ${createTableSql}")
                assertTrue(createTableSql.contains("\"file_cache_ttl_seconds\" = \"${ttlSeconds}\""),
                        "file_cache_ttl_seconds is not ${ttlSeconds} in show create table result: ${createTableSql}")
            }

            def values = (0..<300).collect { i -> "(${i}, 'restart_${i}')" }.join(",")
            sql """insert into ${tableName} values ${values}"""
            sql """insert into ${gainTableName} values ${values}"""
            qt_ex06_preheat """select count(*) from ${tableName} where c1 like 'restart_%'"""
            qt_ex06_gain_preheat """select count(*) from ${gainTableName} where c1 like 'restart_%'"""
            sleep(5000)

            def tabletIds = getTabletIds.call(tableName)
            def gainTabletIds = getTabletIds.call(gainTableName)
            waitForFileCacheType.call(tabletIds, "ttl")
            waitForFileCacheType.call(gainTabletIds, "normal")
            waitForTtlQueueToHold.call(tabletIds)

            // EX-06: the TTL is removed right before a restart, and the other way round, the TTL
            // is added right before a restart. Both must converge after it.
            sql """alter table ${tableName} set ("file_cache_ttl_seconds"="0")"""
            sql """alter table ${gainTableName} set ("file_cache_ttl_seconds"="900")"""
            assertTtlInShowCreate.call(tableName, 0)
            assertTtlInShowCreate.call(gainTableName, 900)
            cluster.restartBackends()
            assertTtlInShowCreate.call(tableName, 0)
            assertTtlInShowCreate.call(gainTableName, 900)
            sleep(10000)

            qt_ex06_after_restart """select count(*) from ${tableName} where c1 like 'restart_%'"""
            qt_ex06_gain_after_restart """select count(*) from ${gainTableName} where c1 like 'restart_%'"""
            waitForFileCacheType.call(tabletIds, "normal")
            waitForFileCacheType.call(gainTabletIds, "ttl")
            waitForTtlQueueToHold.call(gainTabletIds)
        } finally {
            sql """drop table if exists ${tableName}"""
            sql """drop table if exists ${gainTableName}"""
        }
    }
}
