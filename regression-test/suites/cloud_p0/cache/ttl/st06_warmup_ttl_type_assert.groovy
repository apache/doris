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

suite("st06_warmup_ttl_type_assert") {
    def customBeConfig = [
        enable_evict_file_cache_in_advance : false,
        file_cache_enter_disk_resource_limit_mode_percent : 99
    ]

    setBeConfigTemporary(customBeConfig) {
        def clusters = sql "SHOW CLUSTERS"
        if (clusters.size() < 2) {
            logger.info("skip st06_warmup_ttl_type_assert, need at least 2 clusters")
            return
        }

        def sourceCluster = clusters[0][0]
        def targetCluster = clusters[1][0]
        String tableName = "st06_warmup_ttl_tpl"

        sql """use @${sourceCluster};"""
        def ddl = new File("""${context.file.parent}/../ddl/st06_warmup_ttl_type_assert.sql""").text
                .replace("\${TABLE_NAME}", tableName)
        sql ddl

        def values = (0..<200).collect { i -> "(${i}, 'warmup_tpl_${i}')" }.join(",")
        sql """insert into ${tableName} values ${values}"""
        qt_source_preheat """select count(*) from ${tableName}"""

        def sourceTablets = sql """show tablets from ${tableName}"""
        assertTrue(sourceTablets.size() > 0, "No tablets found for table ${tableName} in source cluster ${sourceCluster}")
        def sourceTabletIds = sourceTablets.collect { it[0] as Long }

        // ST-06 部分覆盖点模板：显式断言 warmup 后目标集群缓存类型为 ttl
        def jobIdRows = sql """warm up cluster ${targetCluster} with table ${tableName};"""
        assertTrue(!jobIdRows.isEmpty())
        def jobId = jobIdRows[0][0]

        def waitWarmUpJobFinished = { Object id, long timeoutMs = 600000L, long intervalMs = 5000L ->
            long start = System.currentTimeMillis()
            while (System.currentTimeMillis() - start < timeoutMs) {
                def stateRows = sql """SHOW WARM UP JOB WHERE ID = ${id}"""
                if (stateRows.isEmpty()) {
                    sleep(intervalMs)
                    continue
                }
                def state = stateRows[0][3].toString()
                if ("FINISHED".equalsIgnoreCase(state)) {
                    return
                }
                if ("CANCELLED".equalsIgnoreCase(state) || "FAILED".equalsIgnoreCase(state)) {
                    assertTrue(false, "Warm up job failed, id=${id}, state=${state}")
                }
                sleep(intervalMs)
            }
            assertTrue(false, "Timeout waiting warm up job finished, id=${id}")
        }
        waitWarmUpJobFinished.call(jobId)

        sql """use @${targetCluster};"""
        qt_target_query """select count(*) from ${tableName}"""
        def targetTablets = sql """show tablets from ${tableName}"""
        assertTrue(targetTablets.size() > 0, "No tablets found for table ${tableName} in target cluster ${targetCluster}")
        def targetTabletIds = targetTablets.collect { it[0] as Long }
        assertTrue(sourceTabletIds.size() == targetTabletIds.size(),
                "Tablet size mismatch between source and target, source=${sourceTabletIds.size()}, target=${targetTabletIds.size()}")

        def waitForFileCacheType = { List<Long> sourceIds, List<Long> targetIds, String expectedType, long timeoutMs = 600000L, long intervalMs = 2000L ->
            logger.info("waitForFileCacheType, sourceIds=${sourceIds.toString()}, targetIds=${targetIds.toString()}, expectedType=${expectedType}")
            assertTrue(sourceIds.size() == targetIds.size(),
                    "Tablet size mismatch before waiting file cache type, source=${sourceIds.size()}, target=${targetIds.size()}")
            long start = System.currentTimeMillis()
            while (System.currentTimeMillis() - start < timeoutMs) {
                int sourceMatched = 0
                int targetMatched = 0
                for (Long sourceTabletId in sourceIds) {
                    def sourceTabletIdStr = sql """select * from information_schema.file_cache_info where tablet_id = ${sourceTabletId}"""
                    logger.info("[source tablet] tablet_id=${sourceTabletId}, tablet_cache_info=${sourceTabletIdStr.toString()}")
                    def rows = sql """select type from information_schema.file_cache_info where tablet_id = ${sourceTabletId}"""
                    if (!rows.isEmpty()) {
                        def mismatch = rows.find { row -> !row[0]?.toString()?.equalsIgnoreCase(expectedType) }
                        if (!mismatch) {
                            sourceMatched++
                        }
                    }
                }
                for (Long targetTabletId in targetIds) {
                    def targetTabletIdStr = sql """select * from information_schema.file_cache_info where tablet_id = ${targetTabletId}"""
                    logger.info("[target tablet] tablet_id=${targetTabletId}, tablet_cache_info=${targetTabletIdStr.toString()}")
                    def rows = sql """select type from information_schema.file_cache_info where tablet_id = ${targetTabletId}"""
                    if (!rows.isEmpty()) {
                        def mismatch = rows.find { row -> !row[0]?.toString()?.equalsIgnoreCase(expectedType) }
                        if (!mismatch) {
                            targetMatched++
                        }
                    }
                }
                sleep(intervalMs)
            }
            assertTrue(false, "Timeout waiting for ${expectedType}, sourceTablets=${sourceIds}, targetTablets=${targetIds}")
        }
        waitForFileCacheType.call(sourceTabletIds, targetTabletIds, "ttl")

        // cleanup
        sql """use @${sourceCluster};"""
        sql """drop table if exists ${tableName}"""
    }
}
