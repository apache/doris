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

suite("sync_insert", "nonConcurrent") {
    String sourceCluster = "regression_cluster_name0"
    String targetCluster = "regression_cluster_name1"
    String tableName = "test_dup_tab_basic_int_tab_nullable"
    def values = [
        [9,10,11,12], [9,10,11,12], [21,null,23,null], [1,2,3,4],
        [1,2,3,4], [13,14,15,16], [13,21,22,16], [13,14,15,16],
        [13,21,22,16], [17,18,19,20], [17,18,19,20], [null,21,null,23],
        [22,null,24,25], [26,27,null,29], [5,6,7,8], [5,6,7,8]
    ]
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
    logger.info("Sync insert backends: ${clusterBackends}")
    def tabletIds = []
    def primaryBackends = [:]
    def waitUntil = { String description, Closure condition ->
        long deadline = System.currentTimeMillis() + 120000L
        while (System.currentTimeMillis() < deadline) {
            if (condition()) {
                return
            }
            sleep(1000)
        }
        assertTrue(false, "Timeout after 120000ms: ${description}; see last state in log")
    }
    def getGlobalBytes = {
        [source: WarmupMetricsUtils.getBackendMetricSum(clusterBackends[sourceCluster], "file_cache_cache_size"),
         target: WarmupMetricsUtils.getBackendMetricSum(clusterBackends[targetCluster], "file_cache_cache_size")]
    }
    def getCache = { String clusterName ->
        sql "use @${clusterName}"
        def beIds = clusterBackends[clusterName].collect { it.id }
        def rows = sql """select tablet_id, be_id, lower(type), sum(size), count(*)
            from information_schema.file_cache_info
            where tablet_id in (${tabletIds.join(',')}) and be_id in (${beIds.join(',')})
            group by tablet_id, be_id, lower(type)"""
        rows.groupBy { "${it[0]}:${it[1]}".toString() }.collectEntries { key, entries ->
            [(key): [normalBytes: entries.findAll { it[2]?.toString() == "normal" }.sum(0L) { it[3] as Long },
                     otherBlocks: entries.findAll { it[2]?.toString() != "normal" }.sum(0L) { it[4] as Long },
                     blocks: entries.sum(0L) { it[4] as Long }]]
        }
    }
    def getHeader = { be, long tabletId ->
        def meta = null
        httpTest {
            endpoint "${be.ip}:${be.httpPort}"
            uri "/api/meta/header/${tabletId}"
            op "get"
            check { code, body ->
                if ((code as int) == 200) {
                    meta = parseJson(body.toString())
                }
            }
        }
        meta
    }

    try {
        sql "use @${sourceCluster}"
        String dbName = sql("select database()")[0][0].toString()
        sql "DROP TABLE IF EXISTS ${tableName} FORCE"
        sql """CREATE TABLE ${tableName} (
            siteid INT NULL, citycode INT NULL, userid INT NULL, pv INT NULL
        ) ENGINE=OLAP DUPLICATE KEY(siteid)
        DISTRIBUTED BY HASH(siteid) BUCKETS 1
        PROPERTIES("disable_auto_compaction"="true", "file_cache_ttl_seconds"="0")"""
        tabletIds = sql("show tablets from ${tableName}").collect { it[0] as Long }.unique()
        assertTrue(tabletIds.size() == 1, "Expected one tablet: ${tabletIds}")
        def initialPartitions = sql_return_maparray("show partitions from ${tableName}")
        assertTrue(initialPartitions.size() == 1, "Expected one partition: ${initialPartitions}")
        long initialVersion = initialPartitions[0].VisibleVersion as Long

        waitUntil("source and target primary tablet routes") {
            [sourceCluster, targetCluster].each { clusterName ->
                sql "use @${clusterName}"
                def members = clusterBackends[clusterName].collectEntries { [(it.id): it] }
                def routes = [:]
                sql_return_maparray("show tablets from ${tableName}").each { tablet ->
                    long id = tablet.TabletId as Long
                    long primary = tablet.PrimaryBackendId as Long
                    if (id in tabletIds && members.containsKey(primary) && (tablet.BackendId as Long) == primary) {
                        routes[id] = members[primary]
                    }
                }
                primaryBackends[clusterName] = routes
            }
            logger.info("Sync insert primary routes: ${primaryBackends}")
            [sourceCluster, targetCluster].every { primaryBackends[it].keySet() == tabletIds.toSet() }
        }
        def beforeClear = getGlobalBytes()
        clusterBackends.values().flatten().each { be ->
            WarmupMetricsUtils.clearFileCache(be.ip, be.httpPort)
        }
        def afterClear = getGlobalBytes()
        logger.info("Sync insert clear: before=${beforeClear}, after=${afterClear}, " +
                "source_minus_target_after_clear=${afterClear.source - afterClear.target}, " +
                "source_change_after_clear=${afterClear.source - beforeClear.source}, " +
                "target_change_after_clear=${afterClear.target - beforeClear.target}")
        [sourceCluster, targetCluster].each { clusterName ->
            def cache = getCache(clusterName)
            assertTrue(cache.isEmpty(), "New test tablet must be cold in ${clusterName}: ${cache}")
        }

        connect('root') {
            sql "use @${sourceCluster}"
            sql "use `${dbName.replace('`', '``')}`"
            String previousSyncLoad = sql("select @@enable_multi_cluster_sync_load")[0][0].toString()
            assertTrue(previousSyncLoad.toLowerCase() in ["true", "false", "1", "0"],
                    "Unexpected sync-load setting: ${previousSyncLoad}")
            try {
                sql "set enable_multi_cluster_sync_load = true"
                def tuples = values.collect { row ->
                    "(" + row.collect { it == null ? "NULL" : it.toString() }.join(',') + ")"
                }.join(',')
                sql "INSERT INTO ${tableName} VALUES ${tuples}"
            } finally {
                sql "set enable_multi_cluster_sync_load = ${previousSyncLoad}"
            }
        }

        sql "use @${sourceCluster}"
        long loadedVersion = -1L
        waitUntil("INSERT visible version") {
            def partitions = sql_return_maparray("show partitions from ${tableName}")
            logger.info("Sync insert visible partition state: ${partitions}")
            if (partitions.size() == 1) {
                loadedVersion = partitions[0].VisibleVersion as Long
            }
            loadedVersion > initialVersion
        }
        def expectedBytes = [:]
        waitUntil("source rowset metadata for the inserted rows") {
            def ready = [:]
            tabletIds.each { id ->
                try {
                    def meta = getHeader(primaryBackends[sourceCluster][id], id)
                    if (meta == null || (meta.tablet_id as Long) != id) {
                        return
                    }
                    // Cloud /api/meta/header omits rs_metas. This metadata scanner reads the
                    // source BE's rowset map without scanning data or filling target cache.
                    def rowsets = sql("""select rowset_id, start_version, end_version, rowset_num_rows,
                            data_disk_size, index_disk_size, num_segments
                        from information_schema.rowsets
                        where tablet_id = ${id} and backend_id = ${primaryBackends[sourceCluster][id].id}
                            and start_version > ${initialVersion} and end_version <= ${loadedVersion}""")
                            .collect { row ->
                                [rowset_id_v2: row[0], start_version: row[1] as Long, end_version: row[2] as Long,
                                 num_rows: row[3] as Long, data_disk_size: row[4] as Long,
                                 index_disk_size: row[5] as Long, num_segments: row[6] as Long]
                            }
                    logger.info("Inserted rowsets for ${id}: " + rowsets.collect {
                        [rowset: it.rowset_id_v2, start: it.start_version, end: it.end_version,
                         rows: it.num_rows, dataBytes: it.data_disk_size, indexBytes: it.index_disk_size,
                         segments: it.num_segments]
                    })
                    if (!rowsets.isEmpty() && rowsets.max { it.end_version as Long }.end_version as Long == loadedVersion &&
                            rowsets.sum(0L) { (it.num_rows ?: 0L) as Long } == values.size()) {
                        // This table has no inverted indexes. data_disk_size sums actual segment file sizes,
                        // including packed slices, without depending on a particular encoding or file format.
                        long bytes = rowsets.sum(0L) { (it.data_disk_size ?: 0L) as Long }
                        assertTrue(rowsets.every { ((it.index_disk_size ?: 0L) as Long) == 0L },
                                "Unexpected external index files for ${id}: ${rowsets}")
                        if (bytes > 0L) {
                            ready[id] = bytes
                        }
                    }
                } catch (Exception e) {
                    logger.info("Waiting for inserted rowset metadata on ${id}: ${e.message}")
                }
            }
            if (ready.keySet() == tabletIds.toSet()) {
                expectedBytes = ready
                return true
            }
            false
        }
        logger.info("Sync insert actual segment bytes: ${expectedBytes}")

        waitUntil("source and target NORMAL cache to match actual inserted segment bytes") {
            def source = getCache(sourceCluster)
            def target = getCache(targetCluster)
            logger.info("Sync insert cache: expected=${expectedBytes}, source=${source}, target=${target}, " +
                    "primary=${primaryBackends}")
            // Inspect one complete copy on each cluster's primary BE. Do not add replicated copies:
            // syncLoadForTablets can send the same tablet to multiple BEs in a target cluster.
            [sourceCluster, targetCluster].every { clusterName ->
                def cache = clusterName == sourceCluster ? source : target
                tabletIds.every { id ->
                    String key = "${id}:${primaryBackends[clusterName][id].id}".toString()
                    def copy = cache[key]
                    copy != null && copy.blocks > 0L && copy.otherBlocks == 0L &&
                            copy.normalBytes == expectedBytes[id]
                }
            }
        }

        // Query only after proving automatic download, so reads cannot mask a failed sync load.
        connect('root') {
            sql "use @${targetCluster}"
            sql "use `${dbName.replace('`', '``')}`"
            sql "set enable_sql_cache = false"
            sql "set enable_query_cache = false"
            def actual = sql("select siteid, citycode, userid, pv from ${tableName}")
                    .collect { row -> row.collect { it == null ? null : it as Long } }
            def expected = values.collect { row -> row.collect { it == null ? null : it as Long } }
            assertTrue(actual.countBy { it } == expected.countBy { it },
                    "Target data differs, including duplicate/NULL rows: actual=${actual}, expected=${expected}")
        }
    } finally {
        sql "DROP TABLE IF EXISTS ${tableName} FORCE"
    }
}
