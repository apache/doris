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

import groovy.json.JsonSlurper
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.WarmupMetricsUtils

// Covers table-level event-driven warmup on a MOW table with upsert writes, target reads, and full compaction.
suite('test_warm_up_event_on_tables_mow_compaction', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_warm_up_table_filter_refresh_interval_ms=1000',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
        'enable_compaction_delay_commit_for_warm_up=true',
        'warm_up_rowset_sync_wait_min_timeout_ms=20000',
        'warm_up_rowset_sync_wait_max_timeout_ms=30000',
    ]
    options.cloudMode = true
    options.beNum = 1

    def httpJson = { String method, String url, int readTimeoutMs = 180000 ->
        def conn = new URL(url).openConnection()
        conn.setRequestMethod(method)
        conn.setConnectTimeout(10000)
        conn.setReadTimeout(readTimeoutMs)
        def text = conn.responseCode >= 400 ? conn.errorStream?.text : conn.inputStream.text
        assert text != null && !text.trim().isEmpty() : "empty HTTP response from ${url}"
        return new JsonSlurper().parseText(text.trim())
    }

    def triggerFullCompaction = { ip, port, tabletId ->
        def status = httpJson("POST",
                "http://${ip}:${port}/api/compaction/run?tablet_id=${tabletId}&compact_type=full")
        assert status.status.toString().toLowerCase() in ["success", "already_exist"] :
                "trigger compaction failed on ${ip}:${port}, tablet=${tabletId}, status=${status}"
        return status
    }

    def waitForCompactionFinish = { ip, port, tabletId, timeoutMs ->
        long deadline = System.currentTimeMillis() + timeoutMs
        def lastStatus = null
        while (System.currentTimeMillis() < deadline) {
            lastStatus = httpJson("GET",
                    "http://${ip}:${port}/api/compaction/run_status?tablet_id=${tabletId}", 10000)
            assert lastStatus.status.toLowerCase() == "success" :
                    "compaction failed on ${ip}:${port}, tablet=${tabletId}, status=${lastStatus}"
            if (!lastStatus.run_status) {
                return lastStatus
            }
            sleep(1000)
        }
        assert false : "compaction did not finish on ${ip}:${port}, tablet=${tabletId}, last=${lastStatus}"
    }

    docker(options) {
        Closure sqlRunner = { String q -> sql(q) }

        def srcCluster = "warmup_source"
        def dstCluster = "warmup_target"
        def dbName = "test_on_tables_mow_compaction_db"
        def tableName = "mow_tbl"
        def jobIds = []

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster)

        try {
            sql """use @${srcCluster}"""
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS ${tableName} (
                       id INT NOT NULL,
                       value INT,
                       tag STRING
                   )
                   UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES (
                       "enable_unique_key_merge_on_write" = "true",
                       "file_cache_ttl_seconds" = "3600",
                       "disable_auto_compaction" = "true"
                   )"""

            def jobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (INCLUDE '${dbName}.${tableName}')
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)[0][0]
            jobIds << jobId
            assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, jobId,
                    ["${dbName}.${tableName}".toString()] as Set) ==
                    ["${dbName}.${tableName}".toString()] as Set

            WarmupMetricsUtils.clearFileCacheOnAllBackends(sqlRunner)
            def baseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)

            sql """use @${srcCluster}"""
            sql """use ${dbName}"""
            sql """INSERT INTO ${tableName} VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')"""
            def afterInitialLoad = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    baseMetrics.finished + 1, 60000)
            assert afterInitialLoad.finished >= baseMetrics.finished + 1 :
                    "initial MOW warmup should finish, metrics=${afterInitialLoad}"
            assert afterInitialLoad.failed == baseMetrics.failed :
                    "initial MOW warmup should not fail, metrics=${afterInitialLoad}"

            sql """use @${dstCluster}"""
            sql """use ${dbName}"""
            def initialRead = sql """SELECT count(*), sum(value) FROM ${tableName}"""
            assert initialRead[0][0].toString() == "3" : "target initial MOW count mismatch: ${initialRead}"
            assert initialRead[0][1].toString() == "60" : "target initial MOW sum mismatch: ${initialRead}"

            def beforeUpsert = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            sql """use @${srcCluster}"""
            sql """use ${dbName}"""
            sql """INSERT INTO ${tableName} VALUES (2, 200, 'b2'), (3, 300, 'c2')"""
            def afterUpsert = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    beforeUpsert.finished + 1, 60000)
            assert afterUpsert.finished >= beforeUpsert.finished + 1 :
                    "first MOW upsert warmup should finish, metrics=${afterUpsert}"
            assert afterUpsert.failed == beforeUpsert.failed :
                    "first MOW upsert warmup should not fail, metrics=${afterUpsert}"

            def beforeSecondUpsert = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            sql """INSERT INTO ${tableName} VALUES (2, 220, 'b3'), (4, 40, 'd')"""
            def afterSecondUpsert = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    beforeSecondUpsert.finished + 1, 60000)
            assert afterSecondUpsert.finished >= beforeSecondUpsert.finished + 1 :
                    "second MOW upsert warmup should finish, metrics=${afterSecondUpsert}"
            assert afterSecondUpsert.failed == beforeSecondUpsert.failed :
                    "second MOW upsert warmup should not fail, metrics=${afterSecondUpsert}"

            def tablets = sql_return_maparray """SHOW TABLETS FROM ${tableName}"""
            assert tablets.size() == 1 : "${tableName} should have one tablet, tablets=${tablets}"
            def tabletId = tablets[0].TabletId.toString()
            def sourceBe = WarmupMetricsUtils.getClusterBackends(sqlRunner, srcCluster)[0]
            def beforeCompaction = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)

            triggerFullCompaction(sourceBe[1].toString(), sourceBe[4].toString(), tabletId)
            waitForCompactionFinish(sourceBe[1].toString(), sourceBe[4].toString(), tabletId, 90000)

            def afterCompaction = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    beforeCompaction.finished + 1, 90000)
            assert afterCompaction.finished >= beforeCompaction.finished + 1 :
                    "MOW full compaction rowset warmup should finish, metrics=${afterCompaction}"
            assert afterCompaction.failed == beforeCompaction.failed :
                    "MOW full compaction rowset warmup should not fail, metrics=${afterCompaction}"

            def beforePostCompactionUpsert = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            sql """INSERT INTO ${tableName} VALUES (2, 222, 'b4'), (4, 44, 'd2'), (5, 50, 'e')"""
            def afterPostCompactionUpsert = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    beforePostCompactionUpsert.finished + 1, 60000)
            assert afterPostCompactionUpsert.finished >= beforePostCompactionUpsert.finished + 1 :
                    "post-compaction MOW upsert warmup should finish, metrics=${afterPostCompactionUpsert}"
            assert afterPostCompactionUpsert.failed == beforePostCompactionUpsert.failed :
                    "post-compaction MOW upsert warmup should not fail, metrics=${afterPostCompactionUpsert}"

            def stats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, jobId,
                    { it.seg_num.finish_5m >= 5 && it.seg_num.fail_5m == 0 && it.seg_num.gap_5m == 0 }, 60000)
            logger.info("MOW warmup SyncStats: ${stats}")
            assert stats.seg_num.fail_5m == 0 : "MOW warmup SyncStats should have no failures: ${stats}"
            assert stats.seg_num.gap_5m == 0 : "MOW warmup SyncStats should converge: ${stats}"

            sql """use @${dstCluster}"""
            sql """use ${dbName}"""
            profile("mow_compaction_target_profile") {
                sql """set enable_profile = true"""
                sql """set profile_level = 2"""
                run {
                    def res = sql """/* mow_compaction_target_profile */ SELECT count(*), sum(value) FROM ${tableName}"""
                    assert res[0][0].toString() == "5" : "target final MOW count mismatch: ${res}"
                    assert res[0][1].toString() == "626" : "target final MOW sum mismatch: ${res}"
                    sleep(1000)
                }
                check { profileString, exception ->
                    if (exception != null) {
                        throw exception
                    }
                    assert profileString.contains("NumRemoteIOTotal") : "profile should contain file cache counters"
                    def remoteTotal = WarmupMetricsUtils.sumProfileCounter(profileString, "NumRemoteIOTotal")
                    def localTotal = WarmupMetricsUtils.sumProfileCounter(profileString, "NumLocalIOTotal")
                    logger.info("MOW profile NumRemoteIOTotal=${remoteTotal}, NumLocalIOTotal=${localTotal}")
                    assert remoteTotal == 0 : "warmed MOW target query should not read remote data"
                    assert localTotal > 0 : "warmed MOW target query should hit local file cache"
                }
            }

            def finalRead = sql """SELECT id, value, tag FROM ${tableName} ORDER BY id"""
            assert finalRead.toString() == "[[1, 10, a], [2, 222, b4], [3, 300, c2], [4, 44, d2], [5, 50, e]]" :
                    "target MOW rows mismatch after upsert and full compaction: ${finalRead}"
        } finally {
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use @${srcCluster}"""
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS ${tableName}"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbName}""" } catch (Exception ignored) {}
        }
    }
}
