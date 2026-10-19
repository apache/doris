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
import org.apache.doris.regression.util.NodeType
import org.apache.doris.regression.util.WarmupMetricsUtils

// Test points covered: EX-05 (stats API HTTP 500, read timeout, BE down), EX-07.
suite('test_warm_up_event_on_tables_abnormal_stats_and_failure', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
        'cloud_warm_up_table_filter_refresh_interval_ms=1000',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
    ]
    options.enableDebugPoints()
    options.cloudMode = true

    def waitUntil = { String desc, long timeoutMs, Closure<Boolean> predicate ->
        long deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (predicate()) {
                return
            }
            sleep(500)
        }
        assert false : "Timed out waiting for ${desc}"
    }

    docker(options) {
        Closure sqlRunner = { String q -> sql(q) }

        def srcCluster = "warmup_source"
        def dstCluster = "warmup_target"
        def dbName = "test_on_tables_abnormal_stats_fail_db"
        def tableName = "abnormal_tbl"
        def jobIds = []
        def statsApiDebugBe = null
        def statsApiSleepBe = null
        def downloadDebugBes = []

        def rows = { int begin, int end ->
            (begin..<end).collect { "(${it}, 'value_${it}')" }.join(", ")
        }

        cluster.addBackend(2, srcCluster)
        def dstBeIndexes = cluster.addBackend(2, dstCluster)

        try {
            sql """use @${srcCluster}"""
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS ${tableName} (
                       id INT,
                       val STRING
                   )
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            def jobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (INCLUDE '${dbName}.${tableName}')
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)[0][0]
            jobIds << jobId
            assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, jobId,
                    ["${dbName}.${tableName}".toString()] as Set) ==
                    ["${dbName}.${tableName}".toString()] as Set

            assert WarmupMetricsUtils.getClusterBackends(sqlRunner, srcCluster).size() == 2
            assert WarmupMetricsUtils.getClusterBackends(sqlRunner, dstCluster).size() == 2

            WarmupMetricsUtils.clearFileCacheOnAllBackends(sqlRunner)
            def baseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            def targetFinishedBefore = WarmupMetricsUtils.getClusterMetricValues(sqlRunner,
                    dstCluster, WarmupMetricsUtils.METRIC_FINISHED)

            sql """use @${srcCluster}"""
            sql """use ${dbName}"""
            sql """INSERT INTO ${tableName} VALUES ${rows(0, 32)}"""
            def warmMetrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    baseMetrics.finished + 1, 90000)
            assert warmMetrics.failed == baseMetrics.failed : "initial warmup should not fail, metrics=${warmMetrics}"
            waitUntil("both target BEs to finish initial warmup", 30000) {
                def targetFinishedAfter = WarmupMetricsUtils.getClusterMetricValues(sqlRunner,
                        dstCluster, WarmupMetricsUtils.METRIC_FINISHED)
                def deltas = targetFinishedAfter.collectEntries {
                    [(it.key): it.value - (targetFinishedBefore[it.key] ?: 0)]
                }
                logger.info("initial target finished deltas: ${deltas}")
                return deltas.size() == 2 && deltas.every { it.value > 0 }
            }

            def statsBeforeApiError = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, jobId,
                    { it.seg_num.finish_5m > 0 && it.seg_num.fail_5m == 0 }, 30000)
            logger.info("SyncStats before API error injection: ${statsBeforeApiError}")

            statsApiDebugBe = WarmupMetricsUtils.getClusterBackends(sqlRunner, dstCluster)[0]
            GetDebugPoint().enableDebugPoint(statsApiDebugBe[1].toString(), statsApiDebugBe[4] as int,
                    NodeType.BE, "WarmUpStatsAction.handle.return_error")
            def degradedInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            assert degradedInfo[0][3] in ["RUNNING", "PENDING"] :
                    "SHOW should keep the job visible while one BE stats API fails, row=${degradedInfo[0]}"
            def degradedStats = WarmupMetricsUtils.parseSyncStats(degradedInfo)
            logger.info("SyncStats with one BE API failure: ${degradedStats}")
            assert !degradedStats.isEmpty() : "SHOW should return degraded SyncStats instead of failing"
            assert degradedStats.seg_num.finish_5m > 0 :
                    "remaining target BE stats should still be aggregated, stats=${degradedStats}"

            GetDebugPoint().disableDebugPoint(statsApiDebugBe[1].toString(), statsApiDebugBe[4] as int,
                    NodeType.BE, "WarmUpStatsAction.handle.return_error")
            statsApiDebugBe = null
            def restoredStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, jobId,
                    { it.seg_num.finish_5m >= statsBeforeApiError.seg_num.finish_5m }, 30000)
            logger.info("SyncStats after API error recovery: ${restoredStats}")

            statsApiSleepBe = WarmupMetricsUtils.getClusterBackends(sqlRunner, dstCluster)[0]
            GetDebugPoint().enableDebugPoint(statsApiSleepBe[1].toString(), statsApiSleepBe[4] as int,
                    NodeType.BE, "WarmUpStatsAction.handle.sleep", [sleep_ms: 12000])
            long timeoutStartMs = System.currentTimeMillis()
            def timeoutInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            long timeoutElapsedMs = System.currentTimeMillis() - timeoutStartMs
            assert timeoutInfo[0][3] in ["RUNNING", "PENDING"] :
                    "SHOW should keep the job visible while one BE stats API times out, row=${timeoutInfo[0]}"
            def timeoutStats = WarmupMetricsUtils.parseSyncStats(timeoutInfo)
            logger.info("SyncStats with one BE stats API timeout: ${timeoutStats}, elapsedMs=${timeoutElapsedMs}")
            assert timeoutElapsedMs < 9000 :
                    "FE should use a bounded timeout for BE stats API requests, elapsedMs=${timeoutElapsedMs}"
            assert !timeoutStats.isEmpty() :
                    "SHOW should return degraded SyncStats instead of waiting for the slow BE"
            assert timeoutStats.seg_num.finish_5m > 0 :
                    "remaining target BE stats should still be aggregated after timeout, stats=${timeoutStats}"
            GetDebugPoint().disableDebugPoint(statsApiSleepBe[1].toString(), statsApiSleepBe[4] as int,
                    NodeType.BE, "WarmUpStatsAction.handle.sleep")
            statsApiSleepBe = null

            def targetBes = WarmupMetricsUtils.getClusterBackends(sqlRunner, dstCluster)
            for (be in targetBes) {
                GetDebugPoint().enableDebugPoint(be[1].toString(), be[4] as int, NodeType.BE,
                        "CloudInternalServiceImpl::warm_up_rowset.download_segment.inject_error")
                downloadDebugBes << be
            }

            def beforeFailureMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            def beforeFailureStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, jobId,
                    { !it.isEmpty() }, 30000)
            sql """INSERT INTO ${tableName} VALUES ${rows(100, 108)}"""
            waitUntil("download failure metric", 60000) {
                def m = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
                return m.failed > beforeFailureMetrics.failed && m.finished + m.failed >= m.submitted
            }
            def failedMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            assert failedMetrics.failed > beforeFailureMetrics.failed :
                    "injected download failure should increase failed bvar, before=${beforeFailureMetrics}, after=${failedMetrics}"

            def failedStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, jobId, {
                it.seg_num.fail_5m > beforeFailureStats.seg_num.fail_5m
                        && it.seg_num.gap_5m > beforeFailureStats.seg_num.gap_5m
            }, 30000)
            logger.info("SyncStats after injected download failure: ${failedStats}")
            assert failedStats.seg_num.fail_5m > 0 : "5m fail window should expose download failure"
            assert failedStats.seg_num.fail_30m > 0 : "30m fail window should expose download failure"
            assert failedStats.seg_num.fail_1h > 0 : "1h fail window should expose download failure"
            assert failedStats.seg_num.gap_5m > 0 : "5m gap should expose unfinished failed warmup"
            assert failedStats.seg_num.gap_30m > 0 : "30m gap should expose unfinished failed warmup"
            assert failedStats.seg_num.gap_1h > 0 : "1h gap should expose unfinished failed warmup"

            for (be in downloadDebugBes) {
                GetDebugPoint().disableDebugPoint(be[1].toString(), be[4] as int, NodeType.BE,
                        "CloudInternalServiceImpl::warm_up_rowset.download_segment.inject_error")
            }
            downloadDebugBes.clear()

            def beforeRecoveryMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            sql """INSERT INTO ${tableName} VALUES ${rows(200, 208)}"""
            def afterRecoveryMetrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    beforeRecoveryMetrics.finished + 1, 90000)
            assert afterRecoveryMetrics.finished > beforeRecoveryMetrics.finished :
                    "warmup should recover and finish new downloads, before=${beforeRecoveryMetrics}, after=${afterRecoveryMetrics}"
            assert afterRecoveryMetrics.failed == beforeRecoveryMetrics.failed :
                    "recovered warmup should not add new failures, before=${beforeRecoveryMetrics}, after=${afterRecoveryMetrics}"

            def recoveredStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, jobId, {
                it.seg_num.finish_5m > failedStats.seg_num.finish_5m
                        && it.seg_num.fail_5m >= failedStats.seg_num.fail_5m
            }, 30000)
            logger.info("SyncStats after failure recovery: ${recoveredStats}")

            sql """use @${dstCluster}"""
            sql """use ${dbName}"""
            sql """set query_freshness_tolerance_ms = 5000"""
            def res = sql """SELECT count(*) FROM ${tableName}"""
            assert res[0][0].toString() == "48" : "target query should see all rows after failure recovery: ${res}"

            def stoppedStatsBeIndex = dstBeIndexes[0] as int
            def stoppedStatsBe = cluster.getBeByIndex(stoppedStatsBeIndex)
            cluster.stopBackends(stoppedStatsBeIndex)
            waitUntil("target BE ${stoppedStatsBe.backendId} to be marked dead", 30000) {
                def row = sql("SHOW BACKENDS").find {
                    it[0].toString() == stoppedStatsBe.backendId.toString()
                }
                return row != null && row[9].toString() == "false"
            }
            def beDownInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            assert beDownInfo[0][3] in ["RUNNING", "PENDING"] :
                    "SHOW should keep the job visible while one target BE is down, row=${beDownInfo[0]}"
            def beDownStats = WarmupMetricsUtils.parseSyncStats(beDownInfo)
            logger.info("SyncStats with one target BE down: ${beDownStats}")
            assert !beDownStats.isEmpty() :
                    "SHOW should return degraded SyncStats when one target BE is down"
            assert beDownStats.seg_num.finish_5m > 0 :
                    "remaining target BE stats should still be aggregated when one target BE is down, stats=${beDownStats}"
        } finally {
            if (statsApiDebugBe != null) {
                try {
                    GetDebugPoint().disableDebugPoint(statsApiDebugBe[1].toString(),
                            statsApiDebugBe[4] as int, NodeType.BE,
                            "WarmUpStatsAction.handle.return_error")
                } catch (Exception ignored) {}
            }
            if (statsApiSleepBe != null) {
                try {
                    GetDebugPoint().disableDebugPoint(statsApiSleepBe[1].toString(),
                            statsApiSleepBe[4] as int, NodeType.BE,
                            "WarmUpStatsAction.handle.sleep")
                } catch (Exception ignored) {}
            }
            if (!downloadDebugBes.isEmpty()) {
                try { GetDebugPoint().clearDebugPointsForAllBEs() } catch (Exception ignored) {}
            }
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
