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
import org.apache.doris.regression.util.WarmupMetricsUtils

// Test point covered: ST-06.
suite('test_warm_up_event_on_tables_system_packed_file', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_warm_up_table_filter_refresh_interval_ms=1000',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
        'enable_packed_file=true',
        'small_file_threshold_bytes=102400',
        'disable_auto_compaction=true',
    ]
    options.cloudMode = true

    docker(options) {
        Closure sqlRunner = { String q -> sql(q) }

        def srcCluster = "warmup_source"
        def dstCluster = "warmup_target"
        def dbName = "test_on_tables_system_packed_file_db"
        def tableName = "packed_tbl"
        def jobIds = []
        def loadCount = 30

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster)

        try {
            sql """use @${srcCluster}"""
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS ${tableName} (
                       id INT,
                       name STRING,
                       payload STRING
                   )
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES (
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
            def basePackedFiles = WarmupMetricsUtils.getClusterMetricSum(sqlRunner, srcCluster,
                    "packed_file_total_small_file_num")
            def baseTargetCacheSize = WarmupMetricsUtils.getClusterMetricSum(sqlRunner, dstCluster,
                    "ttl_cache_size")

            sql """use @${srcCluster}"""
            sql """use ${dbName}"""
            for (int i = 0; i < loadCount; i++) {
                sql """INSERT INTO ${tableName} VALUES (${i}, 'packed_${i}', repeat('x', 128))"""
            }

            def metrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    baseMetrics.finished + loadCount, 90000)
            def requestedDelta = metrics.requested - baseMetrics.requested
            def submittedDelta = metrics.submitted - baseMetrics.submitted
            def finishedDelta = metrics.finished - baseMetrics.finished
            def failedDelta = metrics.failed - baseMetrics.failed
            logger.info("packed file bvar deltas requested=${requestedDelta}, submitted=${submittedDelta}, "
                    + "finished=${finishedDelta}, failed=${failedDelta}")
            assert requestedDelta >= loadCount : "source bvar should request packed small-file rowsets"
            assert submittedDelta >= loadCount : "target bvar should submit packed small-file rowsets"
            assert finishedDelta == submittedDelta : "target bvar should finish all submitted packed rowsets"
            assert failedDelta == 0 : "packed-file warmup should not fail"

            def packedFilesDelta = WarmupMetricsUtils.getClusterMetricSum(sqlRunner, srcCluster,
                    "packed_file_total_small_file_num") - basePackedFiles
            logger.info("packed_file_total_small_file_num delta=${packedFilesDelta}")
            assert packedFilesDelta > 0 : "source cluster should write small files into packed file"

            def targetCacheSizeDelta = WarmupMetricsUtils.getClusterMetricSum(sqlRunner, dstCluster,
                    "ttl_cache_size") - baseTargetCacheSize
            logger.info("target ttl_cache_size delta=${targetCacheSizeDelta}")
            assert targetCacheSizeDelta > 0 : "target packed-file warmup should populate TTL file cache"

            def stats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, jobId,
                    { it.seg_num.requested_5m > 0 && it.seg_num.finish_5m >= loadCount && it.seg_num.fail_5m == 0 },
                    60000)
            logger.info("packed file SyncStats: ${stats}")
            assert stats.seg_num.requested_5m > 0 : "SyncStats should observe packed small-file rowset requests"
            assert stats.seg_num.finish_5m >= loadCount : "SyncStats should finish packed small-file rowsets"
            assert stats.seg_num.fail_5m == 0 : "SyncStats should have no packed-file failures"

            sql """use @${dstCluster}"""
            sql """use ${dbName}"""
            profile("st06_packed_file_profile") {
                sql """set enable_profile = true"""
                sql """set profile_level = 2"""
                run {
                    def res = sql """/* st06_packed_file_profile */ SELECT count(*), sum(id) FROM ${tableName}"""
                    assert res[0][0].toString() == loadCount.toString() : "packed table count mismatch: ${res}"
                    assert res[0][1].toString() == "435" : "packed table sum mismatch: ${res}"
                    sleep(1000)
                }
                check { profileString, exception ->
                    if (exception != null) {
                        throw exception
                    }
                    assert profileString.contains("NumRemoteIOTotal") : "profile should contain file cache counters"
                    def remoteTotal = WarmupMetricsUtils.sumProfileCounter(profileString, "NumRemoteIOTotal")
                    def localTotal = WarmupMetricsUtils.sumProfileCounter(profileString, "NumLocalIOTotal")
                    logger.info("packed profile NumRemoteIOTotal=${remoteTotal}, NumLocalIOTotal=${localTotal}")
                    assert remoteTotal == 0 : "warmed packed-file query should not read remote data"
                    assert localTotal > 0 : "warmed packed-file query should hit local file cache"
                }
            }
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
