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

// Test points covered: EX-03, EX-08.
suite('test_warm_up_event_on_tables_abnormal_cancel_empty_recovery', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
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
        def dbName = "test_on_tables_abnormal_cancel_empty_db"
        def jobIds = []
        def targetDebugEnabled = false

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster)

        try {
            sql """use @${srcCluster}"""
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS cancel_tbl (
                       id INT,
                       payload STRING
                   )
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS fact_live (
                       id INT,
                       amount INT
                   )
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            def cancelJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (INCLUDE '${dbName}.cancel_tbl')
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)[0][0]
            jobIds << cancelJobId
            assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, cancelJobId,
                    ["${dbName}.cancel_tbl".toString()] as Set) ==
                    ["${dbName}.cancel_tbl".toString()] as Set

            WarmupMetricsUtils.clearFileCacheOnAllBackends(sqlRunner)
            def baseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            sql """use @${srcCluster}"""
            sql """use ${dbName}"""
            sql """INSERT INTO cancel_tbl VALUES
                   (1, 'seed_1'), (2, 'seed_2'), (3, 'seed_3'), (4, 'seed_4')"""
            def initialMetrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    baseMetrics.finished + 1, 60000)
            assert initialMetrics.failed == baseMetrics.failed :
                    "initial warmup should finish without failures, metrics=${initialMetrics}"
            def initialCacheSize = 0L
            waitUntil("initial warmup to populate target cache", 30000) {
                initialCacheSize = WarmupMetricsUtils.getClusterMetricSum(sqlRunner, dstCluster,
                        "ttl_cache_size")
                return initialCacheSize > 0
            }
            assert initialCacheSize > 0 : "initial warmup should populate target cache"

            def targetBe = WarmupMetricsUtils.getClusterBackends(sqlRunner, dstCluster)[0]
            GetDebugPoint().enableDebugPoint(targetBe[1].toString(), targetBe[4] as int, NodeType.BE,
                    "CloudInternalServiceImpl::warm_up_rowset.download_segment", [sleep: 10])
            targetDebugEnabled = true

            def beforeActiveLoad = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            sql """INSERT INTO cancel_tbl VALUES
                   (100, 'active_100'), (101, 'active_101'), (102, 'active_102'), (103, 'active_103'),
                   (104, 'active_104'), (105, 'active_105'), (106, 'active_106'), (107, 'active_107')"""
            waitUntil("active warmup transfer to be submitted", 20000) {
                def m = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
                return m.submitted > beforeActiveLoad.submitted && m.finished < m.submitted
            }

            sql """CANCEL WARM UP JOB WHERE ID = ${cancelJobId}"""
            waitUntil("cancel job state", 20000) {
                def info = sql """SHOW WARM UP JOB WHERE ID = ${cancelJobId}"""
                return info[0][3] == "CANCELLED"
            }

            def afterCancelStable = WarmupMetricsUtils.waitForMetricsStable(sqlRunner,
                    srcCluster, dstCluster, 50000)
            assert afterCancelStable.submitted > beforeActiveLoad.submitted :
                    "active transfer should have submitted before cancel, before=${beforeActiveLoad}, after=${afterCancelStable}"
            assert afterCancelStable.finished + afterCancelStable.failed >= afterCancelStable.submitted :
                    "active transfer should converge after cancel, metrics=${afterCancelStable}"

            def cacheAfterCancel = WarmupMetricsUtils.getClusterMetricSum(sqlRunner, dstCluster,
                    "ttl_cache_size")
            assert cacheAfterCancel >= initialCacheSize :
                    "cancel should not clear existing target cache, before=${initialCacheSize}, after=${cacheAfterCancel}"

            GetDebugPoint().disableDebugPoint(targetBe[1].toString(), targetBe[4] as int, NodeType.BE,
                    "CloudInternalServiceImpl::warm_up_rowset.download_segment")
            targetDebugEnabled = false

            def beforePostCancelLoad = WarmupMetricsUtils.waitForMetricsStable(sqlRunner,
                    srcCluster, dstCluster, 30000)
            sql """INSERT INTO cancel_tbl VALUES (200, 'after_cancel_200'), (201, 'after_cancel_201')"""
            sleep(5000)
            def afterPostCancelLoad = WarmupMetricsUtils.waitForMetricsStable(sqlRunner,
                    srcCluster, dstCluster, 30000)
            assert afterPostCancelLoad.submitted == beforePostCancelLoad.submitted :
                    "cancelled job should not submit later events, before=${beforePostCancelLoad}, after=${afterPostCancelLoad}"
            assert afterPostCancelLoad.finished == beforePostCancelLoad.finished :
                    "cancelled job should not finish later events, before=${beforePostCancelLoad}, after=${afterPostCancelLoad}"

            def emptyWindowJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (INCLUDE '${dbName}.fact_*')
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)[0][0]
            jobIds << emptyWindowJobId
            assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, emptyWindowJobId,
                    ["${dbName}.fact_live".toString()] as Set) ==
                    ["${dbName}.fact_live".toString()] as Set

            sql """ALTER TABLE ${dbName}.fact_live RENAME archive_live"""
            def emptyMatched = WarmupMetricsUtils.waitForMatchedTables(sqlRunner, emptyWindowJobId,
                    [] as Set,
                    ["${dbName}.fact_live".toString(), "${dbName}.archive_live".toString()] as Set,
                    30000)
            assert emptyMatched.isEmpty() : "MatchedTables should be empty during the non-matching window: ${emptyMatched}"
            def emptyJobInfo = sql """SHOW WARM UP JOB WHERE ID = ${emptyWindowJobId}"""
            assert emptyJobInfo[0][3] in ["RUNNING", "PENDING"] :
                    "job should stay runnable when MatchedTables is empty, row=${emptyJobInfo[0]}"

            def beforeArchiveLoad = WarmupMetricsUtils.waitForMetricsStable(sqlRunner,
                    srcCluster, dstCluster, 30000)
            sql """INSERT INTO archive_live VALUES (1, 10), (2, 20)"""
            sleep(5000)
            def afterArchiveLoad = WarmupMetricsUtils.waitForMetricsStable(sqlRunner,
                    srcCluster, dstCluster, 30000)
            assert afterArchiveLoad.submitted == beforeArchiveLoad.submitted :
                    "non-matching empty-window load should not submit warmup, before=${beforeArchiveLoad}, after=${afterArchiveLoad}"

            sql """ALTER TABLE ${dbName}.archive_live RENAME fact_back"""
            assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, emptyWindowJobId,
                    ["${dbName}.fact_back".toString()] as Set) ==
                    ["${dbName}.fact_back".toString()] as Set

            def beforeRecoveredLoad = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            sql """INSERT INTO fact_back VALUES (3, 30), (4, 40)"""
            def afterRecoveredLoad = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner,
                    srcCluster, dstCluster, beforeRecoveredLoad.finished + 1, 60000)
            assert afterRecoveredLoad.submitted > beforeRecoveredLoad.submitted :
                    "matching table after empty window should submit warmup, before=${beforeRecoveredLoad}, after=${afterRecoveredLoad}"
            assert afterRecoveredLoad.finished > beforeRecoveredLoad.finished :
                    "matching table after empty window should finish warmup, before=${beforeRecoveredLoad}, after=${afterRecoveredLoad}"
        } finally {
            if (targetDebugEnabled) {
                try { GetDebugPoint().clearDebugPointsForAllBEs() } catch (Exception ignored) {}
            }
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use @${srcCluster}"""
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS cancel_tbl"""
                sql """DROP TABLE IF EXISTS fact_live"""
                sql """DROP TABLE IF EXISTS archive_live"""
                sql """DROP TABLE IF EXISTS fact_back"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbName}""" } catch (Exception ignored) {}
        }
    }
}
