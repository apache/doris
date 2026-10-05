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

// Test points covered: ST-01, ST-02, ST-10.
suite('test_warm_up_event_on_tables_system_e2e_multi_be', 'docker') {
    def options = new ClusterOptions()
    options.beNum = 1
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
    options.cloudMode = true

    docker(options) {
        Closure sqlRunner = { String q -> sql(q) }

        def srcCluster = "warmup_source"
        def dstCluster = "warmup_target"
        def dbName = "test_on_tables_system_e2e_db"
        def jobIds = []

        cluster.addBackend(3, srcCluster)
        cluster.addBackend(3, dstCluster)

        try {
            sql """use @${srcCluster}"""
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS orders (id INT, amount INT)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 9
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS customers (id INT, name STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 9
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS audit_log (id INT, msg STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            def ordersJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (INCLUDE '${dbName}.orders')
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)[0][0]
            jobIds << ordersJobId
            def customersJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (INCLUDE '${dbName}.customers')
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)[0][0]
            jobIds << customersJobId
            def wildcardJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (
                    INCLUDE '${dbName}.*',
                    EXCLUDE '${dbName}.audit_*'
                )
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)[0][0]
            jobIds << wildcardJobId

            assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, ordersJobId,
                    ["${dbName}.orders".toString()] as Set,
                    ["${dbName}.customers".toString(), "${dbName}.audit_log".toString()] as Set) ==
                    ["${dbName}.orders".toString()] as Set
            assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, customersJobId,
                    ["${dbName}.customers".toString()] as Set,
                    ["${dbName}.orders".toString(), "${dbName}.audit_log".toString()] as Set) ==
                    ["${dbName}.customers".toString()] as Set
            assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, wildcardJobId,
                    ["${dbName}.orders".toString(), "${dbName}.customers".toString()] as Set,
                    ["${dbName}.audit_log".toString()] as Set) ==
                    ["${dbName}.orders".toString(), "${dbName}.customers".toString()] as Set

            assert WarmupMetricsUtils.getClusterBackends(sqlRunner, srcCluster).size() == 3
            assert WarmupMetricsUtils.getClusterBackends(sqlRunner, dstCluster).size() == 3

            WarmupMetricsUtils.clearFileCacheOnAllBackends(sqlRunner)
            def baseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            def targetFinishedBefore = WarmupMetricsUtils.getClusterMetricValues(sqlRunner,
                    dstCluster, WarmupMetricsUtils.METRIC_FINISHED)

            sql """use @${srcCluster}"""
            sql """use ${dbName}"""
            int rowCount = 90
            long expectedOrderSum = ((long) rowCount - 1) * rowCount / 2 * 10
            def orderValues = (0..<rowCount).collect { "(${it}, ${it * 10})" }.join(", ")
            def customerValues = (0..<rowCount).collect { "(${it}, 'customer_${it}')" }.join(", ")
            def auditValues = (0..<30).collect { "(${it}, 'audit_${it}')" }.join(", ")
            sql """INSERT INTO orders VALUES ${orderValues}"""
            sql """INSERT INTO customers VALUES ${customerValues}"""
            sql """INSERT INTO audit_log VALUES ${auditValues}"""

            def metrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    baseMetrics.finished + 18, 90000)
            def requestedDelta = metrics.requested - baseMetrics.requested
            def submittedDelta = metrics.submitted - baseMetrics.submitted
            def finishedDelta = metrics.finished - baseMetrics.finished
            def failedDelta = metrics.failed - baseMetrics.failed
            logger.info("system e2e deltas requested=${requestedDelta}, submitted=${submittedDelta}, " +
                    "finished=${finishedDelta}, failed=${failedDelta}")
            assert submittedDelta >= 18 : "target should warm every bucket for orders/customers"
            assert requestedDelta >= submittedDelta * 2 :
                    "overlapping jobs should request the same matched rowsets independently, got requested=${requestedDelta}, submitted=${submittedDelta}"
            assert finishedDelta == submittedDelta : "all target downloads should finish"
            assert failedDelta == 0 : "warmup should not fail"

            def targetFinishedAfter = WarmupMetricsUtils.getClusterMetricValues(sqlRunner,
                    dstCluster, WarmupMetricsUtils.METRIC_FINISHED)
            def targetFinishedDeltas = targetFinishedAfter.collectEntries {
                [(it.key): it.value - (targetFinishedBefore[it.key] ?: 0)]
            }
            logger.info("target finished deltas by BE: ${targetFinishedDeltas}")
            assert targetFinishedDeltas.size() == 3 : "target cluster should have 3 BEs"
            assert targetFinishedDeltas.every { it.value > 0 } :
                    "each target BE should finish warmup tasks, got ${targetFinishedDeltas}"

            sql """use @${dstCluster}"""
            sql """use ${dbName}"""
            profile("st01_target_profile") {
                sql """set enable_profile = true"""
                sql """set profile_level = 2"""
                run {
                    def res = sql """/* st01_target_profile */ SELECT count(*), sum(amount) FROM orders"""
                    assert res[0][0].toString() == rowCount.toString() : "target query row count mismatch: ${res}"
                    assert res[0][1].toString() == expectedOrderSum.toString() : "target query sum mismatch: ${res}"
                    sleep(1000)
                }
                check { profileString, exception ->
                    if (exception != null) {
                        throw exception
                    }
                    assert profileString.contains("NumRemoteIOTotal") : "profile should contain file cache counters"
                    def remoteTotal = WarmupMetricsUtils.sumProfileCounter(profileString, "NumRemoteIOTotal")
                    def localTotal = WarmupMetricsUtils.sumProfileCounter(profileString, "NumLocalIOTotal")
                    logger.info("target profile NumRemoteIOTotal=${remoteTotal}, NumLocalIOTotal=${localTotal}")
                    assert remoteTotal == 0 : "warmed target query should not read remote data"
                    assert localTotal > 0 : "warmed target query should hit local file cache"
                }
            }

            def targetTtl = [:]
            long targetTtlDeadline = System.currentTimeMillis() + 30000
            while (System.currentTimeMillis() < targetTtlDeadline) {
                targetTtl = WarmupMetricsUtils.getClusterMetricValues(sqlRunner, dstCluster, "ttl_cache_size")
                if (targetTtl.size() == 3 && targetTtl.values().sum() > 0) {
                    break
                }
                sleep(2000)
            }
            logger.info("target ttl cache by BE: ${targetTtl}")
            assert targetTtl.size() == 3 : "target cluster should have 3 BEs"
            assert targetTtl.values().sum() > 0 : "target cluster should own warmed cache, got ${targetTtl}"

            def ordersStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, ordersJobId, { stats ->
                stats.seg_num.requested_5m > 0
                        && stats.seg_num.finish_5m == stats.seg_num.requested_5m
                        && stats.seg_num.gap_5m == 0
                        && stats.seg_num.fail_5m == 0
                        && stats.seg_size.finish_5m == stats.seg_size.requested_5m
                        && stats.seg_size.gap_5m == "0b"
                        && stats.seg_size.fail_5m == "0b"
            }, 30000)
            logger.info("system e2e SyncStats for orders job ${ordersJobId}: ${ordersStats}")
            assert ordersStats.seg_num.requested_5m > 0 :
                    "orders job should have requested segments in SyncStats: ${ordersStats}"
            assert ordersStats.seg_num.finish_5m == ordersStats.seg_num.requested_5m :
                    "orders job should count already-warmed overlapping rowsets as finished: ${ordersStats}"
            assert ordersStats.seg_num.gap_5m == 0 :
                    "orders job should have no SyncStats segment gap after warmup: ${ordersStats}"
            assert ordersStats.seg_num.fail_5m == 0 :
                    "orders job should have no SyncStats segment failures: ${ordersStats}"
            assert ordersStats.seg_size.finish_5m == ordersStats.seg_size.requested_5m :
                    "orders job should count already-warmed overlapping rowset bytes as finished: ${ordersStats}"
            assert ordersStats.seg_size.gap_5m == "0b" :
                    "orders job should have no SyncStats size gap after warmup: ${ordersStats}"
            assert ordersStats.seg_size.fail_5m == "0b" :
                    "orders job should have no SyncStats size failures: ${ordersStats}"

            def customersStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, customersJobId, { stats ->
                stats.seg_num.requested_5m > 0
                        && stats.seg_num.finish_5m == stats.seg_num.requested_5m
                        && stats.seg_num.gap_5m == 0
                        && stats.seg_num.fail_5m == 0
                        && stats.seg_size.finish_5m == stats.seg_size.requested_5m
                        && stats.seg_size.gap_5m == "0b"
                        && stats.seg_size.fail_5m == "0b"
            }, 30000)
            logger.info("system e2e SyncStats for customers job ${customersJobId}: ${customersStats}")
            assert customersStats.seg_num.requested_5m > 0 :
                    "customers job should have requested segments in SyncStats: ${customersStats}"
            assert customersStats.seg_num.finish_5m == customersStats.seg_num.requested_5m :
                    "customers job should count already-warmed overlapping rowsets as finished: ${customersStats}"
            assert customersStats.seg_num.gap_5m == 0 :
                    "customers job should have no SyncStats segment gap after warmup: ${customersStats}"
            assert customersStats.seg_num.fail_5m == 0 :
                    "customers job should have no SyncStats segment failures: ${customersStats}"
            assert customersStats.seg_size.finish_5m == customersStats.seg_size.requested_5m :
                    "customers job should count already-warmed overlapping rowset bytes as finished: ${customersStats}"
            assert customersStats.seg_size.gap_5m == "0b" :
                    "customers job should have no SyncStats size gap after warmup: ${customersStats}"
            assert customersStats.seg_size.fail_5m == "0b" :
                    "customers job should have no SyncStats size failures: ${customersStats}"

            def wildcardStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, wildcardJobId, { stats ->
                stats.seg_num.requested_5m > 0
                        && stats.seg_num.finish_5m == stats.seg_num.requested_5m
                        && stats.seg_num.gap_5m == 0
                        && stats.seg_num.fail_5m == 0
                        && stats.seg_size.finish_5m == stats.seg_size.requested_5m
                        && stats.seg_size.gap_5m == "0b"
                        && stats.seg_size.fail_5m == "0b"
            }, 30000)
            logger.info("system e2e SyncStats for wildcard job ${wildcardJobId}: ${wildcardStats}")
            assert wildcardStats.seg_num.requested_5m > 0 :
                    "wildcard job should have requested segments in SyncStats: ${wildcardStats}"
            assert wildcardStats.seg_num.finish_5m == wildcardStats.seg_num.requested_5m :
                    "wildcard job should count already-warmed overlapping rowsets as finished: ${wildcardStats}"
            assert wildcardStats.seg_num.gap_5m == 0 :
                    "wildcard job should have no SyncStats segment gap after warmup: ${wildcardStats}"
            assert wildcardStats.seg_num.fail_5m == 0 :
                    "wildcard job should have no SyncStats segment failures: ${wildcardStats}"
            assert wildcardStats.seg_size.finish_5m == wildcardStats.seg_size.requested_5m :
                    "wildcard job should count already-warmed overlapping rowset bytes as finished: ${wildcardStats}"
            assert wildcardStats.seg_size.gap_5m == "0b" :
                    "wildcard job should have no SyncStats size gap after warmup: ${wildcardStats}"
            assert wildcardStats.seg_size.fail_5m == "0b" :
                    "wildcard job should have no SyncStats size failures: ${wildcardStats}"
            def wildcardOverlapMessage = "wildcard job should cover both overlapping tables, orders=${ordersStats}, customers=${customersStats}, wildcard=${wildcardStats}"
            assert wildcardStats.seg_num.requested_5m >=
                    ordersStats.seg_num.requested_5m + customersStats.seg_num.requested_5m :
                    wildcardOverlapMessage
        } finally {
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use @${srcCluster}"""
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS orders"""
                sql """DROP TABLE IF EXISTS customers"""
                sql """DROP TABLE IF EXISTS audit_log"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbName}""" } catch (Exception ignored) {}
        }
    }
}
