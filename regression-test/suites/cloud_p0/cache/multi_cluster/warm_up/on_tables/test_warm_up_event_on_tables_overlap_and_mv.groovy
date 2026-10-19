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

suite('test_warm_up_event_on_tables_overlap_and_mv', 'docker') {
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
    options.cloudMode = true
    options.beNum = 1

    docker(options) {
        Closure sqlRunner = { String q -> sql(q) }

        def dstCluster = "warmup_target"

        def clusters = sql """SHOW CLUSTERS"""
        assert !clusters.isEmpty() : "SHOW CLUSTERS should return the default source cluster"
        def defaultCluster = clusters.find {
            it[1].toString().equalsIgnoreCase("true")
        }
        def srcCluster = (defaultCluster ?: clusters[0])[0].toString()
        logger.info("use default source cluster for overlap and mv warmup case: ${srcCluster}")
        cluster.addBackend(1, dstCluster)

        def overlapDb = "test_on_tables_overlap_extra_db"
        def mvDb = "test_on_tables_mv_extra_db"
        def jobIds = []

        try {
            // FT-10: overlapping table-level jobs can coexist without duplicate target downloads.
            sql """use @${srcCluster}"""
            sql """CREATE DATABASE IF NOT EXISTS ${overlapDb}"""
            sql """use ${overlapDb}"""
            sql """CREATE TABLE IF NOT EXISTS orders (id INT, amount INT)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS customers (id INT, name STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS audit_log (id INT, msg STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            def ordersJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (
                    INCLUDE '${overlapDb}.orders'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """)[0][0]
            jobIds << ordersJobId

            def customersJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (
                    INCLUDE '${overlapDb}.customers'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """)[0][0]
            jobIds << customersJobId

            def overlapJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (
                    INCLUDE '${overlapDb}.*',
                    EXCLUDE '${overlapDb}.audit_*'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """)[0][0]
            jobIds << overlapJobId

            def ordersMatched = WarmupMetricsUtils.waitForMatchedTables(sqlRunner, ordersJobId,
                    ["${overlapDb}.orders".toString()] as Set,
                    ["${overlapDb}.customers".toString(), "${overlapDb}.audit_log".toString()] as Set)
            def customersMatched = WarmupMetricsUtils.waitForMatchedTables(sqlRunner, customersJobId,
                    ["${overlapDb}.customers".toString()] as Set,
                    ["${overlapDb}.orders".toString(), "${overlapDb}.audit_log".toString()] as Set)
            def overlapMatched = WarmupMetricsUtils.waitForMatchedTables(sqlRunner, overlapJobId,
                    ["${overlapDb}.orders".toString(), "${overlapDb}.customers".toString()] as Set,
                    ["${overlapDb}.audit_log".toString()] as Set)
            assert ordersMatched == ["${overlapDb}.orders".toString()] as Set
            assert customersMatched == ["${overlapDb}.customers".toString()] as Set
            assert overlapMatched == ["${overlapDb}.customers".toString(), "${overlapDb}.orders".toString()] as Set
            sleep(3000)

            def overlapBaseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            int rowsPerTable = 4
            for (int i = 0; i < rowsPerTable; i++) {
                sql """INSERT INTO orders VALUES (${i}, ${i * 10})"""
                sql """INSERT INTO customers VALUES (${i}, 'customer_${i}')"""
                sql """INSERT INTO audit_log VALUES (${i}, 'audit_${i}')"""
            }

            int uniqueMatchedSegments = rowsPerTable * 2
            int jobMatchedSegments = rowsPerTable * 4
            def overlapFinalMetrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    overlapBaseMetrics.finished + uniqueMatchedSegments)
            def requestedDelta = overlapFinalMetrics.requested - overlapBaseMetrics.requested
            def submittedDelta = overlapFinalMetrics.submitted - overlapBaseMetrics.submitted
            def finishedDelta = overlapFinalMetrics.finished - overlapBaseMetrics.finished
            def failedDelta = overlapFinalMetrics.failed - overlapBaseMetrics.failed
            logger.info("overlap deltas requested=${requestedDelta}, submitted=${submittedDelta}, "
                    + "finished=${finishedDelta}, failed=${failedDelta}")
            assert requestedDelta >= jobMatchedSegments :
                    "source requested should count each matching job, expected >= ${jobMatchedSegments}, got ${requestedDelta}"
            assert submittedDelta >= uniqueMatchedSegments :
                    "target should warm all unique matched rowsets, expected >= ${uniqueMatchedSegments}, got ${submittedDelta}"
            assert submittedDelta <= uniqueMatchedSegments :
                    "overlap jobs should not amplify target downloads, expected <= ${uniqueMatchedSegments}, got ${submittedDelta}"
            assert finishedDelta >= uniqueMatchedSegments :
                    "target should finish all unique matched rowsets, expected >= ${uniqueMatchedSegments}, got ${finishedDelta}"
            assert failedDelta == 0 : "overlap jobs should not fail, got failed delta ${failedDelta}"

            def ordersStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, ordersJobId, { stats ->
                stats.seg_num.requested_5m >= rowsPerTable
            })
            def customersStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, customersJobId, { stats ->
                stats.seg_num.requested_5m >= rowsPerTable
            })
            def overlapStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, overlapJobId, { stats ->
                stats.seg_num.requested_5m >= rowsPerTable * 2
            })
            assert ordersStats.seg_num.requested_5m < rowsPerTable * 2 :
                    "orders-only job should not include customers/audit, stats=${ordersStats}"
            assert customersStats.seg_num.requested_5m < rowsPerTable * 2 :
                    "customers-only job should not include orders/audit, stats=${customersStats}"
            assert overlapStats.seg_num.requested_5m >= rowsPerTable * 2 :
                    "overlap job should include orders and customers, stats=${overlapStats}"

            for (jid in [ordersJobId, customersJobId, overlapJobId]) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            WarmupMetricsUtils.waitForMetricsStable(sqlRunner, srcCluster, dstCluster)

            // FT-12: async MV is independently matchable, while sync MV/rollup warms with the base table.
            sql """use @${srcCluster}"""
            sql """CREATE DATABASE IF NOT EXISTS ${mvDb}"""
            sql """use ${mvDb}"""
            def baseTable = "fact_rollup"
            def rollupName = "rollup_sum"
            def asyncMv = "mv_async_summary"

            sql """CREATE TABLE IF NOT EXISTS ${baseTable} (k INT, v INT)
                   DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                   PROPERTIES (
                       "file_cache_ttl_seconds" = "3600",
                       "disable_auto_compaction" = "true"
                   )"""
            sql """INSERT INTO ${baseTable} VALUES (1, 10), (2, 20)"""
            sql """DROP MATERIALIZED VIEW IF EXISTS ${rollupName} ON ${baseTable}"""
            sql """CREATE MATERIALIZED VIEW ${rollupName} AS
                   SELECT k AS rollup_k, sum(v) AS rollup_total_v FROM ${baseTable} GROUP BY k"""
            waitingMVTaskFinishedByMvName(mvDb, baseTable, rollupName)

            sql """DROP MATERIALIZED VIEW IF EXISTS ${asyncMv}"""
            sql """
                CREATE MATERIALIZED VIEW ${asyncMv}
                BUILD DEFERRED REFRESH COMPLETE ON MANUAL
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES ('replication_num' = '1')
                AS SELECT k, sum(v) AS total_v FROM ${baseTable} GROUP BY k
            """

            def baseJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (
                    INCLUDE '${mvDb}.${baseTable}'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """)[0][0]
            jobIds << baseJobId

            def mvJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (
                    INCLUDE '${mvDb}.mv_*'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """)[0][0]
            jobIds << mvJobId

            def baseMatched = WarmupMetricsUtils.waitForMatchedTables(sqlRunner, baseJobId,
                    ["${mvDb}.${baseTable}".toString()] as Set,
                    ["${mvDb}.${asyncMv}".toString(), "${mvDb}.${rollupName}".toString()] as Set)
            def mvMatched = WarmupMetricsUtils.waitForMatchedTables(sqlRunner, mvJobId,
                    ["${mvDb}.${asyncMv}".toString()] as Set,
                    ["${mvDb}.${baseTable}".toString(), "${mvDb}.${rollupName}".toString()] as Set)
            assert baseMatched == ["${mvDb}.${baseTable}".toString()] as Set :
                    "base filter should match only base table, got ${baseMatched}"
            assert mvMatched == ["${mvDb}.${asyncMv}".toString()] as Set :
                    "mv_* filter should match only async MV, got ${mvMatched}"
            sleep(3000)

            def mvBaseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            int baseInsertRows = 3
            for (int i = 0; i < baseInsertRows; i++) {
                sql """INSERT INTO ${baseTable} VALUES (${i + 1}, ${i + 1})"""
            }

            int expectedBaseWarmupSegments = baseInsertRows
            def afterBaseLoad = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    mvBaseMetrics.finished + expectedBaseWarmupSegments)
            def baseLoadFinishedDelta = afterBaseLoad.finished - mvBaseMetrics.finished
            logger.info("base table load with rollup warmup finished delta: ${baseLoadFinishedDelta}")
            assert baseLoadFinishedDelta >= expectedBaseWarmupSegments :
                    "base load should warm while rollup exists, expected >= ${expectedBaseWarmupSegments}, got ${baseLoadFinishedDelta}"

            def baseJobStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, baseJobId, { stats ->
                stats.seg_num.requested_5m >= expectedBaseWarmupSegments
                        && stats.seg_num.finish_5m >= expectedBaseWarmupSegments
            })
            assert baseJobStats.seg_num.requested_5m >= expectedBaseWarmupSegments :
                    "base job should warm base table with rollup present without matching rollup as a table, stats=${baseJobStats}"

            def beforeMvRefresh = WarmupMetricsUtils.waitForMetricsStable(sqlRunner, srcCluster, dstCluster)
            sql """REFRESH MATERIALIZED VIEW ${asyncMv} COMPLETE"""
            waitingMTMVTaskFinishedByMvName(asyncMv, mvDb)
            def afterMvRefresh = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    beforeMvRefresh.finished + 1)
            assert afterMvRefresh.finished > beforeMvRefresh.finished :
                    "async MV refresh should trigger event-driven warmup after mv_* job is created"

            def mvJobStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, mvJobId, { stats ->
                stats.seg_num.requested_5m >= 1 && stats.seg_num.finish_5m >= 1
            })
            assert mvJobStats.seg_num.requested_5m >= 1 :
                    "mv_* job should independently warm async MV rowsets, stats=${mvJobStats}"

            sql """use @${dstCluster}"""
            sql """use ${mvDb}"""
            def asyncMvRewriteQuerySql =
                    "SELECT k, sum(v) AS total_v FROM ${baseTable} GROUP BY k ORDER BY k"
            mv_rewrite_success(asyncMvRewriteQuerySql, asyncMv, true)
            profile("ft12_async_mv_target_profile") {
                sql """set enable_profile = true"""
                sql """set profile_level = 2"""
                run {
                    def res = sql """/* ft12_async_mv_target_profile */ ${asyncMvRewriteQuerySql}"""
                    assert res.collect { [it[0].toString(), it[1].toString()] } ==
                            [["1", "11"], ["2", "22"], ["3", "3"]] :
                            "target aggregate query should be rewritten to async MV and return MV data, got ${res}"
                    sleep(1000)
                }
                check { profileString, exception ->
                    if (exception != null) {
                        throw exception
                    }
                    assert profileString.contains("NumRemoteIOTotal") :
                            "profile should contain file cache counters"
                    def remoteTotal = WarmupMetricsUtils.sumProfileCounter(profileString,
                            "NumRemoteIOTotal")
                    def localTotal = WarmupMetricsUtils.sumProfileCounter(profileString,
                            "NumLocalIOTotal")
                    logger.info("async MV target profile NumRemoteIOTotal=${remoteTotal}, " +
                            "NumLocalIOTotal=${localTotal}")
                    assert remoteTotal == 0 :
                            "rewritten async MV query should not read remote data after warmup"
                    assert localTotal > 0 :
                            "rewritten async MV query should hit local file cache after warmup"
                }
            }

            def rollupQuery = sql """SELECT k, sum(v) FROM ${baseTable} GROUP BY k ORDER BY k"""
            assert rollupQuery.collect { [it[0].toString(), it[1].toString()] } ==
                    [["1", "11"], ["2", "22"], ["3", "3"]] :
                    "target cluster should read base table with rollup data correctly, got ${rollupQuery}"
            def asyncMvQuery = sql """SELECT k, total_v FROM ${asyncMv} ORDER BY k"""
            assert asyncMvQuery.collect { [it[0].toString(), it[1].toString()] } ==
                    [["1", "11"], ["2", "22"], ["3", "3"]] :
                    "target cluster should read async MV correctly, got ${asyncMvQuery}"

        } finally {
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use ${overlapDb}"""
                sql """DROP TABLE IF EXISTS orders"""
                sql """DROP TABLE IF EXISTS customers"""
                sql """DROP TABLE IF EXISTS audit_log"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${overlapDb}""" } catch (Exception ignored) {}
            try {
                sql """use @${srcCluster}"""
                sql """use ${mvDb}"""
                sql """DROP MATERIALIZED VIEW IF EXISTS mv_async_summary"""
                sql """DROP MATERIALIZED VIEW IF EXISTS rollup_sum ON fact_rollup"""
                sql """DROP TABLE IF EXISTS fact_rollup"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${mvDb}""" } catch (Exception ignored) {}
        }
    }
}
