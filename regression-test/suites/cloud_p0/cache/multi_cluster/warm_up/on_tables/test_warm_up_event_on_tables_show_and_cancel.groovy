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

suite('test_warm_up_event_on_tables_show_and_cancel', 'docker') {
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

        def srcCluster = "warmup_source"
        def dstCluster = "warmup_target"

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster)

        def showDb = "test_on_tables_show_extra_db"
        def cancelDb = "test_on_tables_cancel_extra_db"
        def jobIds = []
        def slurper = new JsonSlurper()

        def getJobRow = { jobId ->
            def rows = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            assert rows.size() == 1 : "expected one row for job ${jobId}, got ${rows}"
            assert rows[0].size() == 16 : "SHOW WARM UP JOB should expose 16 columns, got ${rows[0].size()}"
            return rows[0]
        }

        def waitForJobState = { jobId, Set<String> expectedStates, long timeoutMs = 60000 ->
            long deadline = System.currentTimeMillis() + timeoutMs
            def row = null
            while (System.currentTimeMillis() < deadline) {
                row = getJobRow(jobId)
                if (expectedStates.contains(row[3].toString())) {
                    return row
                }
                sleep(1000)
            }
            return row
        }

        def assertEmptyNewColumns = { row, String jobDesc ->
            assert row[13]?.toString() == "" : "${jobDesc} should have empty TableFilter, row=${row}"
            assert row[14]?.toString() == "" : "${jobDesc} should have empty MatchedTables, row=${row}"
            assert row[15]?.toString() == "" : "${jobDesc} should have empty SyncStats, row=${row}"
        }

        def assertDetailedSyncStats = { row, String jobDesc ->
            def stats = WarmupMetricsUtils.parseSyncStats([row])
            assert !stats.isEmpty() : "${jobDesc} should have detailed SyncStats, row=${row}"
            assert stats.containsKey("seg_num") : "${jobDesc} detailed SyncStats should contain seg_num: ${stats}"
            assert stats.containsKey("seg_size") : "${jobDesc} detailed SyncStats should contain seg_size: ${stats}"
            assert stats.containsKey("idx_num") : "${jobDesc} detailed SyncStats should contain idx_num: ${stats}"
            assert stats.containsKey("idx_size") : "${jobDesc} detailed SyncStats should contain idx_size: ${stats}"
            assert stats.containsKey("last_trigger_ts") :
                    "${jobDesc} detailed SyncStats should contain last_trigger_ts: ${stats}"
            assert stats.containsKey("last_finish_ts") :
                    "${jobDesc} detailed SyncStats should contain last_finish_ts: ${stats}"
            assert !stats.containsKey("window") : "${jobDesc} WHERE ID output should not be compact summary: ${stats}"
            assert !stats.containsKey("src_size") : "${jobDesc} WHERE ID output should not be compact summary: ${stats}"
            assert !stats.containsKey("dst_size") : "${jobDesc} WHERE ID output should not be compact summary: ${stats}"
            assert !stats.containsKey("gap_size") : "${jobDesc} WHERE ID output should not be compact summary: ${stats}"
            return stats
        }

        def assertSummarySyncStats = { row, String jobDesc ->
            def raw = row[15]?.toString()?.trim()
            assert raw != null && raw.length() > 0 : "${jobDesc} should have compact SyncStats summary, row=${row}"
            def stats = slurper.parseText(raw)
            assert stats.window == "30m" : "${jobDesc} list output should use 30m summary, row=${row}"
            assert stats.src_size instanceof String : "${jobDesc} summary src_size should be a string: ${stats}"
            assert stats.dst_size instanceof String : "${jobDesc} summary dst_size should be a string: ${stats}"
            assert stats.gap_size instanceof String : "${jobDesc} summary gap_size should be a string: ${stats}"
            assert !stats.containsKey("seg_num") : "${jobDesc} list output should not include detailed seg_num"
            assert !stats.containsKey("seg_size") : "${jobDesc} list output should not include detailed seg_size"
            assert !stats.containsKey("idx_num") : "${jobDesc} list output should not include detailed idx_num"
            assert !stats.containsKey("idx_size") : "${jobDesc} list output should not include detailed idx_size"
            assert !stats.containsKey("last_trigger_ts") :
                    "${jobDesc} list output should not include detailed last_trigger_ts"
            assert !stats.containsKey("last_finish_ts") :
                    "${jobDesc} list output should not include detailed last_finish_ts"
            return stats
        }

        try {
            // FT-05: SHOW WARM UP JOB mixes new ON TABLES jobs with old once/periodic/table jobs.
            sql """use @${srcCluster}"""
            sql """CREATE DATABASE IF NOT EXISTS ${showDb}"""
            sql """use ${showDb}"""
            sql """CREATE TABLE IF NOT EXISTS show_base (id INT, val STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS show_extra (id INT, val STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """INSERT INTO show_base VALUES (0, 'seed')"""

            def oldTableJobId = sql("""WARM UP CLUSTER ${dstCluster} WITH TABLE show_base""")[0][0]
            jobIds << oldTableJobId
            def oldTableRow = waitForJobState(oldTableJobId, ["FINISHED", "RUNNING", "PENDING"] as Set)
            assert oldTableRow[4] == "TABLE" : "old WITH TABLE job should be TABLE, row=${oldTableRow}"
            assert oldTableRow[5].toString().startsWith("ONCE") : "old WITH TABLE job should be ONCE, row=${oldTableRow}"
            assert oldTableRow[12].toString().contains("${showDb}.show_base".toString()) :
                    "old WITH TABLE job should show warmed table, row=${oldTableRow}"
            assertEmptyNewColumns(oldTableRow, "old WITH TABLE job")

            def periodicJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                PROPERTIES (
                    "sync_mode" = "periodic",
                    "sync_interval_sec" = "10"
                )
            """)[0][0]
            jobIds << periodicJobId

            def clusterJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """)[0][0]
            jobIds << clusterJobId

            sleep(2000)
            sql """use ${showDb}"""
            sql """INSERT INTO show_base VALUES (1, 'cluster_base')"""
            sql """INSERT INTO show_extra VALUES (1, 'cluster_extra')"""

            def clusterStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, clusterJobId, { stats ->
                stats.seg_num.requested_5m > 0 && stats.seg_num.finish_5m > 0
            }, 8000)
            assert !clusterStats.isEmpty() : "cluster-level event job should expose SyncStats"
            assert clusterStats.seg_num.requested_5m > 0 :
                    "cluster event job SyncStats should observe load requests: ${clusterStats}"
            assert clusterStats.seg_num.finish_5m > 0 :
                    "cluster event job SyncStats should observe target finishes: ${clusterStats}"
            def runningClusterRow = getJobRow(clusterJobId)
            def runningClusterStats = assertDetailedSyncStats(runningClusterRow, "cluster event job")
            assert runningClusterStats.seg_num.requested_5m > 0 :
                    "cluster event job detailed SyncStats should observe load requests: ${runningClusterStats}"
            assert runningClusterStats.seg_num.finish_5m > 0 :
                    "cluster event job detailed SyncStats should observe target finishes: ${runningClusterStats}"
            def runningListRows = sql """SHOW WARM UP JOB"""
            def runningClusterSummaryRow = runningListRows.find { it[0].toString() == clusterJobId.toString() }
            def runningClusterSummary = assertSummarySyncStats(runningClusterSummaryRow, "cluster event job")
            assert !runningClusterSummary.containsKey("data_size") :
                    "cluster event job summary should merge data and index sizes: ${runningClusterSummary}"
            assert !runningClusterSummary.containsKey("index_size") :
                    "cluster event job summary should merge data and index sizes: ${runningClusterSummary}"

            try {
                sql("""
                    WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                    ON TABLES (
                        INCLUDE '${showDb}.show_base'
                    )
                    PROPERTIES (
                        "sync_mode" = "event_driven",
                        "sync_event" = "load"
                    )
                """)
                assert false : "Expected ON TABLES load-event job to conflict with existing cluster-level job"
            } catch (java.sql.SQLException e) {
                logger.info("Expected cross-level conflict: ${e.getMessage()}")
                assert e.getMessage().contains("Cannot create table-level load-event warm up job") : e.getMessage()
                assert e.getMessage().contains("conflicting cluster-level load-event warm up job ${clusterJobId}") :
                        e.getMessage()
                assert e.getMessage().contains("Cancel existing load-event warm up job ${clusterJobId}") :
                        e.getMessage()
            }

            sql """CANCEL WARM UP JOB WHERE ID = ${clusterJobId}"""
            def cancelledClusterRow = getJobRow(clusterJobId)
            assert cancelledClusterRow[3] == "CANCELLED" : "cluster event job should be cancelled, row=${cancelledClusterRow}"

            def tableJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (
                    INCLUDE '${showDb}.show_base'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """)[0][0]
            jobIds << tableJobId

            WarmupMetricsUtils.waitForMatchedTables(sqlRunner, tableJobId,
                    ["${showDb}.show_base".toString()] as Set,
                    ["${showDb}.show_extra".toString()] as Set)

            sleep(2000)
            sql """use ${showDb}"""
            sql """INSERT INTO show_base VALUES (2, 'table_base')"""
            sql """INSERT INTO show_extra VALUES (2, 'table_extra')"""

            def tableStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, tableJobId, { stats ->
                stats.seg_num.requested_5m > 0 && stats.seg_num.finish_5m > 0
            }, 8000)
            assert !tableStats.isEmpty() : "table-level event job should expose SyncStats"
            assert tableStats.seg_num.requested_5m > 0 :
                    "table-level event job SyncStats should observe load requests: ${tableStats}"
            assert tableStats.seg_num.finish_5m > 0 :
                    "table-level event job SyncStats should observe target finishes: ${tableStats}"

            def periodicRow = getJobRow(periodicJobId)
            assert periodicRow[4] == "CLUSTER" : "periodic job type should be CLUSTER, row=${periodicRow}"
            assert periodicRow[5] == "PERIODIC (10s)" : "periodic job sync mode mismatch, row=${periodicRow}"
            assertEmptyNewColumns(periodicRow, "periodic cluster job")

            def clusterRow = getJobRow(clusterJobId)
            assert clusterRow[3] == "CANCELLED" : "cluster event job should remain visible after cancel, row=${clusterRow}"
            assert clusterRow[4] == "CLUSTER" : "cluster event job type should be CLUSTER, row=${clusterRow}"
            assert clusterRow[5] == "EVENT_DRIVEN (LOAD)" : "cluster event sync mode mismatch, row=${clusterRow}"
            assert clusterRow[13] == "" : "cluster event job should not have TableFilter, row=${clusterRow}"
            assert clusterRow[14] == "" : "cluster event job should not have MatchedTables, row=${clusterRow}"

            def tableRow = getJobRow(tableJobId)
            assert tableRow[4] == "TABLES" : "ON TABLES job type should be TABLES, row=${tableRow}"
            assert tableRow[5] == "EVENT_DRIVEN (LOAD)" : "ON TABLES sync mode mismatch, row=${tableRow}"
            def tableFilter = slurper.parseText(tableRow[13].toString())
            assert tableFilter.include == ["${showDb}.show_base".toString()] :
                    "table filter should show the canonical include rule, row=${tableRow}"
            def matched = WarmupMetricsUtils.parseMatchedTables([tableRow])
            assert matched == ["${showDb}.show_base".toString()] as Set :
                    "MatchedTables should contain only show_base, got ${matched}"
            def detailedTableStats = assertDetailedSyncStats(tableRow, "ON TABLES job")
            assert detailedTableStats.seg_num.requested_5m > 0 :
                    "ON TABLES detailed SyncStats should observe load requests: ${detailedTableStats}"
            assert detailedTableStats.seg_num.finish_5m > 0 :
                    "ON TABLES detailed SyncStats should observe target finishes: ${detailedTableStats}"

            def listRows = sql """SHOW WARM UP JOB"""
            for (jobId in [oldTableJobId, periodicJobId, clusterJobId, tableJobId]) {
                def row = listRows.find { it[0].toString() == jobId.toString() }
                assert row != null : "SHOW WARM UP JOB should include job ${jobId}, rows=${listRows}"
                assert row.size() == 16 : "SHOW WARM UP JOB list row should expose 16 columns, row=${row}"
            }
            def tableSummaryRow = listRows.find { it[0].toString() == tableJobId.toString() }
            def tableSummary = assertSummarySyncStats(tableSummaryRow, "ON TABLES job")
            assert !tableSummary.containsKey("data_size") :
                    "ON TABLES job summary should merge data and index sizes: ${tableSummary}"
            assert !tableSummary.containsKey("index_size") :
                    "ON TABLES job summary should merge data and index sizes: ${tableSummary}"

            for (jid in [periodicJobId, clusterJobId, tableJobId]) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }

            // FT-11: cancel keeps existing cache but removes the job from subsequent load triggers.
            sql """use @${srcCluster}"""
            sql """CREATE DATABASE IF NOT EXISTS ${cancelDb}"""
            sql """use ${cancelDb}"""
            sql """CREATE TABLE IF NOT EXISTS cancel_base (id INT, val STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            def cancelJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (
                    INCLUDE '${cancelDb}.cancel_base'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """)[0][0]
            jobIds << cancelJobId
            WarmupMetricsUtils.waitForMatchedTables(sqlRunner, cancelJobId,
                    ["${cancelDb}.cancel_base".toString()] as Set)
            sleep(3000)

            def firstBaseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            int firstLoadRows = 4
            for (int i = 0; i < firstLoadRows; i++) {
                sql """INSERT INTO cancel_base VALUES (${i}, 'before_cancel_${i}')"""
            }
            WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    firstBaseMetrics.finished + firstLoadRows)
            def stableBeforeCancel = WarmupMetricsUtils.waitForMetricsStable(sqlRunner, srcCluster, dstCluster)
            def targetCacheBeforeCancel = WarmupMetricsUtils.getClusterMetricSum(sqlRunner, dstCluster, "ttl_cache_size")
            assert targetCacheBeforeCancel > 0 :
                    "target cache should be populated before cancel, size=${targetCacheBeforeCancel}"

            sql """CANCEL WARM UP JOB WHERE ID = ${cancelJobId}"""
            def cancelledRow = getJobRow(cancelJobId)
            assert cancelledRow[3] == "CANCELLED" : "job should be CANCELLED, row=${cancelledRow}"
            def targetCacheAfterCancel = WarmupMetricsUtils.getClusterMetricSum(sqlRunner, dstCluster, "ttl_cache_size")
            assert targetCacheAfterCancel >= targetCacheBeforeCancel :
                    "cancel should not evict existing cache, before=${targetCacheBeforeCancel}, after=${targetCacheAfterCancel}"

            int afterCancelRows = 3
            for (int i = 0; i < afterCancelRows; i++) {
                sql """INSERT INTO cancel_base VALUES (${i + 100}, 'after_cancel_${i}')"""
            }
            sleep(8000)
            def afterCancelledLoad = WarmupMetricsUtils.logWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            assert afterCancelledLoad.requested == stableBeforeCancel.requested :
                    "cancelled job should not request new segments after load"
            assert afterCancelledLoad.submitted == stableBeforeCancel.submitted :
                    "cancelled job should not submit new segments after load"
            assert afterCancelledLoad.finished == stableBeforeCancel.finished :
                    "cancelled job should not finish new segments after load"
            def targetCacheAfterCancelledLoad =
                    WarmupMetricsUtils.getClusterMetricSum(sqlRunner, dstCluster, "ttl_cache_size")
            assert targetCacheAfterCancelledLoad >= targetCacheBeforeCancel :
                    "existing cache should remain after post-cancel load"

            sql """use @${dstCluster}"""
            sql """use ${cancelDb}"""
            profile("ft11_cancel_target_profile") {
                sql """set enable_profile = true"""
                sql """set profile_level = 2"""
                run {
                    def expectedRows = firstLoadRows + afterCancelRows
                    def expectedSum = (0..<firstLoadRows).sum() + (100..<(100 + afterCancelRows)).sum()
                    def res = sql """/* ft11_cancel_target_profile */ SELECT count(*), sum(id) FROM cancel_base"""
                    assert res[0][0].toString() == expectedRows.toString() :
                            "target query row count mismatch after cancel: ${res}"
                    assert res[0][1].toString() == expectedSum.toString() :
                            "target query sum mismatch after cancel: ${res}"
                    sleep(1000)
                }
                check { profileString, exception ->
                    if (exception != null) {
                        throw exception
                    }
                    assert profileString.contains("NumRemoteIOTotal") : "profile should contain file cache counters"
                    def remoteTotal = WarmupMetricsUtils.sumProfileCounter(profileString, "NumRemoteIOTotal")
                    def localTotal = WarmupMetricsUtils.sumProfileCounter(profileString, "NumLocalIOTotal")
                    logger.info("cancel target profile NumRemoteIOTotal=${remoteTotal}, NumLocalIOTotal=${localTotal}")
                    assert remoteTotal > 0 :
                            "post-cancel target query should read remote data for segments loaded after cancel"
                    assert localTotal > 0 : "post-cancel target query should still hit existing warmed cache"
                }
            }

        } finally {
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use ${showDb}"""
                sql """DROP TABLE IF EXISTS show_base"""
                sql """DROP TABLE IF EXISTS show_extra"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${showDb}""" } catch (Exception ignored) {}
            try {
                sql """use ${cancelDb}"""
                sql """DROP TABLE IF EXISTS cancel_base"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${cancelDb}""" } catch (Exception ignored) {}
        }
    }
}
