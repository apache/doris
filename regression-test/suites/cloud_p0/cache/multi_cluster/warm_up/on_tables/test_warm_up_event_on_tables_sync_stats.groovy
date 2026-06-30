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
import groovy.json.JsonSlurper

suite('test_warm_up_event_on_tables_sync_stats', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_warm_up_table_filter_refresh_interval_ms=1000',
        'cloud_warm_up_sync_stats_refresh_interval_ms=1000',
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
        Closure fetchFeMetrics = { ->
            def masterFe = cluster.getMasterFe()
            WarmupMetricsUtils.getPrometheusMetrics(masterFe.host, masterFe.httpPort)
        }
        Closure waitForWarmUpSyncJobMetrics = {
                Object jobId, String jobType, String srcClusterName, String dstClusterName ->
            def commonLabels = [
                    job_id: jobId.toString(),
                    job_type: jobType,
                    src_cluster_name: srcClusterName,
                    dst_cluster_name: dstClusterName
            ]
            String lastDebug = ""
            long deadline = System.currentTimeMillis() + 30000
            while (System.currentTimeMillis() < deadline) {
                def metricsText = fetchFeMetrics()
                def infoLabels = [
                        job_id: jobId.toString(),
                        job_type: jobType,
                        sync_mode: "EVENT_DRIVEN",
                        sync_event: "LOAD",
                        job_state: "RUNNING",
                        src_cluster_name: srcClusterName,
                        dst_cluster_name: dstClusterName
                ]
                def info = WarmupMetricsUtils.findPrometheusMetricValue(metricsText,
                        "doris_fe_file_cache_warm_up_sync_job_info", infoLabels)
                def sizeMetrics = [:]
                boolean allSizeMetricsPositive = true
                for (window in ["5m", "30m", "1h"]) {
                    for (side in ["src", "dst"]) {
                        def key = "${side}_${window}".toString()
                        sizeMetrics[key] = WarmupMetricsUtils.findPrometheusMetricValue(metricsText,
                                "doris_fe_file_cache_warm_up_sync_job_size_bytes",
                                commonLabels + [side: side, window: window])
                        if (sizeMetrics[key] == null || sizeMetrics[key] <= 0) {
                            allSizeMetricsPositive = false
                        }
                    }
                }

                if (info == 1G && allSizeMetricsPositive) {
                    logger.info("FE warm-up sync metrics for job ${jobId}: ${sizeMetrics}")
                    return
                }
                lastDebug = metricsText.readLines()
                        .findAll { it.contains("file_cache_warm_up_sync_job") && it.contains("job_id=\"${jobId}\"") }
                        .join("\n")
                sleep(1000)
            }
            assert false : "Timed out waiting FE warm-up sync metrics for ${jobType} job ${jobId}. "
                    + "Last matching metrics:\n${lastDebug}"
        }

        def clusterName1 = "warmup_source"
        def clusterName2 = "warmup_target"

        cluster.addBackend(1, clusterName1)
        cluster.addBackend(1, clusterName2)

        sql """use @${clusterName1}"""

        def dbName = "test_sync_stats_db"
        def jobIds = []

        try {
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS t1 (id INT, val STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            // Create event-driven warmup job
            sql """use @${clusterName1}"""
            def jobId_ = sql """
                WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                ON TABLES (
                    INCLUDE '${dbName}.*'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """
            def jobId = jobId_[0][0]
            jobIds << jobId
            logger.info("Warm-up job ID: ${jobId}")

            sleep(3000)

            // Capture baseline BEFORE inserts so we know the target
            def baseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, clusterName1, clusterName2)
            logger.info("Baseline metrics: ${baseMetrics}")

            // Insert data to trigger warmup
            def numInserts = 5
            sql """use ${dbName}"""
            for (int i = 0; i < numInserts; i++) {
                sql """INSERT INTO t1 VALUES (${i}, 'value_${i}')"""
            }

            // Wait for warmup to finish using bvar metrics
            def metrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, clusterName1, clusterName2,
                    baseMetrics.finished + numInserts)
            logger.info("Warmup metrics after finish: ${metrics}")

            // Compute bvar deltas (source submitted, target finished)
            def submittedDelta = metrics.submitted - baseMetrics.submitted
            def finishedDelta = metrics.finished - baseMetrics.finished
            logger.info("Bvar deltas: submitted=${submittedDelta}, finished=${finishedDelta}")

            // Poll SHOW WARM UP JOB until windowed metrics catch up with bvar values
            // (bvar::Window samples every ~1s; values need time to accumulate)
            def syncStats = null
            def syncStatsStr = ""
            long deadline = System.currentTimeMillis() + 30000
            while (System.currentTimeMillis() < deadline) {
                def jobInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
                assert jobInfo.size() > 0 : "SHOW WARM UP JOB returned no rows"
                syncStatsStr = jobInfo[0][15]?.toString()?.trim()
                if (syncStatsStr != null && syncStatsStr.length() > 0) {
                    syncStats = new JsonSlurper().parseText(syncStatsStr)
                    if (syncStats.seg_num.requested_5m == submittedDelta
                            && syncStats.seg_num.finish_5m == finishedDelta
                            && syncStats.seg_num.gap_5m == 0
                            && syncStats.seg_num.fail_5m == 0
                            && syncStats.trigger_gap_ms == 0) {
                        break
                    }
                }
                sleep(2000)
            }
            logger.info("SyncStats column: ${syncStatsStr}")
            assert syncStats != null : "SyncStats should not be empty for event-driven job"

            // Verify top-level keys
            assert syncStats.containsKey("seg_num") : "Missing seg_num"
            assert syncStats.containsKey("seg_size") : "Missing seg_size"
            assert syncStats.containsKey("idx_num") : "Missing idx_num"
            assert syncStats.containsKey("idx_size") : "Missing idx_size"
            assert syncStats.containsKey("last_trigger_ts") : "Missing last_trigger_ts"
            assert syncStats.containsKey("last_finish_ts") : "Missing last_finish_ts"
            assert syncStats.containsKey("trigger_gap_ms") : "Missing trigger_gap_ms"
            assert !syncStats.containsKey("window") : "Detailed SyncStats should not be compact summary"
            assert !syncStats.containsKey("src_size") : "Detailed SyncStats should not be compact summary"
            assert !syncStats.containsKey("dst_size") : "Detailed SyncStats should not be compact summary"
            assert !syncStats.containsKey("gap_size") : "Detailed SyncStats should not be compact summary"

            // Verify detailed stats have the expected window keys.
            def assertWindowFields = { groupName, group ->
                for (window in ["5m", "30m", "1h"]) {
                    for (field in ["requested", "finish", "gap", "fail"]) {
                        def key = "${field}_${window}".toString()
                        assert group.containsKey(key) : "Missing ${groupName}.${key}"
                    }
                }
            }
            def segNum = syncStats.seg_num
            assertWindowFields("seg_num", segNum)
            assertWindowFields("seg_size", syncStats.seg_size)
            assertWindowFields("idx_num", syncStats.idx_num)
            assertWindowFields("idx_size", syncStats.idx_size)

            // Verify absolute segment counts match bvar deltas
            logger.info("seg_num.requested_5m=${segNum.requested_5m}, bvar submitted delta=${submittedDelta}")
            logger.info("seg_num.finish_5m=${segNum.finish_5m}, bvar finished delta=${finishedDelta}")
            assert segNum.requested_5m == submittedDelta :
                    "seg_num.requested_5m(${segNum.requested_5m}) should equal source submitted delta(${submittedDelta})"
            assert segNum.finish_5m == finishedDelta :
                    "seg_num.finish_5m(${segNum.finish_5m}) should equal target finished delta(${finishedDelta})"

            // Verify gap is 0 after warmup completes (all requested segments finished)
            assert segNum.gap_5m == 0 : "Expected gap_5m == 0 after warmup completes, got ${segNum.gap_5m}"
            assert syncStats.trigger_gap_ms == 0 :
                    "Expected trigger_gap_ms == 0 after warmup completes, got ${syncStats.trigger_gap_ms}, stats=${syncStats}"

            // Verify fail count is 0
            assert segNum.fail_5m == 0 : "Expected no failures, got fail_5m=${segNum.fail_5m}"

            // Verify seg_size values are human-readable strings
            def segSize = syncStats.seg_size
            logger.info("seg_size.requested_5m = ${segSize.requested_5m}")
            assert segSize.requested_5m instanceof String : "seg_size values should be strings"
            assert syncStats.idx_size.requested_5m instanceof String : "idx_size values should be strings"
            assert syncStats.idx_num.requested_5m instanceof Number : "idx_num values should be numbers"

            // Verify timestamps are non-empty (warmup has occurred)
            logger.info("last_trigger_ts = ${syncStats.last_trigger_ts}, last_finish_ts = ${syncStats.last_finish_ts}")
            assert syncStats.last_trigger_ts != null && syncStats.last_trigger_ts.toString().length() > 0 :
                    "last_trigger_ts should be non-empty after warmup"
            assert syncStats.last_finish_ts != null && syncStats.last_finish_ts.toString().length() > 0 :
                    "last_finish_ts should be non-empty after warmup"

            // SHOW WARM UP JOB list output should show a compact 30m summary, not the detailed SyncStats.
            def allJobInfo = sql """SHOW WARM UP JOB"""
            def summaryRow = allJobInfo.find { row -> row[0]?.toString() == jobId.toString() }
            assert summaryRow != null : "SHOW WARM UP JOB should include job ${jobId}"
            def summaryStatsStr = summaryRow[15]?.toString()?.trim()
            logger.info("SyncStats summary column: ${summaryStatsStr}")
            assert summaryStatsStr != null && summaryStatsStr.length() > 0 :
                    "SyncStats summary should not be empty for event-driven job"
            def summaryStats = new JsonSlurper().parseText(summaryStatsStr)
            assert summaryStats.window == "30m" : "Summary should use 30m window"
            assert summaryStats.src_size instanceof String : "Summary src_size should be a string"
            assert summaryStats.dst_size instanceof String : "Summary dst_size should be a string"
            assert summaryStats.gap_size instanceof String : "Summary gap_size should be a string"
            assert summaryStats.trigger_gap_ms == 0 :
                    "Summary trigger_gap_ms should be 0 after warmup completes, got ${summaryStats.trigger_gap_ms}"
            assert !summaryStats.containsKey("seg_num") : "List summary should not include detailed seg_num"
            assert !summaryStats.containsKey("seg_size") : "List summary should not include detailed seg_size"
            assert !summaryStats.containsKey("idx_num") : "List summary should not include detailed idx_num"
            assert !summaryStats.containsKey("idx_size") : "List summary should not include detailed idx_size"
            assert !summaryStats.containsKey("last_trigger_ts") : "List summary should not include detailed timestamp"
            assert !summaryStats.containsKey("last_finish_ts") : "List summary should not include detailed timestamp"
            assert !summaryStats.containsKey("data_size") : "List summary should merge data and index sizes"
            assert !summaryStats.containsKey("index_size") : "List summary should merge data and index sizes"

            waitForWarmUpSyncJobMetrics(jobId, "TABLES", clusterName1, clusterName2)

            sql """CANCEL WARM UP JOB WHERE ID = ${jobId}"""
            sleep(1000)

            def clusterJobIdRows = sql """
                WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """
            def clusterJobId = clusterJobIdRows[0][0]
            jobIds << clusterJobId
            logger.info("Cluster-level warm-up job ID: ${clusterJobId}")
            sleep(3000)

            def clusterBaseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, clusterName1, clusterName2)
            logger.info("Cluster job baseline metrics: ${clusterBaseMetrics}")

            for (int i = numInserts; i < numInserts * 2; i++) {
                sql """INSERT INTO t1 VALUES (${i}, 'value_${i}')"""
            }

            def clusterMetrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, clusterName1, clusterName2,
                    clusterBaseMetrics.finished + numInserts)
            logger.info("Cluster job warmup metrics after finish: ${clusterMetrics}")
            def clusterSubmittedDelta = clusterMetrics.submitted - clusterBaseMetrics.submitted
            def clusterFinishedDelta = clusterMetrics.finished - clusterBaseMetrics.finished
            assert clusterSubmittedDelta > 0 : "Cluster-level job should submit source warm-up requests"
            assert clusterFinishedDelta > 0 : "Cluster-level job should finish target warm-up requests"
            waitForWarmUpSyncJobMetrics(clusterJobId, "CLUSTER", clusterName1, clusterName2)

        } finally {
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS t1"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbName}""" } catch (Exception ignored) {}
        }
    }
}
