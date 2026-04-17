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
                    if (syncStats.seg_num.requested_5m >= submittedDelta
                            && syncStats.seg_num.finish_5m >= finishedDelta) {
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

            // Verify seg_num has the expected window keys
            def segNum = syncStats.seg_num
            for (window in ["5m", "30m", "1h"]) {
                assert segNum.containsKey("requested_${window}".toString()) : "Missing seg_num.requested_${window}"
                assert segNum.containsKey("finish_${window}".toString()) : "Missing seg_num.finish_${window}"
                assert segNum.containsKey("gap_${window}".toString()) : "Missing seg_num.gap_${window}"
                assert segNum.containsKey("fail_${window}".toString()) : "Missing seg_num.fail_${window}"
            }

            // Verify absolute segment counts match bvar deltas
            logger.info("seg_num.requested_5m=${segNum.requested_5m}, bvar submitted delta=${submittedDelta}")
            logger.info("seg_num.finish_5m=${segNum.finish_5m}, bvar finished delta=${finishedDelta}")
            assert segNum.requested_5m == submittedDelta :
                    "seg_num.requested_5m(${segNum.requested_5m}) should equal source submitted delta(${submittedDelta})"
            assert segNum.finish_5m == finishedDelta :
                    "seg_num.finish_5m(${segNum.finish_5m}) should equal target finished delta(${finishedDelta})"

            // Verify gap is 0 after warmup completes (all requested segments finished)
            assert segNum.gap_5m == 0 : "Expected gap_5m == 0 after warmup completes, got ${segNum.gap_5m}"

            // Verify fail count is 0
            assert segNum.fail_5m == 0 : "Expected no failures, got fail_5m=${segNum.fail_5m}"

            // Verify seg_size values are human-readable strings
            def segSize = syncStats.seg_size
            logger.info("seg_size.requested_5m = ${segSize.requested_5m}")
            assert segSize.requested_5m instanceof String : "seg_size values should be strings"

            // Verify timestamps are non-empty (warmup has occurred)
            logger.info("last_trigger_ts = ${syncStats.last_trigger_ts}, last_finish_ts = ${syncStats.last_finish_ts}")
            assert syncStats.last_trigger_ts != null && syncStats.last_trigger_ts.toString().length() > 0 :
                    "last_trigger_ts should be non-empty after warmup"
            assert syncStats.last_finish_ts != null && syncStats.last_finish_ts.toString().length() > 0 :
                    "last_finish_ts should be non-empty after warmup"

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
