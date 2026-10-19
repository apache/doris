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

suite('test_warm_up_event_on_tables_multi_dst', 'docker') {
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
        def dstCluster1 = "warmup_target_1"
        def dstCluster2 = "warmup_target_2"

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster1)
        cluster.addBackend(1, dstCluster2)

        sql """use @${srcCluster}"""

        def dbName = "test_on_tables_multi_dst_db"
        def jobIds = []

        try {
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """use ${dbName}"""

            sql """CREATE TABLE IF NOT EXISTS orders (id INT, amount DOUBLE)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS logs (id INT, msg STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            // ===== Create job1: source -> target1, only 'orders' =====
            sql """use @${srcCluster}"""
            def jobId1_ = sql """
                WARM UP CLUSTER ${dstCluster1} WITH CLUSTER ${srcCluster}
                ON TABLES (
                    INCLUDE '${dbName}.orders'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """
            def jobId1 = jobId1_[0][0]
            jobIds << jobId1
            logger.info("Job1 (source -> target1, orders only): ID=${jobId1}")

            // ===== Create job2: source -> target2, all tables =====
            def jobId2_ = sql """
                WARM UP CLUSTER ${dstCluster2} WITH CLUSTER ${srcCluster}
                ON TABLES (
                    INCLUDE '${dbName}.*'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """
            def jobId2 = jobId2_[0][0]
            jobIds << jobId2
            logger.info("Job2 (source -> target2, all tables): ID=${jobId2}")

            sleep(3000)

            // Verify matched tables for each job
            def matched1 = WarmupMetricsUtils.waitForMatchedTables(sqlRunner, jobId1,
                    ["${dbName}.orders".toString()] as Set,
                    ["${dbName}.logs".toString()] as Set)
            logger.info("Job1 MatchedTables: ${matched1}")
            assert "${dbName}.orders".toString() in matched1
            assert !("${dbName}.logs".toString() in matched1)

            def matched2 = WarmupMetricsUtils.waitForMatchedTables(sqlRunner, jobId2,
                    ["${dbName}.orders".toString(), "${dbName}.logs".toString()] as Set)
            logger.info("Job2 MatchedTables: ${matched2}")
            assert "${dbName}.orders".toString() in matched2
            assert "${dbName}.logs".toString() in matched2

            // ===== Test 1: Insert into 'orders' — both targets should warm up =====
            logger.info("===== Test 1: orders -> both targets =====")

            def baseDst1 = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster1)
            def baseDst2 = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster2)
            logger.info("Baseline target1: ${baseDst1}, target2: ${baseDst2}")

            def numInserts = 5
            sql """use ${dbName}"""
            for (int i = 0; i < numInserts; i++) {
                sql """INSERT INTO orders VALUES (${i}, ${i * 10.5})"""
            }

            // Wait for both targets to finish
            def finalDst1 = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster1,
                    baseDst1.finished + numInserts)
            def finalDst2 = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster2,
                    baseDst2.finished + numInserts)
            WarmupMetricsUtils.logWarmupMetrics(sqlRunner, srcCluster, dstCluster1)
            WarmupMetricsUtils.logWarmupMetrics(sqlRunner, srcCluster, dstCluster2)

            // Verify target1
            def dst1SubDelta = finalDst1.submitted - baseDst1.submitted
            def dst1FinDelta = finalDst1.finished - baseDst1.finished
            def dst1FailDelta = finalDst1.failed - baseDst1.failed
            logger.info("Target1 deltas: submitted=${dst1SubDelta}, finished=${dst1FinDelta}, failed=${dst1FailDelta}")
            assert dst1SubDelta >= numInserts : "Target1: expected submitted >= ${numInserts}, got ${dst1SubDelta}"
            assert dst1FinDelta >= numInserts : "Target1: expected finished >= ${numInserts}, got ${dst1FinDelta}"
            assert dst1FailDelta == 0 : "Target1: expected 0 failed, got ${dst1FailDelta}"

            // Verify target2
            def dst2SubDelta = finalDst2.submitted - baseDst2.submitted
            def dst2FinDelta = finalDst2.finished - baseDst2.finished
            def dst2FailDelta = finalDst2.failed - baseDst2.failed
            logger.info("Target2 deltas: submitted=${dst2SubDelta}, finished=${dst2FinDelta}, failed=${dst2FailDelta}")
            assert dst2SubDelta >= numInserts : "Target2: expected submitted >= ${numInserts}, got ${dst2SubDelta}"
            assert dst2FinDelta >= numInserts : "Target2: expected finished >= ${numInserts}, got ${dst2FinDelta}"
            assert dst2FailDelta == 0 : "Target2: expected 0 failed, got ${dst2FailDelta}"

            // ===== Test 2: Insert into 'logs' — only target2 should warm up =====
            logger.info("===== Test 2: logs -> only target2 =====")

            // Wait for metrics to stabilize before negative proof
            WarmupMetricsUtils.waitForMetricsStable(sqlRunner, srcCluster, dstCluster1)
            def baseDst1ForLogs = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster1)
            def baseDst2ForLogs = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster2)

            sql """use ${dbName}"""
            for (int i = 0; i < numInserts; i++) {
                sql """INSERT INTO logs VALUES (${i}, 'log_${i}')"""
            }

            // Wait for target2 to finish (logs is matched by job2)
            def finalDst2ForLogs = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster2,
                    baseDst2ForLogs.finished + numInserts)
            WarmupMetricsUtils.logWarmupMetrics(sqlRunner, srcCluster, dstCluster2)

            // Verify target2 warmed logs
            def dst2LogsSubDelta = finalDst2ForLogs.submitted - baseDst2ForLogs.submitted
            def dst2LogsFinDelta = finalDst2ForLogs.finished - baseDst2ForLogs.finished
            def dst2LogsFailDelta = finalDst2ForLogs.failed - baseDst2ForLogs.failed
            logger.info("Target2 logs deltas: submitted=${dst2LogsSubDelta}, finished=${dst2LogsFinDelta}, failed=${dst2LogsFailDelta}")
            assert dst2LogsSubDelta >= numInserts : "Target2: expected submitted >= ${numInserts}, got ${dst2LogsSubDelta}"
            assert dst2LogsFinDelta >= numInserts : "Target2: expected finished >= ${numInserts}, got ${dst2LogsFinDelta}"
            assert dst2LogsFailDelta == 0 : "Target2: expected 0 failed, got ${dst2LogsFailDelta}"

            // Verify target1 did NOT warm logs (negative proof)
            sleep(5000)
            def finalDst1ForLogs = WarmupMetricsUtils.logWarmupMetrics(sqlRunner, srcCluster, dstCluster1)
            def dst1LogsSubDelta = finalDst1ForLogs.submitted - baseDst1ForLogs.submitted
            def dst1LogsFinDelta = finalDst1ForLogs.finished - baseDst1ForLogs.finished
            logger.info("Target1 logs deltas: submitted=${dst1LogsSubDelta}, finished=${dst1LogsFinDelta}")
            assert dst1LogsSubDelta == 0 : "Target1 should NOT warm logs, submitted delta=${dst1LogsSubDelta}"
            assert dst1LogsFinDelta == 0 : "Target1 should NOT warm logs, finished delta=${dst1LogsFinDelta}"

            // ===== Verify SHOW WARM UP JOB for both jobs =====
            logger.info("===== Verify SHOW WARM UP JOB output =====")

            def jobInfo1 = sql """SHOW WARM UP JOB WHERE ID = ${jobId1}"""
            assert jobInfo1[0][2] == dstCluster1
            assert jobInfo1[0][3] in ["RUNNING", "PENDING"]
            def filter1 = new JsonSlurper().parseText(jobInfo1[0][13])
            assert filter1.include.contains("${dbName}.orders".toString())
            assert !filter1.containsKey("exclude")

            def jobInfo2 = sql """SHOW WARM UP JOB WHERE ID = ${jobId2}"""
            assert jobInfo2[0][2] == dstCluster2
            assert jobInfo2[0][3] in ["RUNNING", "PENDING"]
            def filter2 = new JsonSlurper().parseText(jobInfo2[0][13])
            assert filter2.include.contains("${dbName}.*".toString())

        } finally {
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS orders"""
                sql """DROP TABLE IF EXISTS logs"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbName}""" } catch (Exception ignored) {}
        }
    }
}
