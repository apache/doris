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
import groovy.json.JsonSlurper

suite('test_warm_up_event_on_tables_include', 'docker') {
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

    // ===== Helper closures =====
    def getBrpcMetrics = { ip, port, name ->
        def url = "http://${ip}:${port}/brpc_metrics"
        def metrics = new URL(url).text
        def matcher = metrics =~ ~"${name}\\s+(\\d+)"
        if (matcher.find()) {
            return matcher[0][1] as long
        } else {
            throw new RuntimeException("${name} not found for ${ip}:${port}")
        }
    }

    def getClusterMetricSum = { clusterName, metricName ->
        def backends = sql """SHOW BACKENDS"""
        def clusterBes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${clusterName}\"""") }
        long sum = 0
        for (be in clusterBes) {
            sum += getBrpcMetrics(be[1], be[5], metricName)
        }
        return sum
    }

    // requested is on SOURCE cluster; submitted/finished/failed on DESTINATION cluster
    def getWarmupMetrics = { srcCluster, dstCluster ->
        return [
            requested: getClusterMetricSum(srcCluster, "file_cache_event_driven_warm_up_requested_segment_num"),
            submitted: getClusterMetricSum(dstCluster, "file_cache_event_driven_warm_up_submitted_segment_num"),
            finished : getClusterMetricSum(dstCluster, "file_cache_event_driven_warm_up_finished_segment_num"),
            failed   : getClusterMetricSum(dstCluster, "file_cache_event_driven_warm_up_failed_segment_num"),
        ]
    }

    def logWarmUpMetrics = { srcCluster, dstCluster ->
        def m = getWarmupMetrics(srcCluster, dstCluster)
        logger.info("warmup metrics [src=${srcCluster}, dst=${dstCluster}]: requested=${m.requested}, submitted=${m.submitted}, finished=${m.finished}, failed=${m.failed}")
        return m
    }

    def parseMatchedTables = { jobInfo ->
        def raw = jobInfo[0][14]?.toString()?.trim()
        if (raw == null || raw.isEmpty()) {
            return [] as Set
        }
        return raw.split(/,\s*/).collect { it.trim() }.findAll { !it.isEmpty() }.toSet()
    }

    def waitForWarmupFinish = { srcCluster, dstCluster, expectedFinished, timeoutMs = 60000 ->
        long deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            def m = getWarmupMetrics(srcCluster, dstCluster)
            if (m.finished >= expectedFinished && m.finished + m.failed >= m.submitted) {
                return m
            }
            sleep(2000)
        }
        logger.warn("waitForWarmupFinish timed out after ${timeoutMs}ms, expected finished >= ${expectedFinished}")
        return getWarmupMetrics(srcCluster, dstCluster)
    }

    docker(options) {
        def clusterName1 = "warmup_source"
        def clusterName2 = "warmup_target"

        cluster.addBackend(1, clusterName1)
        cluster.addBackend(1, clusterName2)

        sql """use @${clusterName1}"""

        def dbName = "test_on_tables_inc_db"
        def dbExcluded = "test_on_tables_exc_db"
        def jobIds = []

        try {
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """CREATE DATABASE IF NOT EXISTS ${dbExcluded}"""

            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS orders (id INT, amount DOUBLE)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS customers (id INT, name STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            sql """use ${dbExcluded}"""
            sql """CREATE TABLE IF NOT EXISTS logs (id INT, msg STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            // Create INCLUDE wildcard job
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

            def baseMetrics = getWarmupMetrics(clusterName1, clusterName2)
            logger.info("Baseline metrics: ${baseMetrics}")

            // Negative proof: insert ONLY into excluded db
            def numExcludedInserts = 5
            sql """use ${dbExcluded}"""
            for (int i = 0; i < numExcludedInserts; i++) {
                sql """INSERT INTO logs VALUES (${i}, 'log_message_${i}')"""
            }
            sleep(5000)

            def metricsAfterExcluded = logWarmUpMetrics(clusterName1, clusterName2)
            def excludedSubmittedDelta = metricsAfterExcluded.submitted - baseMetrics.submitted
            def excludedFinishedDelta = metricsAfterExcluded.finished - baseMetrics.finished
            assert excludedSubmittedDelta == 0 : "Excluded inserts should not submit segments, delta=${excludedSubmittedDelta}"
            assert excludedFinishedDelta == 0 : "Excluded inserts should not finish segments, delta=${excludedFinishedDelta}"

            // Positive proof: insert into included db
            def numIncludedInserts = 5
            def expectedSegments = numIncludedInserts * 2  // 2 tables: orders + customers
            sql """use ${dbName}"""
            for (int i = 0; i < numIncludedInserts; i++) {
                sql """INSERT INTO orders VALUES (${i}, ${i * 10.5})"""
                sql """INSERT INTO customers VALUES (${i}, 'customer_${i}')"""
            }

            def finalMetrics = waitForWarmupFinish(clusterName1, clusterName2, metricsAfterExcluded.finished + expectedSegments)
            logWarmUpMetrics(clusterName1, clusterName2)

            def requestedDelta = finalMetrics.requested - metricsAfterExcluded.requested
            def submittedDelta = finalMetrics.submitted - metricsAfterExcluded.submitted
            def finishedDelta = finalMetrics.finished - metricsAfterExcluded.finished
            def failedDelta = finalMetrics.failed - metricsAfterExcluded.failed
            logger.info("Included warmup deltas: requested=${requestedDelta}, submitted=${submittedDelta}, finished=${finishedDelta}, failed=${failedDelta}")
            assert requestedDelta >= expectedSegments : "Expected requested >= ${expectedSegments}, got ${requestedDelta}"
            assert submittedDelta >= expectedSegments : "Expected submitted >= ${expectedSegments}, got ${submittedDelta}"
            assert finishedDelta >= expectedSegments : "Expected finished >= ${expectedSegments}, got ${finishedDelta}"
            assert failedDelta == 0 : "Expected 0 failed segments, got ${failedDelta}"

            // Verify SHOW WARM UP JOB output
            def jobInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            assert jobInfo[0][0] == jobId
            assert jobInfo[0][1] == clusterName1
            assert jobInfo[0][2] == clusterName2
            assert jobInfo[0][3] in ["RUNNING", "PENDING"]
            assert jobInfo[0][4] == "CLUSTER"
            assert jobInfo[0][5] == "EVENT_DRIVEN (LOAD)"

            def tableFilter = jobInfo[0][13]
            logger.info("TableFilter: ${tableFilter}")
            assert tableFilter != null && tableFilter.length() > 0
            def filterJson = new JsonSlurper().parseText(tableFilter)
            assert filterJson.include.contains("${dbName}.*".toString())
            assert !filterJson.containsKey("exclude")

            def matchedSet = parseMatchedTables(jobInfo)
            logger.info("MatchedTables set: ${matchedSet}")
            assert "${dbName}.orders".toString() in matchedSet
            assert "${dbName}.customers".toString() in matchedSet
            assert !matchedSet.any { it.startsWith("${dbExcluded}.") }

        } finally {
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS orders"""
                sql """DROP TABLE IF EXISTS customers"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbName}""" } catch (Exception ignored) {}
            try {
                sql """use ${dbExcluded}"""
                sql """DROP TABLE IF EXISTS logs"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbExcluded}""" } catch (Exception ignored) {}
        }
    }
}
