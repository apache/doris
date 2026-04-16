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

suite('test_warm_up_event_on_tables_multi_include', 'docker') {
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

        def dbName = "test_on_tables_mi_db"
        def dbOther = "test_on_tables_mi_other_db"
        def jobIds = []

        try {
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """CREATE DATABASE IF NOT EXISTS ${dbOther}"""

            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS orders (id INT, amount DOUBLE)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS customers (id INT, name STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            sql """use ${dbOther}"""
            sql """CREATE TABLE IF NOT EXISTS logs (id INT, msg STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            // Multiple INCLUDE: orders from dbName + logs from dbOther (but NOT customers)
            sql """use @${clusterName1}"""
            def jobId_ = sql """
                WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                ON TABLES (
                    INCLUDE '${dbName}.orders',
                    INCLUDE '${dbOther}.logs'
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

            // Insert into matched (orders, logs) and unmatched (customers)
            def numInserts = 5
            sql """use ${dbName}"""
            for (int i = 0; i < numInserts; i++) {
                sql """INSERT INTO orders VALUES (${i}, ${i * 30.0})"""
                sql """INSERT INTO customers VALUES (${i}, 'extra_${i}')"""
            }
            sql """use ${dbOther}"""
            for (int i = 0; i < numInserts; i++) {
                sql """INSERT INTO logs VALUES (${i}, 'important_${i}')"""
            }

            // Expected: orders(5) + logs(5) = 10 segments; customers(5) NOT included
            def expectedSeg = numInserts * 2  // orders + logs
            def finalMetrics = waitForWarmupFinish(clusterName1, clusterName2, baseMetrics.finished + expectedSeg)
            logWarmUpMetrics(clusterName1, clusterName2)

            def reqDelta = finalMetrics.requested - baseMetrics.requested
            def subDelta = finalMetrics.submitted - baseMetrics.submitted
            def finDelta = finalMetrics.finished - baseMetrics.finished
            def failDelta = finalMetrics.failed - baseMetrics.failed
            logger.info("Multi-include deltas: requested=${reqDelta}, submitted=${subDelta}, finished=${finDelta}, failed=${failDelta}")
            assert reqDelta >= expectedSeg : "Expected requested >= ${expectedSeg}, got ${reqDelta}"
            assert subDelta >= expectedSeg : "Expected submitted >= ${expectedSeg}, got ${subDelta}"
            assert finDelta >= expectedSeg : "Expected finished >= ${expectedSeg}, got ${finDelta}"
            // customers(5 inserts) should NOT contribute; if they did, submitted would be >= 15
            assert subDelta < expectedSeg + numInserts : \
                "customers should NOT be warmed (submitted=${subDelta} should be < ${expectedSeg + numInserts})"
            assert failDelta == 0 : "Expected 0 failed, got ${failDelta}"

            def jobInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            def matchedSet = parseMatchedTables(jobInfo)
            logger.info("MatchedTables set: ${matchedSet}")
            assert "${dbName}.orders".toString() in matchedSet
            assert "${dbOther}.logs".toString() in matchedSet
            assert !("${dbName}.customers".toString() in matchedSet)

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
                sql """use ${dbOther}"""
                sql """DROP TABLE IF EXISTS logs"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbOther}""" } catch (Exception ignored) {}
        }
    }
}
