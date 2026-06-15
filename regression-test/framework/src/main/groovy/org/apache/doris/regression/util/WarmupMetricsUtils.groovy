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
package org.apache.doris.regression.util

import groovy.json.JsonSlurper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.regex.Pattern

/**
 * Utility methods for event-driven warmup regression tests.
 *
 * Methods that need database access accept a {@code Closure sqlRunner}
 * parameter — callers pass {@code { String q -> sql(q) }} from the
 * suite context.
 */
class WarmupMetricsUtils {

    static final Logger logger = LoggerFactory.getLogger(WarmupMetricsUtils.class)

    // Bvar metric names
    static final String METRIC_REQUESTED = "file_cache_event_driven_warm_up_requested_segment_num"
    static final String METRIC_SUBMITTED = "file_cache_event_driven_warm_up_submitted_segment_num"
    static final String METRIC_FINISHED  = "file_cache_event_driven_warm_up_finished_segment_num"
    static final String METRIC_FAILED    = "file_cache_event_driven_warm_up_failed_segment_num"

    /**
     * Fetch a single bvar metric value from a BE's brpc_metrics endpoint.
     */
    static long getBrpcMetric(String ip, String port, String metricName) {
        def url = "http://${ip}:${port}/brpc_metrics"
        def text = new URL(url).text
        def matcher = text =~ ~"${metricName}\\s+(\\d+)"
        if (matcher.find()) {
            return matcher[0][1] as long
        }
        throw new RuntimeException("${metricName} not found for ${ip}:${port}")
    }

    static String getPrometheusMetrics(String ip, Object port) {
        return new URL("http://${ip}:${port}/metrics").text
    }

    static BigDecimal findPrometheusMetricValue(String metricsText, String metricName, Map labels) {
        def line = metricsText.readLines().find { metricLine ->
            metricLine.startsWith("${metricName}{")
                    && labels.every { entry -> metricLine.contains(prometheusLabel(entry.key.toString(), entry.value)) }
        }
        if (line == null) {
            return null
        }
        return new BigDecimal(line.substring(line.lastIndexOf(' ') + 1).trim())
    }

    static String prometheusLabel(String key, Object value) {
        def text = value == null ? "" : value.toString()
        text = text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
        return "${key}=\"${text}\"".toString()
    }

    /**
     * Sum a bvar metric across all BEs in the given cluster.
     */
    static long getClusterMetricSum(Closure sqlRunner, String clusterName, String metricName) {
        def clusterBes = getClusterBackends(sqlRunner, clusterName)
        long sum = 0
        for (be in clusterBes) {
            sum += getBrpcMetric(be[1].toString(), be[5].toString(), metricName)
        }
        return sum
    }

    static List getClusterBackends(Closure sqlRunner, String clusterName) {
        def backends = sqlRunner("SHOW BACKENDS")
        return backends.findAll {
            it[19].contains("\"compute_group_name\" : \"${clusterName}\"".toString())
        }
    }

    static Map getClusterMetricValues(Closure sqlRunner, String clusterName, String metricName) {
        Map values = [:]
        for (be in getClusterBackends(sqlRunner, clusterName)) {
            values[be[0].toString()] = getBrpcMetric(be[1].toString(), be[5].toString(), metricName)
        }
        return values
    }

    static void clearFileCache(String ip, String httpPort) {
        def response = new URL("http://${ip}:${httpPort}/api/file_cache?op=clear&sync=true").text
        def json = new JsonSlurper().parseText(response)
        if (json.status != "OK") {
            throw new RuntimeException("Clear cache on ${ip}:${httpPort} failed: ${json.status}")
        }
    }

    static void clearFileCacheOnAllBackends(Closure sqlRunner, long waitMs = 5000) {
        for (be in sqlRunner("SHOW BACKENDS")) {
            clearFileCache(be[1].toString(), be[4].toString())
        }
        Thread.sleep(waitMs)
    }

    static long sumProfileCounter(String profileText, String counterName) {
        def matcher = profileText =~ ~"(?m)(?<![A-Za-z0-9_])${Pattern.quote(counterName)}\\s*:\\s*([0-9,]+)"
        long sum = 0
        while (matcher.find()) {
            sum += matcher.group(1).replace(",", "") as long
        }
        return sum
    }

    /**
     * Collect all four warmup metrics.
     * <p>{@code requested} is from the SOURCE cluster; the other three from DESTINATION.</p>
     *
     * @return Map with keys: requested, submitted, finished, failed
     */
    static Map getWarmupMetrics(Closure sqlRunner, String srcCluster, String dstCluster) {
        return [
            requested: getClusterMetricSum(sqlRunner, srcCluster, METRIC_REQUESTED),
            submitted: getClusterMetricSum(sqlRunner, dstCluster, METRIC_SUBMITTED),
            finished : getClusterMetricSum(sqlRunner, dstCluster, METRIC_FINISHED),
            failed   : getClusterMetricSum(sqlRunner, dstCluster, METRIC_FAILED),
        ]
    }

    /**
     * Log and return warmup metrics.
     */
    static Map logWarmupMetrics(Closure sqlRunner, String srcCluster, String dstCluster) {
        def m = getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
        logger.info("warmup metrics [src=${srcCluster}, dst=${dstCluster}]: " +
                "requested=${m.requested}, submitted=${m.submitted}, " +
                "finished=${m.finished}, failed=${m.failed}")
        return m
    }

    /**
     * Poll until enough segments have finished warming up.
     *
     * @param expectedFinished absolute finished count to wait for
     * @param timeoutMs        polling timeout in milliseconds
     * @return latest metrics snapshot
     */
    static Map waitForWarmupFinish(Closure sqlRunner, String srcCluster, String dstCluster,
                                   long expectedFinished, long timeoutMs = 60000) {
        long deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            def m = getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            if (m.finished >= expectedFinished && m.finished + m.failed >= m.submitted) {
                return m
            }
            Thread.sleep(2000)
        }
        logger.warn("waitForWarmupFinish timed out after ${timeoutMs}ms, " +
                "expected finished >= ${expectedFinished}")
        return getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
    }

    /**
     * Parse the MatchedTables column (index 14) from SHOW WARM UP JOB output.
     */
    static Set<String> parseMatchedTables(List jobInfo) {
        def raw = jobInfo[0][14]?.toString()?.trim()
        if (raw == null || raw.isEmpty()) {
            return [] as Set
        }
        return raw.split(/,\s*/).collect { it.trim() }.findAll { !it.isEmpty() }.toSet()
    }

    /**
     * Poll until MatchedTables contains (and excludes) the expected table names.
     *
     * @return last observed MatchedTables set
     */
    static Set<String> waitForMatchedTables(Closure sqlRunner, Object jobId,
                                            Set<String> expectedContains,
                                            Set<String> expectedNotContains = [] as Set,
                                            long timeoutMs = 30000) {
        long deadline = System.currentTimeMillis() + timeoutMs
        Set<String> lastMatched = [] as Set
        while (System.currentTimeMillis() < deadline) {
            def info = sqlRunner("SHOW WARM UP JOB WHERE ID = ${jobId}")
            lastMatched = parseMatchedTables(info)
            boolean allContained = expectedContains.every { lastMatched.contains(it) }
            boolean noneExcluded = expectedNotContains.every { !lastMatched.contains(it) }
            if (allContained && noneExcluded) {
                return lastMatched
            }
            Thread.sleep(2000)
        }
        return lastMatched
    }

    /**
     * Parse the SyncStats column (index 15) from SHOW WARM UP JOB output.
     */
    static Map parseSyncStats(List jobInfo) {
        def raw = jobInfo[0][15]?.toString()?.trim()
        if (raw == null || raw.isEmpty()) {
            return [:]
        }
        return new JsonSlurper().parseText(raw) as Map
    }

    /**
     * Poll SHOW WARM UP JOB WHERE ID until SyncStats exists and satisfies the predicate.
     *
     * @return last parsed SyncStats map
     */
    static Map waitForJobSyncStats(Closure sqlRunner, Object jobId, Closure<Boolean> predicate,
                                   long timeoutMs = 30000) {
        long deadline = System.currentTimeMillis() + timeoutMs
        Map lastStats = [:]
        while (System.currentTimeMillis() < deadline) {
            def info = sqlRunner("SHOW WARM UP JOB WHERE ID = ${jobId}")
            lastStats = parseSyncStats(info)
            if (!lastStats.isEmpty() && predicate(lastStats)) {
                return lastStats
            }
            Thread.sleep(2000)
        }
        return lastStats
    }

    /**
     * Wait for warmup metrics to stabilize (no new submissions for a sustained period).
     * Uses a double-check pattern: waits 5s initially, then verifies stability over 3s.
     *
     * @return stabilized metrics snapshot
     */
    static Map waitForMetricsStable(Closure sqlRunner, String srcCluster, String dstCluster,
                                    long timeoutMs = 30000) {
        long deadline = System.currentTimeMillis() + timeoutMs
        def prev = getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
        Thread.sleep(5000)
        while (System.currentTimeMillis() < deadline) {
            def cur = getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            if (cur.submitted == prev.submitted && cur.finished == prev.finished
                    && cur.finished + cur.failed >= cur.submitted) {
                Thread.sleep(3000)
                def verify = getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
                if (verify.submitted == cur.submitted && verify.finished == cur.finished) {
                    return verify
                }
            }
            prev = cur
            Thread.sleep(2000)
        }
        logger.warn("waitForMetricsStable timed out after ${timeoutMs}ms")
        return getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
    }
}
