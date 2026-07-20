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
        def latest = getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
        throw new RuntimeException("waitForWarmupFinish timed out after ${timeoutMs}ms, "
                + "expected finished >= ${expectedFinished}, latest=${latest}")
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

    static List<Map> showWarmupJobs(Closure sqlRunner) {
        def rows = sqlRunner("SHOW WARM UP JOB")
        return rows.collect { row -> normalizeWarmupJobRow(row) }
    }

    static Map showWarmupJob(Closure sqlRunner, Object jobId) {
        def rows = sqlRunner("SHOW WARM UP JOB WHERE ID = ${jobId}")
        if (rows == null || rows.isEmpty()) {
            throw new RuntimeException("warmup job ${jobId} not found")
        }
        return normalizeWarmupJobRow(rows[0])
    }

    static Map normalizeWarmupJobRow(Object row) {
        List values = row as List
        return [
                jobId          : values[0]?.toString(),
                srcCluster     : values[1]?.toString(),
                dstCluster     : values[2]?.toString(),
                status         : values[3]?.toString(),
                type           : values[4]?.toString(),
                syncMode       : values[5]?.toString(),
                createTime     : values[6]?.toString(),
                startTime      : values[7]?.toString(),
                finishedTime   : values[8]?.toString(),
                allBatch       : values[9]?.toString(),
                finishedBatch  : values[10]?.toString(),
                errMsg         : values[11]?.toString(),
                tableOrPattern : values.size() > 12 ? values[12]?.toString() : "",
                tableFilter    : values.size() > 13 ? values[13]?.toString() : "",
                matchedTables  : values.size() > 14 ? values[14]?.toString() : "",
                syncStats      : values.size() > 15 ? values[15]?.toString() : "",
                raw            : values,
        ]
    }

    static boolean isNormalWarmupJob(Map row) {
        String syncMode = row.syncMode ?: ""
        return syncMode.startsWith("ONCE") || syncMode.startsWith("PERIODIC")
    }

    static boolean isEventDrivenWarmupJob(Map row) {
        String syncMode = row.syncMode ?: ""
        return syncMode.startsWith("EVENT_DRIVEN")
    }

    static boolean isActiveWarmupJob(Map row) {
        return row.status in ["PENDING", "RUNNING", "WAITING"]
    }

    static boolean isClusterWarmupJob(Map row) {
        return row.type == "CLUSTER"
    }

    static List<Map> showWarmupJobsByDst(Closure sqlRunner, String dstCluster) {
        return showWarmupJobs(sqlRunner).findAll { it.dstCluster == dstCluster }
    }

    static long countRunningNormalWarmupByDst(Closure sqlRunner, String dstCluster) {
        return showWarmupJobsByDst(sqlRunner, dstCluster).count {
            isNormalWarmupJob(it) && it.status == "RUNNING"
        } as long
    }

    static List<Map> snapshotWarmupJobs(Closure sqlRunner) {
        return showWarmupJobs(sqlRunner).collect { new LinkedHashMap<>(it) }
    }

    static Map<String, Map> snapshotWarmupJobsById(Closure sqlRunner) {
        Map<String, Map> result = [:]
        snapshotWarmupJobs(sqlRunner).each { result[it.jobId] = it }
        return result
    }

    static void waitForOnlyOneRunningNormalWarmup(Closure sqlRunner, String dstCluster,
                                                  long timeoutMs = 30000,
                                                  Collection<String> expectedJobIds = null,
                                                  long observeMs = 5000) {
        long deadline = System.currentTimeMillis() + timeoutMs
        List<Map> lastRunningRows = []
        while (System.currentTimeMillis() < deadline) {
            List<Map> runningRows = showWarmupJobsByDst(sqlRunner, dstCluster).findAll {
                isNormalWarmupJob(it) && it.status == "RUNNING"
            }
            lastRunningRows = runningRows
            if (runningRows.size() > 1) {
                throw new RuntimeException("expected at most one running normal warmup on ${dstCluster}, "
                        + "runningRows=${runningRows}")
            }
            boolean hasExpectedRunning = expectedJobIds == null || runningRows.any {
                expectedJobIds.contains(it.jobId)
            }
            if (runningRows.size() == 1 && hasExpectedRunning) {
                assertAtMostOneRunningNormalWarmupDuring(sqlRunner, dstCluster, observeMs)
                return
            }
            Thread.sleep(1000)
        }
        throw new RuntimeException("expected one normal warmup to reach RUNNING on ${dstCluster}, "
                + "expectedJobIds=${expectedJobIds}, lastRunningRows=${lastRunningRows}")
    }

    static void assertAtMostOneRunningNormalWarmupDuring(Closure sqlRunner, String dstCluster, long observeMs) {
        long deadline = System.currentTimeMillis() + observeMs
        while (System.currentTimeMillis() < deadline) {
            List<Map> runningRows = showWarmupJobsByDst(sqlRunner, dstCluster).findAll {
                isNormalWarmupJob(it) && it.status == "RUNNING"
            }
            if (runningRows.size() > 1) {
                throw new RuntimeException("expected at most one running normal warmup on ${dstCluster}, "
                        + "runningRows=${runningRows}")
            }
            Thread.sleep(1000)
        }
    }

    static Map waitForWarmupJobsRecovered(Closure sqlRunner, Map<String, Map> beforeSnapshot,
                                          Closure<Boolean> predicate, long timeoutMs = 60000) {
        long deadline = System.currentTimeMillis() + timeoutMs
        Map<String, Map> current = [:]
        while (System.currentTimeMillis() < deadline) {
            current = snapshotWarmupJobsById(sqlRunner)
            if (predicate(beforeSnapshot, current)) {
                return current
            }
            Thread.sleep(1000)
        }
        return current
    }

    static Map waitForPeriodicCycles(Closure sqlRunner, Object jobId, int minTransitions,
                                     long timeoutMs = 120000) {
        long deadline = System.currentTimeMillis() + timeoutMs
        List<String> states = []
        String lastState = null
        while (System.currentTimeMillis() < deadline) {
            def row = showWarmupJob(sqlRunner, jobId)
            if (row.status != lastState) {
                states << row.status
                lastState = row.status
            }
            int transitions = 0
            for (int i = 1; i < states.size(); i++) {
                if (states[i - 1] == "RUNNING" && (states[i] == "PENDING" || states[i] == "WAITING")) {
                    transitions++
                }
            }
            if (transitions >= minTransitions) {
                return [states: states, row: row]
            }
            Thread.sleep(1000)
        }
        throw new RuntimeException("expected periodic job ${jobId} to observe ${minTransitions} "
                + "RUNNING-to-waiting transitions within ${timeoutMs}ms, states=${states}")
    }

    static List<Map> sampleJobTimeline(Closure sqlRunner, Object jobId, long durationMs,
                                       long intervalMs = 1000) {
        long deadline = System.currentTimeMillis() + durationMs
        List<Map> samples = []
        while (System.currentTimeMillis() < deadline) {
            def row = showWarmupJob(sqlRunner, jobId)
            samples << [
                    ts      : System.currentTimeMillis(),
                    status  : row.status,
                    start   : row.startTime,
                    finished: row.finishedTime,
                    syncMode: row.syncMode,
            ]
            Thread.sleep(intervalMs)
        }
        return samples
    }

    static List<Map> collectWarmupJobsByCluster(Closure sqlRunner, String clusterName) {
        return showWarmupJobs(sqlRunner).findAll {
            it.srcCluster == clusterName || it.dstCluster == clusterName
        }
    }

    static void assertAffectedJobsCancelled(Closure sqlRunner, Collection<String> jobIds,
                                            long timeoutMs = 60000) {
        long deadline = System.currentTimeMillis() + timeoutMs
        Map<String, String> lastStates = [:]
        while (System.currentTimeMillis() < deadline) {
            boolean allCancelled = true
            for (String jobId in jobIds) {
                def row = showWarmupJob(sqlRunner, jobId)
                lastStates[jobId] = row.status
                if (row.status != "CANCELLED") {
                    allCancelled = false
                }
            }
            if (allCancelled) {
                return
            }
            Thread.sleep(1000)
        }
        throw new RuntimeException("expected jobs cancelled, lastStates=${lastStates}")
    }

    static void assertUnrelatedJobsUnaffected(Closure sqlRunner, Collection<String> jobIds,
                                              Collection<String> allowedStatuses = ["RUNNING", "PENDING", "FINISHED"]) {
        for (String jobId in jobIds) {
            def row = showWarmupJob(sqlRunner, jobId)
            if (!allowedStatuses.contains(row.status)) {
                throw new RuntimeException("unrelated warmup job ${jobId} entered unexpected status ${row.status}")
            }
        }
    }

    static String expectCreateConflict(Closure sqlRunner, Closure createJob) {
        try {
            createJob.call()
        } catch (Throwable t) {
            return t.message ?: t.toString()
        }
        throw new RuntimeException("expected warmup create conflict, but statement succeeded")
    }

    static void assertConflictMessage(String message, Collection<String> expectedKeywords) {
        expectedKeywords.each { keyword ->
            if (!(message?.toLowerCase()?.contains(keyword.toLowerCase()))) {
                throw new RuntimeException("expected conflict message to contain '${keyword}', actual=${message}")
            }
        }
    }

    static List<Map> getVcgWarmupJobs(Closure sqlRunner, String srcCluster, String dstCluster) {
        return showWarmupJobs(sqlRunner).findAll {
            it.srcCluster == srcCluster && it.dstCluster == dstCluster
                    && isActiveWarmupJob(it) && isClusterWarmupJob(it)
                    && (it.syncMode.startsWith("PERIODIC") || it.syncMode.startsWith("EVENT_DRIVEN"))
        }
    }

    static List<String> getVcgWarmupJobIds(Closure sqlRunner, String srcCluster, String dstCluster) {
        return getVcgWarmupJobs(sqlRunner, srcCluster, dstCluster).collect { it.jobId }
    }

    static boolean hasPeriodicAndEventDrivenWarmup(Collection<Map> rows) {
        return rows.any { it.syncMode.startsWith("PERIODIC") }
                && rows.any { it.syncMode.startsWith("EVENT_DRIVEN") }
    }

    static List<Map> waitForWarmupJobsByPair(Closure sqlRunner, String srcCluster, String dstCluster,
                                             int minCount = 1, long timeoutMs = 120000) {
        long deadline = System.currentTimeMillis() + timeoutMs
        List<Map> rows = []
        while (System.currentTimeMillis() < deadline) {
            rows = showWarmupJobs(sqlRunner).findAll {
                it.srcCluster == srcCluster && it.dstCluster == dstCluster
            }
            if (rows.size() >= minCount) {
                return rows
            }
            Thread.sleep(1000)
        }
        return rows
    }

    static List<String> waitForVcgWarmupRecreated(Closure sqlRunner, String srcCluster, String dstCluster,
                                                  Collection<String> oldJobIds, int minNewJobs = 1,
                                                  long timeoutMs = 120000) {
        long deadline = System.currentTimeMillis() + timeoutMs
        List<Map> newRows = []
        while (System.currentTimeMillis() < deadline) {
            newRows = getVcgWarmupJobs(sqlRunner, srcCluster, dstCluster).findAll {
                !oldJobIds.contains(it.jobId)
            }
            if (newRows.size() >= minNewJobs && hasPeriodicAndEventDrivenWarmup(newRows)) {
                return newRows.collect { it.jobId }
            }
            Thread.sleep(1000)
        }
        throw new RuntimeException("expected active VCG cluster warmup jobs recreated for "
                + "${srcCluster}->${dstCluster}, oldJobIds=${oldJobIds}, lastRows=${newRows}")
    }

    static void assertHistoricalJobsCancelled(Closure sqlRunner, Collection<String> jobIds,
                                              long timeoutMs = 120000) {
        assertAffectedJobsCancelled(sqlRunner, jobIds.collect { it.toString() }, timeoutMs)
    }

    static Map waitForWarmupStatsResume(Closure sqlRunner, Object jobId, Closure<Boolean> predicate,
                                        long timeoutMs = 120000) {
        return waitForJobSyncStats(sqlRunner, jobId, predicate, timeoutMs)
    }

    static void waitForJobStatus(Closure sqlRunner, Object jobId, Collection<String> expectedStatuses,
                                 long timeoutMs = 60000) {
        long deadline = System.currentTimeMillis() + timeoutMs
        String lastStatus = null
        while (System.currentTimeMillis() < deadline) {
            def row = showWarmupJob(sqlRunner, jobId)
            lastStatus = row.status
            if (expectedStatuses.contains(lastStatus)) {
                return
            }
            Thread.sleep(1000)
        }
        throw new RuntimeException("warmup job ${jobId} status ${lastStatus} not in ${expectedStatuses}")
    }
}
