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
import org.apache.doris.regression.util.NodeType
import org.apache.doris.regression.util.WarmupMetricsUtils

// Test point covered: ST-04.
suite('test_warm_up_event_on_tables_system_compaction_sync_wait', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_warm_up_table_filter_refresh_interval_ms=1000',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
        'warm_up_rowset_slow_log_ms=1',
        'enable_compaction_delay_commit_for_warm_up=true',
        'warm_up_rowset_sync_wait_min_timeout_ms=20000',
        'warm_up_rowset_sync_wait_max_timeout_ms=30000',
    ]
    options.enableDebugPoints()
    options.cloudMode = true

    def waitForMetricAtLeast = { ip, port, metricName, target, timeoutMs ->
        long deadline = System.currentTimeMillis() + timeoutMs
        long last = 0
        while (System.currentTimeMillis() < deadline) {
            last = WarmupMetricsUtils.getBrpcMetric(ip.toString(), port.toString(), metricName)
            if (last >= target) {
                return last
            }
            sleep(500)
        }
        assert false : "metric ${metricName} on ${ip}:${port} did not reach ${target}, last=${last}"
    }

    def httpJson = { String method, String url, int readTimeoutMs = 180000 ->
        def conn = new URL(url).openConnection()
        conn.setRequestMethod(method)
        conn.setConnectTimeout(10000)
        conn.setReadTimeout(readTimeoutMs)
        def text = conn.responseCode >= 400 ? conn.errorStream?.text : conn.inputStream.text
        assert text != null && !text.trim().isEmpty() : "empty HTTP response from ${url}"
        return parseJson(text.trim())
    }

    def triggerCumulativeCompaction = { ip, port, tabletId ->
        def status = httpJson("POST",
                "http://${ip}:${port}/api/compaction/run?tablet_id=${tabletId}&compact_type=cumulative")
        assert status.status.toLowerCase() in ["success", "already_exist"] :
                "trigger compaction failed on ${ip}:${port}, tablet=${tabletId}, status=${status}"
        return status
    }

    def waitForCompactionFinish = { ip, port, tabletId, timeoutMs ->
        long deadline = System.currentTimeMillis() + timeoutMs
        def lastStatus = null
        while (System.currentTimeMillis() < deadline) {
            lastStatus = httpJson("GET",
                    "http://${ip}:${port}/api/compaction/run_status?tablet_id=${tabletId}", 10000)
            assert lastStatus.status.toLowerCase() == "success" :
                    "compaction failed on ${ip}:${port}, tablet=${tabletId}, status=${lastStatus}"
            if (!lastStatus.run_status) {
                return lastStatus
            }
            sleep(1000)
        }
        assert false : "compaction did not finish on ${ip}:${port}, tablet=${tabletId}, last=${lastStatus}"
    }

    docker(options) {
        Closure sqlRunner = { String q -> sql(q) }

        def srcCluster = "warmup_source"
        def dstCluster = "warmup_target"
        def dbName = "test_on_tables_system_compaction_db"
        def jobIds = []
        def debugEnabled = false
        def targetBe = null
        def sourceBe = null
        def compactionFuture = null
        def loadCount = 8

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster)

        try {
            sql """use @${srcCluster}"""
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS compact_tbl (
                       id INT NOT NULL,
                       payload STRING
                   )
                   UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES (
                       "file_cache_ttl_seconds" = "3600",
                       "disable_auto_compaction" = "true"
                   )"""

            def jobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (INCLUDE '${dbName}.compact_tbl')
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)[0][0]
            jobIds << jobId
            assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, jobId,
                    ["${dbName}.compact_tbl".toString()] as Set) ==
                    ["${dbName}.compact_tbl".toString()] as Set

            WarmupMetricsUtils.clearFileCacheOnAllBackends(sqlRunner)
            def baseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)

            sql """use @${srcCluster}"""
            sql """use ${dbName}"""
            for (int i = 0; i < loadCount; i++) {
                sql """INSERT INTO compact_tbl VALUES (${i}, 'row_${i}')"""
            }
            def metrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    baseMetrics.finished + loadCount, 90000)
            assert metrics.failed == baseMetrics.failed : "initial rowset warmup should not fail, metrics=${metrics}"
            sleep(15000)

            def tablets = sql_return_maparray """SHOW TABLETS FROM compact_tbl"""
            assert tablets.size() == 1 : "compact_tbl should have one tablet, tablets=${tablets}"
            def tabletId = tablets[0].TabletId.toString()
            sourceBe = WarmupMetricsUtils.getClusterBackends(sqlRunner, srcCluster)[0]
            targetBe = WarmupMetricsUtils.getClusterBackends(sqlRunner, dstCluster)[0]
            def beforeSubmitted = WarmupMetricsUtils.getBrpcMetric(targetBe[1].toString(), targetBe[5].toString(),
                    WarmupMetricsUtils.METRIC_SUBMITTED)
            def beforeFinished = WarmupMetricsUtils.getBrpcMetric(targetBe[1].toString(), targetBe[5].toString(),
                    WarmupMetricsUtils.METRIC_FINISHED)
            def beforeWaitCompaction = WarmupMetricsUtils.getBrpcMetric(targetBe[1].toString(), targetBe[5].toString(),
                    "file_cache_warm_up_rowset_wait_for_compaction_num")
            def beforeWaitTimeout = WarmupMetricsUtils.getBrpcMetric(targetBe[1].toString(), targetBe[5].toString(),
                    "file_cache_warm_up_rowset_wait_for_compaction_timeout_num")

            GetDebugPoint().enableDebugPoint(targetBe[1].toString(), targetBe[4] as int, NodeType.BE,
                    "S3FileReader::read_at_impl.io_slow", [sleep: 10])
            debugEnabled = true

            compactionFuture = thread {
                sql """use @${srcCluster}"""
                sql """use ${dbName}"""
                triggerCumulativeCompaction(sourceBe[1].toString(), sourceBe[4].toString(), tabletId)
                waitForCompactionFinish(sourceBe[1].toString(), sourceBe[4].toString(), tabletId, 90000)
            }

            waitForMetricAtLeast(targetBe[1], targetBe[5],
                    "file_cache_warm_up_rowset_wait_for_compaction_num", beforeWaitCompaction + 1, 60000)
            assert WarmupMetricsUtils.getBrpcMetric(targetBe[1].toString(), targetBe[5].toString(),
                    WarmupMetricsUtils.METRIC_SUBMITTED) >= beforeSubmitted + 1 :
                    "compaction rowset should submit one more target warmup"
            assert WarmupMetricsUtils.getBrpcMetric(targetBe[1].toString(), targetBe[5].toString(),
                    WarmupMetricsUtils.METRIC_FINISHED) >= beforeFinished :
                    "finished warmup metric should not regress"

            sql """use @${dstCluster}"""
            sql """use ${dbName}"""
            assert sql("""SELECT count(*) FROM compact_tbl""")[0][0].toString() == loadCount.toString()

            compactionFuture.get()
            assert WarmupMetricsUtils.getBrpcMetric(targetBe[1].toString(), targetBe[5].toString(),
                    WarmupMetricsUtils.METRIC_FINISHED) >= beforeFinished + 1 :
                    "compaction rowset warmup should finish after sync wait"
            assert WarmupMetricsUtils.getBrpcMetric(targetBe[1].toString(), targetBe[5].toString(),
                    "file_cache_warm_up_rowset_wait_for_compaction_timeout_num") == beforeWaitTimeout :
                    "compaction sync wait should not time out"

            sql """use @${srcCluster}"""
            sql """use ${dbName}"""
            sql """INSERT INTO compact_tbl VALUES (${loadCount + 1}, 'after_compaction')"""
            WarmupMetricsUtils.waitForMetricsStable(sqlRunner, srcCluster, dstCluster, 30000)
            sql """use @${dstCluster}"""
            sql """use ${dbName}"""
            assert sql("""SELECT count(*) FROM compact_tbl""")[0][0].toString() == (loadCount + 1).toString()

            def stats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, jobId,
                    { it.seg_num.finish_5m >= loadCount + 2 && it.seg_num.fail_5m == 0 && it.seg_num.gap_5m == 0 },
                    60000)
            assert stats.seg_num.gap_5m == 0 : "compaction warmup should converge, stats=${stats}"
        } finally {
            if (debugEnabled) {
                try { GetDebugPoint().clearDebugPointsForAllBEs() } catch (Exception ignored) {}
            }
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use @${srcCluster}"""
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS compact_tbl"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbName}""" } catch (Exception ignored) {}
        }
    }
}
