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

suite('test_warm_up_cluster_periodic_scheduler_semantics', 'docker') {
    def options = new ClusterOptions()
    options.feNum = 3
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'fetch_cluster_cache_hotspot_interval_ms=1000',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
    ]
    options.cloudMode = true

    docker(options) {
        Closure sqlRunner = { String q -> sql(q) }
        def srcCluster = "warmup_periodic_src"
        def dstCluster = "warmup_periodic_dst"
        def dbName = "test_warmup_periodic_scheduler_db"
        def tableName = "periodic_tbl"

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster)

        def restartMasterFe = {
            def oldMasterFe = cluster.getMasterFe()
            cluster.restartFrontends(oldMasterFe.index)
            boolean restarted = false
            for (int i = 0; i < 30; i++) {
                if (cluster.getFeByIndex(oldMasterFe.index).alive) {
                    restarted = true
                    break
                }
                sleep(1000)
            }
            assertTrue(restarted)
            context.reconnectFe()
        }

        def getPeriodicWarmupMetrics = {
            [
                    submitted: WarmupMetricsUtils.getClusterMetricSum(sqlRunner, dstCluster,
                            "file_cache_once_or_periodic_warm_up_submitted_segment_num"),
                    finished : WarmupMetricsUtils.getClusterMetricSum(sqlRunner, dstCluster,
                            "file_cache_once_or_periodic_warm_up_finished_segment_num"),
            ]
        }

        def waitForPeriodicWarmupFinish = { Map before, long timeoutMs ->
            long deadline = System.currentTimeMillis() + timeoutMs
            Map latest = getPeriodicWarmupMetrics()
            while (System.currentTimeMillis() < deadline) {
                latest = getPeriodicWarmupMetrics()
                if (latest.submitted > before.submitted && latest.finished > before.finished
                        && latest.finished >= latest.submitted) {
                    return latest
                }
                sleep(2000)
            }
            logger.warn("periodic warmup metrics did not advance after ${timeoutMs}ms, "
                    + "before=${before}, latest=${latest}")
            return latest
        }

        sql """use @${srcCluster}"""
        sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
        sql """use ${dbName}"""
        sql """CREATE TABLE IF NOT EXISTS ${tableName} (
                   id INT,
                   value STRING
               )
               DUPLICATE KEY(id)
               DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
        for (int i = 0; i < 12; i++) {
            sql """INSERT INTO ${tableName} VALUES (${i}, 'before_${i}')"""
        }

        def periodicJobId = sql("""
            WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
            PROPERTIES (
                "sync_mode" = "periodic",
                "sync_interval_sec" = "1"
            )
        """)[0][0]

        def cycles = WarmupMetricsUtils.waitForPeriodicCycles(sqlRunner, periodicJobId, 1, 120000)
        assertTrue(cycles.states.contains("RUNNING"))
        assertTrue(cycles.states.any { it in ["PENDING", "WAITING"] })

        def timelineBeforeRestart = WarmupMetricsUtils.sampleJobTimeline(sqlRunner, periodicJobId, 5000, 1000)
        assertTrue(timelineBeforeRestart*.status.any { it in ["RUNNING", "PENDING", "WAITING"] })

        restartMasterFe()
        sql """use @${srcCluster}"""
        sql """use ${dbName}"""

        def timelineAfterRestart = WarmupMetricsUtils.sampleJobTimeline(sqlRunner, periodicJobId, 8000, 1000)
        assertTrue(timelineAfterRestart*.status.any { it in ["RUNNING", "PENDING", "WAITING"] },
                "periodic job should continue after FE restart, timeline=${timelineAfterRestart}")

        def beforeMetrics = getPeriodicWarmupMetrics()
        for (int i = 12; i < 16; i++) {
            sql """INSERT INTO ${tableName} VALUES (${i}, 'after_restart_${i}')"""
        }
        for (int i = 0; i < 1000; i++) {
            sql """SELECT * FROM ${tableName}"""
        }
        def afterMetrics = waitForPeriodicWarmupFinish(beforeMetrics, 120000)
        assertTrue(afterMetrics.finished > beforeMetrics.finished,
                "periodic job should trigger another round after restart, before=${beforeMetrics}, after=${afterMetrics}")

        sql """CANCEL WARM UP JOB WHERE ID = ${periodicJobId}"""
        def cancelledRow = WarmupMetricsUtils.showWarmupJob(sqlRunner, periodicJobId)
        assertEquals("CANCELLED", cancelledRow.status)
    }
}
