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

suite('test_warm_up_normal_queue_semantics', 'docker') {
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

        def srcCluster = "warmup_queue_src"
        def dstCluster = "warmup_queue_dst"
        def auxCluster = "warmup_queue_aux"
        def dbName = "test_warmup_queue_semantics_db"
        def tableName = "queue_tbl"

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster)
        cluster.addBackend(1, auxCluster)

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

        sql """use @${srcCluster}"""
        sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
        sql """use ${dbName}"""
        sql """DROP TABLE IF EXISTS ${tableName}"""
        sql """CREATE TABLE ${tableName} (
                   id INT,
                   name STRING
               )
               DUPLICATE KEY(id)
               DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
        for (int i = 0; i < 20; i++) {
            sql """INSERT INTO ${tableName} VALUES (${i}, 'name_${i}')"""
        }

        def periodicJobId = sql("""
            WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
            PROPERTIES (
                "sync_mode" = "periodic",
                "sync_interval_sec" = "1"
            )
        """)[0][0]
        def onceTableJobId = sql("""WARM UP CLUSTER ${dstCluster} WITH TABLE ${tableName}""")[0][0]
        def onceClusterJobId = sql("""WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${auxCluster}""")[0][0]
        def eventJobId = sql("""
            WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
            ON TABLES (INCLUDE '${dbName}.${tableName}')
            PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
        """)[0][0]

        def queueRows = WarmupMetricsUtils.showWarmupJobsByDst(sqlRunner, dstCluster)
        assert queueRows.find { it.jobId == periodicJobId.toString() } != null
        assert queueRows.find { it.jobId == onceTableJobId.toString() } != null
        assert queueRows.find { it.jobId == onceClusterJobId.toString() } != null
        assert queueRows.find { it.jobId == eventJobId.toString() } != null

        def normalJobIds = [periodicJobId, onceTableJobId, onceClusterJobId].collect { it.toString() }
        WarmupMetricsUtils.waitForOnlyOneRunningNormalWarmup(sqlRunner, dstCluster, 30000, normalJobIds)
        long runningNormal = WarmupMetricsUtils.countRunningNormalWarmupByDst(sqlRunner, dstCluster)
        assertTrue(runningNormal <= 1,
                "same dst cluster should have at most one running normal warmup, got ${runningNormal}")

        def eventRow = WarmupMetricsUtils.showWarmupJob(sqlRunner, eventJobId)
        assertTrue(WarmupMetricsUtils.isEventDrivenWarmupJob(eventRow))
        assertTrue(eventRow.status in ["RUNNING", "PENDING", "WAITING"])

        Map<String, Map> beforeRestartSnapshot = WarmupMetricsUtils.snapshotWarmupJobsById(sqlRunner)
        restartMasterFe()
        sql """use @${srcCluster}"""
        sql """use ${dbName}"""
        Map<String, Map> afterRestartSnapshot = WarmupMetricsUtils.waitForWarmupJobsRecovered(
                sqlRunner, beforeRestartSnapshot, { before, current ->
            [periodicJobId, onceTableJobId, onceClusterJobId, eventJobId].every { current.containsKey(it.toString()) }
        }, 60000)
        assertTrue(afterRestartSnapshot.keySet().containsAll(
                [periodicJobId, onceTableJobId, onceClusterJobId, eventJobId].collect { it.toString() }))

        WarmupMetricsUtils.waitForOnlyOneRunningNormalWarmup(sqlRunner, dstCluster, 30000, normalJobIds)
        runningNormal = WarmupMetricsUtils.countRunningNormalWarmupByDst(sqlRunner, dstCluster)
        assertTrue(runningNormal <= 1,
                "after FE restart same dst cluster should still have at most one running normal warmup, "
                        + "got ${runningNormal}")

        def beforeMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
        for (int i = 20; i < 24; i++) {
            sql """INSERT INTO ${tableName} VALUES (${i}, 'after_restart_${i}')"""
        }
        def afterMetrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                beforeMetrics.finished + 1, 120000)
        assertTrue(afterMetrics.finished >= beforeMetrics.finished + 1,
                "event-driven warmup should finish after FE restart, before=${beforeMetrics}, after=${afterMetrics}")
        assertTrue(afterMetrics.finished + afterMetrics.failed >= afterMetrics.submitted,
                "event-driven warmup should drain submitted segments, after=${afterMetrics}")
        assertTrue(afterMetrics.requested > beforeMetrics.requested,
                "event-driven warmup should continue after FE restart, before=${beforeMetrics}, after=${afterMetrics}")

        [onceTableJobId, onceClusterJobId, periodicJobId, eventJobId].each { jid ->
            try {
                sql """CANCEL WARM UP JOB WHERE ID = ${jid}"""
            } catch (Throwable ignored) {
            }
        }
    }
}
