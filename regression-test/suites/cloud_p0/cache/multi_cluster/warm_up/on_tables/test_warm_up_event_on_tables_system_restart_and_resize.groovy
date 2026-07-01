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

// Test points covered: ST-07, ST-08.
suite('test_warm_up_event_on_tables_system_restart_and_resize', 'docker') {
    def options = new ClusterOptions()
    options.feNum = 3
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
        'cloud_warm_up_table_filter_refresh_interval_ms=1000',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
    ]
    options.cloudMode = true

    def restartMasterFe = {
        def oldMasterFe = cluster.getMasterFe()
        cluster.restartFrontends(oldMasterFe.index)
        boolean hasRestart = false
        for (int i = 0; i < 30; i++) {
            if (cluster.getFeByIndex(oldMasterFe.index).alive) {
                hasRestart = true
                break
            }
            sleep(1000)
        }
        assert hasRestart : "master FE did not restart"
        context.reconnectFe()
    }

    docker(options) {
        Closure sqlRunner = { String q -> sql(q) }

        def srcCluster = "warmup_source"
        def dstCluster = "warmup_target"
        def dbName = "test_on_tables_system_restart_resize_db"
        def jobIds = []

        def srcBeIndexes = cluster.addBackend(1, srcCluster)
        def dstBeIndexes = cluster.addBackend(1, dstCluster)

        def assertWarmupReached = { Map metrics, long expectedFinished, String phase ->
            assert metrics.finished >= expectedFinished :
                    "${phase}: expected finished >= ${expectedFinished}, metrics=${metrics}"
            assert metrics.finished + metrics.failed >= metrics.submitted :
                    "${phase}: submitted warmup tasks should be terminal, metrics=${metrics}"
        }

        def aliveFrontends = { String phase ->
            def fes = []
            for (int i = 0; i < 30; i++) {
                fes = cluster.getAllFrontends(true)
                if (fes.size() == options.feNum) {
                    return fes
                }
                sleep(1000)
            }
            assert false : "${phase}: expected ${options.feNum} alive FEs, got ${fes}"
        }

        def assertShowWarmupOnAllFes = { Object jobId, Set<String> expectedTables, String phase ->
            for (fe in aliveFrontends(phase)) {
                def feLabel = "fe-${fe.index}"
                def jdbcUrl = String.format(
                        "jdbc:mysql://%s:%s/?useLocalSessionState=true&allowLoadLocalInfile=false",
                        fe.host, fe.queryPort)
                connect(context.config.jdbcUser, context.config.jdbcPassword, jdbcUrl) {
                    def rows = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
                    assert rows.size() == 1 : "${phase}: ${feLabel} should show one warmup job, rows=${rows}"
                    def row = rows[0]
                    assert row[0].toString() == jobId.toString() :
                            "${phase}: ${feLabel} job id mismatch, row=${row}"
                    assert row[1].toString() == srcCluster :
                            "${phase}: ${feLabel} source cluster mismatch, row=${row}"
                    assert row[2].toString() == dstCluster :
                            "${phase}: ${feLabel} target cluster mismatch, row=${row}"
                    assert row[3] in ["RUNNING", "PENDING"] :
                            "${phase}: ${feLabel} job should be running or pending, row=${row}"
                    assert row[4].toString() == "TABLES" :
                            "${phase}: ${feLabel} job type mismatch, row=${row}"
                    assert row[5].toString().startsWith("EVENT_DRIVEN") :
                            "${phase}: ${feLabel} sync mode mismatch, row=${row}"
                    def matched = WarmupMetricsUtils.parseMatchedTables(rows)
                    assert matched.containsAll(expectedTables) :
                            "${phase}: ${feLabel} matched tables mismatch, expected=${expectedTables}, matched=${matched}"
                    logger.info("${phase}: SHOW WARM UP JOB on ${feLabel}(${fe.host}:${fe.queryPort}) row=${row}")
                }
            }
        }

        try {
            sql """use @${srcCluster}"""
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS ha_tbl (
                       id INT,
                       val STRING
                   )
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            def jobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (INCLUDE '${dbName}.*')
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)[0][0]
            jobIds << jobId
            assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, jobId,
                    ["${dbName}.ha_tbl".toString()] as Set).contains("${dbName}.ha_tbl".toString())
            assertShowWarmupOnAllFes(jobId, ["${dbName}.ha_tbl".toString()] as Set,
                    "after creating table-level warmup job")

            restartMasterFe()
            sql """use @${srcCluster}"""
            sql """use ${dbName}"""

            WarmupMetricsUtils.clearFileCacheOnAllBackends(sqlRunner)
            def baseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            for (int i = 0; i < 4; i++) {
                sql """INSERT INTO ha_tbl VALUES (${i}, 'before_restart_${i}')"""
            }
            def afterFeRestart = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    baseMetrics.finished + 4, 90000)
            assertWarmupReached(afterFeRestart, baseMetrics.finished + 4, "after master FE restart")
            assert afterFeRestart.failed == baseMetrics.failed :
                    "warmup should continue after master FE restart, metrics=${afterFeRestart}"
            assertShowWarmupOnAllFes(jobId, ["${dbName}.ha_tbl".toString()] as Set,
                    "after master FE restart")

            cluster.restartBackends(dstBeIndexes[0] as int)
            sleep(5000)
            sql """use @${srcCluster}"""
            sql """use ${dbName}"""
            def beforeTargetRestartLoad = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            for (int i = 4; i < 8; i++) {
                sql """INSERT INTO ha_tbl VALUES (${i}, 'after_target_restart_${i}')"""
            }
            def afterTargetRestart = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    beforeTargetRestartLoad.finished + 4, 90000)
            assertWarmupReached(afterTargetRestart, beforeTargetRestartLoad.finished + 4,
                    "after target BE restart")
            assert afterTargetRestart.failed == beforeTargetRestartLoad.failed :
                    "warmup should continue after target BE restart, metrics=${afterTargetRestart}"

            def targetBeforeScale = WarmupMetricsUtils.getClusterBackends(sqlRunner, dstCluster)
                    .collect { it[0].toString() } as Set
            cluster.addBackend(1, dstCluster)
            sleep(5000)
            def targetAfterScale = WarmupMetricsUtils.getClusterBackends(sqlRunner, dstCluster)
            def newTargetBes = targetAfterScale.findAll { !targetBeforeScale.contains(it[0].toString()) }
            assert newTargetBes.size() == 1 : "expected one new target BE, before=${targetBeforeScale}, after=${targetAfterScale}"
            def newTargetBe = newTargetBes[0]

            sql """use @${srcCluster}"""
            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS scale_tbl (
                       id INT,
                       val STRING
                   )
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, jobId,
                    ["${dbName}.ha_tbl".toString(), "${dbName}.scale_tbl".toString()] as Set)
                    .contains("${dbName}.scale_tbl".toString())
            assertShowWarmupOnAllFes(jobId,
                    ["${dbName}.ha_tbl".toString(), "${dbName}.scale_tbl".toString()] as Set,
                    "after target scale-out table match")

            def newBeFinishedBefore = WarmupMetricsUtils.getBrpcMetric(newTargetBe[1].toString(),
                    newTargetBe[5].toString(), WarmupMetricsUtils.METRIC_FINISHED)
            def beforeScaleLoad = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            for (int i = 0; i < 8; i++) {
                sql """INSERT INTO scale_tbl VALUES (${i}, 'scale_${i}')"""
            }
            def afterScaleLoad = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    beforeScaleLoad.finished + 8, 90000)
            assertWarmupReached(afterScaleLoad, beforeScaleLoad.finished + 8, "after target scale-out")
            assert afterScaleLoad.failed == beforeScaleLoad.failed :
                    "warmup should continue after target scale-out, metrics=${afterScaleLoad}"
            def newBeFinishedAfter = WarmupMetricsUtils.getBrpcMetric(newTargetBe[1].toString(),
                    newTargetBe[5].toString(), WarmupMetricsUtils.METRIC_FINISHED)
            assert newBeFinishedAfter > newBeFinishedBefore :
                    "new target BE should participate in later table-level warmup"

            sql """use @${dstCluster}"""
            sql """use ${dbName}"""
            assert sql("""SELECT count(*) FROM ha_tbl""")[0][0].toString() == "8"
            assert sql("""SELECT count(*) FROM scale_tbl""")[0][0].toString() == "8"
        } finally {
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use @${srcCluster}"""
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS ha_tbl"""
                sql """DROP TABLE IF EXISTS scale_tbl"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbName}""" } catch (Exception ignored) {}
        }
    }
}
