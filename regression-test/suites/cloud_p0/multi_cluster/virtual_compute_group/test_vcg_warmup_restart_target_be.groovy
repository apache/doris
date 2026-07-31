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

import groovy.json.JsonOutput
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.WarmupMetricsUtils

suite('test_vcg_warmup_restart_target_be', 'docker') {
    def options = new ClusterOptions()
    options.feNum = 3
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
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

        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        def instanceId = "default_instance_id"
        def srcCluster = "vcg_be_restart_src"
        def dstCluster = "vcg_be_restart_dst"
        def vcgName = "vcgWarmupRestartBe"
        def vcgId = "vcgWarmupRestartBeId"
        def dbName = "test_vcg_warmup_restart_be_db"
        def tableName = "be_restart_tbl"
        def knownJobIds = []

        cluster.addBackend(1, srcCluster)
        def dstBeIndexes = cluster.addBackend(1, dstCluster)

        def addClusterApi = { requestBody, Closure checkFunc ->
            httpTest {
                endpoint msHttpPort
                uri "/MetaService/http/add_cluster?token=$token"
                body requestBody
                check checkFunc
            }
        }

        def vcgBody = JsonOutput.toJson([
                instance_id: instanceId,
                cluster: [
                        cluster_name   : vcgName,
                        cluster_id     : vcgId,
                        type           : "VIRTUAL",
                        cluster_names  : [srcCluster, dstCluster],
                        cluster_policy : [type: "ActiveStandby", active_cluster_name: srcCluster,
                                          standby_cluster_names: [dstCluster]]
                ]
        ])

        try {
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

            addClusterApi(vcgBody) { respCode, body ->
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            def beforeRestartRows = WarmupMetricsUtils.waitForWarmupJobsByPair(sqlRunner, srcCluster, dstCluster, 2, 120000)
            knownJobIds.addAll(beforeRestartRows*.jobId)
            assertTrue(beforeRestartRows.any { it.syncMode.startsWith("PERIODIC") })
            assertTrue(beforeRestartRows.any { it.syncMode.startsWith("EVENT_DRIVEN") })

            cluster.restartBackends(dstBeIndexes[0] as int)
            sleep(8000)
            sql """use @${srcCluster}"""
            sql """use ${dbName}"""

            def afterRestartRows = knownJobIds.collect { WarmupMetricsUtils.showWarmupJob(sqlRunner, it) }
            assertTrue(afterRestartRows.any { it.syncMode.startsWith("PERIODIC") && it.status in ["RUNNING", "PENDING", "WAITING"] })
            assertTrue(afterRestartRows.any { it.syncMode.startsWith("EVENT_DRIVEN") && it.status in ["RUNNING", "PENDING"] })

            def beforeMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
            for (int i = 0; i < 4; i++) {
                sql """INSERT INTO ${tableName} VALUES (${i}, 'after_be_restart_${i}')"""
            }
            def afterMetrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    beforeMetrics.finished + 1, 120000)
            assertTrue(afterMetrics.requested > beforeMetrics.requested,
                    "vcg auto event-driven warmup should continue after target BE restart, before=${beforeMetrics}, after=${afterMetrics}")
        } finally {
            knownJobIds.each { jobId ->
                try {
                    sql """CANCEL WARM UP JOB WHERE ID = ${jobId}"""
                } catch (Throwable ignored) {
                }
            }
            try {
                sql """use @${srcCluster}"""
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS ${tableName}"""
                sql """DROP DATABASE IF EXISTS ${dbName}"""
            } catch (Throwable ignored) {
            }
        }
    }
}
