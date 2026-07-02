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

suite('test_vcg_warmup_restart_master_fe', 'docker') {
    def options = new ClusterOptions()
    options.feNum = 3
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'fetch_cluster_cache_hotspot_interval_ms=1000',
    ]
    options.cloudMode = true

    docker(options) {
        Closure sqlRunner = { String q -> sql(q) }

        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        def instanceId = "default_instance_id"
        def srcCluster = "vcg_restart_src"
        def dstCluster = "vcg_restart_dst"
        def vcgName = "vcgWarmupRestartMasterFe"
        def vcgId = "vcgWarmupRestartMasterFeId"
        def knownJobIds = []

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster)

        def addClusterApi = { requestBody, Closure checkFunc ->
            httpTest {
                endpoint msHttpPort
                uri "/MetaService/http/add_cluster?token=$token"
                body requestBody
                check checkFunc
            }
        }

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
            addClusterApi(vcgBody) { respCode, body ->
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            def beforeRestartRows = WarmupMetricsUtils.waitForWarmupJobsByPair(sqlRunner, srcCluster, dstCluster, 2, 120000)
            knownJobIds.addAll(beforeRestartRows*.jobId)
            assertTrue(beforeRestartRows.any { it.syncMode.startsWith("PERIODIC") })
            assertTrue(beforeRestartRows.any { it.syncMode.startsWith("EVENT_DRIVEN") })

            Map<String, Map> snapshot = WarmupMetricsUtils.snapshotWarmupJobsById(sqlRunner)
            restartMasterFe()
            def afterRestart = WarmupMetricsUtils.waitForWarmupJobsRecovered(sqlRunner, snapshot, { before, current ->
                knownJobIds.every { current.containsKey(it) }
            }, 60000)
            assertTrue(afterRestart.keySet().containsAll(knownJobIds))

            def rowsAfterRestart = knownJobIds.collect { WarmupMetricsUtils.showWarmupJob(sqlRunner, it) }
            assertTrue(rowsAfterRestart.any { it.syncMode.startsWith("PERIODIC") && it.status in ["RUNNING", "PENDING", "WAITING"] })
            assertTrue(rowsAfterRestart.any { it.syncMode.startsWith("EVENT_DRIVEN") && it.status in ["RUNNING", "PENDING"] })
        } finally {
            knownJobIds.each { jobId ->
                try {
                    sql """CANCEL WARM UP JOB WHERE ID = ${jobId}"""
                } catch (Throwable ignored) {
                }
            }
        }
    }
}
