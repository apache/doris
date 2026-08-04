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

suite('test_vcg_warmup_allows_manual_periodic_jobs', 'docker') {
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
        def srcCluster = "vcg_periodic_src"
        def dstCluster = "vcg_periodic_dst"
        def extraDst = "vcg_periodic_extra"
        def vcgName = "vcgWarmupPeriodicAllowed"
        def vcgId = "vcgWarmupPeriodicAllowedId"
        def createdJobIds = []

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster)
        cluster.addBackend(1, extraDst)

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
            addClusterApi(vcgBody) { respCode, body ->
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            def autoRows = WarmupMetricsUtils.waitForWarmupJobsByPair(sqlRunner, srcCluster, dstCluster, 2, 120000)
            createdJobIds.addAll(autoRows*.jobId)
            assertTrue(autoRows.any { it.syncMode.startsWith("PERIODIC") })
            assertTrue(autoRows.any { it.syncMode.startsWith("EVENT_DRIVEN") })

            def manualPeriodicId = sql("""
                WARM UP CLUSTER ${extraDst} WITH CLUSTER ${srcCluster}
                PROPERTIES ("sync_mode" = "periodic", "sync_interval_sec" = "600")
            """)[0][0]
            createdJobIds << manualPeriodicId.toString()

            def manualPeriodicRow = WarmupMetricsUtils.showWarmupJob(sqlRunner, manualPeriodicId)
            assertEquals(srcCluster, manualPeriodicRow.srcCluster)
            assertEquals(extraDst, manualPeriodicRow.dstCluster)
            assertTrue(manualPeriodicRow.syncMode.startsWith("PERIODIC"))

            def originalAutoRows = WarmupMetricsUtils.waitForWarmupJobsByPair(sqlRunner, srcCluster, dstCluster, 2, 30000)
            assertTrue(originalAutoRows*.jobId.toSet().containsAll(autoRows*.jobId as Set))
        } finally {
            createdJobIds.each { jobId ->
                try {
                    sql """CANCEL WARM UP JOB WHERE ID = ${jobId}"""
                } catch (Throwable ignored) {
                }
            }
        }
    }
}
