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

suite('test_vcg_warmup_with_manual_once_queue_semantics', 'docker') {
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
        def vcgSrc = "vcg_queue_src"
        def sharedDst = "vcg_queue_dst"
        def manualSrc = "vcg_queue_manual_src"
        def vcgName = "vcgWarmupManualOnceQueue"
        def vcgId = "vcgWarmupManualOnceQueueId"
        def createdJobIds = []

        cluster.addBackend(1, vcgSrc)
        cluster.addBackend(1, sharedDst)
        cluster.addBackend(1, manualSrc)

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
                        cluster_names  : [vcgSrc, sharedDst],
                        cluster_policy : [type: "ActiveStandby", active_cluster_name: vcgSrc,
                                          standby_cluster_names: [sharedDst]]
                ]
        ])

        try {
            addClusterApi(vcgBody) { respCode, body ->
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            def autoRows = WarmupMetricsUtils.waitForWarmupJobsByPair(sqlRunner, vcgSrc, sharedDst, 2, 120000)
            createdJobIds.addAll(autoRows*.jobId)
            assertTrue(autoRows.any { it.syncMode.startsWith("PERIODIC") })
            assertTrue(autoRows.any { it.syncMode.startsWith("EVENT_DRIVEN") })

            def manualOnceId = sql("""WARM UP CLUSTER ${sharedDst} WITH CLUSTER ${manualSrc}""")[0][0]
            createdJobIds << manualOnceId.toString()

            def rowsByDst = WarmupMetricsUtils.showWarmupJobsByDst(sqlRunner, sharedDst)
            assertTrue(rowsByDst*.jobId.contains(manualOnceId.toString()))

            WarmupMetricsUtils.waitForOnlyOneRunningNormalWarmup(sqlRunner, sharedDst, 30000)
            long runningNormal = WarmupMetricsUtils.countRunningNormalWarmupByDst(sqlRunner, sharedDst)
            assertTrue(runningNormal <= 1,
                    "vcg auto periodic plus manual once should still serialize normal warmup on same dst, running=${runningNormal}")

            def manualOnceRow = WarmupMetricsUtils.showWarmupJob(sqlRunner, manualOnceId)
            assertTrue(WarmupMetricsUtils.isNormalWarmupJob(manualOnceRow))
            assertEquals(sharedDst, manualOnceRow.dstCluster)

            def eventRows = rowsByDst.findAll { it.syncMode.startsWith("EVENT_DRIVEN") }
            assertTrue(eventRows.size() >= 1)
            assertTrue(eventRows.every { it.status in ["RUNNING", "PENDING"] })
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
