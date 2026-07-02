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

suite('test_vcg_warmup_switch_and_drop_cancels_history', 'docker') {
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
        def srcCluster = "vcg_switch_src"
        def dstCluster = "vcg_switch_dst"
        def vcgName = "vcgWarmupSwitchHistory"
        def vcgId = "vcgWarmupSwitchHistoryId"

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

        def alterClusterInfoApi = { requestBody, Closure checkFunc ->
            httpTest {
                endpoint msHttpPort
                uri "/MetaService/http/alter_vcluster_info?token=$token"
                body requestBody
                check checkFunc
            }
        }

        def buildVcgBody = { String active, String standby ->
            def clusterPolicy = [type: "ActiveStandby", active_cluster_name: active,
                                 standby_cluster_names: [standby]]
            def clusterMap = [cluster_name: vcgName, cluster_id: vcgId, type: "VIRTUAL",
                              cluster_names: [srcCluster, dstCluster], cluster_policy: clusterPolicy]
            return JsonOutput.toJson([instance_id: instanceId, cluster: clusterMap])
        }

        try {
            addClusterApi(buildVcgBody(srcCluster, dstCluster)) { respCode, body ->
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            def initialRows = WarmupMetricsUtils.waitForWarmupJobsByPair(sqlRunner, srcCluster, dstCluster, 2, 120000)
            def initialJobIds = initialRows*.jobId
            assertTrue(initialRows.any { it.type == "CLUSTER" && it.syncMode.startsWith("PERIODIC") })
            assertTrue(initialRows.any { it.type == "CLUSTER" && it.syncMode.startsWith("EVENT_DRIVEN") })

            alterClusterInfoApi(buildVcgBody(dstCluster, srcCluster)) { respCode, body ->
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            WarmupMetricsUtils.assertHistoricalJobsCancelled(sqlRunner, initialJobIds, 120000)
            def switchedJobIds = WarmupMetricsUtils.waitForVcgWarmupRecreated(sqlRunner, dstCluster, srcCluster,
                    initialJobIds, 2, 120000)
            assertTrue(switchedJobIds.size() >= 2)
            def switchedRows = switchedJobIds.collect { WarmupMetricsUtils.showWarmupJob(sqlRunner, it) }
            assertTrue(switchedRows.any { it.type == "CLUSTER" && it.syncMode.startsWith("PERIODIC") })
            assertTrue(switchedRows.any { it.type == "CLUSTER" && it.syncMode.startsWith("EVENT_DRIVEN") })

            drop_cluster(vcgName, vcgId, ms)
            WarmupMetricsUtils.assertHistoricalJobsCancelled(sqlRunner, switchedJobIds, 120000)
        } finally {
            WarmupMetricsUtils.showWarmupJobs(sqlRunner).findAll {
                it.jobId != null && (
                        (it.srcCluster == srcCluster && it.dstCluster == dstCluster)
                                || (it.srcCluster == dstCluster && it.dstCluster == srcCluster)
                )
            }.each { row ->
                try {
                    sql """CANCEL WARM UP JOB WHERE ID = ${row.jobId}"""
                } catch (Throwable ignored) {
                }
            }
        }
    }
}
