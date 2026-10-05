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

suite('test_vcg_warmup_waits_for_existing_cluster_jobs', 'docker') {
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
        def srcCluster = "vcg_wait_src"
        def dstCluster = "vcg_wait_dst"
        def vcgName = "vcgWarmupWaitsExisting"
        def vcgId = "vcgWarmupWaitsExistingId"
        def allKnownJobIds = []

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

        def vcgBody = {
            def clusterPolicy = [type: "ActiveStandby", active_cluster_name: srcCluster,
                                 standby_cluster_names: [dstCluster]]
            def clusterMap = [cluster_name: vcgName, cluster_id: vcgId, type: "VIRTUAL",
                              cluster_names: [srcCluster, dstCluster], cluster_policy: clusterPolicy]
            return JsonOutput.toJson([instance_id: instanceId, cluster: clusterMap])
        }

        try {
            def periodicJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                PROPERTIES ("sync_mode" = "periodic", "sync_interval_sec" = "600")
            """)[0][0]
            def clusterEventJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)[0][0]
            allKnownJobIds.addAll([periodicJobId.toString(), clusterEventJobId.toString()])

            addClusterApi(vcgBody()) { respCode, body ->
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            sleep(8000)
            def rowsBeforeCancel = WarmupMetricsUtils.waitForWarmupJobsByPair(sqlRunner, srcCluster, dstCluster, 2, 30000)
            assertEquals(2, rowsBeforeCancel.size())
            assertTrue(rowsBeforeCancel*.jobId.toSet() == [periodicJobId.toString(), clusterEventJobId.toString()] as Set,
                    "vcg should wait existing cluster-level jobs before creating auto jobs, rows=${rowsBeforeCancel}")

            sql """CANCEL WARM UP JOB WHERE ID = ${periodicJobId}"""
            sql """CANCEL WARM UP JOB WHERE ID = ${clusterEventJobId}"""
            WarmupMetricsUtils.assertHistoricalJobsCancelled(sqlRunner,
                    [periodicJobId.toString(), clusterEventJobId.toString()], 120000)

            def recreatedJobIds = WarmupMetricsUtils.waitForVcgWarmupRecreated(sqlRunner, srcCluster, dstCluster,
                    [periodicJobId.toString(), clusterEventJobId.toString()], 2, 120000)
            allKnownJobIds.addAll(recreatedJobIds)
            assertTrue(recreatedJobIds.size() >= 2)
            def recreatedRows = recreatedJobIds.collect { WarmupMetricsUtils.showWarmupJob(sqlRunner, it) }
            assertTrue(recreatedRows.any { it.type == "CLUSTER" && it.syncMode.startsWith("PERIODIC") })
            assertTrue(recreatedRows.any { it.type == "CLUSTER" && it.syncMode.startsWith("EVENT_DRIVEN") })
        } finally {
            allKnownJobIds.each { jobId ->
                try {
                    sql """CANCEL WARM UP JOB WHERE ID = ${jobId}"""
                } catch (Throwable ignored) {
                }
            }
        }
    }
}
