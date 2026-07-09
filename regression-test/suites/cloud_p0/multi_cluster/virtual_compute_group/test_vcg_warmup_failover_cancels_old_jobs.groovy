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

suite('test_vcg_warmup_failover_cancels_old_jobs', 'docker') {
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
        def clusterA = "vcg_failover_a"
        def clusterB = "vcg_failover_b"
        def vcgName = "vcgWarmupFailoverCancel"
        def vcgId = "vcgWarmupFailoverCancelId"

        cluster.addBackend(2, clusterA)
        cluster.addBackend(2, clusterB)

        def addClusterApi = { requestBody, Closure checkFunc ->
            httpTest {
                endpoint msHttpPort
                uri "/MetaService/http/add_cluster?token=$token"
                body requestBody
                check checkFunc
            }
        }

        def buildBody = { String active, String standby ->
            JsonOutput.toJson([
                    instance_id: instanceId,
                    cluster: [
                            cluster_name   : vcgName,
                            cluster_id     : vcgId,
                            type           : "VIRTUAL",
                            cluster_names  : [clusterA, clusterB],
                            cluster_policy : [type: "ActiveStandby", active_cluster_name: active,
                                              standby_cluster_names: [standby], failover_failure_threshold: 3]
                    ]
            ])
        }

        try {
            addClusterApi(buildBody(clusterA, clusterB)) { respCode, body ->
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            def oldRows = WarmupMetricsUtils.waitForWarmupJobsByPair(sqlRunner, clusterA, clusterB, 2, 120000)
            def oldJobIds = oldRows*.jobId
            assertTrue(oldRows.any { it.syncMode.startsWith("PERIODIC") })
            assertTrue(oldRows.any { it.syncMode.startsWith("EVENT_DRIVEN") })

            sql """USE @${vcgName}"""
            sql """DROP TABLE IF EXISTS test_vcg_warmup_failover_cancel_tbl"""
            sql """
                CREATE TABLE test_vcg_warmup_failover_cancel_tbl (
                    k1 INT
                )
                DUPLICATE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                )
            """
            sql """INSERT INTO test_vcg_warmup_failover_cancel_tbl VALUES (1)"""

            cluster.stopBackends(4, 5)
            awaitUntil(50, 3) {
                sql """USE @${vcgName}"""
                sql """SELECT COUNT(*) FROM test_vcg_warmup_failover_cancel_tbl"""
                def groups = sql_return_maparray """SHOW COMPUTE GROUPS"""
                def vcg = groups.find { it.Name == vcgName }
                vcg != null && vcg.Policy.contains("\"activeComputeGroup\":\"${clusterB}\"")
            }

            WarmupMetricsUtils.assertHistoricalJobsCancelled(sqlRunner, oldJobIds, 120000)
            def newJobIds = WarmupMetricsUtils.waitForVcgWarmupRecreated(sqlRunner, clusterB, clusterA,
                    oldJobIds, 2, 120000)
            assertTrue(newJobIds.size() >= 2)
            def newRows = newJobIds.collect { WarmupMetricsUtils.showWarmupJob(sqlRunner, it) }
            assertTrue(newRows.any { it.syncMode.startsWith("PERIODIC") })
            assertTrue(newRows.any { it.syncMode.startsWith("EVENT_DRIVEN") })
        } finally {
            cluster.startBackends(4, 5)
            WarmupMetricsUtils.showWarmupJobs(sqlRunner).findAll {
                it.jobId != null && (
                        (it.srcCluster == clusterA && it.dstCluster == clusterB)
                                || (it.srcCluster == clusterB && it.dstCluster == clusterA)
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
