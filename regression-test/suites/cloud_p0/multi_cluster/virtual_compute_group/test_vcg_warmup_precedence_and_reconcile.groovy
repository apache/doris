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

suite('test_vcg_warmup_precedence_and_reconcile', 'docker') {
    def options = new ClusterOptions()
    options.feNum = 3
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'fetch_cluster_cache_hotspot_interval_ms=1000',
        'cloud_warm_up_table_filter_refresh_interval_ms=1000',
    ]
    options.cloudMode = true

    docker(options) {
        Closure sqlRunner = { String q -> sql(q) }

        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        def instanceId = "default_instance_id"
        def srcCluster = "vcg_prec_src"
        def dstCluster = "vcg_prec_dst"
        def extraCluster = "vcg_prec_extra"
        def vcgName = "vcgWarmupPrecedence"
        def vcgId = "vcgWarmupPrecedenceId"
        def dbName = "test_vcg_precedence_db"
        def tableName = "vcg_tbl"

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster)
        cluster.addBackend(1, extraCluster)

        def addClusterApi = { requestBody, Closure checkFunc ->
            httpTest {
                endpoint msHttpPort
                uri "/MetaService/http/add_cluster?token=$token"
                body requestBody
                check checkFunc
            }
        }

        def vcgBody = {
            def clusterPolicy = [type: "ActiveStandby", active_cluster_name: srcCluster, standby_cluster_names: [dstCluster]]
            def clusterMap = [cluster_name: vcgName, cluster_id: vcgId, type: "VIRTUAL",
                              cluster_names: [srcCluster, dstCluster], cluster_policy: clusterPolicy]
            return JsonOutput.toJson([instance_id: instanceId, cluster: clusterMap])
        }

        def knownJobIds = []
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

            def tableFirstJobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (INCLUDE '${dbName}.${tableName}')
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)[0][0]
            knownJobIds << tableFirstJobId.toString()

            addClusterApi(vcgBody()) { respCode, body ->
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            WarmupMetricsUtils.assertHistoricalJobsCancelled(sqlRunner, [tableFirstJobId.toString()], 120000)
            def cancelledTableJob = WarmupMetricsUtils.showWarmupJob(sqlRunner, tableFirstJobId)
            assertTrue(cancelledTableJob.errMsg.contains(
                    "vcg cancel table-level load-event warm up job before rebuilding file cache jobs"))
            def newAutoJobs = WarmupMetricsUtils.waitForVcgWarmupRecreated(sqlRunner, srcCluster, dstCluster,
                    [tableFirstJobId.toString()], 2, 120000)
            assertTrue(newAutoJobs.size() >= 2,
                    "vcg should recreate auto warmup jobs after table-level cancellation, newJobs=${newAutoJobs}")
            knownJobIds.addAll(newAutoJobs)
            def autoRows = newAutoJobs.collect { WarmupMetricsUtils.showWarmupJob(sqlRunner, it) }
            assertTrue(autoRows.any { it.syncMode.startsWith("EVENT_DRIVEN") && it.type == "CLUSTER" })
            assertTrue(autoRows.any { it.syncMode.startsWith("PERIODIC") && it.type == "CLUSTER" })

            String clusterConflict = WarmupMetricsUtils.expectCreateConflict(sqlRunner, {
                sql("""
                    WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                    PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
                """)
            })
            WarmupMetricsUtils.assertConflictMessage(clusterConflict, ["conflicting", srcCluster, dstCluster])

            String tableConflict = WarmupMetricsUtils.expectCreateConflict(sqlRunner, {
                sql("""
                    WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                    ON TABLES (INCLUDE '${dbName}.${tableName}')
                    PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
                """)
            })
            WarmupMetricsUtils.assertConflictMessage(tableConflict, ["conflicting", srcCluster, dstCluster])

            def secondBody = JsonOutput.toJson([
                    instance_id: instanceId,
                    cluster: [
                            cluster_name   : "vcgWarmupPrecedenceShared",
                            cluster_id     : "vcgWarmupPrecedenceSharedId",
                            type           : "VIRTUAL",
                            cluster_names  : [srcCluster, extraCluster],
                            cluster_policy : [type: "ActiveStandby", active_cluster_name: srcCluster,
                                              standby_cluster_names: [extraCluster]]
                    ]
            ])
            addClusterApi(secondBody) { respCode, body ->
                def json = parseJson(body)
                assertTrue(!json.code.equalsIgnoreCase("OK"))
            }
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
