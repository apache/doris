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

import groovy.json.JsonSlurper
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.WarmupMetricsUtils

suite('test_warm_up_mixed_cluster_change_and_failover', 'docker') {
    def options = new ClusterOptions()
    options.feNum = 3
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'fetch_cluster_cache_hotspot_interval_ms=1000',
        'cloud_warm_up_table_filter_refresh_interval_ms=1000',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
    ]
    options.cloudMode = true
    options.beNum = 1

    docker(options) {
        Closure sqlRunner = { String q -> sql(q) }
        def srcCluster = "warmup_change_src"
        def dstCluster = "warmup_change_dst"
        def unrelatedDst = "warmup_change_unrelated_dst"
        def renamedDst = "warmup_change_dst_renamed"
        def dbName = "test_warmup_mixed_cluster_change_db"
        def tableName = "change_tbl"
        def jobIds = []
        def metaService = cluster.getAllMetaservices().get(0)
        def jsonSlurper = new JsonSlurper()

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster)
        cluster.addBackend(1, unrelatedDst)

        def getClusterId = { String clusterName ->
            def tag = getCloudBeTagByName(clusterName)
            jsonSlurper.parseText(tag).compute_group_id.toString()
        }

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

        def periodicJobId = sql("""
            WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
            PROPERTIES ("sync_mode" = "periodic", "sync_interval_sec" = "1")
        """)[0][0]
        def eventJobId = sql("""
            WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
            ON TABLES (INCLUDE '${dbName}.${tableName}')
            PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
        """)[0][0]
        def unrelatedJobId = sql("""
            WARM UP CLUSTER ${unrelatedDst} WITH CLUSTER ${srcCluster}
            PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
        """)[0][0]
        jobIds.addAll([periodicJobId, eventJobId, unrelatedJobId])
        WarmupMetricsUtils.waitForJobStatus(sqlRunner, periodicJobId, ["RUNNING", "PENDING", "WAITING"], 60000)
        WarmupMetricsUtils.waitForJobStatus(sqlRunner, eventJobId, ["RUNNING", "PENDING"], 60000)
        WarmupMetricsUtils.waitForJobStatus(sqlRunner, unrelatedJobId, ["RUNNING", "PENDING"], 60000)

        def affectedBefore = WarmupMetricsUtils.collectWarmupJobsByCluster(sqlRunner, dstCluster)*.jobId
        assertTrue(affectedBefore.contains(periodicJobId.toString()))
        assertTrue(affectedBefore.contains(eventJobId.toString()))

        sql """ALTER SYSTEM RENAME COMPUTE GROUP ${dstCluster} ${renamedDst}"""
        WarmupMetricsUtils.assertAffectedJobsCancelled(sqlRunner,
                [periodicJobId.toString(), eventJobId.toString()], 120000)
        WarmupMetricsUtils.assertUnrelatedJobsUnaffected(sqlRunner, [unrelatedJobId.toString()])

        sql """ALTER SYSTEM RENAME COMPUTE GROUP ${renamedDst} ${dstCluster}"""
        def recreatedEventId = sql("""
            WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
            ON TABLES (INCLUDE '${dbName}.${tableName}')
            PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
        """)[0][0]
        jobIds << recreatedEventId

        def unrelatedClusterId = getClusterId(unrelatedDst)
        drop_cluster(unrelatedDst, unrelatedClusterId, metaService)
        WarmupMetricsUtils.assertAffectedJobsCancelled(sqlRunner, [unrelatedJobId.toString()], 120000)
        WarmupMetricsUtils.assertUnrelatedJobsUnaffected(sqlRunner, [recreatedEventId.toString()], ["RUNNING", "PENDING", "WAITING"])

        jobIds.each { jid ->
            try {
                sql """CANCEL WARM UP JOB WHERE ID = ${jid}"""
            } catch (Throwable ignored) {
            }
        }
    }
}
