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

suite('test_warm_up_cluster_event_conflict_and_diagnostics', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
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

        def srcCluster = "warmup_conflict_src"
        def dstCluster = "warmup_conflict_dst"
        def dbName = "test_warmup_conflict_db"
        def tableName = "conflict_tbl"
        def jobIds = []

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster)

        sql """use @${srcCluster}"""
        sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
        sql """use ${dbName}"""
        sql """DROP TABLE IF EXISTS ${tableName}"""
        sql """CREATE TABLE ${tableName} (
                   id INT,
                   value STRING
               )
               DUPLICATE KEY(id)
               DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

        def clusterJobId = sql("""
            WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
            PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
        """)[0][0]
        jobIds << clusterJobId

        String clusterFirstConflict = WarmupMetricsUtils.expectCreateConflict(sqlRunner, {
            sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (INCLUDE '${dbName}.${tableName}')
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)
        })
        WarmupMetricsUtils.assertConflictMessage(clusterFirstConflict, [
                "conflicting", "cluster-level", "table-level", srcCluster, dstCluster
        ])

        sql """CANCEL WARM UP JOB WHERE ID = ${clusterJobId}"""
        def cancelledCluster = WarmupMetricsUtils.showWarmupJob(sqlRunner, clusterJobId)
        assertEquals("CANCELLED", cancelledCluster.status)

        def tableJobId = sql("""
            WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
            ON TABLES (INCLUDE '${dbName}.${tableName}')
            PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
        """)[0][0]
        jobIds << tableJobId
        assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, tableJobId,
                ["${dbName}.${tableName}".toString()] as Set) ==
                ["${dbName}.${tableName}".toString()] as Set

        String tableFirstConflict = WarmupMetricsUtils.expectCreateConflict(sqlRunner, {
            sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)
        })
        WarmupMetricsUtils.assertConflictMessage(tableFirstConflict, [
                "conflicting", "table-level", "cluster-level", srcCluster, dstCluster
        ])

        sql """CANCEL WARM UP JOB WHERE ID = ${tableJobId}"""
        def cancelledTable = WarmupMetricsUtils.showWarmupJob(sqlRunner, tableJobId)
        assertEquals("CANCELLED", cancelledTable.status)

        jobIds.each { jid ->
            try {
                sql """CANCEL WARM UP JOB WHERE ID = ${jid}"""
            } catch (Throwable ignored) {
            }
        }
    }
}
