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

// Test point covered: ST-12.
suite('test_warm_up_event_on_tables_system_cluster_change', 'docker') {
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

        def srcCluster = "warmup_st12_source"
        def dstCluster = "warmup_st12_target"
        def dstCluster2 = "warmup_st12_target2"
        def dstClusterRenamed = "warmup_st12_target_renamed"
        def srcClusterRenamed = "warmup_st12_source_renamed"
        def dbName = "test_on_tables_system_cluster_change_db"
        def tableName = "base_tbl"
        def jobIds = []
        def jsonSlurper = new JsonSlurper()
        def metaService = cluster.getAllMetaservices().get(0)

        def waitForCluster = { String clusterName, boolean expectedPresent ->
            List clusters = []
            for (int i = 0; i < 60; i++) {
                clusters = sql """SHOW CLUSTERS"""
                boolean present = clusters.any { it[0].toString() == clusterName }
                if (present == expectedPresent) {
                    return
                }
                sleep(1000)
            }
            assert false : "cluster ${clusterName} present=${!expectedPresent} did not become ${expectedPresent}, clusters=${clusters}"
        }

        def getClusterId = { String clusterName ->
            def tag = getCloudBeTagByName(clusterName)
            return jsonSlurper.parseText(tag).compute_group_id.toString()
        }

        def prepareSourceTable = { String clusterName ->
            sql """use @${clusterName}"""
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS ${tableName} (
                       id INT,
                       val STRING
                   )
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
        }

        def createTableWarmupJob = { String source, String target ->
            prepareSourceTable(source)
            def jobId = sql("""
                WARM UP CLUSTER ${target} WITH CLUSTER ${source}
                ON TABLES (INCLUDE '${dbName}.${tableName}')
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)[0][0]
            jobIds << jobId
            assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, jobId,
                    ["${dbName}.${tableName}".toString()] as Set) ==
                    ["${dbName}.${tableName}".toString()] as Set
            return jobId
        }

        def showJob = { jobId ->
            def rows = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            assert !rows.isEmpty() : "warmup job ${jobId} should exist"
            return rows[0]
        }

        def waitForSystemCancelled = { jobId, String phase ->
            def row = null
            for (int i = 0; i < 60; i++) {
                row = showJob(jobId)
                if (row[3].toString() == "CANCELLED"
                        && row[11].toString().toLowerCase().contains("system cancel")) {
                    return row
                }
                sleep(1000)
            }
            assert false : "${phase}: expected system-cancelled warmup job ${jobId}, row=${row}"
        }

        try {
            cluster.addBackend(1, srcCluster)
            cluster.addBackend(1, dstCluster)
            waitForCluster(srcCluster, true)
            waitForCluster(dstCluster, true)

            def alterJobId = createTableWarmupJob(srcCluster, dstCluster)
            sql """ALTER COMPUTE GROUP ${srcCluster} PROPERTIES ('balance_type'='without_warmup')"""
            sql """ALTER COMPUTE GROUP ${dstCluster} PROPERTIES ('balance_type'='without_warmup')"""
            sleep(5000)
            def alterRow = showJob(alterJobId)
            assert alterRow[3].toString() in ["RUNNING", "PENDING"] :
                    "altering compute group properties should not cancel table warmup job, row=${alterRow}"

            sql """ALTER SYSTEM RENAME COMPUTE GROUP ${dstCluster} ${dstClusterRenamed}"""
            waitForCluster(dstClusterRenamed, true)
            waitForSystemCancelled(alterJobId, "target rename")

            sql """ALTER SYSTEM RENAME COMPUTE GROUP ${dstClusterRenamed} ${dstCluster}"""
            waitForCluster(dstCluster, true)

            def sourceRenameJobId = createTableWarmupJob(srcCluster, dstCluster)
            sql """ALTER SYSTEM RENAME COMPUTE GROUP ${srcCluster} ${srcClusterRenamed}"""
            waitForCluster(srcClusterRenamed, true)
            waitForSystemCancelled(sourceRenameJobId, "source rename")

            sql """ALTER SYSTEM RENAME COMPUTE GROUP ${srcClusterRenamed} ${srcCluster}"""
            waitForCluster(srcCluster, true)

            def targetDropJobId = createTableWarmupJob(srcCluster, dstCluster)
            def dstClusterId = getClusterId(dstCluster)
            drop_cluster(dstCluster, dstClusterId, metaService)
            waitForCluster(dstCluster, false)
            waitForSystemCancelled(targetDropJobId, "target drop")

            cluster.addBackend(1, dstCluster2)
            waitForCluster(dstCluster2, true)
            def sourceDropJobId = createTableWarmupJob(srcCluster, dstCluster2)
            def srcClusterId = getClusterId(srcCluster)
            drop_cluster(srcCluster, srcClusterId, metaService)
            waitForCluster(srcCluster, false)
            waitForSystemCancelled(sourceDropJobId, "source drop")
        } finally {
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use @${srcCluster}"""
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS ${tableName}"""
                sql """DROP DATABASE IF EXISTS ${dbName}"""
            } catch (Exception ignored) {}
        }
    }
}
