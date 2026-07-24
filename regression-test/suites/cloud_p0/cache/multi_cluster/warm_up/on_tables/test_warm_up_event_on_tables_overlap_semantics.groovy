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

suite('test_warm_up_event_on_tables_overlap_semantics', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_warm_up_job_scheduler_interval_millisecond=100',
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

        def srcCluster = "warmup_overlap_src"
        def dstCluster = "warmup_overlap_dst"
        def dbName = "test_warmup_overlap_semantics_db"
        def tableName = "overlap_tbl"
        def otherTable = "overlap_other_tbl"
        def jobIds = []

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster)

        sql """use @${srcCluster}"""
        sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
        sql """use ${dbName}"""
        sql """CREATE TABLE IF NOT EXISTS ${tableName} (id INT, val STRING)
               DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
        sql """CREATE TABLE IF NOT EXISTS ${otherTable} (id INT, val STRING)
               DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

        def preciseJobId = sql("""
            WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
            ON TABLES (INCLUDE '${dbName}.${tableName}')
            PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
        """)[0][0]
        def overlapJobId = sql("""
            WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
            ON TABLES (
                INCLUDE '${dbName}.*',
                EXCLUDE '${dbName}.${otherTable}'
            )
            PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
        """)[0][0]
        def containerJobId = sql("""
            WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
            ON TABLES (INCLUDE '${dbName}.*')
            PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
        """)[0][0]
        jobIds.addAll([preciseJobId, overlapJobId, containerJobId])

        assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, preciseJobId,
                ["${dbName}.${tableName}".toString()] as Set,
                ["${dbName}.${otherTable}".toString()] as Set) ==
                ["${dbName}.${tableName}".toString()] as Set
        assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, overlapJobId,
                ["${dbName}.${tableName}".toString()] as Set,
                ["${dbName}.${otherTable}".toString()] as Set)
                .contains("${dbName}.${tableName}".toString())
        assert WarmupMetricsUtils.waitForMatchedTables(sqlRunner, containerJobId,
                ["${dbName}.${tableName}".toString(), "${dbName}.${otherTable}".toString()] as Set)
                .containsAll(["${dbName}.${tableName}".toString(), "${dbName}.${otherTable}".toString()] as Set)
        jobIds.each { jobId ->
            WarmupMetricsUtils.waitForJobStatus(sqlRunner, jobId, ["RUNNING"], 60000)
        }
        Thread.sleep(1000)

        def baseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)
        for (int i = 0; i < 4; i++) {
            sql """INSERT INTO ${tableName} VALUES (${i}, 'target_${i}')"""
            sql """INSERT INTO ${otherTable} VALUES (${i}, 'other_${i}')"""
        }
        def finalMetrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                baseMetrics.finished + 1, 120000)
        assertTrue(finalMetrics.requested > baseMetrics.requested,
                "overlap jobs should all receive triggers, before=${baseMetrics}, after=${finalMetrics}")

        def preciseStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, preciseJobId, {
            it.seg_num.requested_5m >= 4
        }, 120000)
        def overlapStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, overlapJobId, {
            it.seg_num.requested_5m >= 4
        }, 120000)
        def containerStats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, containerJobId, {
            it.seg_num.requested_5m >= 8
        }, 120000)
        assertTrue(preciseStats.seg_num.requested_5m >= 4)
        assertTrue(overlapStats.seg_num.requested_5m >= 4)
        assertTrue(containerStats.seg_num.requested_5m >= 8)

        jobIds.each { jid ->
            try {
                sql """CANCEL WARM UP JOB WHERE ID = ${jid}"""
            } catch (Throwable ignored) {
            }
        }
    }
}
