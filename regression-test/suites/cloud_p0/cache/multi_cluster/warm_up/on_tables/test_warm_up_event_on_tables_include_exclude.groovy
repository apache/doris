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
import groovy.json.JsonSlurper

suite('test_warm_up_event_on_tables_include_exclude', 'docker') {
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

        def clusterName1 = "warmup_source"
        def clusterName2 = "warmup_target"

        cluster.addBackend(1, clusterName1)
        cluster.addBackend(1, clusterName2)

        sql """use @${clusterName1}"""

        def dbName = "test_on_tables_ie_db"
        def jobIds = []

        try {
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS orders (id INT, amount DOUBLE)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS customers (id INT, name STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS tmp_staging (id INT, data STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS orders_bak (id INT, amount DOUBLE)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            sql """use @${clusterName1}"""
            def jobId_ = sql """
                WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                ON TABLES (
                    INCLUDE '${dbName}.*',
                    EXCLUDE '${dbName}.tmp_*',
                    EXCLUDE '${dbName}.*_bak'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """
            def jobId = jobId_[0][0]
            jobIds << jobId
            logger.info("Warm-up job ID: ${jobId}")

            sleep(3000)
            def baseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, clusterName1, clusterName2)

            // Negative proof: insert only into excluded tables
            def numExcInserts = 5
            sql """use ${dbName}"""
            for (int i = 0; i < numExcInserts; i++) {
                sql """INSERT INTO tmp_staging VALUES (${i}, 'staging_${i}')"""
                sql """INSERT INTO orders_bak VALUES (${i}, ${i * 5.0})"""
            }
            sleep(5000)

            def metricsAfterExc = WarmupMetricsUtils.logWarmupMetrics(sqlRunner, clusterName1, clusterName2)
            assert metricsAfterExc.submitted - baseMetrics.submitted == 0 : \
                "Excluded tables should not submit warmup segments"
            assert metricsAfterExc.finished - baseMetrics.finished == 0 : \
                "Excluded tables should not finish warmup segments"

            // Positive proof: insert into included tables
            def numIncInserts = 5
            def expectedSeg = numIncInserts * 2  // orders + customers
            for (int i = 0; i < numIncInserts; i++) {
                sql """INSERT INTO orders VALUES (${i + 100}, ${i * 20.5})"""
                sql """INSERT INTO customers VALUES (${i + 100}, 'new_customer_${i}')"""
            }

            def finalMetrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, clusterName1, clusterName2,
                    metricsAfterExc.finished + expectedSeg)
            WarmupMetricsUtils.logWarmupMetrics(sqlRunner, clusterName1, clusterName2)

            def reqDelta = finalMetrics.requested - metricsAfterExc.requested
            def subDelta = finalMetrics.submitted - metricsAfterExc.submitted
            def finDelta = finalMetrics.finished - metricsAfterExc.finished
            def failDelta = finalMetrics.failed - metricsAfterExc.failed
            logger.info("Included deltas: requested=${reqDelta}, submitted=${subDelta}, finished=${finDelta}, failed=${failDelta}")
            assert reqDelta >= expectedSeg : "Expected requested >= ${expectedSeg}, got ${reqDelta}"
            assert subDelta >= expectedSeg : "Expected submitted >= ${expectedSeg}, got ${subDelta}"
            assert finDelta >= expectedSeg : "Expected finished >= ${expectedSeg}, got ${finDelta}"
            assert failDelta == 0 : "Expected 0 failed, got ${failDelta}"

            // Verify SHOW output
            def jobInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            def tableFilter = jobInfo[0][13]
            logger.info("TableFilter: ${tableFilter}")
            def filterJson = new JsonSlurper().parseText(tableFilter)
            assert filterJson.include.contains("${dbName}.*".toString())
            assert filterJson.exclude.contains("${dbName}.*_bak".toString())
            assert filterJson.exclude.contains("${dbName}.tmp_*".toString())

            def matchedSet = WarmupMetricsUtils.parseMatchedTables(jobInfo)
            logger.info("MatchedTables set: ${matchedSet}")
            assert "${dbName}.orders".toString() in matchedSet
            assert "${dbName}.customers".toString() in matchedSet
            assert !("${dbName}.tmp_staging".toString() in matchedSet)
            assert !("${dbName}.orders_bak".toString() in matchedSet)

        } finally {
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS orders"""
                sql """DROP TABLE IF EXISTS customers"""
                sql """DROP TABLE IF EXISTS tmp_staging"""
                sql """DROP TABLE IF EXISTS orders_bak"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbName}""" } catch (Exception ignored) {}
        }
    }
}
