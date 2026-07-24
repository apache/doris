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

suite('test_warm_up_event_on_tables_dynamic', 'docker') {
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

        def dbName = "test_on_tables_dynamic_db"
        def jobIds = []

        try {
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """use ${dbName}"""

            // ===== Test 1: New table auto-included after job creation =====
            logger.info("===== Test 1: New table auto-included =====")

            sql """CREATE TABLE IF NOT EXISTS fact_orders (id INT, amount DOUBLE)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            sql """use @${clusterName1}"""
            def jobId_ = sql """
                WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                ON TABLES (
                    INCLUDE '${dbName}.fact_*'
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

            // Verify initial matched tables
            def initMatched = WarmupMetricsUtils.waitForMatchedTables(sqlRunner, jobId,
                    ["${dbName}.fact_orders".toString()] as Set)
            logger.info("Initial MatchedTables: ${initMatched}")
            assert "${dbName}.fact_orders".toString() in initMatched

            // Create a new table that matches the pattern
            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS fact_sales (id INT, revenue DOUBLE)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            // Also create a table that does NOT match the pattern
            sql """CREATE TABLE IF NOT EXISTS dim_product (id INT, name STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            // Poll until new matching table is auto-included
            def matchedAfterCreate = WarmupMetricsUtils.waitForMatchedTables(sqlRunner, jobId,
                    ["${dbName}.fact_orders".toString(), "${dbName}.fact_sales".toString()] as Set,
                    ["${dbName}.dim_product".toString()] as Set)
            logger.info("MatchedTables after create: ${matchedAfterCreate}")
            assert "${dbName}.fact_orders".toString() in matchedAfterCreate
            assert "${dbName}.fact_sales".toString() in matchedAfterCreate
            assert !("${dbName}.dim_product".toString() in matchedAfterCreate)

            // Verify warmup works for the new table — with quantitative metric check
            def baseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, clusterName1, clusterName2)
            def numInserts = 5
            sql """use ${dbName}"""
            for (int i = 0; i < numInserts; i++) {
                sql """INSERT INTO fact_sales VALUES (${i}, ${i * 100.0})"""
            }

            def finalMetrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, clusterName1, clusterName2,
                    baseMetrics.finished + numInserts)
            WarmupMetricsUtils.logWarmupMetrics(sqlRunner, clusterName1, clusterName2)

            def reqDelta = finalMetrics.requested - baseMetrics.requested
            def subDelta = finalMetrics.submitted - baseMetrics.submitted
            def finDelta = finalMetrics.finished - baseMetrics.finished
            def failDelta = finalMetrics.failed - baseMetrics.failed
            logger.info("Test1 deltas: requested=${reqDelta}, submitted=${subDelta}, finished=${finDelta}, failed=${failDelta}")
            assert reqDelta >= numInserts : "Expected requested >= ${numInserts}, got ${reqDelta}"
            assert subDelta >= numInserts : "Expected submitted >= ${numInserts}, got ${subDelta}"
            assert finDelta >= numInserts : "Expected finished >= ${numInserts}, got ${finDelta}"
            assert failDelta == 0 : "Expected 0 failed, got ${failDelta}"

            // Negative proof: insert into dim_product (not matched)
            def metricsBeforeDim = WarmupMetricsUtils.waitForMetricsStable(sqlRunner, clusterName1, clusterName2)
            for (int i = 0; i < numInserts; i++) {
                sql """INSERT INTO dim_product VALUES (${i}, 'product_${i}')"""
            }
            sleep(5000)
            def metricsAfterDim = WarmupMetricsUtils.logWarmupMetrics(sqlRunner, clusterName1, clusterName2)
            def dimSubDelta = metricsAfterDim.submitted - metricsBeforeDim.submitted
            def dimFinDelta = metricsAfterDim.finished - metricsBeforeDim.finished
            assert dimSubDelta == 0 : "dim_product inserts should not trigger warmup, submitted delta=${dimSubDelta}"
            assert dimFinDelta == 0 : "dim_product inserts should not trigger warmup, finished delta=${dimFinDelta}"

            // ===== Test 2: Dropped table auto-excluded =====
            logger.info("===== Test 2: Dropped table auto-excluded =====")

            sql """use ${dbName}"""
            sql """DROP TABLE IF EXISTS fact_orders"""

            // Poll until dropped table is removed
            def matchedAfterDrop = WarmupMetricsUtils.waitForMatchedTables(sqlRunner, jobId,
                    ["${dbName}.fact_sales".toString()] as Set,
                    ["${dbName}.fact_orders".toString()] as Set)
            logger.info("MatchedTables after drop: ${matchedAfterDrop}")
            assert !("${dbName}.fact_orders".toString() in matchedAfterDrop)
            assert "${dbName}.fact_sales".toString() in matchedAfterDrop

            // Job should still be running
            def jobInfoAfterDrop = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            assert jobInfoAfterDrop[0][3] in ["RUNNING", "PENDING"]

            // ===== Test 3: Rename table — pattern re-evaluation =====
            logger.info("===== Test 3: Rename table =====")

            // Rename fact_sales to archive_sales (no longer matches fact_*)
            sql """ALTER TABLE ${dbName}.fact_sales RENAME archive_sales"""

            def matchedAfterRename = WarmupMetricsUtils.waitForMatchedTables(sqlRunner, jobId,
                    [] as Set,
                    ["${dbName}.fact_sales".toString(), "${dbName}.archive_sales".toString()] as Set)
            logger.info("MatchedTables after rename to archive_sales: ${matchedAfterRename}")
            assert !("${dbName}.fact_sales".toString() in matchedAfterRename)
            assert !("${dbName}.archive_sales".toString() in matchedAfterRename)

            // Job still running even with no matched tables
            def jobInfoAfterRename = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            assert jobInfoAfterRename[0][3] in ["RUNNING", "PENDING"]

            // Rename back to a matching name
            sql """ALTER TABLE ${dbName}.archive_sales RENAME fact_revenue"""

            def matchedAfterRenameBack = WarmupMetricsUtils.waitForMatchedTables(sqlRunner, jobId,
                    ["${dbName}.fact_revenue".toString()] as Set)
            logger.info("MatchedTables after rename to fact_revenue: ${matchedAfterRenameBack}")
            assert "${dbName}.fact_revenue".toString() in matchedAfterRenameBack

            // Verify warmup still works after rename-back — with quantitative metric check
            def metricsBeforeRenameInsert = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, clusterName1, clusterName2)
            def numRenameInserts = 5
            sql """use ${dbName}"""
            for (int i = 0; i < numRenameInserts; i++) {
                sql """INSERT INTO fact_revenue VALUES (${i + 100}, ${i * 50.0})"""
            }

            def metricsAfterRenameInsert = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, clusterName1, clusterName2,
                    metricsBeforeRenameInsert.finished + numRenameInserts)
            WarmupMetricsUtils.logWarmupMetrics(sqlRunner, clusterName1, clusterName2)

            def renameReqDelta = metricsAfterRenameInsert.requested - metricsBeforeRenameInsert.requested
            def renameSubDelta = metricsAfterRenameInsert.submitted - metricsBeforeRenameInsert.submitted
            def renameFinDelta = metricsAfterRenameInsert.finished - metricsBeforeRenameInsert.finished
            def renameFailDelta = metricsAfterRenameInsert.failed - metricsBeforeRenameInsert.failed
            logger.info("Rename test deltas: requested=${renameReqDelta}, submitted=${renameSubDelta}, finished=${renameFinDelta}, failed=${renameFailDelta}")
            assert renameReqDelta >= numRenameInserts : "Expected requested >= ${numRenameInserts}, got ${renameReqDelta}"
            assert renameSubDelta >= numRenameInserts : "Expected submitted >= ${numRenameInserts}, got ${renameSubDelta}"
            assert renameFinDelta >= numRenameInserts : "Expected finished >= ${numRenameInserts}, got ${renameFinDelta}"
            assert renameFailDelta == 0 : "Expected 0 failed, got ${renameFailDelta}"

        } finally {
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS fact_orders"""
                sql """DROP TABLE IF EXISTS fact_sales"""
                sql """DROP TABLE IF EXISTS fact_revenue"""
                sql """DROP TABLE IF EXISTS archive_sales"""
                sql """DROP TABLE IF EXISTS dim_product"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbName}""" } catch (Exception ignored) {}
        }
    }
}
