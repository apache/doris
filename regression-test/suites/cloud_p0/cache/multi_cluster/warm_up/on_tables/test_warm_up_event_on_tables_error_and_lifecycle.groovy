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

suite('test_warm_up_event_on_tables_error_and_lifecycle', 'docker') {
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

        def dbName = "test_on_tables_err_db"
        def jobIds = []

        try {
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS base_table (id INT, val STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            sql """use @${clusterName1}"""

            // ===== Error Test 1: Exclude-only (no INCLUDE) =====
            logger.info("===== Error Test 1: Exclude-only =====")
            try {
                sql """
                    WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                    ON TABLES (
                        EXCLUDE '${dbName}.tmp_*'
                    )
                    PROPERTIES (
                        "sync_mode" = "event_driven",
                        "sync_event" = "load"
                    )
                """
                assert false : "Expected error for exclude-only ON TABLES"
            } catch (java.sql.SQLException e) {
                logger.info("Expected error: ${e.getMessage()}")
                assert e.getMessage().contains("at least one INCLUDE") : "Error should mention INCLUDE requirement: ${e.getMessage()}"
            }

            // ===== Error Test 2: Invalid pattern format (missing db.table) =====
            logger.info("===== Error Test 2: Invalid pattern format =====")
            try {
                sql """
                    WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                    ON TABLES (
                        INCLUDE 'orders'
                    )
                    PROPERTIES (
                        "sync_mode" = "event_driven",
                        "sync_event" = "load"
                    )
                """
                assert false : "Expected error for invalid pattern format"
            } catch (java.sql.SQLException e) {
                logger.info("Expected error: ${e.getMessage()}")
                assert e.getMessage().contains("db.table") : "Error should mention db.table format: ${e.getMessage()}"
            }

            // ===== Error Test 3: ON TABLES with non-event-driven sync mode =====
            logger.info("===== Error Test 3: ON TABLES with periodic sync mode =====")
            try {
                sql """
                    WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                    ON TABLES (
                        INCLUDE '${dbName}.*'
                    )
                    PROPERTIES (
                        "sync_mode" = "periodic",
                        "sync_interval_sec" = "10"
                    )
                """
                assert false : "Expected error for ON TABLES with periodic sync"
            } catch (java.sql.SQLException e) {
                logger.info("Expected error: ${e.getMessage()}")
                assert e.getMessage().contains("event_driven") : "Error should mention event_driven requirement: ${e.getMessage()}"
            }

            // ===== Error Test 4: No tables match the pattern =====
            logger.info("===== Error Test 4: No matching tables =====")
            try {
                sql """
                    WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                    ON TABLES (
                        INCLUDE 'nonexistent_db_xyz.*'
                    )
                    PROPERTIES (
                        "sync_mode" = "event_driven",
                        "sync_event" = "load"
                    )
                """
                assert false : "Expected error for no matching tables"
            } catch (java.sql.SQLException e) {
                logger.info("Expected error: ${e.getMessage()}")
                def msg = e.getMessage().toLowerCase()
                assert msg.contains("no tables matched") || msg.contains("no table") : "Error should indicate no tables matched: ${e.getMessage()}"
            }

            // ===== Error Test 5: ON TABLES with ONCE sync mode =====
            logger.info("===== Error Test 5: ON TABLES with once sync mode =====")
            try {
                sql """
                    WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                    ON TABLES (
                        INCLUDE '${dbName}.*'
                    )
                    PROPERTIES (
                        "sync_mode" = "once"
                    )
                """
                assert false : "Expected error for ON TABLES with once sync"
            } catch (java.sql.SQLException e) {
                logger.info("Expected error: ${e.getMessage()}")
                assert e.getMessage().contains("event_driven") : "Error should mention event_driven: ${e.getMessage()}"
            }

            // ===== Lifecycle Test 1: Cluster-level and table-level jobs are mutually exclusive =====
            logger.info("===== Lifecycle Test 1: Cross-level conflict =====")

            // Create cluster-level event-driven job
            def clusterJobId_ = sql """
                WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """
            def clusterJobId = clusterJobId_[0][0]
            jobIds << clusterJobId
            logger.info("Cluster-level job ID: ${clusterJobId}")

            // Creating a table-level load-event job for the same source and destination should fail.
            try {
                sql """
                    WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                    ON TABLES (
                        INCLUDE '${dbName}.base_table'
                    )
                    PROPERTIES (
                        "sync_mode" = "event_driven",
                        "sync_event" = "load"
                    )
                """
                assert false : "Expected table-level job to conflict with existing cluster-level load-event job"
            } catch (java.sql.SQLException e) {
                logger.info("Expected cross-level conflict: ${e.getMessage()}")
                assert e.getMessage().contains("Cannot create table-level load-event warm up job") : e.getMessage()
                assert e.getMessage().contains("conflicting cluster-level load-event warm up job ${clusterJobId}") :
                        e.getMessage()
                assert e.getMessage().contains("Cancel existing load-event warm up job ${clusterJobId}") :
                        e.getMessage()
            }

            def clusterJobInfo = sql """SHOW WARM UP JOB WHERE ID = ${clusterJobId}"""
            assert clusterJobInfo[0][13] == "" : "Cluster-level job should have empty TableFilter"
            assert clusterJobInfo[0][14] == "" : "Cluster-level job should have empty MatchedTables"

            sql """CANCEL WARM UP JOB WHERE ID = ${clusterJobId}"""
            def clusterCancelInfo = sql """SHOW WARM UP JOB WHERE ID = ${clusterJobId}"""
            assert clusterCancelInfo[0][3] == "CANCELLED"

            // Create table-level event-driven job after cancelling the conflicting cluster-level job.
            def tableJobId_ = sql """
                WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                ON TABLES (
                    INCLUDE '${dbName}.base_table'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """
            def tableJobId = tableJobId_[0][0]
            jobIds << tableJobId
            logger.info("Table-level job ID: ${tableJobId}")

            def tableJobInfo = sql """SHOW WARM UP JOB WHERE ID = ${tableJobId}"""

            // Table-level job should have non-empty TableFilter and MatchedTables
            assert tableJobInfo[0][13].length() > 0 : "Table-level job should have non-empty TableFilter"
            def tableJobMatched = WarmupMetricsUtils.parseMatchedTables(tableJobInfo)
            assert "${dbName}.base_table".toString() in tableJobMatched : "Table-level job MatchedTables should contain base_table"

            // Creating a cluster-level load-event job should also fail while the table-level job is running.
            try {
                sql """
                    WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                    PROPERTIES (
                        "sync_mode" = "event_driven",
                        "sync_event" = "load"
                    )
                """
                assert false : "Expected cluster-level job to conflict with existing table-level load-event job"
            } catch (java.sql.SQLException e) {
                logger.info("Expected reverse cross-level conflict: ${e.getMessage()}")
                assert e.getMessage().contains("Cannot create cluster-level load-event warm up job") : e.getMessage()
                assert e.getMessage().contains("conflicting table-level load-event warm up job ${tableJobId}") :
                        e.getMessage()
                assert e.getMessage().contains("Cancel existing load-event warm up job ${tableJobId}") :
                        e.getMessage()
            }

            // ===== Lifecycle Test 2: Duplicate detection with normalized rules =====
            logger.info("===== Lifecycle Test 2: Duplicate detection =====")

            // Try creating same table-level job again
            try {
                sql """
                    WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                    ON TABLES (
                        INCLUDE '${dbName}.base_table'
                    )
                    PROPERTIES (
                        "sync_mode" = "event_driven",
                        "sync_event" = "load"
                    )
                """
                assert false : "Expected duplicate job error"
            } catch (java.sql.SQLException e) {
                logger.info("Expected error for duplicate: ${e.getMessage()}")
                assert e.getMessage().contains("already has a runnable job") : e.getMessage()
            }

            // Different filter should succeed
            def tableJobId2_ = sql """
                WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                ON TABLES (
                    INCLUDE '${dbName}.*'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """
            def tableJobId2 = tableJobId2_[0][0]
            jobIds << tableJobId2
            logger.info("Table-level job2 ID (different filter): ${tableJobId2}")

            // ===== Lifecycle Test 3: Cancel and recreate =====
            logger.info("===== Lifecycle Test 3: Cancel and recreate =====")

            sql """CANCEL WARM UP JOB WHERE ID = ${tableJobId}"""
            def cancelInfo = sql """SHOW WARM UP JOB WHERE ID = ${tableJobId}"""
            assert cancelInfo[0][3] == "CANCELLED"

            // After cancelling, we should be able to create a job with the same filter
            def tableJobId3_ = sql """
                WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                ON TABLES (
                    INCLUDE '${dbName}.base_table'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """
            def tableJobId3 = tableJobId3_[0][0]
            jobIds << tableJobId3
            logger.info("Table-level job3 ID (after cancel+recreate): ${tableJobId3}")

            // Verify new job is running
            def jobInfo3 = sql """SHOW WARM UP JOB WHERE ID = ${tableJobId3}"""
            assert jobInfo3[0][3] in ["RUNNING", "PENDING"] : "Recreated job should be running"

            // ===== Lifecycle Test 4: ? wildcard matching with quantitative metrics =====
            logger.info("===== Lifecycle Test 4: ? wildcard =====")

            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS log_a (id INT, msg STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS log_b (id INT, msg STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS log_ab (id INT, msg STRING)
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

            sql """use @${clusterName1}"""
            def jobIdQ_ = sql """
                WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                ON TABLES (
                    INCLUDE '${dbName}.log_?'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """
            def jobIdQ = jobIdQ_[0][0]
            jobIds << jobIdQ
            logger.info("Wildcard ? job ID: ${jobIdQ}")

            sleep(3000)

            def jobInfoQ = sql """SHOW WARM UP JOB WHERE ID = ${jobIdQ}"""
            def matchedSetQ = WarmupMetricsUtils.parseMatchedTables(jobInfoQ)
            logger.info("MatchedTables for ? wildcard: ${matchedSetQ}")
            assert "${dbName}.log_a".toString() in matchedSetQ : "log_a should match log_? pattern"
            assert "${dbName}.log_b".toString() in matchedSetQ : "log_b should match log_? pattern"
            assert !("${dbName}.log_ab".toString() in matchedSetQ) : "log_ab should NOT match log_? (? matches exactly one char)"

            // Quantitative metric verification for ? wildcard
            def baseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, clusterName1, clusterName2)

            // Insert into matched tables log_a and log_b
            def numInserts = 3
            sql """use ${dbName}"""
            for (int i = 0; i < numInserts; i++) {
                sql """INSERT INTO log_a VALUES (${i}, 'msg_a_${i}')"""
                sql """INSERT INTO log_b VALUES (${i}, 'msg_b_${i}')"""
            }
            def expectedSegments = numInserts * 2  // 2 matched tables

            // Insert into non-matched table log_ab
            for (int i = 0; i < numInserts; i++) {
                sql """INSERT INTO log_ab VALUES (${i}, 'msg_ab_${i}')"""
            }

            def finalMetrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, clusterName1, clusterName2,
                    baseMetrics.finished + expectedSegments)
            WarmupMetricsUtils.logWarmupMetrics(sqlRunner, clusterName1, clusterName2)

            def reqDelta = finalMetrics.requested - baseMetrics.requested
            def subDelta = finalMetrics.submitted - baseMetrics.submitted
            def finDelta = finalMetrics.finished - baseMetrics.finished
            def failDelta = finalMetrics.failed - baseMetrics.failed
            logger.info("? wildcard deltas: requested=${reqDelta}, submitted=${subDelta}, finished=${finDelta}, failed=${failDelta}")

            // Only log_a and log_b should have been warmed, not log_ab
            assert reqDelta >= expectedSegments : "Expected requested >= ${expectedSegments}, got ${reqDelta}"
            assert subDelta >= expectedSegments : "Expected submitted >= ${expectedSegments}, got ${subDelta}"
            assert finDelta >= expectedSegments : "Expected finished >= ${expectedSegments}, got ${finDelta}"
            assert failDelta == 0 : "Expected 0 failed, got ${failDelta}"

        } finally {
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS base_table"""
                sql """DROP TABLE IF EXISTS log_a"""
                sql """DROP TABLE IF EXISTS log_b"""
                sql """DROP TABLE IF EXISTS log_ab"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbName}""" } catch (Exception ignored) {}
        }
    }
}
