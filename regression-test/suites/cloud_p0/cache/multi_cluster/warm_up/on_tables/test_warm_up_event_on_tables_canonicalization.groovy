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
import groovy.json.JsonSlurper

suite('test_warm_up_event_on_tables_canonicalization', 'docker') {
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
        def clusterName1 = "warmup_source"
        def clusterName2 = "warmup_target"

        cluster.addBackend(1, clusterName1)
        cluster.addBackend(1, clusterName2)

        sql """use @${clusterName1}"""

        def dbName = "test_on_tables_canon_db"
        def dbOther = "test_on_tables_canon_other_db"
        def jobIds = []

        try {
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """CREATE DATABASE IF NOT EXISTS ${dbOther}"""

            sql """use ${dbName}"""
            sql """CREATE TABLE orders (id INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"""
            sql """CREATE TABLE tmp_staging (id INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"""
            sql """use ${dbOther}"""
            sql """CREATE TABLE logs (id INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1"""
            sql """use @${clusterName1}"""

            // Create a job with specific rule order
            def jobId_ = sql """
                WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                ON TABLES (
                    INCLUDE '${dbName}.*',
                    EXCLUDE '${dbName}.tmp_*',
                    INCLUDE '${dbOther}.*'
                )
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """
            def jobId = jobId_[0][0]
            jobIds << jobId

            def jobInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            def tableFilter = jobInfo[0][13]
            logger.info("TableFilter: ${tableFilter}")

            // Try creating a "duplicate" with rules in different order — should fail
            // because canonicalization normalizes rule order
            try {
                sql """
                    WARM UP CLUSTER ${clusterName2} WITH CLUSTER ${clusterName1}
                    ON TABLES (
                        INCLUDE '${dbOther}.*',
                        INCLUDE '${dbName}.*',
                        EXCLUDE '${dbName}.tmp_*'
                    )
                    PROPERTIES (
                        "sync_mode" = "event_driven",
                        "sync_event" = "load"
                    )
                """
                assert false : "Expected duplicate job error"
            } catch (java.sql.SQLException e) {
                logger.info("Expected error for duplicate job: ${e.getMessage()}")
                assert e.getMessage().contains("already has a runnable job")
            }

        } finally {
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS orders"""
                sql """DROP TABLE IF EXISTS tmp_staging"""
            } catch (Exception ignored) {}
            try {
                sql """use ${dbOther}"""
                sql """DROP TABLE IF EXISTS logs"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbName}""" } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbOther}""" } catch (Exception ignored) {}
        }
    }
}
