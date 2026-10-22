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

// BUILD INDEX runs on the compute group that submitted it. It is an explicit DDL, so it must
// finish even when another active compute group owns the tablets for compaction.
suite('test_compaction_rw_separation_build_index_cross_group', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
    ]
    options.beConfigs += [
        'enable_compaction_rw_separation=true',
        'compaction_cluster_takeover_timeout_ms=600000',
        'cluster_status_cache_refresh_interval_sec=2',
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'disable_auto_compaction=true',
    ]
    options.cloudMode = true

    def waitForSchemaChange = { tableName ->
        def deadline = System.currentTimeMillis() + 120000L
        while (System.currentTimeMillis() < deadline) {
            def jobs = sql """
                SHOW ALTER TABLE COLUMN WHERE TableName = "${tableName}"
                ORDER BY CreateTime DESC LIMIT 1
            """
            if (jobs.isEmpty() || jobs[0].toString().contains("FINISHED")) {
                return
            }
            sleep(1000)
        }
        assertTrue(false, "schema change on ${tableName} did not finish")
    }

    def waitForBuildIndex = { tableName ->
        def deadline = System.currentTimeMillis() + 180000L
        while (System.currentTimeMillis() < deadline) {
            def jobs = sql """SHOW BUILD INDEX WHERE TableName = "${tableName}" """
            logger.info("build index jobs: ${jobs}")
            assertFalse(jobs.isEmpty(), "no build index job for ${tableName}")
            jobs.each { assertNotEquals('CANCELLED', it[7], "build index job failed: ${it}") }
            if (jobs.every { it[7] == 'FINISHED' }) {
                return
            }
            sleep(1000)
        }
        assertTrue(false, "build index on ${tableName} did not finish")
    }

    docker(options) {
        def ownerCluster = 'build_index_owner'
        def buildCluster = 'build_index_builder'
        cluster.addBackend(1, ownerCluster)
        cluster.addBackend(1, buildCluster)

        def tableName = 'test_compaction_rw_sep_build_index'
        sql "use @${ownerCluster}"
        sql "DROP TABLE IF EXISTS ${tableName} FORCE"
        sql """
            CREATE TABLE ${tableName} (
                k1 INT NOT NULL,
                v1 VARCHAR(32) NOT NULL
            ) DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        // The owner cluster loads every rowset, so it owns the tablet for compaction.
        for (int i = 0; i < 5; i++) {
            sql "INSERT INTO ${tableName} VALUES (${i}, 'v${i}')"
        }

        // A light index add leaves the historical rowsets for BUILD INDEX to backfill.
        sql "SET enable_add_index_for_new_data = true"
        sql "ALTER TABLE ${tableName} ADD INDEX idx_v1 (v1) USING INVERTED"
        waitForSchemaChange(tableName)

        sql "use @${buildCluster}"
        sql "BUILD INDEX ON ${tableName}"
        waitForBuildIndex(tableName)

        def rows = sql "SELECT k1 FROM ${tableName} WHERE v1 MATCH_ANY 'v3' ORDER BY k1"
        assertEquals(1, rows.size())
        assertEquals(3, rows[0][0])
        rows = sql "SELECT COUNT(*) FROM ${tableName}"
        assertEquals(5, rows[0][0])
    }
}
