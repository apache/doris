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
import org.apache.doris.regression.util.NodeType

// The owner epoch advances on every load. A compaction selected by the owner cluster must still
// start after loads commit between the owner snapshot and the meta-service job creation.
suite('test_compaction_rw_separation_owner_concurrent_load', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
    ]
    options.beConfigs += [
        'enable_compaction_rw_separation=true',
        'compaction_cluster_takeover_timeout_ms=600000',
        'cluster_status_cache_refresh_interval_sec=5',
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'disable_auto_compaction=true',
    ]
    options.enableDebugPoints()
    options.cloudMode = true

    def blockPoint = 'CloudFullCompaction::request_global_lock.block'

    def getTabletStatus = { be, tabletId ->
        def response = new URL(
                "http://${be.ip}:${be.httpPort}/api/compaction/show?tablet_id=${tabletId}").text
        return new JsonSlurper().parseText(response)
    }

    def getBeIpAndPort = { clusterName ->
        def backends = sql "SHOW BACKENDS"
        def clusterBes = backends.findAll {
            it[19].contains("\"compute_group_name\" : \"${clusterName}\"")
        }
        assertFalse(clusterBes.isEmpty(), "No BE found for cluster: ${clusterName}")
        def firstBe = clusterBes[0]
        return [ip: firstBe[1], httpPort: firstBe[4]]
    }

    docker(options) {
        def ownerCluster = 'concurrent_load_owner'
        def readCluster = 'concurrent_load_reader'
        cluster.addBackend(1, ownerCluster)
        cluster.addBackend(1, readCluster)
        def ownerBe = getBeIpAndPort(ownerCluster)

        def tableName = 'test_compaction_rw_sep_owner_concurrent_load'
        sql "use @${ownerCluster}"
        sql "DROP TABLE IF EXISTS ${tableName} FORCE"
        sql """
            CREATE TABLE ${tableName} (
                k1 INT NOT NULL,
                v1 INT NOT NULL
            ) DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        for (int i = 0; i < 10; i++) {
            sql "INSERT INTO ${tableName} VALUES (${i}, ${i * 10})"
        }
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
        assertEquals(1, tablets.size())
        def tabletId = tablets[0].TabletId

        def before = getTabletStatus(ownerBe, tabletId)
        def beforeSuccess = before['last full success time']
        def beforeFailure = before['last full failure time']

        try {
            // Hold the full compaction after it captured the owner token and before it asks
            // meta-service to create the job.
            GetDebugPoint().enableDebugPoint(ownerBe.ip, ownerBe.httpPort as int, NodeType.BE,
                    blockPoint, [tablet_id: "${tabletId}"])
            def (code, out, err) = curl('POST',
                    "http://${ownerBe.ip}:${ownerBe.httpPort}/api/compaction/run?tablet_id=${tabletId}&compact_type=full")
            assertEquals(0, code, "Failed to trigger full compaction: ${out}, ${err}")
            def submit = new JsonSlurper().parseText(out.trim())
            assertEquals('success', submit.status.toLowerCase(), "Owner full compaction was not submitted: ${submit}")
            sleep(3000)

            // Each load advances the owner epoch while the compaction is held.
            for (int i = 100; i < 103; i++) {
                sql "INSERT INTO ${tableName} VALUES (${i}, ${i * 10})"
            }
        } finally {
            GetDebugPoint().disableDebugPoint(ownerBe.ip, ownerBe.httpPort as int, NodeType.BE,
                    blockPoint)
        }

        def after = null
        def deadline = System.currentTimeMillis() + 90000L
        while (System.currentTimeMillis() < deadline) {
            after = getTabletStatus(ownerBe, tabletId)
            if (after['last full success time'] != beforeSuccess
                    || after['last full failure time'] != beforeFailure) {
                break
            }
            sleep(250)
        }
        assertNotNull(after)
        assertEquals(beforeFailure, after['last full failure time'],
                "Owner compaction was rejected after concurrent loads: ${after}")
        assertNotEquals(beforeSuccess, after['last full success time'],
                "Owner compaction did not finish after concurrent loads: ${after}")

        def rows = sql "SELECT COUNT(*) FROM ${tableName}"
        assertEquals(13, rows[0][0])
    }
}
