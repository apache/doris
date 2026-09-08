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

suite('test_compaction_rw_separation_schema_change_owner', 'docker') {
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
    options.cloudMode = true

    def getTabletStatus = { ip, port, tabletId ->
        def response = new URL(
                "http://${ip}:${port}/api/compaction/show?tablet_id=${tabletId}").text
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

    def runAndWaitFullCompaction = { be, tabletId, boolean expectSuccess ->
        def before = getTabletStatus(be.ip, be.httpPort, tabletId)
        def beforeSuccess = before['last full success time']
        def beforeFailure = before['last full failure time']
        def (code, out, err) = curl('POST',
                "http://${be.ip}:${be.httpPort}/api/compaction/run?tablet_id=${tabletId}&compact_type=full")
        assertEquals(0, code, "Failed to trigger full compaction: ${out}, ${err}")
        def submitResult = new JsonSlurper().parseText(out.trim())
        assertEquals('success', submitResult.status.toLowerCase(),
                "Failed to submit full compaction: ${submitResult}")

        def after = null
        def deadline = System.currentTimeMillis() + 90000L
        while (System.currentTimeMillis() < deadline) {
            after = getTabletStatus(be.ip, be.httpPort, tabletId)
            if (after['last full success time'] != beforeSuccess
                    || after['last full failure time'] != beforeFailure) {
                break
            }
            sleep(250)
        }
        assertNotNull(after)
        if (expectSuccess) {
            assertNotEquals(beforeSuccess, after['last full success time'],
                    "Schema-change owner did not finish full compaction: ${after}")
        } else {
            assertEquals(beforeSuccess, after['last full success time'],
                    "Read cluster unexpectedly compacted the schema-change tablet: ${after}")
            assertNotEquals(beforeFailure, after['last full failure time'],
                    "Read-cluster compaction was not rejected: ${after}")
        }
    }

    docker(options) {
        def writeCluster = 'schema_change_owner_write'
        def readCluster = 'schema_change_owner_read'
        cluster.addBackend(1, writeCluster)
        cluster.addBackend(1, readCluster)

        def writeBe = getBeIpAndPort(writeCluster)
        def readBe = getBeIpAndPort(readCluster)

        sql "use @${writeCluster}"
        sql "DROP TABLE IF EXISTS test_compaction_rw_sep_schema_change_owner FORCE"
        sql """
            CREATE TABLE test_compaction_rw_sep_schema_change_owner (
                k1 INT NOT NULL,
                v1 INT NOT NULL,
                v2 INT NOT NULL
            ) UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "light_schema_change" = "false"
            )
        """

        for (int i = 0; i < 10; i++) {
            sql "INSERT INTO test_compaction_rw_sep_schema_change_owner VALUES (${i}, ${i}, ${i})"
        }

        sql "use @${readCluster}"
        def beforeSchemaChange = sql "SELECT COUNT(*) FROM test_compaction_rw_sep_schema_change_owner"
        assertEquals(10, beforeSchemaChange[0][0])

        sql "use @${writeCluster}"
        sql "ALTER TABLE test_compaction_rw_sep_schema_change_owner DROP COLUMN v2"

        def alterState = ''
        for (int i = 0; i < 120; i++) {
            def alterResult = sql """
                SHOW ALTER TABLE COLUMN
                WHERE IndexName = 'test_compaction_rw_sep_schema_change_owner'
                ORDER BY createtime DESC LIMIT 1
            """
            if (!alterResult.isEmpty()) {
                alterState = alterResult[0][9]
                if (alterState == 'FINISHED' || alterState == 'CANCELLED') {
                    break
                }
            }
            sleep(1000)
        }
        assertEquals('FINISHED', alterState, "Schema change did not finish: ${alterState}")

        sql "use @${readCluster}"
        def tablets = sql_return_maparray "SHOW TABLETS FROM test_compaction_rw_sep_schema_change_owner"
        assertEquals(1, tablets.size())
        def tabletId = tablets[0].TabletId
        def afterSchemaChange = sql "SELECT COUNT(*) FROM test_compaction_rw_sep_schema_change_owner"
        assertEquals(10, afterSchemaChange[0][0])

        runAndWaitFullCompaction(readBe, tabletId, false)
        runAndWaitFullCompaction(writeBe, tabletId, true)
    }
}
