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

suite('test_compaction_rw_separation_load_commit_owner', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
    ]
    options.beConfigs += [
        'enable_compaction_rw_separation=true',
        'enable_cloud_make_rs_visible_on_be=false',
        'compaction_cluster_takeover_timeout_ms=600000',
        'cluster_status_cache_refresh_interval_sec=5',
        'tablet_sync_interval_s=3600',
        'schedule_sync_tablets_interval_s=3600',
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

    def triggerFullCompaction = { be, tabletId ->
        def (code, out, err) = curl('POST',
                "http://${be.ip}:${be.httpPort}/api/compaction/run?tablet_id=${tabletId}&compact_type=full")
        assertEquals(0, code, "Failed to trigger full compaction: ${out}, ${err}")
        def result = new JsonSlurper().parseText(out.trim())
        assertEquals('success', result.status.toLowerCase(), "Failed to submit full compaction: ${result}")
    }

    def runAndWaitFullCompaction = { be, tabletId, boolean expectSuccess ->
        def before = getTabletStatus(be.ip, be.httpPort, tabletId)
        def beforeSuccess = before['last full success time']
        def beforeFailure = before['last full failure time']
        triggerFullCompaction(be, tabletId)

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
                    "Owner cluster did not finish full compaction: ${after}")
        } else {
            assertEquals(beforeSuccess, after['last full success time'],
                    "Non-owner cluster unexpectedly compacted the tablet: ${after}")
            assertNotEquals(beforeFailure, after['last full failure time'],
                    "Non-owner compaction was not rejected: ${after}")
        }
    }

    docker(options) {
        def firstCluster = 'load_commit_owner_first'
        def secondCluster = 'load_commit_owner_second'
        cluster.addBackend(1, firstCluster)
        cluster.addBackend(1, secondCluster)

        def firstBe = getBeIpAndPort(firstCluster)
        def secondBe = getBeIpAndPort(secondCluster)

        sql "use @${firstCluster}"
        sql "DROP TABLE IF EXISTS test_compaction_rw_sep_load_commit_owner FORCE"
        sql """
            CREATE TABLE test_compaction_rw_sep_load_commit_owner (
                k1 INT NOT NULL,
                v1 INT NOT NULL
            ) DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """

        for (int i = 0; i < 10; i++) {
            sql "INSERT INTO test_compaction_rw_sep_load_commit_owner VALUES (${i}, ${i * 10})"
        }

        def tablets = sql_return_maparray "SHOW TABLETS FROM test_compaction_rw_sep_load_commit_owner"
        assertEquals(1, tablets.size())
        def tabletId = tablets[0].TabletId

        sql "use @${secondCluster}"
        def cachedRows = sql "SELECT COUNT(*) FROM test_compaction_rw_sep_load_commit_owner"
        assertEquals(10, cachedRows[0][0])

        // Direct-to-BE stream load has no query ConnectContext on the begin-txn RPC. The
        // receiving backend, not the deliberately conflicting header, is the writer owner.
        streamLoad {
            table 'test_compaction_rw_sep_load_commit_owner'
            directToBe secondBe.ip, secondBe.httpPort.toInteger()
            set 'column_separator', ','
            set 'compute_group', firstCluster
            file 'load_commit_owner.csv'
            time 10000
            check { loadResult, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def result = new JsonSlurper().parseText(loadResult)
                assertEquals('success', result.Status.toLowerCase())
                assertEquals(2, result.NumberTotalRows)
                assertEquals(0, result.NumberFilteredRows)
            }
        }
        sql 'SYNC'
        def afterStreamLoad = sql "SELECT COUNT(*) FROM test_compaction_rw_sep_load_commit_owner"
        assertEquals(12, afterStreamLoad[0][0])

        // The old owner must be rejected even if its local owner cache was stale. The new owner
        // can compact immediately after the direct stream-load response is consumed.
        runAndWaitFullCompaction(firstBe, tabletId, false)
        runAndWaitFullCompaction(secondBe, tabletId, true)

        // Switch ownership back to the first cluster, then use a zero-segment delete predicate
        // from the second cluster. A logical delete still changes the compaction owner.
        sql "use @${firstCluster}"
        for (int i = 200; i < 208; i++) {
            sql "INSERT INTO test_compaction_rw_sep_load_commit_owner VALUES (${i}, ${i * 10})"
        }
        sql "use @${secondCluster}"
        sql "DELETE FROM test_compaction_rw_sep_load_commit_owner WHERE k1 = 0"
        sql 'SYNC'
        def afterDelete = sql "SELECT COUNT(*) FROM test_compaction_rw_sep_load_commit_owner"
        assertEquals(19, afterDelete[0][0])

        runAndWaitFullCompaction(firstBe, tabletId, false)
        runAndWaitFullCompaction(secondBe, tabletId, true)
    }
}
