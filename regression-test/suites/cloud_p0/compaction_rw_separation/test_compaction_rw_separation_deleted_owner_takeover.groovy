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

// A deleted owner cluster is unavailable like a suspended one: another cluster takes over its
// tablets only after the takeover timeout, counted from when the deletion is first observed.
suite('test_compaction_rw_separation_deleted_owner_takeover', 'docker') {
    def takeoverTimeoutMs = 20000L
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
    ]
    options.beConfigs += [
        'enable_compaction_rw_separation=true',
        "compaction_cluster_takeover_timeout_ms=${takeoverTimeoutMs}",
        'cluster_status_cache_refresh_interval_sec=2',
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'disable_auto_compaction=true',
    ]
    options.cloudMode = true

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
        return [ip: firstBe[1], httpPort: firstBe[4],
                clusterId: new JsonSlurper().parseText(firstBe[19]).compute_group_id]
    }

    // Returns true when the full compaction finished, false when it was rejected either at
    // submission or while requesting the meta-service job.
    def runFullCompaction = { be, tabletId ->
        def before = getTabletStatus(be, tabletId)
        def beforeSuccess = before['last full success time']
        def beforeFailure = before['last full failure time']
        def (code, out, err) = curl('POST',
                "http://${be.ip}:${be.httpPort}/api/compaction/run?tablet_id=${tabletId}&compact_type=full")
        assertEquals(0, code, "Failed to call full compaction: ${out}, ${err}")
        def submit = new JsonSlurper().parseText(out.trim())
        logger.info("full compaction submit result: ${submit}")
        if (submit.status.toLowerCase() != 'success') {
            return false
        }
        def after = null
        def deadline = System.currentTimeMillis() + 90000L
        while (System.currentTimeMillis() < deadline) {
            after = getTabletStatus(be, tabletId)
            if (after['last full success time'] != beforeSuccess
                    || after['last full failure time'] != beforeFailure) {
                break
            }
            sleep(250)
        }
        assertNotNull(after)
        logger.info("full compaction status: ${after}")
        return after['last full success time'] != beforeSuccess
    }

    docker(options) {
        def ownerCluster = 'deleted_owner'
        def readCluster = 'deleted_owner_reader'
        cluster.addBackend(1, ownerCluster)
        cluster.addBackend(1, readCluster)
        def readBe = getBeIpAndPort(readCluster)
        // Match the exact compute group name: every BE tag contains the suite name.
        def ownerClusterId = getBeIpAndPort(ownerCluster).clusterId

        def tableName = 'test_compaction_rw_sep_deleted_owner'
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

        sql "use @${readCluster}"
        def rows = sql "SELECT COUNT(*) FROM ${tableName}"
        assertEquals(10, rows[0][0])

        // The reader is not the owner while the owner cluster is healthy.
        assertFalse(runFullCompaction(readBe, tabletId),
                "Reader compacted a tablet owned by a healthy cluster")

        def ms = cluster.getAllMetaservices().get(0)
        drop_cluster(ownerCluster, ownerClusterId, ms)
        def droppedAt = System.currentTimeMillis()

        // Let the reader refresh its cluster status cache and observe the deletion, but stay
        // well inside the takeover window.
        sleep(6000)
        assertFalse(runFullCompaction(readBe, tabletId),
                "Reader took over a deleted owner before the takeover timeout")

        // The window starts when the reader first observes the deletion, at most one refresh
        // interval after the drop.
        def waitMs = droppedAt + takeoverTimeoutMs + 8000L - System.currentTimeMillis()
        if (waitMs > 0) {
            sleep(waitMs)
        }
        assertTrue(runFullCompaction(readBe, tabletId),
                "Reader did not take over the deleted owner after the takeover timeout")

        rows = sql "SELECT COUNT(*) FROM ${tableName}"
        assertEquals(10, rows[0][0])
    }
}
