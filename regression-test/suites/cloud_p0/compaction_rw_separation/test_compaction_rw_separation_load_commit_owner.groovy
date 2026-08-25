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

    def updateCompaction = { be, boolean enabled ->
        def (code, out, err) = curl(
                'POST',
                "http://${be.ip}:${be.httpPort}/api/update_config?disable_auto_compaction=${!enabled}")
        assertEquals(0, code, "Failed to update compaction config: ${out}, ${err}")
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
            ) UNIQUE KEY(k1)
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
        sleep(7000)

        updateCompaction(firstBe, true)
        sleep(30000)
        def firstStatus = getTabletStatus(firstBe.ip, firstBe.httpPort, tabletId)
        assertTrue(firstStatus['last cumulative success time'] != '1970-01-01 08:00:00.000',
                'The first cluster should compact before the owner changes')
        updateCompaction(firstBe, false)

        for (int i = 10; i < 20; i++) {
            sql "INSERT INTO test_compaction_rw_sep_load_commit_owner VALUES (${i}, ${i * 10})"
        }

        updateCompaction(secondBe, true)
        sleep(30000)
        def secondStatus = getTabletStatus(secondBe.ip, secondBe.httpPort, tabletId)
        assertTrue(secondStatus['last cumulative success time'] != '1970-01-01 08:00:00.000',
                'The second cluster should refresh its owner after load commit and compact')
    }
}
