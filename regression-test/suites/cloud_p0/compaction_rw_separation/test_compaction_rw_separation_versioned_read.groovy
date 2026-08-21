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

suite('test_compaction_rw_separation_versioned_read', 'docker') {
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
    options.msConfigs += [
        'enable_multi_version_status=true',
    ]
    options.cloudMode = true

    def getTabletStatus = { ip, port, tabletId ->
        def url = "http://${ip}:${port}/api/compaction/show?tablet_id=${tabletId}"
        def response = new URL(url).text
        return new JsonSlurper().parseText(response)
    }

    def getBeIpAndPort = { clusterName ->
        def backends = sql """SHOW BACKENDS"""
        def clusterBes = backends.findAll {
            it[19].contains("""\"compute_group_name\" : \"${clusterName}\"""")
        }
        assertFalse(clusterBes.isEmpty(), "No BE found for cluster: ${clusterName}")
        def firstBe = clusterBes[0]
        return [ip: firstBe[1], httpPort: firstBe[4]]
    }

    docker(options) {
        def writeCluster = 'versioned_write_cluster'
        def readCluster = 'versioned_read_cluster'
        cluster.addBackend(1, writeCluster)
        cluster.addBackend(1, readCluster)

        def readBe = getBeIpAndPort(readCluster)

        sql """use @${writeCluster}"""
        sql """DROP TABLE IF EXISTS test_compaction_rw_sep_versioned_read"""
        sql """
            CREATE TABLE test_compaction_rw_sep_versioned_read (
                k1 INT NOT NULL,
                v1 INT NOT NULL
            ) UNIQUE KEY(`k1`)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """

        for (int i = 0; i < 10; i++) {
            sql """INSERT INTO test_compaction_rw_sep_versioned_read VALUES (${i}, ${i * 10})"""
        }

        def tablets = sql_return_maparray """SHOW TABLETS FROM test_compaction_rw_sep_versioned_read"""
        assertEquals(1, tablets.size())
        def tabletId = tablets[0].TabletId

        sql """use @${readCluster}"""
        def result = sql """SELECT * FROM test_compaction_rw_sep_versioned_read ORDER BY k1"""
        assertEquals(10, result.size())

        // Auto compaction is still disabled while the read cluster refreshes the write cluster state.
        sleep(7000)
        def statusBefore = getTabletStatus(readBe.ip, readBe.httpPort, tabletId)
        def lastCumuTimeBefore = statusBefore['last cumulative success time']

        def (code, out, err) = curl(
            'POST',
            String.format(
                'http://%s:%s/api/update_config?disable_auto_compaction=false',
                readBe.ip,
                readBe.httpPort))
        assertEquals(0, code, "Failed to enable compaction on read cluster: ${out}, ${err}")

        sleep(30000)
        def statusAfter = getTabletStatus(readBe.ip, readBe.httpPort, tabletId)
        def lastCumuTimeAfter = statusAfter['last cumulative success time']
        assertEquals(
            lastCumuTimeBefore,
            lastCumuTimeAfter,
            "Read cluster should skip compaction in MULTI_VERSION_READ_WRITE mode")
    }
}
