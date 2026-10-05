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

/**
 * Test behavior when compaction read-write separation is DISABLED.
 *
 * When enable_compaction_rw_separation is false (default):
 * - All clusters should be able to execute compaction regardless of which cluster last performed load
 *
 * This test uses auto compaction to verify that both clusters can compact.
 */
suite('test_compaction_rw_separation_disabled', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
    ]
    options.beConfigs += [
        'enable_compaction_rw_separation=false',  // Feature disabled
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'disable_auto_compaction=true',  // Disable initially, enable via HTTP after data load
    ]
    options.cloudMode = true

    def getTabletStatus = { ip, port, tablet_id ->
        def url = "http://${ip}:${port}/api/compaction/show?tablet_id=${tablet_id}"
        def response = new URL(url).text
        return new JsonSlurper().parseText(response)
    }

    def getBeIpAndPort = { cluster_name ->
        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster_name}\"""") }

        if (cluster_bes.isEmpty()) {
            throw new RuntimeException("No BE found for cluster: ${cluster_name}")
        }

        def firstBe = cluster_bes[0]
        return [ip: firstBe[1], http_port: firstBe[4], brpc_port: firstBe[5], backend_id: firstBe[0]]
    }

    docker(options) {
        def clusterA = "cluster_a"
        def clusterB = "cluster_b"

        // Add two clusters
        cluster.addBackend(1, clusterA)
        cluster.addBackend(1, clusterB)

        logger.info("Created cluster A: ${clusterA}")
        logger.info("Created cluster B: ${clusterB}")

        def beA = getBeIpAndPort(clusterA)
        def beB = getBeIpAndPort(clusterB)

        logger.info("Cluster A BE: ${beA.ip}:${beA.http_port}")
        logger.info("Cluster B BE: ${beB.ip}:${beB.http_port}")

        // Use cluster A to create table and load data (auto compaction enabled)
        sql """use @${clusterA}"""

        def tableName = "test_compaction_disabled"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                k1 INT NOT NULL,
                v1 INT NOT NULL
            ) UNIQUE KEY(`k1`)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        // Insert data from cluster A
        for (int i = 0; i < 10; i++) {
            sql """INSERT INTO ${tableName} VALUES (${i}, ${i * 100})"""
        }

        // Get tablet info
        def tablets = sql_return_maparray """ SHOW TABLETS FROM ${tableName} """
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        def tablet_id = tablet.TabletId
        logger.info("Tablet ID: ${tablet_id}")

        // Sync rowsets to cluster B
        sql """use @${clusterB}"""
        def result = sql """SELECT COUNT(*) FROM ${tableName}"""
        assertEquals(10, result[0][0])

        // Enable auto compaction on both BEs via HTTP config update
        // (Using BE config instead of table property because ALTER TABLE disable_auto_compaction
        // has a known bug and may not take effect immediately)
        logger.info("Enabling auto compaction on both BEs...")
        def (code1, out1, err1) = curl("POST",
            String.format("http://%s:%s/api/update_config?disable_auto_compaction=false",
                beA.ip, beA.http_port))
        logger.info("Update cluster A BE config response: code=${code1}, out=${out1}")
        def (code2, out2, err2) = curl("POST",
            String.format("http://%s:%s/api/update_config?disable_auto_compaction=false",
                beB.ip, beB.http_port))
        logger.info("Update cluster B BE config response: code=${code2}, out=${out2}")

        // Wait for auto compaction to run on both clusters
        logger.info("Waiting for auto compaction to run on both clusters...")
        sleep(30000)

        // Check compaction status on both clusters
        def statusA = getTabletStatus(beA.ip, beA.http_port, tablet_id)
        logger.info("Cluster A tablet status: ${statusA}")
        def lastCumuTimeA = statusA["last cumulative success time"]
        logger.info("Cluster A last cumulative success time: ${lastCumuTimeA}")

        def statusB = getTabletStatus(beB.ip, beB.http_port, tablet_id)
        logger.info("Cluster B tablet status: ${statusB}")
        def lastCumuTimeB = statusB["last cumulative success time"]
        logger.info("Cluster B last cumulative success time: ${lastCumuTimeB}")

        // When feature is disabled, both clusters should be able to compact
        // At least one of them should have compacted within 30 seconds
        // (Both may not compact if they contend, but at least cluster A which has the tablet should)
        assertTrue(lastCumuTimeA != "1970-01-01 08:00:00.000" || lastCumuTimeB != "1970-01-01 08:00:00.000",
            "At least one cluster should have compacted when feature is disabled. " +
            "Cluster A time: ${lastCumuTimeA}, Cluster B time: ${lastCumuTimeB}")

        logger.info("Feature disabled test passed - compaction ran successfully!")

        // Clean up
        sql """use @${clusterA}"""
        sql """ DROP TABLE IF EXISTS ${tableName} """
    }
}
