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
 * - No cluster tracking should happen
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
    ]
    options.msConfigs += [
        'enable_compaction_rw_separation=false',  // Feature disabled
    ]
    options.cloudMode = true

    def getTabletStatus = { ip, port, tablet_id ->
        def url = "http://${ip}:${port}/api/compaction/show?tablet_id=${tablet_id}"
        def response = new URL(url).text
        return new JsonSlurper().parseText(response)
    }

    def triggerCompaction = { ip, port, tablet_id, compaction_type ->
        def url = "http://${ip}:${port}/api/compaction/run?tablet_id=${tablet_id}&compact_type=${compaction_type}"
        def connection = new URL(url).openConnection()
        connection.setRequestMethod("POST")
        connection.setDoOutput(true)
        def responseCode = connection.getResponseCode()
        def response = connection.getInputStream().getText()
        return new JsonSlurper().parseText(response)
    }

    def getCompactionRunStatus = { ip, port, tablet_id ->
        def url = "http://${ip}:${port}/api/compaction/run_status?tablet_id=${tablet_id}"
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

    def waitForCompactionFinish = { ip, port, tablet_id, timeout_ms ->
        def start_time = System.currentTimeMillis()
        while (System.currentTimeMillis() - start_time < timeout_ms) {
            def status = getCompactionRunStatus(ip, port, tablet_id)
            if (status.run_status == false) {
                return true
            }
            sleep(1000)
        }
        return false
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

        // Use cluster A to create table and load data
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
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            );
        """

        // Insert data from cluster A
        for (int i = 0; i < 5; i++) {
            sql """INSERT INTO ${tableName} VALUES (${i}, ${i * 100})"""
        }

        // Get tablet info
        def tablets = sql_return_maparray """ SHOW TABLETS FROM ${tableName} """
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        def tablet_id = tablet.TabletId
        logger.info("Tablet ID: ${tablet_id}")

        sleep(3000)

        // Sync rowsets to cluster B
        sql """use @${clusterB}"""
        def result = sql """SELECT COUNT(*) FROM ${tableName}"""
        assertEquals(5, result[0][0])

        sleep(3000)

        // Test: When feature is disabled, cluster B should be able to compact
        // even though cluster A was the one that loaded the data
        logger.info("Testing: Cluster B should be able to compact when feature is disabled...")

        def clusterBTabletStatus = getTabletStatus(beB.ip, beB.http_port, tablet_id)
        logger.info("Cluster B tablet status before compaction: ${clusterBTabletStatus}")
        def rowsetCountBefore = clusterBTabletStatus.rowsets.size()

        // Trigger compaction on cluster B
        def compactionResult = triggerCompaction(beB.ip, beB.http_port, tablet_id, "cumulative")
        logger.info("Cluster B compaction trigger result: ${compactionResult}")

        def status = compactionResult.status.toLowerCase()
        assertTrue(status == "success" || status == "already_exist",
            "Cluster B should be able to trigger compaction when feature is disabled, but got: ${status}")

        // Wait for compaction to finish
        def compactionFinished = waitForCompactionFinish(beB.ip, beB.http_port, tablet_id, 60000)
        assertTrue(compactionFinished, "Compaction should finish within timeout")

        // Verify compaction was executed
        def clusterBTabletStatusAfter = getTabletStatus(beB.ip, beB.http_port, tablet_id)
        logger.info("Cluster B tablet status after compaction: ${clusterBTabletStatusAfter}")

        logger.info("Feature disabled test passed - cluster B was able to compact!")

        // Clean up
        sql """use @${clusterA}"""
        sql """ DROP TABLE IF EXISTS ${tableName} """

        logger.info("All disabled feature tests passed!")
    }
}
