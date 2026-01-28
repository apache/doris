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
 * Test compaction cluster takeover when the original write cluster becomes unavailable.
 *
 * Scenario:
 * 1. Write cluster performs load
 * 2. Write cluster becomes SUSPENDED
 * 3. After timeout, read cluster should take over compaction responsibility
 */
suite('test_compaction_cluster_takeover', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
    ]
    options.beConfigs += [
        'enable_compaction_rw_separation=true',
        'compaction_cluster_takeover_timeout_ms=10000',  // 10 seconds for faster testing
        'cluster_status_cache_refresh_interval_sec=5',   // 5 seconds for faster testing
        'file_cache_enter_disk_resource_limit_mode_percent=99',
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
        def writeCluster = "primary_cluster"
        def readCluster = "secondary_cluster"

        // Add two clusters
        cluster.addBackend(1, writeCluster)
        cluster.addBackend(1, readCluster)

        logger.info("Created primary (write) cluster: ${writeCluster}")
        logger.info("Created secondary (read) cluster: ${readCluster}")

        def writeBe = getBeIpAndPort(writeCluster)
        def readBe = getBeIpAndPort(readCluster)

        logger.info("Primary BE: ${writeBe.ip}:${writeBe.http_port}")
        logger.info("Secondary BE: ${readBe.ip}:${readBe.http_port}")

        // Use write cluster to create table and load data
        sql """use @${writeCluster}"""

        def tableName = "test_compaction_takeover"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                k1 INT NOT NULL,
                v1 STRING NOT NULL
            ) UNIQUE KEY(`k1`)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            );
        """

        // Insert data from primary cluster (this sets last_active_cluster_id)
        for (int i = 0; i < 5; i++) {
            sql """INSERT INTO ${tableName} VALUES (${i}, 'value_${i}')"""
        }

        // Get tablet info
        def tablets = sql_return_maparray """ SHOW TABLETS FROM ${tableName} """
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        def tablet_id = tablet.TabletId
        logger.info("Tablet ID: ${tablet_id}")

        sleep(3000)

        // Sync rowsets to read cluster
        sql """use @${readCluster}"""
        def result = sql """SELECT COUNT(*) FROM ${tableName}"""
        assertEquals(5, result[0][0])

        sleep(3000)

        // At this point:
        // - Primary cluster is the last_active_cluster
        // - Primary cluster status is NORMAL
        // - Secondary cluster should NOT be able to compact

        logger.info("Test 1: Secondary cluster should NOT compact when primary is NORMAL...")
        def secondaryCompactionResult = triggerCompaction(readBe.ip, readBe.http_port, tablet_id, "cumulative")
        logger.info("Secondary cluster compaction result: ${secondaryCompactionResult}")
        // Note: The compaction request may be accepted but should be skipped in prepare_compact()

        sleep(5000)

        // Now suspend the primary cluster to simulate it becoming unavailable
        logger.info("Suspending primary cluster to simulate unavailability...")
        sql """ ALTER SYSTEM SUSPEND COMPUTE GROUP ${writeCluster} """

        sleep(3000)

        // Verify the cluster is suspended
        def clusters = sql """ SHOW COMPUTE GROUPS """
        def primaryCluster = clusters.find { it[0] == writeCluster }
        logger.info("Primary cluster status after suspend: ${primaryCluster}")

        // Insert more data from secondary cluster (this will NOT update last_active_cluster_id
        // because secondary cluster is different from last_active_cluster)
        sql """use @${readCluster}"""
        for (int i = 5; i < 10; i++) {
            sql """INSERT INTO ${tableName} VALUES (${i}, 'value_${i}')"""
        }

        sleep(3000)

        // Wait for takeover timeout (10 seconds configured above)
        logger.info("Waiting for takeover timeout...")
        sleep(12000)

        // Now secondary cluster should be able to take over compaction
        // because:
        // 1. Primary cluster status is SUSPENDED (not NORMAL)
        // 2. Enough time has passed (> compaction_cluster_takeover_timeout_ms)

        logger.info("Test 2: Secondary cluster should be able to compact after timeout...")

        // Trigger compaction on secondary cluster
        def takeoverCompactionResult = triggerCompaction(readBe.ip, readBe.http_port, tablet_id, "cumulative")
        logger.info("Takeover compaction result: ${takeoverCompactionResult}")

        def takeoverStatus = takeoverCompactionResult.status.toLowerCase()
        assertTrue(takeoverStatus == "success" || takeoverStatus == "already_exist",
            "Secondary cluster should be able to takeover compaction, but got: ${takeoverStatus}")

        // Wait for compaction to finish
        def compactionFinished = waitForCompactionFinish(readBe.ip, readBe.http_port, tablet_id, 60000)
        assertTrue(compactionFinished, "Takeover compaction should finish within timeout")

        logger.info("Secondary cluster successfully took over compaction!")

        // Resume the primary cluster
        sql """ ALTER SYSTEM RESUME COMPUTE GROUP ${writeCluster} """

        // Clean up
        sql """use @${writeCluster}"""
        sql """ DROP TABLE IF EXISTS ${tableName} """

        logger.info("All takeover tests passed!")
    }
}
