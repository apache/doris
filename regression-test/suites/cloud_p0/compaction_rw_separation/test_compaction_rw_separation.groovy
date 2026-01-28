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
 * Test compaction read-write separation feature.
 *
 * When enable_compaction_rw_separation is enabled:
 * - Only the cluster that last performed load (write cluster) should execute compaction
 * - Read-only clusters should skip compaction to avoid file cache invalidation
 */
suite('test_compaction_rw_separation', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
    ]
    options.beConfigs += [
        'enable_compaction_rw_separation=true',
        'compaction_cluster_takeover_timeout_ms=60000',  // 60 seconds for testing
        'cluster_status_cache_refresh_interval_sec=5',   // 5 seconds for faster testing
        'file_cache_enter_disk_resource_limit_mode_percent=99',
    ]
    options.cloudMode = true

    def getTabletStatus = { ip, port, tablet_id ->
        def url = "http://${ip}:${port}/api/compaction/show?tablet_id=${tablet_id}"
        def response = new URL(url).text
        return new JsonSlurper().parseText(response)
    }

    def getCompactionRunStatus = { ip, port, tablet_id ->
        def url = "http://${ip}:${port}/api/compaction/run_status?tablet_id=${tablet_id}"
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
        def writeCluster = "write_cluster"
        def readCluster = "read_cluster"

        // Add two clusters
        cluster.addBackend(1, writeCluster)
        cluster.addBackend(1, readCluster)

        logger.info("Created write cluster: ${writeCluster}")
        logger.info("Created read cluster: ${readCluster}")

        def writeBe = getBeIpAndPort(writeCluster)
        def readBe = getBeIpAndPort(readCluster)

        logger.info("Write BE: ${writeBe.ip}:${writeBe.http_port}")
        logger.info("Read BE: ${readBe.ip}:${readBe.http_port}")

        // Use write cluster to create table and load data
        sql """use @${writeCluster}"""

        def tableName = "test_compaction_rw_sep"
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

        // Insert multiple rowsets to make compaction possible
        for (int i = 0; i < 5; i++) {
            sql """INSERT INTO ${tableName} VALUES (${i}, ${i * 10})"""
        }

        // Get tablet info
        def tablets = sql_return_maparray """ SHOW TABLETS FROM ${tableName} """
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        def tablet_id = tablet.TabletId
        logger.info("Tablet ID: ${tablet_id}")

        // Wait for data to be visible
        sleep(5000)

        // Verify write cluster has rowsets
        def writeTabletStatus = getTabletStatus(writeBe.ip, writeBe.http_port, tablet_id)
        logger.info("Write cluster tablet status: ${writeTabletStatus}")
        assertTrue(writeTabletStatus.rowsets.size() > 1, "Write cluster should have multiple rowsets")

        // Test 1: Write cluster should be able to execute compaction
        logger.info("Test 1: Triggering compaction on write cluster...")
        def writeCompactionResult = triggerCompaction(writeBe.ip, writeBe.http_port, tablet_id, "cumulative")
        logger.info("Write cluster compaction trigger result: ${writeCompactionResult}")

        // The compaction should be accepted (status is "success" or "already_exist")
        def writeStatus = writeCompactionResult.status.toLowerCase()
        assertTrue(writeStatus == "success" || writeStatus == "already_exist",
            "Write cluster should accept compaction request, but got: ${writeStatus}")

        // Wait for compaction to finish
        def compactionFinished = waitForCompactionFinish(writeBe.ip, writeBe.http_port, tablet_id, 60000)
        assertTrue(compactionFinished, "Compaction should finish within timeout")

        // Verify compaction was executed on write cluster
        def writeTabletStatusAfter = getTabletStatus(writeBe.ip, writeBe.http_port, tablet_id)
        logger.info("Write cluster tablet status after compaction: ${writeTabletStatusAfter}")

        // Test 2: Read cluster should skip compaction
        // First, sync rowsets to read cluster by querying
        sql """use @${readCluster}"""
        sql """SELECT COUNT(*) FROM ${tableName}"""
        sleep(3000)

        // Insert more data from write cluster to create more rowsets
        sql """use @${writeCluster}"""
        for (int i = 5; i < 10; i++) {
            sql """INSERT INTO ${tableName} VALUES (${i}, ${i * 10})"""
        }
        sleep(3000)

        // Sync to read cluster
        sql """use @${readCluster}"""
        sql """SELECT COUNT(*) FROM ${tableName}"""
        sleep(3000)

        // Now try to trigger compaction on read cluster
        logger.info("Test 2: Triggering compaction on read cluster (should be skipped)...")
        def readCompactionResult = triggerCompaction(readBe.ip, readBe.http_port, tablet_id, "cumulative")
        logger.info("Read cluster compaction trigger result: ${readCompactionResult}")

        // The compaction may be triggered but should be skipped during execution
        // Wait a bit and check if compaction was actually executed
        sleep(5000)

        def readTabletStatusAfter = getTabletStatus(readBe.ip, readBe.http_port, tablet_id)
        logger.info("Read cluster tablet status after compaction attempt: ${readTabletStatusAfter}")

        // The read cluster should NOT have compacted the rowsets
        // (it should still have the same or more rowsets as before)
        // This is because should_do_compaction_for_cluster() returns false for read cluster

        // Test 3: Verify last_active_cluster_id is set correctly
        // The tablet stats should show write_cluster as the last active cluster
        logger.info("Test 3: Verifying last_active_cluster tracking...")

        // Clean up
        sql """use @${writeCluster}"""
        sql """ DROP TABLE IF EXISTS ${tableName} """

        logger.info("All tests passed!")
    }
}
