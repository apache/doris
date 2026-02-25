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
 *
 * This test uses auto compaction (not manual trigger) because manual trigger bypasses
 * the get_topn_tablets_to_compact logic where should_skip_compaction is checked.
 */
suite('test_compaction_rw_separation', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
    ]
    options.beConfigs += [
        'enable_compaction_rw_separation=true',
        'compaction_cluster_takeover_timeout_ms=600000',  // 10 min, ensure no takeover in test
        'cluster_status_cache_refresh_interval_sec=5',
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

        // Use write cluster to create table (auto compaction enabled)
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
                "replication_num" = "1"
            );
        """

        // Insert multiple rowsets from write cluster to trigger auto compaction
        for (int i = 0; i < 10; i++) {
            sql """INSERT INTO ${tableName} VALUES (${i}, ${i * 10})"""
        }

        // Get tablet info
        def tablets = sql_return_maparray """ SHOW TABLETS FROM ${tableName} """
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        def tablet_id = tablet.TabletId
        logger.info("Tablet ID: ${tablet_id}")

        // Sync rowsets to read cluster
        sql """use @${readCluster}"""
        def result = sql """SELECT COUNT(*) FROM ${tableName}"""
        assertEquals(10, result[0][0])

        // Enable auto compaction on both BEs via HTTP config update
        // (Using BE config instead of table property because ALTER TABLE disable_auto_compaction
        // has a known bug and may not take effect immediately)
        logger.info("Enabling auto compaction on both BEs...")
        def (code1, out1, err1) = curl("POST",
            String.format("http://%s:%s/api/update_config?disable_auto_compaction=false",
                writeBe.ip, writeBe.http_port))
        logger.info("Update write BE config response: code=${code1}, out=${out1}")
        def (code2, out2, err2) = curl("POST",
            String.format("http://%s:%s/api/update_config?disable_auto_compaction=false",
                readBe.ip, readBe.http_port))
        logger.info("Update read BE config response: code=${code2}, out=${out2}")

        // Wait for auto compaction to run on both clusters
        // Write cluster should compact, read cluster should skip
        logger.info("Waiting for auto compaction to run...")
        sleep(30000)

        // Check compaction status on write cluster
        def writeStatus = getTabletStatus(writeBe.ip, writeBe.http_port, tablet_id)
        logger.info("Write cluster tablet status: ${writeStatus}")
        def writeLastCumuTime = writeStatus["last cumulative success time"]
        logger.info("Write cluster last cumulative success time: ${writeLastCumuTime}")

        // Check compaction status on read cluster
        def readStatus = getTabletStatus(readBe.ip, readBe.http_port, tablet_id)
        logger.info("Read cluster tablet status: ${readStatus}")
        def readLastCumuTime = readStatus["last cumulative success time"]
        logger.info("Read cluster last cumulative success time: ${readLastCumuTime}")

        // Write cluster should have compacted (last success time is not the initial value)
        assertTrue(writeLastCumuTime != "1970-01-01 08:00:00.000",
            "Write cluster should have executed compaction, but last cumulative success time is: ${writeLastCumuTime}")

        // Read cluster should NOT have compacted (last success time should still be initial)
        assertTrue(readLastCumuTime == "1970-01-01 08:00:00.000",
            "Read cluster should NOT have executed compaction, but last cumulative success time is: ${readLastCumuTime}")

        logger.info("Test passed: Write cluster compacted, read cluster skipped!")

        // Clean up
        sql """use @${writeCluster}"""
        sql """ DROP TABLE IF EXISTS ${tableName} """
    }
}
