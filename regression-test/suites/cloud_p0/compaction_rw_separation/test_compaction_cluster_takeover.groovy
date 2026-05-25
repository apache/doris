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
import groovy.json.JsonOutput

/**
 * Test compaction cluster takeover when the original write cluster becomes unavailable.
 *
 * Scenario:
 * 1. Write cluster performs load, auto compaction is disabled at BE level initially
 * 2. Write cluster becomes SUSPENDED via MS API
 * 3. After timeout, enable auto compaction on read cluster BE via HTTP config update
 * 4. Read cluster should take over compaction via auto compaction
 *
 * This test uses auto compaction (not manual trigger) because manual trigger bypasses
 * the get_topn_tablets_to_compact logic where should_skip_compaction is checked.
 * Auto compaction is controlled via BE config (not table property) because ALTER TABLE
 * disable_auto_compaction has a known bug and may not take effect immediately.
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
        'cluster_status_cache_refresh_interval_sec=3',   // 3 seconds for faster testing
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'disable_auto_compaction=true',  // Disable auto compaction initially, enable via HTTP later
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

    def getCloudClusterId = { cluster_name ->
        def backends = sql """SHOW BACKENDS"""
        def cluster_be = backends.find { it[19].contains("""\"compute_group_name\" : \"${cluster_name}\"""") }
        if (cluster_be == null) {
            throw new RuntimeException("No BE found for cluster: ${cluster_name}")
        }
        def tag = cluster_be[19]
        def jsonSlurper = new JsonSlurper()
        def jsonObject = jsonSlurper.parseText(tag)
        return [cluster_id: jsonObject.compute_group_id, unique_id: jsonObject.cloud_unique_id]
    }

    // Helper function to set cluster status via MS HTTP API
    def setClusterStatus = { String unique_id, String cluster_id, String status, def ms ->
        def jsonOutput = new JsonOutput()
        def reqBody = [
            cloud_unique_id: unique_id,
            cluster: [
                cluster_id: cluster_id,
                cluster_status: status
            ]
        ]
        def js = jsonOutput.toJson(reqBody)
        logger.info("set_cluster_status request: ${js}")

        httpTest {
            endpoint ms.host + ':' + ms.httpPort
            uri "/MetaService/http/set_cluster_status?token=greedisgood9999"
            body js
            check { respCode, body ->
                logger.info("set_cluster_status response: ${body} ${respCode}")
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"), "Failed to set cluster status: ${body}")
            }
        }
    }

    docker(options) {
        def writeCluster = "primary_cluster"
        def readCluster = "secondary_cluster"

        // Add two clusters
        cluster.addBackend(1, writeCluster)
        cluster.addBackend(1, readCluster)

        // Get MS instance
        def ms = cluster.getAllMetaservices().get(0)
        logger.info("Meta service: ${ms.host}:${ms.httpPort}")

        def writeBe = getBeIpAndPort(writeCluster)
        def readBe = getBeIpAndPort(readCluster)
        def writeClusterInfo = getCloudClusterId(writeCluster)

        logger.info("Primary BE: ${writeBe.ip}:${writeBe.http_port}")
        logger.info("Secondary BE: ${readBe.ip}:${readBe.http_port}")
        logger.info("Primary cluster ID: ${writeClusterInfo.cluster_id}")

        // Use write cluster to create table and load data
        // Auto compaction is disabled at BE level, so no compaction will run initially
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
                "replication_num" = "1"
            );
        """

        // Insert data from primary cluster (this sets last_active_cluster_id)
        for (int i = 0; i < 10; i++) {
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
        assertEquals(10, result[0][0])

        // Now suspend the primary cluster using MS HTTP API
        logger.info("Suspending primary cluster to simulate unavailability...")
        setClusterStatus(writeClusterInfo.unique_id, writeClusterInfo.cluster_id, "SUSPENDED", ms)

        // Wait for cluster status cache to refresh + takeover timeout
        // cluster_status_cache_refresh_interval_sec=3, compaction_cluster_takeover_timeout_ms=10000
        logger.info("Waiting for cluster status cache refresh and takeover timeout...")
        sleep(15000)

        // Enable auto compaction on the read cluster BE via HTTP config update
        // (Using BE config instead of ALTER TABLE because ALTER TABLE disable_auto_compaction
        // has a known bug and may not take effect immediately)
        logger.info("Enabling auto compaction on read cluster BE...")
        def (code, out, err) = curl("POST",
            String.format("http://%s:%s/api/update_config?disable_auto_compaction=false",
                readBe.ip, readBe.http_port))
        logger.info("Update config response: code=${code}, out=${out}")

        // Wait for auto compaction to run on the read cluster
        logger.info("Waiting for auto compaction on read cluster after takeover...")
        sleep(30000)

        // Check that read cluster has compacted
        def readStatus = getTabletStatus(readBe.ip, readBe.http_port, tablet_id)
        logger.info("Read cluster tablet status: ${readStatus}")
        def readLastCumuTime = readStatus["last cumulative success time"]
        logger.info("Read cluster last cumulative success time: ${readLastCumuTime}")

        assertTrue(readLastCumuTime != "1970-01-01 08:00:00.000",
            "Read cluster should have taken over compaction after timeout, but last cumulative success time is: ${readLastCumuTime}")

        logger.info("Secondary cluster successfully took over compaction!")

        // ==========================================
        // Phase 2: Primary resumes, loads data, and takes back compaction control
        // ==========================================
        logger.info("=== Phase 2: Testing primary cluster recovery ===")

        // Resume the primary cluster
        logger.info("Resuming primary cluster...")
        setClusterStatus(writeClusterInfo.unique_id, writeClusterInfo.cluster_id, "NORMAL", ms)

        // Record secondary's compaction timestamp before primary recovery
        def readStatusBefore = getTabletStatus(readBe.ip, readBe.http_port, tablet_id)
        def readCumuTimeBefore = readStatusBefore["last cumulative success time"]
        logger.info("Secondary last cumulative success time before primary recovery: ${readCumuTimeBefore}")

        // Insert more data from primary cluster — this updates last_active_cluster_id back to primary
        sql """use @${writeCluster}"""
        for (int i = 10; i < 20; i++) {
            sql """INSERT INTO ${tableName} VALUES (${i}, 'value_${i}')"""
        }
        logger.info("Inserted more data from primary cluster")

        // Sync to secondary so both clusters have the new rowsets
        sql """use @${readCluster}"""
        def result2 = sql """SELECT COUNT(*) FROM ${tableName}"""
        assertEquals(20, result2[0][0])

        // Wait for cluster status cache to refresh so both clusters see updated last_active_cluster
        sleep(5000)

        // Enable auto compaction on both clusters
        logger.info("Enabling auto compaction on both cluster BEs...")
        def (code3, out3, err3) = curl("POST",
            String.format("http://%s:%s/api/update_config?disable_auto_compaction=false",
                writeBe.ip, writeBe.http_port))
        logger.info("Enable primary auto compaction response: code=${code3}, out=${out3}")
        def (code4, out4, err4) = curl("POST",
            String.format("http://%s:%s/api/update_config?disable_auto_compaction=false",
                readBe.ip, readBe.http_port))
        logger.info("Enable secondary auto compaction response: code=${code4}, out=${out4}")

        // Wait for auto compaction to run
        logger.info("Waiting for auto compaction after primary recovery...")
        sleep(30000)

        // Check that primary cluster has compacted after recovery
        def writeStatus = getTabletStatus(writeBe.ip, writeBe.http_port, tablet_id)
        logger.info("Primary cluster tablet status after recovery: ${writeStatus}")
        def writeLastCumuTime = writeStatus["last cumulative success time"]
        logger.info("Primary cluster last cumulative success time: ${writeLastCumuTime}")

        assertTrue(writeLastCumuTime != "1970-01-01 08:00:00.000",
            "Primary cluster should have resumed compaction after recovery, but last cumulative success time is: ${writeLastCumuTime}")

        logger.info("Primary cluster successfully took back compaction control!")

        // Check that secondary cluster did NOT do new compaction after primary took back control
        def readStatusAfter = getTabletStatus(readBe.ip, readBe.http_port, tablet_id)
        def readCumuTimeAfter = readStatusAfter["last cumulative success time"]
        logger.info("Secondary last cumulative success time after primary recovery: ${readCumuTimeAfter}")

        assertEquals(readCumuTimeBefore, readCumuTimeAfter,
            "Secondary cluster should have stopped compaction after primary recovered, " +
            "but cumulative time changed from ${readCumuTimeBefore} to ${readCumuTimeAfter}")

        // Clean up
        sql """ DROP TABLE IF EXISTS ${tableName} """

        logger.info("All takeover tests passed!")
    }
}
