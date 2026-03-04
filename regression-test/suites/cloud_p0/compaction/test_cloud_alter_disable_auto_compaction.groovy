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

suite("test_cloud_alter_disable_auto_compaction", "p0,docker") {
    if (!isCloudMode()) {
        logger.info("not cloud mode, skip this test")
        return
    }

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
    ]
    options.beConfigs += [
        // Short sync interval for faster property propagation
        'tablet_sync_interval_s=5',
        // Trigger cumulative compaction after 5 rowsets
        'cumulative_compaction_min_deltas=5',
        // Enable file cache (required for sync_meta to work)
        'enable_file_cache=true',
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true

    docker(options) {
        def tableName = "test_cloud_alter_disable_auto_compaction"

        // Get BE address for metrics
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
        def beIp = backendId_to_backendIP.values()[0]
        def beHttpPort = backendId_to_backendHttpPort.values()[0]
        logger.info("BE address: ${beIp}:${beHttpPort}")

        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NULL,
                `name` varchar(255) NULL,
                `score` int(11) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        // Verify initial state: disable_auto_compaction should be false (default)
        def result = sql """ SHOW CREATE TABLE ${tableName}; """
        logger.info("Initial SHOW CREATE TABLE result: ${result}")
        assertFalse(result[0][1].contains('"disable_auto_compaction" = "true"'))

        // ============================================================
        // Test 1: Verify compaction works when enabled (default)
        // ============================================================
        logger.info("Test 1: Verify compaction works when enabled")

        // Get initial compaction metric
        def getCompactionMetric = {
            return get_be_metric(beIp, beHttpPort, 'compaction_deltas_total', "cumulative")
        }
        def initialCompactionCount = getCompactionMetric()
        logger.info("Initial compaction count: ${initialCompactionCount}")

        // Insert enough rowsets to trigger compaction (need > 5 for cumulative)
        for (int i = 0; i < 8; i++) {
            sql """ INSERT INTO ${tableName} VALUES (${i}, "test${i}", ${i * 100}); """
        }

        // Wait for compaction to trigger (may take a few seconds)
        def compactionTriggered = false
        for (int retry = 0; retry < 30; retry++) {
            sleep(2000)
            def currentCount = getCompactionMetric()
            logger.info("Retry ${retry}: current compaction count = ${currentCount}")
            if (currentCount > initialCompactionCount) {
                compactionTriggered = true
                break
            }
        }
        assertTrue(compactionTriggered, "Expected compaction to be triggered when disable_auto_compaction is false")

        // Verify data is correct
        def count = sql """ SELECT count(*) FROM ${tableName}; """
        assertEquals(8, count[0][0])

        // ============================================================
        // Test 2: ALTER TABLE to disable compaction
        // ============================================================
        logger.info("Test 2: Verify compaction is disabled after ALTER TABLE")

        sql """ ALTER TABLE ${tableName} SET ("disable_auto_compaction" = "true"); """

        // Verify the property is updated in FE
        result = sql """ SHOW CREATE TABLE ${tableName}; """
        logger.info("After setting true, SHOW CREATE TABLE result: ${result}")
        assertTrue(result[0][1].contains('"disable_auto_compaction" = "true"'))

        // Wait for sync_meta to propagate the change to BE
        // tablet_sync_interval_s is set to 5 seconds, so wait a bit longer
        logger.info("Waiting for sync_meta to propagate disable_auto_compaction=true to BE...")
        sleep(10000)

        // Record compaction count after disabling
        def countAfterDisable = getCompactionMetric()
        logger.info("Compaction count after disabling: ${countAfterDisable}")

        // Insert more rowsets - these should NOT trigger compaction
        for (int i = 8; i < 16; i++) {
            sql """ INSERT INTO ${tableName} VALUES (${i}, "test${i}", ${i * 100}); """
        }

        // Wait and verify compaction does NOT happen
        sleep(15000) // Wait longer than typical compaction interval
        def countAfterInsert = getCompactionMetric()
        logger.info("Compaction count after inserting with disabled compaction: ${countAfterInsert}")

        // The compaction count should not have increased significantly
        // Allow for small variations due to other internal operations
        assertTrue(countAfterInsert <= countAfterDisable + 1,
            "Compaction should not be triggered when disable_auto_compaction is true. " +
            "Before: ${countAfterDisable}, After: ${countAfterInsert}")

        // Verify data is still correct
        count = sql """ SELECT count(*) FROM ${tableName}; """
        assertEquals(16, count[0][0])

        // ============================================================
        // Test 3: Re-enable compaction and verify it works again
        // ============================================================
        logger.info("Test 3: Verify compaction resumes after re-enabling")

        sql """ ALTER TABLE ${tableName} SET ("disable_auto_compaction" = "false"); """

        // Verify the property is updated
        result = sql """ SHOW CREATE TABLE ${tableName}; """
        logger.info("After setting false, SHOW CREATE TABLE result: ${result}")
        assertFalse(result[0][1].contains('"disable_auto_compaction" = "true"'))

        // Wait for sync_meta to propagate the change
        logger.info("Waiting for sync_meta to propagate disable_auto_compaction=false to BE...")
        sleep(10000)

        def countBeforeReEnable = getCompactionMetric()
        logger.info("Compaction count before re-enabling: ${countBeforeReEnable}")

        // Insert more rowsets - these SHOULD trigger compaction
        for (int i = 16; i < 24; i++) {
            sql """ INSERT INTO ${tableName} VALUES (${i}, "test${i}", ${i * 100}); """
        }

        // Wait for compaction to trigger
        compactionTriggered = false
        for (int retry = 0; retry < 30; retry++) {
            sleep(2000)
            def currentCount = getCompactionMetric()
            logger.info("Retry ${retry}: current compaction count = ${currentCount}")
            if (currentCount > countBeforeReEnable) {
                compactionTriggered = true
                break
            }
        }
        assertTrue(compactionTriggered, "Expected compaction to resume after re-enabling")

        // Verify final data
        count = sql """ SELECT count(*) FROM ${tableName}; """
        assertEquals(24, count[0][0])

        // Clean up
        sql """ DROP TABLE IF EXISTS ${tableName}; """

        logger.info("All tests passed!")
    }
}
