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

suite("test_version_syncer", "docker") {
    if (!isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.setFeNum(2)
    options.setBeNum(1)
    options.cloudMode = true
    options.enableDebugPoints()
    options.connectToFollower = true

    docker(options) {
        def tbl = 'test_version_syncer_tbl'
        sql """ DROP TABLE IF EXISTS ${tbl} """

        def enableSyncerConfig = sql_return_maparray """ ADMIN SHOW FRONTEND CONFIG LIKE '%cloud_enable_version_syncer%' """
        assertTrue(enableSyncerConfig.size() > 0, "Expected to find cloud_enable_version_syncer config")
        def enableSyncer = enableSyncerConfig[0].Value.toBoolean()
        logger.info("cloud_enable_version_syncer = ${enableSyncer}")
        if (!enableSyncer) {
            logger.info("FE.CloudGlobalTransactionMgr.updateVersion.disabled")
            return
        }
        def configResult = sql_return_maparray """ ADMIN SHOW FRONTEND CONFIG LIKE '%cloud_version_syncer_interval_second%' """
        assertTrue(configResult.size() > 0, "Expected to find cloud_version_syncer_interval_second config")
        def syncerInterval = configResult[0].Value.toInteger()
        logger.info("cloud_version_syncer_interval_second = ${syncerInterval}")

        sql """
                CREATE TABLE IF NOT EXISTS ${tbl} (
                    id INT NOT NULL,
                    name VARCHAR(50) NOT NULL,
                    value INT NOT NULL
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                );
        """

        def result = sql_return_maparray """ select * from ${tbl} """
        assertEquals(0, result.size())

        sql """ INSERT INTO ${tbl} VALUES (1, 'initial', 100) """
        result = sql_return_maparray """ select * from ${tbl} where id = 1 """
        assertEquals(1, result.size())

        DebugPoint.enableDebugPointForAllFEs('FE.CloudGlobalTransactionMgr.updateVersion.disabled')

        sql """ INSERT INTO ${tbl} VALUES (2, 'test', 200) """
        result = sql_return_maparray """ select * from ${tbl} where id = 2 """
        assertEquals(0, result.size(), "Data should not be visible before version sync")

        def maxWaitMs = (syncerInterval * 2 + 5) * 1000
        def intervalMs = 1000
        def startTime = System.currentTimeMillis()
        def found = false
        while (true) {
            result = sql_return_maparray """ select * from ${tbl} where id = 2 """
            if (result.size() == 1) {
                assertEquals('test', result[0].name)
                assertEquals(200, result[0].value)
                found = true
                break
            }
            if (System.currentTimeMillis() - startTime > maxWaitMs) {
                throw new IllegalStateException("Timeout waiting for data to be visible. Waited ${maxWaitMs}ms")
            }
            Thread.sleep(intervalMs)
        }
        assertTrue(found, "Data should eventually be visible after version syncer runs")

        result = sql_return_maparray """ select * from ${tbl} order by id """
        assertEquals(2, result.size())

        // Test cloud_force_sync_version session variable
        // Step 1: Insert data with version sync disabled, query should return no data
        sql """ INSERT INTO ${tbl} VALUES (3, 'force_sync', 300) """
        result = sql_return_maparray """ select * from ${tbl} where id = 3 """
        assertEquals(0, result.size(), "Data should not be visible before force sync")

        // Step 2: Enable session variable cloud_force_sync_version = true and execute show partitions
        sql """ set cloud_force_sync_version = true """
        def showPartitionsResult = sql_return_maparray """ show partitions from ${tbl} """
        logger.info("Show partitions result: ${showPartitionsResult}")

        // Step 3: Query again, data should now be visible
        result = sql_return_maparray """ select * from ${tbl} where id = 3 """
        assertEquals(1, result.size(), "Data should be visible after force sync")
    }
}
