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

suite("test_tablet_stat_syncer", "docker") {
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
        def tbl = 'test_tablet_stat_syncer_tbl'
        sql """ DROP TABLE IF EXISTS ${tbl} """

        def configResult = sql_return_maparray """ ADMIN SHOW FRONTEND CONFIG LIKE '%cloud_get_tablet_stats_version%' """
        assertTrue(configResult.size() > 0, "Expected to find cloud_get_tablet_stats_version config")
        def tabletStatsVersion = configResult[0].Value.toInteger()
        logger.info("cloud_get_tablet_stats_version = ${tabletStatsVersion}")
        if (tabletStatsVersion != 2) {
            logger.info("cloud_get_tablet_stats_version is not 2, skip test")
            return
        }

        def intervalConfigResult = sql_return_maparray """ ADMIN SHOW FRONTEND CONFIG LIKE '%tablet_stat_update_interval_second%' """
        assertTrue(intervalConfigResult.size() > 0, "Expected to find tablet_stat_update_interval_second config")
        def statUpdateInterval = intervalConfigResult[0].Value.toInteger()
        logger.info("tablet_stat_update_interval_second = ${statUpdateInterval}")

        sql """
            CREATE TABLE IF NOT EXISTS ${tbl} (
                id INT NOT NULL,
                name VARCHAR(50) NOT NULL,
                value INT NOT NULL
            )
            ENGINE = olap
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

        def tablets = sql_return_maparray """ show tablets from ${tbl} """
        assertTrue(tablets.size() > 0, "Expected at least one tablet")
        def tabletIds = tablets.collect { it.TabletId }.join(",")
        logger.info("Tablet IDs: ${tabletIds}")

        def initialRowCount = tablets[0].RowCount
        logger.info("Initial RowCount: ${initialRowCount}")

        GetDebugPoint().enableDebugPointForAllFEs("FE.CloudTabletStatMgr.addActiveTablets.ignore.tablets", [value: tabletIds])

        sql """ INSERT INTO ${tbl} VALUES (2, 'test', 200) """
        result = sql_return_maparray """ select * from ${tbl} where id = 2 """
        assertEquals(1, result.size(), "Data should be visible in query")

        def tabletsAfterInsert = sql_return_maparray """ show tablets from ${tbl} """
        def rowCountAfterInsert = tabletsAfterInsert[0].RowCount
        logger.info("RowCount after insert (before sync): ${rowCountAfterInsert}")

        GetDebugPoint().disableDebugPointForAllFEs("FE.CloudTabletStatMgr.addActiveTablets.ignore.tablets")

        def maxWaitMs = (statUpdateInterval * 3 + 5) * 1000
        def intervalMs = 1000
        def startTime = System.currentTimeMillis()
        def updated = false
        while (true) {
            def tabletsNow = sql_return_maparray """ show tablets from ${tbl} """
            def rowCountNow = tabletsNow[0].RowCount.toInteger()
            logger.info("RowCount get: ${rowCountNow}")
            if (rowCountNow == 2) {
                logger.info("RowCount updated: ${rowCountNow}")
                updated = true
                break
            }
            if (System.currentTimeMillis() - startTime > maxWaitMs) {
                throw new IllegalStateException("Timeout waiting for tablet stat update. Waited ${maxWaitMs}ms, RowCount still ${rowCountNow}")
            }
            Thread.sleep(intervalMs)
        }
        assertTrue(updated, "Tablet stat should eventually update after debug point disabled")
        tabletsAfterInsert = sql_return_maparray """ show tablets from ${tbl} """
        rowCountAfterInsert = tabletsAfterInsert[0].RowCount.toInteger()
        logger.info("RowCount after stat sync: ${rowCountAfterInsert}")
        assertEquals(2, rowCountAfterInsert, "RowCount should reflect the new row after stat sync")

        sql """ INSERT INTO ${tbl} VALUES (3, 'force_sync', 300) """
        def tabletsBeforeSync = sql_return_maparray """ show tablets from ${tbl} """
        def localDataSizeBeforeSync = tabletsBeforeSync[0].LocalDataSize
        def rowCountBeforeSync = tabletsBeforeSync[0].RowCount
        logger.info("LocalDataSize before force sync: ${localDataSizeBeforeSync}, RowCount: ${rowCountBeforeSync}")

        sql """ set cloud_force_sync_tablet_stats = true """
        def tabletsAfterForceSync = sql_return_maparray """ show tablets from ${tbl} """
        def localDataSizeAfterForceSync = tabletsAfterForceSync[0].LocalDataSize
        def rowCountAfterForceSync = tabletsAfterForceSync[0].RowCount
        logger.info("LocalDataSize after force sync: ${localDataSizeAfterForceSync}, RowCount: ${rowCountAfterForceSync}")
        sql """ set cloud_force_sync_tablet_stats = false """

        startTime = System.currentTimeMillis()
        updated = false
        while (true) {
            def tabletsNow = sql_return_maparray """ show tablets from ${tbl} """
            def rowCountNow = tabletsNow[0].RowCount.toInteger()
            logger.info("RowCount get 3: ${rowCountNow}")
            if (rowCountNow == 3) {
                logger.info("RowCount updated 3: ${rowCountNow}")
                updated = true
                break
            }
            if (System.currentTimeMillis() - startTime > maxWaitMs) {
                throw new IllegalStateException("Timeout waiting for tablet stat update. Waited ${maxWaitMs}ms, RowCount still ${rowCountNow}")
            }
            Thread.sleep(intervalMs)
        }
        assertTrue(updated, "Tablet stat should eventually update after debug point disabled")
        tabletsAfterInsert = sql_return_maparray """ show tablets from ${tbl} """
        rowCountAfterInsert = tabletsAfterInsert[0].RowCount.toInteger()
        logger.info("RowCount after stat sync: ${rowCountAfterInsert}")
        assertEquals(3, rowCountAfterInsert, "RowCount should reflect the new row after stat sync")
    }
}
