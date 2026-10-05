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

import org.junit.Assert
import org.awaitility.Awaitility

import java.util.concurrent.TimeUnit

suite("test_cloud_full_compaction_do_lease","nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def tableName = "test_cloud_full_compaction_do_lease"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName}
            (k int, v1 int, v2 int )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH (k) 
            BUCKETS 1  PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write"="true",
                "disable_auto_compaction" = "true");
        """

    (1..20).each{ id -> 
        sql """insert into ${tableName} select number, number, number from numbers("number"="10");"""
    }

    qt_sql "select count(1) from ${tableName};"

    def backends = sql_return_maparray('show backends')
    def tabletStats = sql_return_maparray("show tablets from ${tableName};")
    assert tabletStats.size() == 1
    def tabletId = tabletStats[0].TabletId
    def tabletBackendId = tabletStats[0].BackendId
    def tabletBackend
    for (def be : backends) {
        if (be.BackendId == tabletBackendId) {
            tabletBackend = be
            break;
        }
    }
    logger.info("tablet ${tabletId} on backend ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}");

    def getTabletCompactionStatus = {
        def (code, out, err) = be_show_tablet_status(
                tabletBackend.Host, tabletBackend.HttpPort, tabletId)
        assert code == 0: "show tablet status failed, out=${out}, err=${err}"
        return parseJson(out.trim())
    }

    def isFullCompactionRunning = {
        def tasks = sql_return_maparray """
            SELECT COMPACTION_ID
            FROM information_schema.be_compaction_tasks
            WHERE BACKEND_ID = ${tabletBackendId}
              AND TABLET_ID = ${tabletId}
              AND COMPACTION_TYPE = 'full'
              AND TRIGGER_METHOD = 'MANUAL'
              AND STATUS = 'RUNNING'
        """
        return !tasks.isEmpty()
    }

    def hasActiveCumulativeCompaction = {
        def tasks = sql_return_maparray """
            SELECT COMPACTION_ID
            FROM information_schema.be_compaction_tasks
            WHERE BACKEND_ID = ${tabletBackendId}
              AND TABLET_ID = ${tabletId}
              AND COMPACTION_TYPE = 'cumulative'
              AND TRIGGER_METHOD = 'MANUAL'
              AND STATUS IN ('PENDING', 'RUNNING')
        """
        return !tasks.isEmpty()
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def customBeConfig = [
        lease_compaction_interval_seconds : 2
    ]
    def originalLeaseIntervals = get_be_param("lease_compaction_interval_seconds")
    int maxOriginalLeaseInterval = originalLeaseIntervals.values()
            .collect { Integer.parseInt(it.toString()) }
            .max()

    setBeConfigTemporary(customBeConfig) {
        // A lease thread that started before the config update may still be sleeping with the old
        // interval. Wait for that sleep plus one new interval before creating the full compaction.
        Thread.sleep((maxOriginalLeaseInterval + customBeConfig.lease_compaction_interval_seconds + 1) * 1000L)
        try {
            // block the full compaction
            GetDebugPoint().enableDebugPointForAllBEs("CloudFullCompaction::modify_rowsets.block")

            GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                    [tablet_id:"${tabletId}", start_version:"2", end_version:"10"]);

            def fullStatusBeforeTrigger = getTabletCompactionStatus()
            def fullSuccessTimeBeforeTrigger = fullStatusBeforeTrigger["last full success time"]
            def fullFailureTimeBeforeTrigger = fullStatusBeforeTrigger["last full failure time"]

            // The HTTP API only confirms that the task was queued. RUNNING is set after the worker
            // has acquired the Meta Service global lock, so it is the correct start of the lease window.
            logger.info("trigger full compaction on BE ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}")
            def (code, out, err) = be_run_full_compaction(tabletBackend.Host, tabletBackend.HttpPort, tabletId)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assert code == 0
            def compactJson = parseJson(out.trim())
            assert "success" == compactJson.status.toLowerCase()

            Awaitility.await().atMost(60, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS).until {
                isFullCompactionRunning()
            }

            // The initial lease is valid for lease_compaction_interval_seconds * 4 = 8 seconds.
            // Keep the full compaction blocked for longer so the test depends on periodic renewal.
            Thread.sleep(10000)
            assertTrue(isFullCompactionRunning(),
                    "full compaction should still be running after the initial lease expires")

            // trigger cumu compaction
            def cumuStatusBeforeTrigger = getTabletCompactionStatus()
            def cumuFailureTimeBeforeTrigger = cumuStatusBeforeTrigger["last cumulative failure time"]
            def cumuSuccessTimeBeforeTrigger = cumuStatusBeforeTrigger["last cumulative success time"]

            logger.info("trigger cumu compaction on BE ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}")
            (code, out, err) = be_run_cumulative_compaction(tabletBackend.Host, tabletBackend.HttpPort, tabletId)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assert code == 0
            compactJson = parseJson(out.trim())
            // Cloud compaction submission is asynchronous. The submit request succeeds;
            // the queued cumulative task then observes the existing full compaction.
            assert "success" == compactJson.status.toLowerCase()

            // A global-lock failure happens before execute_compact(), so it updates the failure
            // timestamp but not "last cumulative status". Wait until the competing task has
            // failed and left the active-task list, then verify it did not compact any rowsets.
            Awaitility.await().atMost(30, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS).until {
                def tabletStatus = getTabletCompactionStatus()
                return tabletStatus["last cumulative failure time"] != cumuFailureTimeBeforeTrigger &&
                        !hasActiveCumulativeCompaction()
            }

            def cumuStatusAfterTrigger = getTabletCompactionStatus()
            assertEquals(cumuSuccessTimeBeforeTrigger,
                    cumuStatusAfterTrigger["last cumulative success time"])
            assertTrue(isFullCompactionRunning(),
                    "full compaction should retain the tablet job after cumulative compaction is rejected")

            // unblock full compaction
            GetDebugPoint().disableDebugPointForAllBEs("CloudFullCompaction::modify_rowsets.block")

            // Full compaction updates its local tablet cache before publishing the success time.
            // Wait for both signals instead of relying on a fixed sleep or lazy-commit cache refresh.
            Awaitility.await().atMost(60, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS).until {
                def tabletStatus = getTabletCompactionStatus()
                return tabletStatus["last full success time"] != fullSuccessTimeBeforeTrigger &&
                        tabletStatus["rowsets"].toString().contains("[2-21]")
            }

            def finalTabletStatus = getTabletCompactionStatus()
            assertEquals(fullFailureTimeBeforeTrigger,
                    finalTabletStatus["last full failure time"])
            assertTrue(finalTabletStatus["rowsets"].toString().contains("[2-21]"))

        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("CloudFullCompaction::modify_rowsets.block")
            GetDebugPoint().disableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets")
        }
    }
}
