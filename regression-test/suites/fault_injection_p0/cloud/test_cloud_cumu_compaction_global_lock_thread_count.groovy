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

import groovy.json.JsonSlurper
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

suite('test_cloud_cumu_compaction_global_lock_thread_count', 'docker') {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.enableDebugPoints()
    options.beConfigs += [
        'enable_java_support=false',
        'cumulative_compaction_min_deltas=2',
        'cumulative_compaction_max_deltas=3',
        'max_cumu_compaction_threads=2',
        'large_cumu_compaction_task_min_thread_num=2',
        'large_cumu_compaction_task_row_num_threshold=1',
        'disable_auto_compaction=true',
    ]
    options.beNum = 3

    docker(options) {
        GetDebugPoint().clearDebugPointsForAllBEs()
        def backends = sql_return_maparray('SHOW BACKENDS')
        def lockHolderBe = backends[0]
        def lockFailedBe = backends[1]
        assertNotNull(lockHolderBe)
        assertNotNull(lockFailedBe)

        def getCompactionStatus = { be, tabletId ->
            def (code, out, err) = be_get_compaction_status(be.Host, be.HttpPort, tabletId)
            assertEquals(0, code, "failed to get compaction status: ${out}, ${err}")
            return new JsonSlurper().parseText(out.trim())
        }

        def waitForCompactionRunning = { be, tabletId ->
            def deadline = System.currentTimeMillis() + 30000
            while (System.currentTimeMillis() < deadline) {
                if (getCompactionStatus(be, tabletId).run_status) {
                    return
                }
                sleep(100)
            }
            assertTrue(false, "compaction did not start for tablet ${tabletId}")
        }

        def waitForCompactionFinished = { be, tabletId ->
            def deadline = System.currentTimeMillis() + 30000
            while (System.currentTimeMillis() < deadline) {
                if (!getCompactionStatus(be, tabletId).run_status) {
                    return
                }
                sleep(100)
            }
            assertTrue(false, "compaction did not finish for tablet ${tabletId}")
        }

        def getTabletStatus = { be, tabletId ->
            def (code, out, err) = be_show_tablet_status(be.Host, be.HttpPort, tabletId)
            assertEquals(0, code, "failed to get tablet status: ${out}, ${err}")
            return new JsonSlurper().parseText(out.trim())
        }

        def getRowsetCount = { be, tabletId ->
            def status = getTabletStatus(be, tabletId)
            assertTrue(status.rowsets instanceof List)
            return status.rowsets.size()
        }

        def createTable = { table ->
            sql "DROP TABLE IF EXISTS ${table} FORCE"
            sql """
                CREATE TABLE ${table} (
                    k INT,
                    v INT
                ) DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true"
                )
            """
            for (int value = 0; value < 5; value++) {
                sql "INSERT INTO ${table} VALUES (${value}, ${value})"
            }
            sql "SELECT COUNT(*) FROM ${table}"
            def tablet = sql_return_maparray("SHOW TABLETS FROM ${table}")[0]
            return tablet.TabletId
        }

        def runCumulativeCompaction = { be, tabletId ->
            def (code, out, err) = be_run_cumulative_compaction(be.Host, be.HttpPort, tabletId)
            assertEquals(0, code, "failed to submit cumulative compaction: ${out}, ${err}")
        }

        def conflictTabletId = createTable('test_cloud_cumu_compaction_global_lock_conflict')
        def conflictRowsetsBefore = getRowsetCount(lockFailedBe, conflictTabletId)
        def holderRowsetsBefore = getRowsetCount(lockHolderBe, conflictTabletId)
        def blockModifyRowsets = 'CloudCumulativeCompaction::modify_rowsets.enable_spin_wait'
        def blockModifyRowsetsSwitch = 'CloudCumulativeCompaction::modify_rowsets.block'
        def holdTaskAfterExecution = 'CloudStorageEngine._submit_cumulative_compaction_task.sleep'

        try {
            DebugPoint.enableDebugPoint(lockHolderBe.Host, lockHolderBe.HttpPort.toInteger(),
                    NodeType.BE, blockModifyRowsets)
            DebugPoint.enableDebugPoint(lockHolderBe.Host, lockHolderBe.HttpPort.toInteger(),
                    NodeType.BE, blockModifyRowsetsSwitch)

            runCumulativeCompaction(lockHolderBe, conflictTabletId)
            waitForCompactionRunning(lockHolderBe, conflictTabletId)

            runCumulativeCompaction(lockFailedBe, conflictTabletId)
            sleep(1000)
            waitForCompactionFinished(lockFailedBe, conflictTabletId)
            assertEquals(conflictRowsetsBefore, getRowsetCount(lockFailedBe, conflictTabletId))

            DebugPoint.disableDebugPoint(lockHolderBe.Host, lockHolderBe.HttpPort.toInteger(),
                    NodeType.BE, blockModifyRowsetsSwitch)
            DebugPoint.disableDebugPoint(lockHolderBe.Host, lockHolderBe.HttpPort.toInteger(),
                    NodeType.BE, blockModifyRowsets)
            waitForCompactionFinished(lockHolderBe, conflictTabletId)
            assertTrue(getRowsetCount(lockHolderBe, conflictTabletId) < holderRowsetsBefore)

            def runningTabletId = createTable('test_cloud_cumu_compaction_global_lock_thread_holder')
            def candidateTabletId = createTable('test_cloud_cumu_compaction_global_lock_thread_candidate')
            def candidateRowsetsBefore = getRowsetCount(lockFailedBe, candidateTabletId)
            def runningRowsetsBefore = getRowsetCount(lockFailedBe, runningTabletId)

            DebugPoint.enableDebugPoint(lockFailedBe.Host, lockFailedBe.HttpPort.toInteger(),
                    NodeType.BE, holdTaskAfterExecution)
            DebugPoint.enableDebugPoint(lockFailedBe.Host, lockFailedBe.HttpPort.toInteger(),
                    NodeType.BE, blockModifyRowsets)
            DebugPoint.enableDebugPoint(lockFailedBe.Host, lockFailedBe.HttpPort.toInteger(),
                    NodeType.BE, blockModifyRowsetsSwitch)

            runCumulativeCompaction(lockFailedBe, runningTabletId)
            waitForCompactionRunning(lockFailedBe, runningTabletId)

            DebugPoint.disableDebugPoint(lockFailedBe.Host, lockFailedBe.HttpPort.toInteger(),
                    NodeType.BE, blockModifyRowsetsSwitch)
            DebugPoint.disableDebugPoint(lockFailedBe.Host, lockFailedBe.HttpPort.toInteger(),
                    NodeType.BE, blockModifyRowsets)
            def deadline = System.currentTimeMillis() + 30000
            while (System.currentTimeMillis() < deadline &&
                    getRowsetCount(lockFailedBe, runningTabletId) >= runningRowsetsBefore) {
                sleep(100)
            }
            assertTrue(getRowsetCount(lockFailedBe, runningTabletId) < runningRowsetsBefore)
            assertTrue(getCompactionStatus(lockFailedBe, runningTabletId).run_status)

            runCumulativeCompaction(lockFailedBe, candidateTabletId)
            sleep(1000)
            assertEquals(candidateRowsetsBefore, getRowsetCount(lockFailedBe, candidateTabletId),
                    'a second large compaction must be delayed while the first task is active')
            waitForCompactionFinished(lockFailedBe, candidateTabletId)
            waitForCompactionFinished(lockFailedBe, runningTabletId)
        } finally {
            DebugPoint.disableDebugPoint(lockHolderBe.Host, lockHolderBe.HttpPort.toInteger(),
                    NodeType.BE, blockModifyRowsetsSwitch)
            DebugPoint.disableDebugPoint(lockHolderBe.Host, lockHolderBe.HttpPort.toInteger(),
                    NodeType.BE, blockModifyRowsets)
            DebugPoint.disableDebugPoint(lockFailedBe.Host, lockFailedBe.HttpPort.toInteger(),
                    NodeType.BE, holdTaskAfterExecution)
            DebugPoint.disableDebugPoint(lockFailedBe.Host, lockFailedBe.HttpPort.toInteger(),
                    NodeType.BE, blockModifyRowsetsSwitch)
            DebugPoint.disableDebugPoint(lockFailedBe.Host, lockFailedBe.HttpPort.toInteger(),
                    NodeType.BE, blockModifyRowsets)
        }
    }
}
