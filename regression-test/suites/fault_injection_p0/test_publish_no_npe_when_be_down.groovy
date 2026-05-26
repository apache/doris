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
import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

// Regression test: when AgentTaskCleanupDaemon force-finishes in-flight PublishVersionTasks
// for a dead BE, the master FE must NOT enter a permanent NPE loop in
// PublishVersionDaemon -> DatabaseTransactionMgr.checkReplicaContinuousVersionSucc due to
// PublishVersionTask.getSuccTablets() returning null.
//
// Pre-fix behaviour: master FE log fills with
//   NullPointerException: ... PublishVersionTask.getSuccTablets() is null
//     at DatabaseTransactionMgr.checkReplicaContinuousVersionSucc(...:1478)
// for the affected transaction at ~tens of times per second, indefinitely.
//
// Post-fix behaviour: replicas of the dead BE are routed through the normal error/version-
// failed branches; the transaction either succeeds (if quorum holds on remaining BEs) or
// fails cleanly via the standard publish timeout. No NPE shows up in the FE log.
suite("test_publish_no_npe_when_be_down", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.beNum = 3
    options.feNum = 1
    options.enableDebugPoints()
    // Speed up AgentTaskCleanupDaemon: 5s interval * 3 failures = 15s to force-finish.
    options.feConfigs += [
        "agent_task_health_check_intervals_ms=5000",
        "publish_version_interval_ms=10",
        "publish_version_timeout_second=60",
        // Enable debug logs for AgentTaskCleanupDaemon so we can assert the
        // force-finish path actually fired below.
        "sys_log_verbose_modules=org.apache.doris.task.AgentTaskCleanupDaemon",
    ]

    docker(options) {
        GetDebugPoint().clearDebugPointsForAllBEs()

        def tblName = "test_publish_no_npe_when_be_down"
        sql """ DROP TABLE IF EXISTS ${tblName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tblName} (
                `k` int NOT NULL,
                `v` int NOT NULL
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 8
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 3"
            )
        """

        // Slow down BE-side publish so we can stop a BE while publish is in-flight.
        // The spin-wait debug point keeps the task in AgentTaskQueue until BE down,
        // so AgentTaskCleanupDaemon will be the actor that flips isFinished=true without
        // populating succTablets - exactly the regression trigger path.
        GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait")
        GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.block")

        // Kick off a write that produces an in-flight publish.
        def loadFuture = thread {
            try {
                sql """
                    INSERT INTO ${tblName}
                    SELECT number, number FROM numbers("number" = "1024")
                """
            } catch (Throwable t) {
                logger.warn("expected: insert may fail when BE is killed mid-publish: ${t.message}")
            }
        }

        // Let the publish actually start before we kill the BE.
        sleep(2000)

        // Stop one BE — the in-flight PublishVersionTask for that BE will be force-finished
        // by AgentTaskCleanupDaemon after MAX_FAILURE_TIMES (3) * interval (5s) = ~15s.
        cluster.stopBackends(1)

        // Wait for cleanup to fire (~20s) plus a few more daemon poll cycles, so any latent
        // NPE has time to surface on the master FE.
        sleep(45000)

        // The smoking gun: master FE log must not contain the getSuccTablets() NPE signature.
        def feIndex = 1
        def fe = cluster.getFeByIndex(feIndex)
        assertNotNull(fe, "master FE handle missing")
        def feLogPath = "${fe.path}/log/fe.log"

        def npeCmdResult = ["bash", "-c",
                "grep -c 'PublishVersionTask.getSuccTablets()\\\" is null' ${feLogPath} || true"
        ].execute().text.trim()
        logger.info("getSuccTablets NPE occurrences in master FE log: ${npeCmdResult}")
        assertEquals("0", npeCmdResult,
                "master FE log must not contain the getSuccTablets() NPE after the fix")

        def stackCmdResult = ["bash", "-c",
                "grep -c 'checkReplicaContinuousVersionSucc' ${feLogPath} || true"
        ].execute().text.trim()
        logger.info("checkReplicaContinuousVersionSucc stack frames in master FE log: ${stackCmdResult}")
        assertEquals("0", stackCmdResult,
                "no NPE stack should mention checkReplicaContinuousVersionSucc")

        // Confirm AgentTaskCleanupDaemon actually fired for the stopped BE — proves we
        // exercised the exact regression path, not a no-op.
        def cleanupCmdResult = ["bash", "-c",
                "grep -c 'BE down, remove agent task' ${feLogPath} || true"
        ].execute().text.trim()
        logger.info("AgentTaskCleanupDaemon force-finish count: ${cleanupCmdResult}")
        assertTrue(Integer.parseInt(cleanupCmdResult) > 0,
                "AgentTaskCleanupDaemon should have force-finished tasks for the stopped BE")

        cluster.getAllBackends(true).each { be ->
            DebugPoint.disableDebugPoint(be.host, be.httpPort, NodeType.BE, "EnginePublishVersionTask::execute.block")
            DebugPoint.disableDebugPoint(be.host, be.httpPort, NodeType.BE,
                    "EnginePublishVersionTask::execute.enable_spin_wait")
        }
        loadFuture.get()

        // Cluster should still be healthy: the table is queryable, transactions are not stuck.
        cluster.startBackends(1)
        sleep(5000)
        GetDebugPoint().clearDebugPointsForAllBEs()

        def rowCount = sql """ SELECT COUNT(*) FROM ${tblName} """
        logger.info("rowCount after recovery: ${rowCount}")
    }
}
