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

suite('test_decommission_unfinished_2pc_txn', 'multi_cluster,docker') {
    def runDecommissionCaseWithConflictTxn = { boolean enableAbortConflictTxn, boolean expectDecommissionTimeout ->
        def options = new ClusterOptions()
        options.cloudMode = true
        options.setFeNum(1)
        options.setBeNum(2)
        options.feConfigs += [
                'cloud_cluster_check_interval_second=1',
                'cloud_upgrade_mgr_interval_second=1',
                'cloud_tablet_rebalancer_interval_second=1',
                'heartbeat_interval_second=1',
                'drop_backend_after_decommission=false',
                'enable_abort_txn_by_checking_coordinator_be=false',
                "enable_abort_txn_by_checking_conflict_txn=${enableAbortConflictTxn}",
        ]

        docker(options) {
            logger.info("start case: enable_abort_txn_by_checking_conflict_txn={}, expectDecommissionTimeout={}",
                    enableAbortConflictTxn, expectDecommissionTimeout)
            def clusterInfo = sql_return_maparray """show clusters"""
            def currentCluster = clusterInfo.find { it.is_current == 'TRUE' }
            assertNotNull(currentCluster)
            sql """use @${currentCluster.cluster}"""

            def dbName = context.config.getDbNameByFile(context.file)
            def tableName = "test_decommission_unfinished_2pc_txn_tbl_${enableAbortConflictTxn ? 'on' : 'off'}"
            Long txnId = null
            def finalStatuses = ['VISIBLE', 'ABORTED'] as Set

            def getTxnInfo = { long id ->
                def txns = sql_return_maparray "show transaction from ${dbName} where id = ${id}"
                assertEquals(1, txns.size())
                txns[0]
            }

            def getTxnStatus = { long id ->
                getTxnInfo(id).TransactionStatus as String
            }

            def getCoordinatorBeHost = { long id ->
                String coordinator = getTxnInfo(id).Coordinator as String
                assertTrue(coordinator.startsWith('BE:'), "expect BE coordinator but got ${coordinator}")
                coordinator.substring(coordinator.indexOf(':') + 1).trim()
            }

            def waitBackendAlive = { String host, boolean expectAlive, int timeoutSeconds ->
                for (int i = 0; i < timeoutSeconds; i++) {
                    def backend = sql_return_maparray("SHOW BACKENDS").find { it.Host == host }
                    boolean alive = backend != null && backend.Alive.toString().toBoolean()
                    if (i == 0 || i % 5 == 0 || alive == expectAlive) {
                        logger.info("wait backend alive round={}, host={}, expectAlive={}, actualAlive={}, txnId={}, txnStatus={}",
                                i, host, expectAlive, alive, txnId, txnId == null ? "N/A" : getTxnStatus(txnId))
                    }
                    if (alive == expectAlive) {
                        return true
                    }
                    sleep(1000)
                }
                logger.info("wait backend alive timeout, host={}, expectAlive={}, txnId={}, txnStatus={}",
                        host, expectAlive, txnId, txnId == null ? "N/A" : getTxnStatus(txnId))
                return false
            }

            def doStreamLoad2pcOperation = { long id, String operation ->
                def feHttpAddress = context.getFeHttpAddress()
                def command = "curl -sS -X PUT --location-trusted -u root: " +
                        " -H txn_id:${id}" +
                        " -H txn_operation:${operation}" +
                        " http://${feHttpAddress.hostString}:${feHttpAddress.port}/api/${dbName}/${tableName}/_stream_load_2pc"
                logger.info("execute stream load 2pc operation: {}", command)

                def process = command.execute()
                def code = process.waitFor()
                def out = process.text
                logger.info("stream load 2pc {} result: {}", operation, out)
                assertEquals(0, code)

                def resultText = out == null ? "" : out.trim()
                assertTrue(resultText.startsWith("{"), "stream load 2pc ${operation} non-json response: ${resultText}")
                def json = parseJson(resultText)
                def status = ((json.status != null ? json.status : json.Status) as String).toLowerCase()
                assertEquals('success', status)
            }

            def getDecommissionState = { String host, int heartbeatPort ->
                def backend = sql_return_maparray("SHOW BACKENDS").find {
                    it.Host == host && Integer.valueOf(it.HeartbeatPort.toString()) == heartbeatPort
                }
                if (backend == null) {
                    return [exists: false, active: false, systemDecommissioned: false, clusterDecommissioned: false]
                }
                def status = parseJson(backend.Status.toString())
                boolean active = status.isActive.toString().toBoolean()
                boolean systemDecommissioned = backend.SystemDecommissioned?.toString()?.toBoolean()
                boolean clusterDecommissioned = backend.ClusterDecommissioned?.toString()?.toBoolean()
                [exists: true, active: active, systemDecommissioned: systemDecommissioned,
                        clusterDecommissioned: clusterDecommissioned]
            }

            def waitDecommissionFinished = { String host, int heartbeatPort, int timeoutSeconds ->
                for (int i = 0; i < timeoutSeconds; i++) {
                    def state = getDecommissionState(host, heartbeatPort)
                    boolean finished = state.exists && !state.active
                            && (state.systemDecommissioned || state.clusterDecommissioned)
                    if (i == 0 || i % 5 == 0 || finished) {
                        logger.info("wait decommission round={}, host={}, heartbeatPort={}, exists={}, isActive={},"
                                + " systemDecommissioned={}, clusterDecommissioned={}, txnId={}, txnStatus={}",
                                i, host, heartbeatPort, state.exists, state.active, state.systemDecommissioned,
                                state.clusterDecommissioned, txnId, txnId == null ? "N/A" : getTxnStatus(txnId))
                    }
                    if (finished) {
                        return true
                    }
                    sleep(1000)
                }
                def state = getDecommissionState(host, heartbeatPort)
                logger.info("wait decommission timeout, host={}, heartbeatPort={}, exists={}, isActive={},"
                        + " systemDecommissioned={}, clusterDecommissioned={}, txnId={}, txnStatus={}",
                        host, heartbeatPort, state.exists, state.active, state.systemDecommissioned,
                        state.clusterDecommissioned, txnId, txnId == null ? "N/A" : getTxnStatus(txnId))
                return false
            }

            try {
                sql """
                    CREATE TABLE IF NOT EXISTS ${tableName} (
                        k1 INT,
                        k2 INT
                    )
                    DUPLICATE KEY(k1)
                    DISTRIBUTED BY HASH(k1) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                """

                String content = "1,10\n2,20\n3,30\n"
                streamLoad {
                    table "${tableName}"
                    set 'column_separator', ','
                    set 'two_phase_commit', 'true'
                    inputStream new ByteArrayInputStream(content.getBytes())
                    time 10000

                    check { loadResult, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        logger.info("stream load result: {}", loadResult)
                        def json = parseJson(loadResult)
                        def status = ((json.status != null ? json.status : json.Status) as String).toLowerCase()
                        assertEquals('success', status)
                        txnId = Long.valueOf(json.TxnId.toString())
                        assertTrue(txnId != null && txnId > 0)
                    }
                }

                def rowCount = sql "select count(*) from ${tableName}"
                assertEquals(0, rowCount[0][0] as int)

                for (int i = 0; i < 10; i++) {
                    def txnStatus = getTxnStatus(txnId)
                    logger.info("txn {} status after {}s: {}", txnId, i, txnStatus)
                    assertTrue(!finalStatuses.contains(txnStatus))
                    sleep(1000)
                }

                String coordinatorBeHost = getCoordinatorBeHost(txnId)
                def coordinatorBeNode = cluster.getAllBackends().find { it.host == coordinatorBeHost }
                assertNotNull(coordinatorBeNode)
                def txnInfo = getTxnInfo(txnId)
                logger.info("txn detail before restart, txnId={}, coordinator={}, status={}, label={}",
                        txnId, txnInfo.Coordinator, txnInfo.TransactionStatus, txnInfo.Label)

                // Why restart the coordinator BE here:
                // 1) CloudUpgradeMgr only aborts conflict txns when
                //    enable_abort_txn_by_checking_conflict_txn=true.
                // 2) checkFailedTxns marks a BE-coordinated txn as failed when the coordinator BE
                //    has restarted (be.lastStartTime > coordinator.startTime) or is offline.
                // 3) Without a coordinator BE restart, this unfinished 2PC txn usually stays PRECOMMITTED
                //    and is not treated as failed, so the true/false branches can behave similarly.
                // 4) Restarting the coordinator BE makes the behavior deterministic:
                //    - config=false: txn is not auto-aborted, decommission should timeout.
                //    - config=true: txn is detected as failed and aborted, decommission should finish.
                cluster.stopBackends(coordinatorBeNode.index)
                assertTrue(waitBackendAlive(coordinatorBeHost, false, 30),
                        "coordinator backend should become dead after stop")
                cluster.startBackends(coordinatorBeNode.index)
                assertTrue(waitBackendAlive(coordinatorBeHost, true, 60),
                        "coordinator backend should become alive after restart")
                assertTrue(!finalStatuses.contains(getTxnStatus(txnId)))
                logger.info("finish coordinator restart, txnId={}, coordinatorBeHost={}, txnStatus={}",
                        txnId, coordinatorBeHost, getTxnStatus(txnId))

                def showBes = sql_return_maparray """SHOW BACKENDS"""
                def decommissionBe = showBes.find { it.Host == coordinatorBeHost }
                assertNotNull(decommissionBe)
                def tag = parseJson(decommissionBe.Tag.toString())
                String cloudUniqueId = tag.cloud_unique_id as String
                String clusterName = (tag.compute_group_name != null ? tag.compute_group_name
                        : tag.cloud_cluster_name) as String
                String clusterId = (tag.compute_group_id != null ? tag.compute_group_id : tag.cloud_cluster_id) as String
                assertNotNull(cloudUniqueId)
                assertNotNull(clusterName)
                assertNotNull(clusterId)

                int heartbeatPort = Integer.valueOf(decommissionBe.HeartbeatPort.toString())
                logger.info("begin decommission backend, txnId={}, host={}, heartbeatPort={}, cloudUniqueId={}",
                        txnId, decommissionBe.Host, heartbeatPort, cloudUniqueId)
                def ms = cluster.getAllMetaservices().get(0)
                d_node.call(cloudUniqueId, decommissionBe.Host, heartbeatPort, clusterName, clusterId, ms)

                int timeoutSeconds = expectDecommissionTimeout ? 30 : 180
                boolean finished = waitDecommissionFinished(decommissionBe.Host as String, heartbeatPort, timeoutSeconds)
                if (expectDecommissionTimeout) {
                    assertFalse(finished, "decommission should timeout when "
                            + "enable_abort_txn_by_checking_conflict_txn=false")
                    logger.info("decommission timed out as expected, txnId={}, txnStatus={}",
                            txnId, getTxnStatus(txnId))
                    assertTrue(!finalStatuses.contains(getTxnStatus(txnId)))
                } else {
                    assertTrue(finished, "decommission should finish when "
                            + "enable_abort_txn_by_checking_conflict_txn=true")
                    awaitUntil(30) {
                        getTxnStatus(txnId) == 'ABORTED'
                    }
                    logger.info("decommission finished as expected, txnId={}, txnStatus={}", txnId, getTxnStatus(txnId))
                }
            } finally {
                if (txnId != null) {
                    try {
                        logger.info("cleanup begin, txnId={}, txnStatus={}", txnId, getTxnStatus(txnId))
                        if (!finalStatuses.contains(getTxnStatus(txnId))) {
                            doStreamLoad2pcOperation(txnId, 'abort')
                            awaitUntil(30) {
                                getTxnStatus(txnId) == 'ABORTED'
                            }
                            logger.info("cleanup abort done, txnId={}, txnStatus={}", txnId, getTxnStatus(txnId))
                        }
                    } catch (Exception e) {
                        logger.info("abort unfinished txn {} failed in cleanup: {}", txnId, e.getMessage())
                    }
                }
                sql "drop table if exists ${tableName}"
            }
        }
    }

    runDecommissionCaseWithConflictTxn(false, true)
    runDecommissionCaseWithConflictTxn(true, false)
}
