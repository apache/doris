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

suite('test_sql_mode_node_mgr', 'docker,p1') {
    if (!isCloudMode()) {
        return;
    }

    def clusterOptions = [
        new ClusterOptions(),
        new ClusterOptions(),
    ]

    for (options in clusterOptions) {
        options.feConfigs += [
            'cloud_cluster_check_interval_second=1',
        ]
        options.cloudMode = true
        options.sqlModeNodeMgr = true
        options.waitTimeout = 0
        options.feNum = 3
        options.feConfigs += ["resource_not_ready_sleep_seconds=1",
                "heartbeat_interval_second=1",]
    }

    clusterOptions[0].beCloudInstanceId = true;
    clusterOptions[0].beMetaServiceEndpoint = true;

    clusterOptions[1].beCloudInstanceId = false;
    clusterOptions[1].beMetaServiceEndpoint = false;

    for (options in clusterOptions) {
        docker(options) {
            logger.info("docker started");

            def checkFrontendsAndBackends = {
                // Check frontends
                def frontendResult = sql_return_maparray """show frontends;"""
                logger.info("show frontends result {}", frontendResult)
                // Check that we have the expected number of frontends
                assert frontendResult.size() == 3, "Expected 3 frontends, but got ${frontendResult.size()}"

                // Check that all required columns are present
                def requiredColumns = ['Name', 'Host', 'EditLogPort', 'HttpPort', 'QueryPort', 'RpcPort', 'Role', 'IsMaster', 'ClusterId', 'Join', 'Alive', 'ReplayedJournalId', 'LastHeartbeat', 'IsHelper', 'ErrMsg']
                def actualColumns = frontendResult[0].keySet()
                assert actualColumns.containsAll(requiredColumns), "Missing required columns. Expected: ${requiredColumns}, Actual: ${actualColumns}"

                // Check that we have one master and two followers
                def masterCount = frontendResult.count { it['IsMaster'] == 'true' }
                assert masterCount == 1, "Expected 1 master, but got ${masterCount}"

                def followerCount = frontendResult.count { it['IsMaster'] == 'false' }
                assert followerCount == 2, "Expected 2 followers, but got ${followerCount}"

                // Check that all frontends are alive
                def aliveCount = frontendResult.count { it['Alive'] == 'true' }
                assert aliveCount == 3, "Expected all 3 frontends to be alive, but only ${aliveCount} are alive"

                // Check backends
                def backendResult = sql_return_maparray """show backends;"""
                logger.info("show backends result {}", backendResult)
                // Check that we have the expected number of backends
                assert backendResult.size() == 3, "Expected 3 backends, but got ${backendResult.size()}"

                // Check that all required columns are present
                def requiredBackendColumns = ['Host', 'HeartbeatPort', 'BePort', 'HttpPort', 'BrpcPort', 'LastStartTime', 'LastHeartbeat', 'Alive', 'SystemDecommissioned', 'TabletNum', 'DataUsedCapacity', 'AvailCapacity', 'TotalCapacity', 'UsedPct', 'MaxDiskUsedPct', 'Tag', 'ErrMsg', 'Version', 'Status']
                def actualBackendColumns = backendResult[0].keySet()
                assert actualBackendColumns.containsAll(requiredBackendColumns), "Missing required backend columns. Expected: ${requiredBackendColumns}, Actual: ${actualBackendColumns}"

                // Check that all backends are alive
                def aliveBackendCount = backendResult.count { it['Alive'] == 'true' }
                assert aliveBackendCount == 3, "Expected all 3 backends to be alive, but only ${aliveBackendCount} are alive"

                // Check that no backends are decommissioned
                def decommissionedCount = backendResult.count { it['SystemDecommissioned'] == 'true' || it['ClusterDecommissioned'] == 'true' }
                assert decommissionedCount == 0, "Expected no decommissioned backends, but found ${decommissionedCount}"

                // Check that all backends have valid capacities
                backendResult.each { backend ->
                    assert backend['DataUsedCapacity'] != null && backend['AvailCapacity'] != null && backend['TotalCapacity'] != null, "Backend ${backend['BackendId']} has invalid capacity values"
                    assert backend['UsedPct'] != null && backend['MaxDiskUsedPct'] != null, "Backend ${backend['BackendId']} has invalid disk usage percentages"
                }

                logger.info("All backend checks passed successfully")
            }

            // Call the function to check frontends and backends
            checkFrontendsAndBackends()

            def checkClusterStatus = { int expectedFeNum, int expectedBeNum, int existsRows ->
                logger.info("Checking cluster status...")
                
                // Check FE number
                def frontendResult = sql_return_maparray """SHOW FRONTENDS;"""
                assert frontendResult.size() == expectedFeNum, "Expected ${expectedFeNum} frontends, but got ${frontendResult.size()}"
                logger.info("FE number check passed: ${frontendResult.size()} FEs found")

                // Check BE number
                def backendResult = sql_return_maparray """SHOW BACKENDS;"""
                assert backendResult.size() == expectedBeNum, "Expected ${expectedBeNum} backends, but got ${backendResult.size()}"
                logger.info("BE number check passed: ${backendResult.size()} BEs found")

                // Create table if not exists
                sql """
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT,
                    name VARCHAR(50)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES("replication_num" = "3");
                """

                logger.info("Table 'test_table' created or already exists")
                sql """ INSERT INTO test_table VALUES (1, 'test1') """

                def result = sql """ SELECT * FROM test_table ORDER BY id """
                assert result.size() == existsRows + 1, "Expected ${existsRows + 1} rows, but got ${result.size()}"
                logger.info("Read/write check passed: ${result.size()} rows found")

                logger.info("All cluster status checks passed successfully")
            }

            // Call the function to check cluster status
            checkClusterStatus(3, 3, 0)

            // CASE 1 . check restarting fe and be work.
            logger.info("Restarting frontends and backends...")
            cluster.restartFrontends();
            cluster.restartBackends();

            sleep(30000)
            context.reconnectFe()

            checkClusterStatus(3, 3, 1)

            // CASE 2. If a be is dropped, query and writing also work.
            // Get the list of backends
            def backends = sql_return_maparray("SHOW BACKENDS")
            logger.info("Current backends: {}", backends)

            // Find a backend to drop
            def backendToDrop = backends[0]
            def backendHost = backendToDrop['Host']
            def backendHeartbeatPort = backendToDrop['HeartbeatPort']

            logger.info("Dropping backend: {}:{}", backendHost, backendHeartbeatPort)

            // Drop the selected backend
            sql """ ALTER SYSTEM DROPP BACKEND "${backendHost}:${backendHeartbeatPort}"; """

            // Wait for the backend to be fully dropped
            def maxWaitSeconds = 300
            def waited = 0
            while (waited < maxWaitSeconds) {
                def currentBackends = sql_return_maparray("SHOW BACKENDS")
                if (currentBackends.size() == 2) {
                    logger.info("Backend successfully dropped")
                    break
                }
                sleep(10000)
                waited += 10
            }

            if (waited >= maxWaitSeconds) {
                throw new Exception("Timeout waiting for backend to be dropped")
            }

            checkClusterStatus(3, 2, 2)

            // CASE 3. Add the dropped backend back
            logger.info("Adding back the dropped backend: {}:{}", backendHost, backendHeartbeatPort)
            sql """ ALTER SYSTEM ADD BACKEND "${backendHost}:${backendHeartbeatPort}"; """

            // Wait for the backend to be fully added back
            maxWaitSeconds = 300
            waited = 0
            while (waited < maxWaitSeconds) {
                def currentBackends = sql_return_maparray("SHOW BACKENDS")
                if (currentBackends.size() == 3) {
                    logger.info("Backend successfully added back")
                    break
                }
                sleep(10000)
                waited += 10
            }

            if (waited >= maxWaitSeconds) {
                throw new Exception("Timeout waiting for backend to be added back")
            }

            checkClusterStatus(3, 3, 3)

            // CASE 4. If a fe is dropped, query and writing also work.
            // Get the list of frontends
            def frontends = sql_return_maparray("SHOW FRONTENDS")
            logger.info("Current frontends: {}", frontends)

            // Find a non-master frontend to drop
            def feToDropMap = frontends.find { it['IsMaster'] == "false" }
            assert feToDropMap != null, "No non-master frontend found to drop"

            def feHost = feToDropMap['Host']
            def feEditLogPort = feToDropMap['EditLogPort']

            logger.info("Dropping non-master frontend: {}:{}", feHost, feEditLogPort)

            // Drop the selected non-master frontend
            sql """ ALTER SYSTEM DROP FOLLOWER "${feHost}:${feEditLogPort}"; """

            // Wait for the frontend to be fully dropped
            maxWaitSeconds = 300
            waited = 0
            while (waited < maxWaitSeconds) {
                def currentFrontends = sql_return_maparray("SHOW FRONTENDS")
                if (currentFrontends.size() == frontends.size() - 1) {
                    logger.info("Non-master frontend successfully dropped")
                    break
                }
                sleep(10000)
                waited += 10
            }

            if (waited >= maxWaitSeconds) {
                throw new Exception("Timeout waiting for non-master frontend to be dropped")
            }

            checkClusterStatus(2, 3, 4)

            // CASE 5. Add dropped frontend back
            logger.info("Adding dropped frontend back")

            // Get the current list of frontends
            def currentFrontends = sql_return_maparray("SHOW FRONTENDS")
            
            // Find the dropped frontend by comparing with the original list
            def droppedFE = frontends.find { fe ->
                !currentFrontends.any { it['Host'] == fe['Host'] && it['EditLogPort'] == fe['EditLogPort'] }
            }
            
            assert droppedFE != null, "Could not find the dropped frontend"

            feHost = droppedFE['Host']
            feEditLogPort = droppedFE['EditLogPort']

            logger.info("Adding back frontend: {}:{}", feHost, feEditLogPort)

            // Add the frontend back
            sql """ ALTER SYSTEM ADD FOLLOWER "${feHost}:${feEditLogPort}"; """

            // Wait for the frontend to be fully added back
            maxWaitSeconds = 300
            waited = 0
            while (waited < maxWaitSeconds) {
                def updatedFrontends = sql_return_maparray("SHOW FRONTENDS")
                if (updatedFrontends.size() == frontends.size()) {
                    logger.info("Frontend successfully added back")
                    break
                }
                sleep(10000)
                waited += 10
            }

            if (waited >= maxWaitSeconds) {
                throw new Exception("Timeout waiting for frontend to be added back")
            }

            // Verify cluster status after adding the frontend back
            checkClusterStatus(3, 3, 5)

            logger.info("Frontend successfully added back and cluster status verified")

            // CASE 6. If fe can not drop itself.
            // 6. Attempt to drop the master FE and expect an exception
            logger.info("Attempting to drop the master frontend")

            // Get the master frontend information
            def masterFE = frontends.find { it['IsMaster'] == "true" }
            assert masterFE != null, "No master frontend found"

            def masterHost = masterFE['Host']
            def masterEditLogPort = masterFE['EditLogPort']

            logger.info("Attempting to drop master frontend: {}:{}", masterHost, masterEditLogPort)

            try {
                sql """ ALTER SYSTEM DROP FOLLOWER "${masterHost}:${masterEditLogPort}"; """
                throw new Exception("Expected an exception when trying to drop master frontend, but no exception was thrown")
            } catch (Exception e) {
                logger.info("Received expected exception when trying to drop master frontend: {}", e.getMessage())
                assert e.getMessage().contains("can not drop current master node."), "Unexpected exception message when trying to drop master frontend"
            }

            // Verify that the master frontend is still present
            currentFrontends = sql_return_maparray("SHOW FRONTENDS")
            assert currentFrontends.find { it['IsMaster'] == "true" && it['Host'] == masterHost && it['EditLogPort'] == masterEditLogPort } != null, "Master frontend should still be present"
            logger.info("Successfully verified that the master frontend cannot be dropped")


            // CASE 7. Attempt to drop a non-existent backend
            logger.info("Attempting to drop a non-existent backend")

            // Generate a non-existent host and port
            def nonExistentHost = "non.existent.host"
            def nonExistentPort = 12345

            try {
                sql """ ALTER SYSTEM DROPP BACKEND "${nonExistentHost}:${nonExistentPort}"; """
                throw new Exception("Expected an exception when trying to drop non-existent backend, but no exception was thrown")
            } catch (Exception e) {
                logger.info("Received expected exception when trying to drop non-existent backend: {}", e.getMessage())
                assert e.getMessage().contains("backend does not exists"), "Unexpected exception message when trying to drop non-existent backend"
            }

            // Verify that the number of backends remains unchanged
            def currentBackends = sql_return_maparray("SHOW BACKENDS")
            def originalBackendCount = 3 // As per the initial setup in this test
            assert currentBackends.size() == originalBackendCount, "Number of backends should remain unchanged after attempting to drop a non-existent backend"

            checkClusterStatus(3, 3, 6)

            // CASE 8. Decommission a backend and verify the process
            logger.info("Attempting to decommission a backend")

            // Get the list of current backends
            backends = sql_return_maparray("SHOW BACKENDS")
            assert backends.size() >= 1, "Not enough backends to perform decommission test"

            // Select a backend to decommission (not the first one, as it might be the master)
            def backendToDecommission = backends[1]
            def decommissionHost = backendToDecommission['Host']
            def decommissionPort = backendToDecommission['HeartbeatPort']

            logger.info("Decommissioning backend: {}:{}", decommissionHost, decommissionPort)

            // Decommission the selected backend
            sql """ ALTER SYSTEM DECOMMISSION BACKEND "${decommissionHost}:${decommissionPort}"; """

            // Wait for the decommission process to complete (this may take some time in a real environment)
            int maxAttempts = 30
            int attempts = 0
            boolean decommissionComplete = false

            while (attempts < maxAttempts && !decommissionComplete) {
                currentBackends = sql_return_maparray("SHOW BACKENDS")
                def decommissionedBackend = currentBackends.find { it['Host'] == decommissionHost && it['HeartbeatPort'] == decommissionPort }

                logger.info("decomissionedBackend {}", decommissionedBackend)
                // TODO: decomisssion is alive?
                if (decommissionedBackend && decommissionedBackend['Alive'] == "true" && decommissionedBackend['SystemDecommissioned'] == "true") {
                    decommissionComplete = true
                } else {
                    attempts++
                    sleep(1000) // Wait for 1 second before checking again
                }
            }

            assert decommissionComplete, "Backend decommission did not complete within the expected time"

            // Verify that the decommissioned backend is no longer active
            def finalBackends = sql_return_maparray("SHOW BACKENDS")
            def decommissionedBackend = finalBackends.find { it['Host'] == decommissionHost && it['HeartbeatPort'] == decommissionPort }
            assert decommissionedBackend['Alive'] == "true", "Decommissioned backend should not be alive"
            assert decommissionedBackend['SystemDecommissioned'] == "true", "Decommissioned backend should have SystemDecommissioned set to true"

            logger.info("Successfully decommissioned backend and verified its status")

            checkClusterStatus(3, 3, 7)
        }
    }

}