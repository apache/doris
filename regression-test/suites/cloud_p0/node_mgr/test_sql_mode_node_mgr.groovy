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

suite('test_sql_mode_node_mgr', 'p1') {
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
                def requiredColumns = ['Name', 'IP', 'EditLogPort', 'HttpPort', 'QueryPort', 'RpcPort', 'Role', 'IsMaster', 'ClusterId', 'Join', 'Alive', 'ReplayedJournalId', 'LastHeartbeat', 'IsHelper', 'ErrMsg']
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
                def requiredBackendColumns = ['BackendId', 'Cluster', 'IP', 'HeartbeatPort', 'BePort', 'HttpPort', 'BrpcPort', 'LastStartTime', 'LastHeartbeat', 'Alive', 'SystemDecommissioned', 'ClusterDecommissioned', 'TabletNum', 'DataUsedCapacity', 'AvailCapacity', 'TotalCapacity', 'UsedPct', 'MaxDiskUsedPct', 'Tag', 'ErrMsg', 'Version', 'Status']
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

            // 2. check read and write work.
            sql """ drop table if exists example_table """
            sql """ CREATE TABLE IF NOT EXISTS example_table (
                        id BIGINT,
                        username VARCHAR(20)
                    )
                    DISTRIBUTED BY HASH(id) BUCKETS 2
                    PROPERTIES (
                        "replication_num" = "1"
                    ); """
            def result = sql """ insert into example_table values(1, "1") """
            result = sql """ select * from example_table """
            assert result.size() == 1

            // 3. check restarting fe and be work.
            logger.info("Restarting frontends and backends...")
            cluster.restartFrontends();
            cluster.restartBackends();

            sleep(30000)
            context.reconnectFe()

            result = sql """ select * from example_table """
            assert result.size() == 1

            sql """ insert into example_table values(1, "1") """

            result = sql """ select * from example_table order by id """
            assert result.size() == 2

            // 4. If a be is dropped, query and writing also work.
            // Get the list of backends
            def backends = sql_return_maparray("SHOW BACKENDS")
            logger.info("Current backends: {}", backends)

            // Find a backend to drop
            def backendToDrop = backends[0]
            def backendHost = backendToDrop['Host']
            def backendHeartbeatPort = backendToDrop['HeartbeatPort']

            logger.info("Dropping backend: {}:{}", backendHost, backendHeartbeatPort)

            // Drop the selected backend
            sql """ ALTER SYSTEM DECOMMISSION BACKEND "${backendHost}:${backendHeartbeatPort}"; """

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

            // Verify the backend was dropped
            def remainingBackends = sql_return_maparray("SHOW BACKENDS")
            logger.info("Remaining backends: {}", remainingBackends)
            assert remainingBackends.size() == 2, "Expected 2 remaining backends"

            // Verify that write operations still work after dropping a backend
            sql """ INSERT INTO example_table VALUES (2, '2'); """

            // Verify that read operations still work after dropping a backend
            result = sql """ SELECT * FROM example_table ORDER BY id; """
            logger.info("Query result after dropping backend: {}", result)
            assert result.size() == 3, "Expected 3 rows in example_table after dropping backend"

            // 5. If a fe is dropped, query and writing also work.
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

            // Verify the frontend was dropped
            def remainingFrontends = sql_return_maparray("SHOW FRONTENDS")
            logger.info("Remaining frontends: {}", remainingFrontends)
            assert remainingFrontends.size() == frontends.size() - 1, "Expected ${frontends.size() - 1} remaining frontends"

            // Verify that write operations still work after dropping a frontend
            sql """ INSERT INTO example_table VALUES (3, '3'); """

            // Verify that read operations still work after dropping a frontend
            result = sql """ SELECT * FROM example_table ORDER BY id; """
            logger.info("Query result after dropping frontend: {}", result)
            assert result.size() == 4, "Expected 4 rows in example_table after dropping frontend"

            // 6. If fe can not drop itself.
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
                assert e.getMessage().contains("Cannot drop master"), "Unexpected exception message when trying to drop master frontend"
            }

            // Verify that the master frontend is still present
            def currentFrontends = sql_return_maparray("SHOW FRONTENDS")
            assert currentFrontends.find { it['IsMaster'] == "true" && it['Host'] == masterHost && it['EditLogPort'] == masterEditLogPort } != null, "Master frontend should still be present"

            logger.info("Successfully verified that the master frontend cannot be dropped")
        }
    }

}