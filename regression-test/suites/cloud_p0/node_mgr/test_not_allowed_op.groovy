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

suite('test_not_allowed_op', 'p0') {
    if (!isCloudMode()) {
        return;
    }

    // Test modifying frontend is not allowed
    try {
        // Get current frontend information
        def frontendResult = sql_return_maparray """SHOW FRONTENDS"""
        logger.info("Current frontends: ${frontendResult}")

        // Extract the first frontend's information
        def firstFrontend = frontendResult[0]
        def frontendHost = firstFrontend['Host']
        def frontendEditLogPort = firstFrontend['EditLogPort']

        // Construct the frontend address
        def frontendAddress = "${frontendHost}:${frontendEditLogPort}"
        logger.info("Attempting to modify frontend: ${frontendAddress}")
        def result = sql """ ALTER SYSTEM MODIFY FRONTEND "${frontendAddress}" HOSTNAME 'localhost' """
        logger.info("Modify frontend result: ${result}")
        throw new IllegalStateException("Expected exception was not thrown")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Modifying frontend hostname is not supported in cloud mode"))
    }

    // Get current backend information
    def backendResult = sql_return_maparray """SHOW BACKENDS"""
    logger.info("Current backends: ${backendResult}")

    // Extract the first backend's information
    def firstBackend = backendResult[0]
    def backendHost = firstBackend['Host']
    def backendHeartbeatPort = firstBackend['HeartbeatPort']
    
    // Construct the backend address
    def backendAddress = "${backendHost}:${backendHeartbeatPort}"
    // Test modifying backend is not allowed
    try {
        logger.info("Attempting to modify backend: ${backendAddress}")
        def result = sql """ ALTER SYSTEM MODIFY BACKEND '${backendAddress}' SET("tag.location" = "tag1") """
        logger.info("Modify backend result: ${result}")
        throw new IllegalStateException("Expected exception was not thrown")
    } catch (Exception e) {
        logger.info("Caught expected exception: ${e.getMessage()}")
        assertTrue(e.getMessage().contains("Modifying backends is not supported in cloud mode"))
    }

    // Test modifying backend hostname is not allowed
    try {
        sql """ ALTER SYSTEM MODIFY BACKEND "${backendAddress}" HOSTNAME 'localhost' """
        throw new IllegalStateException("Expected exception was not thrown")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Modifying backend hostname is not supported in cloud mode"))
    }

    logger.info("All tests for disallowed operations in cloud mode passed successfully")
}