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

suite("show_trash_nereids") {
    // Retrieve backend info dynamically to adapt to any environment (1 node or N nodes)
    def allBackends = sql """SHOW BACKENDS"""
    assertTrue(allBackends.size() > 0, "Test requires at least 1 backend in the cluster")

    // Extract HostPorts dynamically.
    // Based on actual SHOW BACKENDS output:
    // Index 1: Host (e.g., 172.17.0.1)
    // Index 2: HeartbeatPort (e.g., 9050) <--- This is the port used by SHOW TRASH!
    String firstBackendHost = allBackends[0][1].toString()
    String firstBackendPort = allBackends[0][2].toString()
    String firstBackendHostPort = "${firstBackendHost}:${firstBackendPort}"

    String nonExistentBackend = "1.2.3.4:9999" // A non-existent backend address

    // ========================================
    // Test 1: Verify backward compatibility (No filter)
    // ========================================
    def allTrashResults = sql """SHOW TRASH"""
    assertTrue(allTrashResults.size() == allBackends.size(),
            "Without filter, should return trash info for all backends. Expected: ${allBackends.size()}, Got: ${allTrashResults.size()}")

    // ========================================
    // Test 2: Verify legacy single backend syntax (Backward compatibility)
    // ========================================
    def legacySingleResult = sql """SHOW TRASH ON \"${firstBackendHostPort}\""""
    assertTrue(legacySingleResult.size() == 1,
            "With legacy single backend syntax, should return exactly 1 row. Expected: 1, Got: ${legacySingleResult.size()}")

    // Verify the returned backend matches the requested one
    // FIX: Column 1 is the 'Backend' string (IP:Port), not Column 0 (BackendId)
    assertTrue(legacySingleResult[0][1].toString() == firstBackendHostPort,
            "The returned backend should match the requested one. Expected: ${firstBackendHostPort}, Got: ${legacySingleResult[0][1]}")

    // ========================================
    // Test 3: Verify new syntax single backend filter
    // ========================================
    def newSingleResult = sql """SHOW TRASH ON (\"${firstBackendHostPort}\")"""
    assertTrue(newSingleResult.size() == 1,
            "With new syntax single backend filter, should return exactly 1 row. Expected: 1, Got: ${newSingleResult.size()}")

    assertTrue(newSingleResult[0][1].toString() == firstBackendHostPort,
            "The returned backend should match the requested one. Expected: ${firstBackendHostPort}, Got: ${newSingleResult[0][1]}")

    // ========================================
    // Test 4: Verify filter with non-existent backend
    // ========================================
    def mixedResults = sql """SHOW TRASH ON (\"${firstBackendHostPort}\", \"${nonExistentBackend}\")"""
    assertTrue(mixedResults.size() == 1,
            "With one existent and one non-existent backend, should return only 1 row. Expected: 1, Got: ${mixedResults.size()}")

    // ========================================
    // Test 5: Verify result with all non-existent backends
    // ========================================
    def emptyResults = sql """SHOW TRASH ON (\"${nonExistentBackend}\", \"5.6.7.8:9999\")"""
    assertTrue(emptyResults.size() == 0,
            "With all non-existent backends, should return empty result. Expected: 0, Got: ${emptyResults.size()}")

    // ========================================
    // Test 6: Verify filter for multiple backends (Only if >= 2 backends exist)
    // ========================================
    if (allBackends.size() >= 2) {
        // FIX: Use the same indices as Test 2 for consistency
        String secondBackendHost = allBackends[1][1].toString()
        String secondBackendPort = allBackends[1][2].toString()
        String secondBackendHostPort = "${secondBackendHost}:${secondBackendPort}"

        def multiResults = sql """SHOW TRASH ON (\"${firstBackendHostPort}\", \"${secondBackendHostPort}\")"""
        assertTrue(multiResults.size() == 2,
                "With filter for 2 backends, should return exactly 2 rows. Expected: 2, Got: ${multiResults.size()}")

        // Verify content
        // FIX: Column 1 is the 'Backend' string, not Column 0
        def r1 = multiResults[0][1].toString()
        def r2 = multiResults[1][1].toString()
        assertTrue(
                (r1 == firstBackendHostPort && r2 == secondBackendHostPort) ||
                        (r1 == secondBackendHostPort && r2 == firstBackendHostPort),
                "Returned backends should match the requested ones. Expected: [${firstBackendHostPort}, ${secondBackendHostPort}] in any order, Got: [${r1}, ${r2}]"
        )
    }

    // cloud-mode checks
    if (isCloudMode()) {
        // Add cloud mode specific logic here
    }
}
