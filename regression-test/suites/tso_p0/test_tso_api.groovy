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

import org.apache.doris.regression.util.Http
import groovy.json.JsonSlurper

suite("test_tso_api", "nonConcurrent") {
    def tsoFeatureConfig = sql "SHOW FRONTEND CONFIG like '%experimental_enable_tso_feature%';"
    def tsoPersistConfig = sql "SHOW FRONTEND CONFIG like '%enable_tso_persist_journal%';"
    logger.info("${tsoFeatureConfig}")
    logger.info("${tsoPersistConfig}")
    try {
        sql "ADMIN SET FRONTEND CONFIG ('enable_tso_persist_journal' = 'true')"
        sql "ADMIN SET FRONTEND CONFIG ('experimental_enable_tso_feature' = 'true')"
        sleep(1000)
        def currentTime = System.currentTimeMillis()
        def masterFeHttpAddress = "${getMasterIp()}:${getMasterPort('http')}"

        // Test TSO API endpoint
        def url = String.format("http://%s/api/tso", masterFeHttpAddress)

        // Test 1: Basic TSO API access
        def result = Http.GET(url, true)
        logger.info("TSO API response: ${result}")

        assertTrue(result.code == 0)
        assertEquals(result.msg, "success")

        // Check that all expected fields are present in the response
        def data = result.data
        assertTrue(data.containsKey("window_end_physical_time"))
        assertTrue(data.containsKey("current_tso"))
        assertTrue(data.containsKey("current_tso_physical_time"))
        assertTrue(data.containsKey("current_tso_logical_counter"))

        // Validate that TSO values are reasonable
        assertTrue(data.window_end_physical_time > 0)
        assertTrue(data.current_tso > 0)
        assertTrue(data.current_tso_physical_time > 0)
        assertTrue(data.current_tso_logical_counter >= 0)

        // Test 2: Multiple TSO API calls should return consistent increasing values
        def result1 = Http.GET(url, true)
        Thread.sleep(10) // Small delay to ensure time progression
        def result2 = Http.GET(url, true)

        assertTrue(result1.code == 0)
        assertTrue(result2.code == 0)

        def tso1 = result1.data.current_tso
        def tso2 = result2.data.current_tso

        // TSO should be monotonically increasing
        assertTrue(tso2 >= tso1)

        // Test 3: Validate TSO timestamp structure
        def physicalTime1 = result1.data.current_tso_physical_time
        def logicalCounter1 = result1.data.current_tso_logical_counter
        def physicalTime2 = result2.data.current_tso_physical_time
        def logicalCounter2 = result2.data.current_tso_logical_counter

        // Physical time should be consistent with TSO
        assertTrue(physicalTime1 <= tso1)
        assertTrue(physicalTime2 <= tso2)

        // Test 4: Validate window end time is in the future
        def windowEndTime = result1.data.window_end_physical_time
        assertTrue(windowEndTime >= currentTime)

        // Test 5: Test unauthorized access (without credentials)
        try {
            def unauthorizedResult = Http.GET(url, false) // false means no auth
            // Depending on server configuration, this might return 401 or still work
            logger.info("Unauthorized access result: ${unauthorizedResult}")
        } catch (Exception e) {
            logger.info("Expected unauthorized access exception: ${e.getMessage()}")
        }

        // Test 6: Validate TSO timestamp composition
        def tsoValue = result1.data.current_tso
        def physicalTime = result1.data.current_tso_physical_time
        def logicalCounter = result1.data.current_tso_logical_counter

        // Validate that the TSO is composed correctly from physical time and logical counter
        // TSO format: 46 bits physical time + 18 bits logical counter
        def expectedTSO = (physicalTime << 18) | (logicalCounter & 0x3FFFFL)
        // Note: We're not checking exact equality because of the reserved bits in the middle

        // At least verify that the physical time part matches
        def extractedPhysicalTime = (tsoValue >> 18) & 0x3FFFFFFFFFFL // 46 bits mask
        assertEquals(physicalTime, extractedPhysicalTime)

        // And that the logical counter part matches (lowest 18 bits)
        def extractedLogicalCounter = tsoValue & 0x3FFFFL // 18 bits mask
        assertEquals(logicalCounter, extractedLogicalCounter)
    } finally {
        sql "ADMIN SET FRONTEND CONFIG ('experimental_enable_tso_feature' = 'false')"
        sql "ADMIN SET FRONTEND CONFIG ('enable_tso_persist_journal' = '${tsoPersistConfig[0][1]}')"
        sql "ADMIN SET FRONTEND CONFIG ('experimental_enable_tso_feature' = '${tsoFeatureConfig[0][1]}')"
    }

    logger.info("TSO API test completed successfully")
}
