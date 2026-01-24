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

// This suit test the `backends` tvf
suite("test_backends_tvf", "p0,external") {
    // 1. Determine if FQDN mode is enabled
    def fqdnResult = sql "SHOW FRONTEND CONFIG LIKE '%enable_fqdn_mode%'"
    def isFqdnEnabled = fqdnResult.size() > 0 && fqdnResult[0][1].toLowerCase() == "true"

    // 2. Fetch dynamic schema using desc function
    List<List<Object>> titleNames = sql """ describe function backends(); """
    assertTrue(titleNames.size() > 0)

    // Extract column names dynamically
    def tvfColumns = titleNames.collect { it[0] }

    // Helper function: get column index by name
    def getIndex = { String colName ->
        return tvfColumns.indexOf(colName)
    }

    // 3. Verify Ip column existence based on FQDN mode
    int ipIdx = getIndex("Ip")
    if (isFqdnEnabled) {
        assertTrue(ipIdx != -1, "Ip column should exist when FQDN is enabled")
    } else {
        assertEquals(-1, ipIdx, "Ip column should not exist when FQDN is disabled")
    }

    // 4. Execute SELECT query using dynamically generated column list
    def quotedColumnStr = tvfColumns.collect { "`${it}`" }.join(", ")
    List<List<Object>> table = sql """ select ${quotedColumnStr} from backends(); """
    assertTrue(table.size() > 0)

    // 5. Verify row size matches the schema size
    assertEquals(tvfColumns.size(), table[0].size())

    // 6. Verify SHOW BACKENDS command data matches TVF schema
    // Note: Due to MySQL protocol handling, SHOW commands might not return metadata via JDBC.
    // We verify the column count consistency instead.
    List<List<Object>> showBackendsResult = sql """ SHOW BACKENDS """
    assertTrue(showBackendsResult.size() > 0)
    assertEquals(tvfColumns.size(), showBackendsResult[0].size(),
            "Column count mismatch between SHOW BACKENDS and TVF")

    // 7. Verify SHOW PROC '/backends' matches TVF schema
    // Note: SHOW PROC also returns data rows without explicit metadata via JDBC.
    List<List<Object>> procResult = sql """ SHOW PROC '/backends' """
    assertTrue(procResult.size() > 0)
    assertEquals(tvfColumns.size(), procResult[0].size(),
            "Column count mismatch between SHOW PROC '/backends' and TVF")

    // 8. Conditionally verify the Ip column value if FQDN is enabled
    // NOTE: We only assert non-null here. We do NOT assert it's not "Unknown"
    // because DNS resolution depends on the external environment and could fail.
    table = sql """ select ${quotedColumnStr} from backends(); """
    if (isFqdnEnabled) {
        logger.info("FQDN mode is enabled. Verifying Ip column value...")
        def ipValue = table[0][ipIdx]
        logger.info("Backend IP is: ${ipValue}")
        assertNotNull(ipValue)
    }

    // 9. test exception
    test {
        sql """ select * from backends("backendId" = "10003"); """

        // check exception
        exception "backends table-valued-function does not support any params"
    }
}
