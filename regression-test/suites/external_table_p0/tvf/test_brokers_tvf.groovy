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

// This suit test the `brokers` tvf
suite("test_brokers_tvf","p0") {
    def brokerClusterA = "brokers_tvf_cluster_a"
    def brokerClusterB = "brokers_tvf_cluster_b"
    def nonAdminUser = "brokers_tvf_non_admin_user"
    def nonAdminPwd = "12345"

    try {
        // 0. Clean up possible residual test clusters and users
        try { sql """ALTER SYSTEM DROP ALL BROKER ${brokerClusterA};""" } catch (Exception e) { log.info("Ignored: ${e.getMessage()}") }
        try { sql """ALTER SYSTEM DROP ALL BROKER ${brokerClusterB};""" } catch (Exception e) { log.info("Ignored: ${e.getMessage()}") }
        try { sql """DROP USER IF EXISTS ${nonAdminUser};""" } catch (Exception e) { log.info("Ignored: ${e.getMessage()}") }

        def address1 = "172.18.0.1"
        def address2 = "172.18.0.2"
        def address3 = "172.18.0.3"
        def address4 = "172.18.0.4"
        def port = 18310

        // 1. Get column metadata and build expected column expression
        def columns = new ArrayList()
        def columnMeta = sql """describe function brokers();"""
        def size = columnMeta.size()
        for (i in 0..<size) {
            columns.add(columnMeta[i][0])
        }
        def columnsExpr = String.join(",", columns)
        log.info("columns expr: ${columnsExpr}")

        // 2. Verify initial state is isolated and empty for both clusters
        def brokersA = sql """select ${columnsExpr} from brokers("cluster_name" = "${brokerClusterA}")"""
        assertTrue(brokersA.size() == 0)
        def brokersB = sql """select ${columnsExpr} from brokers("cluster_name" = "${brokerClusterB}")"""
        assertTrue(brokersB.size() == 0)

        // 3. Add two different broker clusters
        sql """alter system add broker ${brokerClusterA} "${address1}:${port}","${address2}:${port}";"""
        sql """alter system add broker ${brokerClusterB} "${address3}:${port}","${address4}:${port}";"""

        // 4. Verify scoped query returns exactly the nodes we added for each cluster
        brokersA = sql """select ${columnsExpr} from brokers("cluster_name" = "${brokerClusterA}") order by host;"""
        assertTrue(brokersA.size() == 2)
        assertTrue(brokersA[0][1] == "${address1}")
        assertTrue(brokersA[1][1] == "${address2}")

        brokersB = sql """select ${columnsExpr} from brokers("cluster_name" = "${brokerClusterB}") order by host;"""
        assertTrue(brokersB.size() == 2)
        assertTrue(brokersB[0][1] == "${address3}")
        assertTrue(brokersB[1][1] == "${address4}")

        // 5. Verify EXPLAIN plan contains TVF name and all columns
        def explainResult = sql """explain verbose select * from brokers();"""
        log.info("explain result: ${explainResult}")
        def explainStr = explainResult.collect { row -> row.join("") }.join("\n")
        assertTrue(explainStr.contains("BrokersTableValuedFunction"))
        for (i in 0..<size) {
            def colName = columns[i]
            log.info("current column name: ${colName}")
            assertTrue(explainStr.contains("${colName}"))
        }

        // 6. Compare TVF, SHOW BROKER, SHOW PROC with strict column mapping (Isolated via WHERE)
        // Query global brokers but filter by our test cluster names to avoid environmental dependencies
        def tvfSelectResult = sql """select ${columnsExpr} from brokers() where name in ('${brokerClusterA}', '${brokerClusterB}') order by host;"""
        def showBrokerResult = sql """show broker;"""
        def showBrokerProcResult = sql """show proc '/brokers';"""

        log.info("tvfSelectResult0: ${tvfSelectResult}")
        log.info("showBrokerResult0: ${showBrokerResult}")
        log.info("showBrokerProcResult0: ${showBrokerProcResult}")

        assertTrue(tvfSelectResult.size() == 4)

        // Filter global SHOW BROKER and SHOW PROC results to our test clusters and sort by host
        def filteredShowBroker = showBrokerResult.findAll { row -> row[0] in [brokerClusterA, brokerClusterB] }.sort { it[1] }
        def filteredShowProc = showBrokerProcResult.findAll { row -> row[0] in [brokerClusterA, brokerClusterB] }.sort { it[1] }

        assertTrue(filteredShowBroker.size() == 4)
        assertTrue(filteredShowProc.size() == 4)

        // Strictly compare column values (excluding last 3 volatile columns: LastStartTime, LastUpdateTime, ErrMsg)
        for (i in 0..<tvfSelectResult.size()) {
            for (j in 0..<columns.size() - 3) {
                // check Name,Host,Port,Alive columns values are exactly the same
                assertTrue(filteredShowBroker[i][j] == tvfSelectResult[i][j])
                assertTrue(filteredShowProc[i][j] == tvfSelectResult[i][j])
            }
        }

        // 7. Test WHERE clause filtering on global brokers
        tvfSelectResult = sql """select * from brokers() where host='${address1}';"""
        log.info("tvfSelectResult1: ${tvfSelectResult}")
        assertTrue(tvfSelectResult.size() == 1)
        assertTrue(tvfSelectResult[0][1] == "${address1}")

        tvfSelectResult = sql """select * from brokers() where host in('${address2}', '${address3}') order by host;"""
        log.info("tvfSelectResult2: ${tvfSelectResult}")
        assertTrue(tvfSelectResult.size() == 2)
        assertTrue(tvfSelectResult[0][1] == "${address2}")
        assertTrue(tvfSelectResult[1][1] == "${address3}")

        // 8. Test normal parameter for Cluster A
        def filterExistResultA = sql """select * from brokers("cluster_name" = "${brokerClusterA}") order by host;"""
        log.info("filterExistResultA: ${filterExistResultA}")
        assertTrue(filterExistResultA.size() == 2)
        for (row in filterExistResultA) {
            assertTrue(row[0] == "${brokerClusterA}")
        }

        // 9. Test normal parameter for Cluster B
        def filterExistResultB = sql """select * from brokers("cluster_name" = "${brokerClusterB}") order by host;"""
        log.info("filterExistResultB: ${filterExistResultB}")
        assertTrue(filterExistResultB.size() == 2)
        for (row in filterExistResultB) {
            assertTrue(row[0] == "${brokerClusterB}")
        }

        // 10. Test non-existent cluster name
        def filterNotExistResult = sql """select * from brokers("cluster_name" = "this_cluster_does_not_exist");"""
        log.info("filterNotExistResult: ${filterNotExistResult}")
        assertTrue(filterNotExistResult.size() == 0)

        // 11. Test uppercase key (case-insensitive) for Cluster A
        def filterUpperCaseKey = sql """select * from brokers("CLUSTER_NAME" = "${brokerClusterA}") order by host;"""
        log.info("filterUpperCaseKey: ${filterUpperCaseKey}")
        assertTrue(filterUpperCaseKey.size() == 2)

        // 12. Test mixed case key (case-insensitive) for Cluster B
        def filterMixedCaseKey = sql """select * from brokers("Cluster_Name" = "${brokerClusterB}") order by host;"""
        log.info("filterMixedCaseKey: ${filterMixedCaseKey}")
        assertTrue(filterMixedCaseKey.size() == 2)

        // 13. Test uppercase key must filter something out
        def filterUpperCaseKeyNotExist = sql """select * from brokers("CLUSTER_NAME" = "this_cluster_does_not_exist");"""
        log.info("filterUpperCaseKeyNotExist: ${filterUpperCaseKeyNotExist}")
        assertTrue(filterUpperCaseKeyNotExist.size() == 0)

        // 14. Test mixed case key must filter something out
        def filterMixedCaseKeyNotExist = sql """select * from brokers("Cluster_Name" = "this_cluster_does_not_exist");"""
        log.info("filterMixedCaseKeyNotExist: ${filterMixedCaseKeyNotExist}")
        assertTrue(filterMixedCaseKeyNotExist.size() == 0)

        // 15. Test combined filter (TVF property + WHERE clause)
        def combinedFilterResult = sql """select * from brokers("cluster_name" = "${brokerClusterA}") where host='${address1}';"""
        log.info("combinedFilterResult: ${combinedFilterResult}")
        assertTrue(combinedFilterResult.size() == 1)
        assertTrue(combinedFilterResult[0][0] == "${brokerClusterA}")
        assertTrue(combinedFilterResult[0][1] == "${address1}")

        // 16. Test exception for empty string
        try {
            sql """select * from brokers("cluster_name" = "");"""
            assertTrue(false)
        } catch (Exception e) {
            log.info("Expected exception for empty cluster_name: ${e.getMessage()}")
            assertTrue(e.getMessage().contains("Invalid brokers param value"))
        }

        // 17. Test exception for blank space
        try {
            sql """select * from brokers("cluster_name" = "   ");"""
            assertTrue(false)
        } catch (Exception e) {
            log.info("Expected exception for blank cluster_name: ${e.getMessage()}")
            assertTrue(e.getMessage().contains("Invalid brokers param value"))
        }

        // 18. Test invalid param key
        try {
            sql """select * from brokers("invalid_param" = "value");"""
            assertTrue(false)
        } catch (Exception e) {
            log.info("Expected exception for invalid param key: ${e.getMessage()}")
            assertTrue(e.getMessage().contains("is invalid property"))
        }

        // 19. Test auth: Create non-admin user and verify deny behavior
        sql """CREATE USER '${nonAdminUser}' IDENTIFIED BY '${nonAdminPwd}';"""
        // Connect as non-admin user and verify that querying brokers() is denied
        connect(nonAdminUser, nonAdminPwd, "127.0.0.1", context.config.jdbcPort) {
            try {
                sql """select * from brokers();"""
                assertTrue(false, "Expected permission denied for non-admin user")
            } catch (Exception e) {
                log.info("Expected permission denied for non-admin user: ${e.getMessage()}")
                assertTrue(e.getMessage().contains("Permission denied") || e.getMessage().contains("denied"))
            }
        }

        // 20. Test auth: Grant ADMIN privilege and verify allow behavior
        sql """GRANT ADMIN_PRIV ON *.*.* TO '${nonAdminUser}';"""
        // Connect as non-admin user with ADMIN_PRIV and verify that querying brokers() succeeds
        connect(nonAdminUser, nonAdminPwd, "127.0.0.1", context.config.jdbcPort) {
            try {
                def result = sql """select * from brokers();"""
                log.info("Non-admin user with ADMIN_PRIV successfully queried brokers TVF: ${result}")
                assertTrue(result.size() >= 0, "Expected successful query for user with ADMIN_PRIV")
            } catch (Exception e) {
                assertTrue(false, "Should not throw exception for user with ADMIN_PRIV: ${e.getMessage()}")
            }
        }

        // 21. Test auth: Revoke ADMIN privilege and verify deny behavior again
        sql """REVOKE ADMIN_PRIV ON *.*.* FROM '${nonAdminUser}';"""
        // Connect as non-admin user and verify that querying brokers() is denied again
        connect(nonAdminUser, nonAdminPwd, "127.0.0.1", context.config.jdbcPort) {
            try {
                sql """select * from brokers();"""
                assertTrue(false, "Expected permission denied after revoking ADMIN_PRIV")
            } catch (Exception e) {
                log.info("Expected permission denied after revoking ADMIN_PRIV: ${e.getMessage()}")
                assertTrue(e.getMessage().contains("Permission denied") || e.getMessage().contains("denied"))
            }
        }

    } finally {
        // Clean up test clusters and users
        try { sql """ALTER SYSTEM DROP ALL BROKER ${brokerClusterA};""" } catch (Exception e) { log.info("Ignored: ${e.getMessage()}") }
        try { sql """ALTER SYSTEM DROP ALL BROKER ${brokerClusterB};""" } catch (Exception e) { log.info("Ignored: ${e.getMessage()}") }
        try { sql """DROP USER IF EXISTS ${nonAdminUser};""" } catch (Exception e) { log.info("Ignored: ${e.getMessage()}") }
    }
}
