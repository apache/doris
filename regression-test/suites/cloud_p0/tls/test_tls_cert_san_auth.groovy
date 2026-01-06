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

suite("test_tls_cert_san_auth") {
    // This test verifies TLS certificate-based authentication with SAN (Subject Alternative Name).
    // It requires:
    // 1. Enterprise version with CertificateBasedAuthVerifier
    // 2. TLS enabled (enableTLS=true in regression config)
    // 3. MySQL client 5.7+ installed
    //
    // The test certificates (san-client-cert.pem) contain these SANs:
    //   - Email: test@example.com
    //   - DNS: testclient.example.com
    //   - URI: spiffe://example.com/testclient

    // Only run when TLS is enabled (opposite of test_mysql_connection.groovy)
    if ((context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false) {
        
        // === Configuration ===
        def sanEmail = "test@example.com"
        def sanDns = "testclient.example.com"
        def sanUri = "spiffe://example.com/testclient"
        def sanMismatch = "wrong@example.com"
        
        def testUserBase = "test_san_auth_user"
        def testPassword = "Test_123456"
        
        def sslCertPath = context.config.sslCertificatePath
        def sanClientCa = "${sslCertPath}/san-ca.pem"
        def sanClientCert = "${sslCertPath}/san-client-cert.pem"
        def sanClientKey = "${sslCertPath}/san-client-key.pem"
        def noSanClientCert = "${sslCertPath}/no-san-client-cert.pem"
        def noSanClientKey = "${sslCertPath}/no-san-client-key.pem"
        
        // Get MySQL connection info
        String jdbcUrl = context.config.jdbcUrl
        String tempStr = jdbcUrl.substring(jdbcUrl.indexOf("jdbc:mysql://") + 13)
        String mysqlHost = tempStr.substring(0, tempStr.indexOf(":"))
        String mysqlPort = tempStr.substring(tempStr.indexOf(":") + 1, tempStr.indexOf("/"))
        
        logger.info("MySQL host: ${mysqlHost}, port: ${mysqlPort}")
        logger.info("SSL certificate path: ${sslCertPath}")
        
        // === Helper Functions ===
        def executeMySQLCommand = { String command, boolean expectSuccess = true ->
            def cmds = ["/bin/bash", "-c", command]
            logger.info("Execute: ${command}")
            Process p = cmds.execute()
            def errMsg = new StringBuilder()
            def msg = new StringBuilder()
            p.waitForProcessOutput(msg, errMsg)
            
            logger.info("stdout: ${msg}")
            logger.info("stderr: ${errMsg}")
            logger.info("exitValue: ${p.exitValue()}")
            
            if (expectSuccess) {
                return p.exitValue() == 0
            } else {
                return p.exitValue() != 0
            }
        }
        
        // Build MySQL command with SAN certificate
        def buildMySQLCmdWithCert = { String user, String password, String query ->
            return "mysql -u${user} -p'${password}' -h${mysqlHost} -P${mysqlPort} " +
                   "--ssl-mode=VERIFY_CA " +
                   "--tls-version=TLSv1.2 " +
                   "--ssl-ca=${sanClientCa} " +
                   "--ssl-cert=${sanClientCert} --ssl-key=${sanClientKey} " +
                   "-e \"${query}\""
        }
        
        // Build MySQL command without certificate
        def buildMySQLCmdNoCert = { String user, String password, String query ->
            return "mysql -u${user} -p'${password}' -h${mysqlHost} -P${mysqlPort} " +
                   "--ssl-mode=REQUIRED " +
                   "-e \"${query}\""
        }
        
        // Build MySQL command with no-SAN certificate (for REQUIRE NONE testing)
        def buildMySQLCmdWithNoSanCert = { String user, String password, String query ->
            return "mysql -u${user} -p'${password}' -h${mysqlHost} -P${mysqlPort} " +
                   "--ssl-mode=VERIFY_CA " +
                   "--tls-version=TLSv1.2 " +
                   "--ssl-ca=${sanClientCa} " +
                   "--ssl-cert=${noSanClientCert} --ssl-key=${noSanClientKey} " +
                   "-e \"${query}\""
        }
        
        // Save original config value for cleanup
        def origIgnorePassword = "false"
        try {
            def configResult = sql "SHOW FRONTEND CONFIG LIKE 'tls_cert_based_auth_ignore_password'"
            if (!configResult.isEmpty()) {
                origIgnorePassword = configResult[0][1]
            }
        } catch (Exception e) {
            logger.info("Could not get original config value: ${e.message}")
        }
        logger.info("Original tls_cert_based_auth_ignore_password: ${origIgnorePassword}")
        
        // === Cleanup function ===
        def cleanup = {
            logger.info("Cleaning up test users and restoring config...")
            try_sql("DROP USER IF EXISTS '${testUserBase}_1'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_2'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_3'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_4'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_5'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_6'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_7'@'%'")
            try {
                sql "ADMIN SET FRONTEND CONFIG ('tls_cert_based_auth_ignore_password' = '${origIgnorePassword}')"
            } catch (Exception e) {
                logger.warn("Failed to restore config: ${e.message}")
            }
        }

        cleanup()

        try {
            // Ensure ignore_password is false for most tests
            sql "ADMIN SET FRONTEND CONFIG ('tls_cert_based_auth_ignore_password' = 'false')"

            // === Test 1: REQUIRE SAN + no certificate -> failure ===
            logger.info("=== Test 1: REQUIRE SAN + no certificate ===")
            sql "CREATE USER '${testUserBase}_1'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanEmail}'"

            def cmd1 = buildMySQLCmdNoCert("${testUserBase}_1", testPassword, "SELECT 1")
            def result1 = executeMySQLCommand(cmd1, false)
            assertTrue(result1, "Test 1 should fail: no certificate provided for user with REQUIRE SAN")
            logger.info("Test 1 PASSED: Connection rejected without certificate")

            // === Test 2: REQUIRE SAN + matching cert + correct password -> success ===
            logger.info("=== Test 2: REQUIRE SAN + matching cert + correct password ===")
            sql "CREATE USER '${testUserBase}_2'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanEmail}'"

            def cmd2 = buildMySQLCmdWithCert("${testUserBase}_2", testPassword, "SELECT 1")
            def result2 = executeMySQLCommand(cmd2, true)
            assertTrue(result2, "Test 2 should succeed: matching SAN + correct password")
            logger.info("Test 2 PASSED: Login successful with matching SAN and password")
            
            // === Test 3: REQUIRE SAN + matching cert + wrong password -> failure ===
            logger.info("=== Test 3: REQUIRE SAN + matching cert + wrong password ===")
            sql "CREATE USER '${testUserBase}_3'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanEmail}'"
            
            def cmd3 = buildMySQLCmdWithCert("${testUserBase}_3", "wrong_password", "SELECT 1")
            def result3 = executeMySQLCommand(cmd3, false)
            assertTrue(result3, "Test 3 should fail: wrong password even with matching SAN")
            logger.info("Test 3 PASSED: Connection rejected with wrong password")
            
            // === Test 4: REQUIRE SAN + mismatched SAN -> failure ===
            logger.info("=== Test 4: REQUIRE SAN + mismatched SAN ===")
            sql "CREATE USER '${testUserBase}_4'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanMismatch}'"
            
            def cmd4 = buildMySQLCmdWithCert("${testUserBase}_4", testPassword, "SELECT 1")
            def result4 = executeMySQLCommand(cmd4, false)
            assertTrue(result4, "Test 4 should fail: SAN mismatch")
            logger.info("Test 4 PASSED: Connection rejected with mismatched SAN")
            
            // === Test 5: REQUIRE SAN + matching cert + ignore_password=true -> success ===
            logger.info("=== Test 5: REQUIRE SAN + ignore_password=true ===")
            sql "ADMIN SET FRONTEND CONFIG ('tls_cert_based_auth_ignore_password' = 'true')"
            sql "CREATE USER '${testUserBase}_5'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanEmail}'"
            
            // Use wrong password - should still succeed because ignore_password=true
            def cmd5 = buildMySQLCmdWithCert("${testUserBase}_5", "any_wrong_password", "SELECT 1")
            def result5 = executeMySQLCommand(cmd5, true)
            assertTrue(result5, "Test 5 should succeed: ignore_password=true allows login with cert only")
            logger.info("Test 5 PASSED: Login successful with certificate only (password ignored)")
            
            // Reset config for remaining tests
            sql "ADMIN SET FRONTEND CONFIG ('tls_cert_based_auth_ignore_password' = 'false')"
            
            // === Test 6: ALTER USER add/remove REQUIRE SAN ===
            logger.info("=== Test 6: ALTER USER add/remove REQUIRE SAN ===")
            sql "CREATE USER '${testUserBase}_6'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanEmail}'"
            
            // First verify it works with cert
            def cmd6a = buildMySQLCmdWithCert("${testUserBase}_6", testPassword, "SELECT 1")
            def result6a = executeMySQLCommand(cmd6a, true)
            assertTrue(result6a, "Test 6a should succeed: initial REQUIRE SAN with matching cert")
            logger.info("Test 6a PASSED: REQUIRE SAN works with matching cert")
            
            // Remove REQUIRE SAN
            sql "ALTER USER '${testUserBase}_6'@'%' REQUIRE NONE"
            
            // Now should work with certificate that has no SAN (REQUIRE NONE does not check SAN)
            def cmd6b = buildMySQLCmdWithNoSanCert("${testUserBase}_6", testPassword, "SELECT 1")
            def result6b = executeMySQLCommand(cmd6b, true)
            assertTrue(result6b, "Test 6b should succeed: REQUIRE NONE allows cert without SAN")
            logger.info("Test 6b PASSED: REQUIRE NONE works with no-SAN certificate")
            
            // Add back REQUIRE SAN
            sql "ALTER USER '${testUserBase}_6'@'%' REQUIRE SAN '${sanEmail}'"
            
            // Now should fail without certificate
            def cmd6c = buildMySQLCmdNoCert("${testUserBase}_6", testPassword, "SELECT 1")
            def result6c = executeMySQLCommand(cmd6c, false)
            assertTrue(result6c, "Test 6c should fail: REQUIRE SAN re-added, no cert provided")
            logger.info("Test 6c PASSED: REQUIRE SAN re-added works")
            
            // === Test 7: Multiple SAN types (DNS, URI, Email) ===
            logger.info("=== Test 7: Multiple SAN types ===")
            sql "CREATE USER '${testUserBase}_7'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanEmail}'"
            
            // Test with Email SAN (already covered, but explicit)
            def cmd7a = buildMySQLCmdWithCert("${testUserBase}_7", testPassword, "SELECT 1")
            def result7a = executeMySQLCommand(cmd7a, true)
            assertTrue(result7a, "Test 7a should succeed: Email SAN match")
            logger.info("Test 7a PASSED: Email SAN match works")
            
            // Test with DNS SAN
            sql "ALTER USER '${testUserBase}_7'@'%' REQUIRE SAN '${sanDns}'"
            def cmd7b = buildMySQLCmdWithCert("${testUserBase}_7", testPassword, "SELECT 1")
            def result7b = executeMySQLCommand(cmd7b, true)
            assertTrue(result7b, "Test 7b should succeed: DNS SAN match")
            logger.info("Test 7b PASSED: DNS SAN match works")
            
            // Test with URI SAN
            sql "ALTER USER '${testUserBase}_7'@'%' REQUIRE SAN '${sanUri}'"
            def cmd7c = buildMySQLCmdWithCert("${testUserBase}_7", testPassword, "SELECT 1")
            def result7c = executeMySQLCommand(cmd7c, true)
            assertTrue(result7c, "Test 7c should succeed: URI SAN match")
            logger.info("Test 7c PASSED: URI SAN match works")
            
            logger.info("=== All TLS SAN authentication tests PASSED ===")
            
        } finally {
            cleanup()
        }
        
    } else {
        logger.info("Skipping test_tls_cert_san_auth: TLS is not enabled (enableTLS != true)")
    }
}
