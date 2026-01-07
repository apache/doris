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
    //
    // SAN matching is EXACT STRING MATCH - the full SAN string from the certificate
    // must exactly match the REQUIRE SAN value (including order, prefixes, and separators).
    // Format: "email:xxx, DNS:xxx, URI:xxx, IP Address:xxx" (comma + space separated)

    // Only run when TLS is enabled (opposite of test_mysql_connection.groovy)
    if ((context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false) {
        
        // === Configuration ===
        // Full SAN string from the test certificate (exact match required)
        def sanFull = "email:test@example.com, DNS:testclient.example.com, URI:spiffe://example.com/testclient"
        // A SAN string that won't match the certificate
        def sanMismatch = "email:wrong@example.com"
        
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
            // MySQL protocol test users (Tests 1-6)
            try_sql("DROP USER IF EXISTS '${testUserBase}_1'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_2'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_3'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_4'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_5'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_6'@'%'")
            // HTTPS test users (HTTP Tests 1-7)
            try_sql("DROP USER IF EXISTS '${testUserBase}_http1'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_http2'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_http3'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_http4'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_http5'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_http6'@'%'")
            try_sql("DROP USER IF EXISTS '${testUserBase}_http7'@'%'")
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
            sql "CREATE USER '${testUserBase}_1'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"

            def cmd1 = buildMySQLCmdNoCert("${testUserBase}_1", testPassword, "SELECT 1")
            def result1 = executeMySQLCommand(cmd1, false)
            assertTrue(result1, "Test 1 should fail: no certificate provided for user with REQUIRE SAN")
            logger.info("Test 1 PASSED: Connection rejected without certificate")

            // === Test 2: REQUIRE SAN + matching cert + correct password -> success ===
            logger.info("=== Test 2: REQUIRE SAN + matching cert + correct password ===")
            sql "CREATE USER '${testUserBase}_2'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"

            def cmd2 = buildMySQLCmdWithCert("${testUserBase}_2", testPassword, "SELECT 1")
            def result2 = executeMySQLCommand(cmd2, true)
            assertTrue(result2, "Test 2 should succeed: matching SAN + correct password")
            logger.info("Test 2 PASSED: Login successful with matching SAN and password")
            
            // === Test 3: REQUIRE SAN + matching cert + wrong password -> failure ===
            logger.info("=== Test 3: REQUIRE SAN + matching cert + wrong password ===")
            sql "CREATE USER '${testUserBase}_3'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
            
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
            sql "CREATE USER '${testUserBase}_5'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
            
            // Use wrong password - should still succeed because ignore_password=true
            def cmd5 = buildMySQLCmdWithCert("${testUserBase}_5", "any_wrong_password", "SELECT 1")
            def result5 = executeMySQLCommand(cmd5, true)
            assertTrue(result5, "Test 5 should succeed: ignore_password=true allows login with cert only")
            logger.info("Test 5 PASSED: Login successful with certificate only (password ignored)")
            
            // Reset config for remaining tests
            sql "ADMIN SET FRONTEND CONFIG ('tls_cert_based_auth_ignore_password' = 'false')"
            
            // === Test 6: ALTER USER add/remove REQUIRE SAN ===
            logger.info("=== Test 6: ALTER USER add/remove REQUIRE SAN ===")
            sql "CREATE USER '${testUserBase}_6'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
            
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
            sql "ALTER USER '${testUserBase}_6'@'%' REQUIRE SAN '${sanFull}'"
            
            // Now should fail without certificate
            def cmd6c = buildMySQLCmdNoCert("${testUserBase}_6", testPassword, "SELECT 1")
            def result6c = executeMySQLCommand(cmd6c, false)
            assertTrue(result6c, "Test 6c should fail: REQUIRE SAN re-added, no cert provided")
            logger.info("Test 6c PASSED: REQUIRE SAN re-added works")
            
            logger.info("=== All MySQL protocol TLS SAN authentication tests PASSED ===")
            
            // ==================================================================================
            // HTTPS/HTTP Certificate-Based Authentication Tests
            // These tests verify that FE's HTTP endpoints also support cert-based auth
            // ==================================================================================
            logger.info("=== Starting HTTPS certificate-based auth tests ===")
            
            // Get FE HTTP connection info
            def feHttpAddress = (context.config.feHttpAddress ?: "${mysqlHost}:8030").toString()
            def feHostIp = feHttpAddress.contains(":") ? feHttpAddress.substring(0, feHttpAddress.indexOf(":")) : feHttpAddress
            def httpPort = feHttpAddress.contains(":") ? feHttpAddress.substring(feHttpAddress.indexOf(":") + 1) : "8030"
            def httpEndpoint = "/api/bootstrap"  // Simple endpoint that requires auth
            
            logger.info("FE HTTPS host IP: ${feHostIp}, port: ${httpPort}")
            logger.info("Test endpoint: ${httpEndpoint}")
            
            // Helper: Execute curl command and check result based on JSON response code
            // FE REST API always returns HTTP 200, real status is in JSON body's "code" field:
            //   code = 0   -> success
            //   code = 401 -> unauthorized (cert-based auth failed or password wrong)
            def executeCurlCommand = { String command, boolean expectSuccess = true ->
                def cmds = ["/bin/bash", "-c", command]
                logger.info("Execute curl: ${command}")
                Process p = cmds.execute()
                def errMsg = new StringBuilder()
                def msg = new StringBuilder()
                p.waitForProcessOutput(msg, errMsg)
                
                def output = msg.toString().trim()
                def error = errMsg.toString().trim()
                logger.info("stdout: ${output}")
                logger.info("stderr: ${error}")
                logger.info("exitValue: ${p.exitValue()}")
                
                // Check curl exit code first (non-zero means connection/TLS error)
                if (p.exitValue() != 0) {
                    // curl failed (e.g., TLS handshake failure, connection refused)
                    logger.info("curl failed with exit code ${p.exitValue()}, expectSuccess=${expectSuccess}")
                    return !expectSuccess
                }
                
                // Parse JSON response to get internal code
                try {
                    def json = new groovy.json.JsonSlurper().parseText(output)
                    def code = json.code
                    logger.info("Response JSON code: ${code}, msg: ${json.msg}")
                    
                    if (expectSuccess) {
                        return code == 0
                    } else {
                        return code != 0  // 401 for unauthorized, or other error codes
                    }
                } catch (Exception e) {
                    logger.warn("Failed to parse JSON response: ${e.message}")
                    // If we can't parse JSON, treat as failure
                    return !expectSuccess
                }
            }
            
            // Helper: Build curl command with SAN certificate
            // Use --resolve to map the server cert's SAN domain to actual IP, avoiding SNI issues
            // Use -k to skip server certificate verification (we're testing client cert auth)
            // Note: Server cert has SAN "testclient.example.com", so we use that as the hostname
            def sniHostname = "testclient.example.com"
            
            def buildCurlWithCert = { String user, String password, String endpoint ->
                return "curl -s -k " +
                       "--resolve '${sniHostname}:${httpPort}:${feHostIp}' " +
                       "-u '${user}:${password}' " +
                       "--cert ${sanClientCert} --key ${sanClientKey} " +
                       "https://${sniHostname}:${httpPort}${endpoint} 2>&1"
            }
            
            // Helper: Build curl command without client certificate
            def buildCurlNoCert = { String user, String password, String endpoint ->
                return "curl -s -k " +
                       "--resolve '${sniHostname}:${httpPort}:${feHostIp}' " +
                       "-u '${user}:${password}' " +
                       "https://${sniHostname}:${httpPort}${endpoint} 2>&1"
            }
            
            // Helper: Build curl command with no-SAN certificate
            def buildCurlWithNoSanCert = { String user, String password, String endpoint ->
                return "curl -s -k " +
                       "--resolve '${sniHostname}:${httpPort}:${feHostIp}' " +
                       "-u '${user}:${password}' " +
                       "--cert ${noSanClientCert} --key ${noSanClientKey} " +
                       "https://${sniHostname}:${httpPort}${endpoint} 2>&1"
            }
            
            // Helper: Build curl command with mismatched SAN certificate (not currently used)
            def buildCurlWithMismatchCert = { String user, String password, String endpoint ->
                return "curl -s -k " +
                       "--resolve '${sniHostname}:${httpPort}:${feHostIp}' " +
                       "-u '${user}:${password}' " +
                       "--cert ${sanClientCert} --key ${sanClientKey} " +
                       "https://${sniHostname}:${httpPort}${endpoint} 2>&1"
            }
            
            // Reset config for HTTPS tests
            sql "ADMIN SET FRONTEND CONFIG ('tls_cert_based_auth_ignore_password' = 'false')"
            
            // === HTTP Test 1: REQUIRE SAN + matching cert + correct password -> success ===
            logger.info("=== HTTP Test 1: REQUIRE SAN + matching cert + correct password ===")
            sql "CREATE USER '${testUserBase}_http1'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
            sql "GRANT ADMIN_PRIV ON *.*.* TO '${testUserBase}_http1'@'%'"
            
            def httpCmd1 = buildCurlWithCert("${testUserBase}_http1", testPassword, httpEndpoint)
            def httpResult1 = executeCurlCommand(httpCmd1, true)
            assertTrue(httpResult1, "HTTP Test 1 should succeed: matching SAN + correct password")
            logger.info("HTTP Test 1 PASSED: HTTPS request successful with matching SAN and password")
            
            // === HTTP Test 2: REQUIRE SAN + matching cert + wrong password -> failure ===
            logger.info("=== HTTP Test 2: REQUIRE SAN + matching cert + wrong password ===")
            sql "CREATE USER '${testUserBase}_http2'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
            sql "GRANT ADMIN_PRIV ON *.*.* TO '${testUserBase}_http2'@'%'"
            
            def httpCmd2 = buildCurlWithCert("${testUserBase}_http2", "wrong_password", httpEndpoint)
            def httpResult2 = executeCurlCommand(httpCmd2, false)
            assertTrue(httpResult2, "HTTP Test 2 should fail: wrong password even with matching SAN")
            logger.info("HTTP Test 2 PASSED: HTTPS request rejected with wrong password")
            
            // === HTTP Test 3: REQUIRE SAN + mismatched SAN cert -> failure ===
            logger.info("=== HTTP Test 3: REQUIRE SAN + mismatched SAN ===")
            sql "CREATE USER '${testUserBase}_http3'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanMismatch}'"
            sql "GRANT ADMIN_PRIV ON *.*.* TO '${testUserBase}_http3'@'%'"
            
            def httpCmd3 = buildCurlWithCert("${testUserBase}_http3", testPassword, httpEndpoint)
            def httpResult3 = executeCurlCommand(httpCmd3, false)
            assertTrue(httpResult3, "HTTP Test 3 should fail: SAN mismatch")
            logger.info("HTTP Test 3 PASSED: HTTPS request rejected with mismatched SAN")
            
            // === HTTP Test 4: REQUIRE SAN + no certificate -> failure ===
            // Note: This test depends on tls_verify_mode=verify_fail_if_no_peer_cert
            // If the server requires client cert, curl without --cert will fail at TLS handshake
            logger.info("=== HTTP Test 4: REQUIRE SAN + no certificate ===")
            sql "CREATE USER '${testUserBase}_http4'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
            sql "GRANT ADMIN_PRIV ON *.*.* TO '${testUserBase}_http4'@'%'"
            
            def httpCmd4 = buildCurlNoCert("${testUserBase}_http4", testPassword, httpEndpoint)
            def httpResult4 = executeCurlCommand(httpCmd4, false)
            assertTrue(httpResult4, "HTTP Test 4 should fail: no certificate provided for user with REQUIRE SAN")
            logger.info("HTTP Test 4 PASSED: HTTPS request rejected without client certificate")
            
            // === HTTP Test 5: REQUIRE SAN + matching cert + ignore_password=true -> success ===
            logger.info("=== HTTP Test 5: REQUIRE SAN + ignore_password=true ===")
            sql "ADMIN SET FRONTEND CONFIG ('tls_cert_based_auth_ignore_password' = 'true')"
            sql "CREATE USER '${testUserBase}_http5'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
            sql "GRANT ADMIN_PRIV ON *.*.* TO '${testUserBase}_http5'@'%'"
            
            // Use wrong password - should still succeed because ignore_password=true
            def httpCmd5 = buildCurlWithCert("${testUserBase}_http5", "any_wrong_password", httpEndpoint)
            def httpResult5 = executeCurlCommand(httpCmd5, true)
            assertTrue(httpResult5, "HTTP Test 5 should succeed: ignore_password=true allows login with cert only")
            logger.info("HTTP Test 5 PASSED: HTTPS request successful with certificate only (password ignored)")
            
            // Reset config
            sql "ADMIN SET FRONTEND CONFIG ('tls_cert_based_auth_ignore_password' = 'false')"
            
            // === HTTP Test 6: REQUIRE NONE + no-SAN cert + correct password -> success ===
            logger.info("=== HTTP Test 6: REQUIRE NONE + no-SAN cert ===")
            sql "CREATE USER '${testUserBase}_http6'@'%' IDENTIFIED BY '${testPassword}'"
            sql "GRANT ADMIN_PRIV ON *.*.* TO '${testUserBase}_http6'@'%'"
            
            def httpCmd6 = buildCurlWithNoSanCert("${testUserBase}_http6", testPassword, httpEndpoint)
            def httpResult6 = executeCurlCommand(httpCmd6, true)
            assertTrue(httpResult6, "HTTP Test 6 should succeed: no TLS requirements, password auth works")
            logger.info("HTTP Test 6 PASSED: HTTPS request successful for user without TLS requirements")
            
            // === HTTP Test 7: ALTER USER add/remove REQUIRE SAN for HTTP ===
            logger.info("=== HTTP Test 7: ALTER USER add/remove REQUIRE SAN for HTTP ===")
            sql "CREATE USER '${testUserBase}_http7'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
            sql "GRANT ADMIN_PRIV ON *.*.* TO '${testUserBase}_http7'@'%'"
            
            // First verify it works with matching cert
            def httpCmd7a = buildCurlWithCert("${testUserBase}_http7", testPassword, httpEndpoint)
            def httpResult7a = executeCurlCommand(httpCmd7a, true)
            assertTrue(httpResult7a, "HTTP Test 7a should succeed: initial REQUIRE SAN with matching cert")
            logger.info("HTTP Test 7a PASSED: REQUIRE SAN works with matching cert via HTTPS")
            
            // Remove REQUIRE SAN
            sql "ALTER USER '${testUserBase}_http7'@'%' REQUIRE NONE"
            
            // Now should work with no-SAN certificate
            def httpCmd7b = buildCurlWithNoSanCert("${testUserBase}_http7", testPassword, httpEndpoint)
            def httpResult7b = executeCurlCommand(httpCmd7b, true)
            assertTrue(httpResult7b, "HTTP Test 7b should succeed: REQUIRE NONE allows any cert")
            logger.info("HTTP Test 7b PASSED: REQUIRE NONE works with no-SAN certificate via HTTPS")
            
            // Add back REQUIRE SAN with full SAN string
            sql "ALTER USER '${testUserBase}_http7'@'%' REQUIRE SAN '${sanFull}'"
            
            // Now should work with matching cert
            def httpCmd7c = buildCurlWithCert("${testUserBase}_http7", testPassword, httpEndpoint)
            def httpResult7c = executeCurlCommand(httpCmd7c, true)
            assertTrue(httpResult7c, "HTTP Test 7c should succeed: full SAN match")
            logger.info("HTTP Test 7c PASSED: REQUIRE SAN works via HTTPS after ALTER USER")
            
            logger.info("=== All HTTPS certificate-based auth tests PASSED ===")
            
            logger.info("=== All TLS SAN authentication tests (MySQL + HTTPS) PASSED ===")
            
        } finally {
            cleanup()
        }
        
    } else {
        logger.info("Skipping test_tls_cert_san_auth: TLS is not enabled (enableTLS != true)")
    }
}
