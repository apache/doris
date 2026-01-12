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

suite('test_tls_cert_san_auth', 'docker, p0') {
    // This test verifies TLS certificate-based authentication with SAN (Subject Alternative Name).
    // It tests both MySQL protocol and HTTPS endpoints.
    //
    // Test cases:
    //   MySQL Protocol Tests 1-6: REQUIRE SAN various scenarios
    //   HTTPS Tests 1-7: FE HTTP endpoint certificate-based auth
    //
    // The test certificates (client_san.crt) contain these SANs:
    //   - Email: test@example.com
    //   - DNS: testclient.example.com
    //   - URI: spiffe://example.com/testclient
    //
    // SAN matching is EXACT STRING MATCH - the full SAN string from the certificate
    // must exactly match the REQUIRE SAN value (including order, prefixes, and separators).
    // Format: "email:xxx, DNS:xxx, URI:xxx, IP Address:xxx" (comma + space separated)

    def testName = "test_tls_cert_san_auth"

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true

    // Add TLS configuration to all node types
    def tlsConfigs = [
        'enable_tls=false',
        'tls_verify_mode=verify_fail_if_no_peer_cert',
        'tls_certificate_path=/tmp/certs/certificate.crt',
        'tls_private_key_path=/tmp/certs/certificate.key',
        'tls_ca_certificate_path=/tmp/certs/ca.crt',
        'tls_cert_refresh_interval_seconds=5',
        'enable_all_http_auth=true'
    ]

    options.feConfigs += tlsConfigs
    options.beConfigs += tlsConfigs
    options.msConfigs += tlsConfigs
    options.recycleConfigs += tlsConfigs

    def localCertDir = "/tmp/${testName}"
    def certFileDir = "${localCertDir}/certs1"

    docker(options) {
        sql """ CREATE DATABASE IF NOT EXISTS ${context.dbName}; """

        // Get all FE IPs
        def frontends = cluster.getAllFrontends()
        logger.info("=== Frontend nodes ===")
        frontends.each { fe ->
            logger.info("FE[${fe.index}] - Host: ${fe.host}, HTTP Port: ${fe.httpPort}, Query Port: ${fe.queryPort}")
        }

        // Get all BE IPs
        def backends = cluster.getAllBackends()
        logger.info("=== Backend nodes ===")
        backends.each { be ->
            logger.info("BE[${be.index}] - Host: ${be.host}, HTTP Port: ${be.httpPort}, Heartbeat Port: ${be.heartbeatPort}")
        }

        // Get all MS IPs (MetaService)
        def metaservices = cluster.getAllMetaservices()
        logger.info("=== MetaService nodes ===")
        metaservices.each { ms ->
            logger.info("MS - Host: ${ms.host}, HTTP Port: ${ms.httpPort}")
        }

        // Get all Recycler nodes
        def recyclers = cluster.getAllRecyclers(false)
        logger.info("=== Recycler nodes ===")
        recyclers.each { rc ->
            logger.info("Recycler[${rc.index}] - Host: ${rc.host}, HTTP Port: ${rc.httpPort}")
        }

        // Collect all unique IPs
        def allIps = []
        frontends.each { fe -> if (!allIps.contains(fe.host)) allIps.add(fe.host) }
        backends.each { be -> if (!allIps.contains(be.host)) allIps.add(be.host) }
        metaservices.each { ms -> if (!allIps.contains(ms.host)) allIps.add(ms.host) }
        recyclers.each { rc -> if (!allIps.contains(rc.host)) allIps.add(rc.host) }
        logger.info("All unique IPs: ${allIps}")

        // Helper: Run command
        def runCommand = { String cmd, String errorMsg ->
            logger.info("Executing command: ${cmd}")
            def proc = ["bash", "-lc", cmd].execute()
            def stdout = new StringBuilder()
            def stderr = new StringBuilder()
            proc.waitForProcessOutput(stdout, stderr)
            if (proc.exitValue() != 0) {
                logger.error("Command failed with exit ${proc.exitValue()}: ${cmd}")
                if (stdout.length() > 0) {
                    logger.error("stdout: ${stdout.toString()}")
                }
                if (stderr.length() > 0) {
                    logger.error("stderr: ${stderr.toString()}")
                }
                assert false : errorMsg
            }
            return stdout.toString()
        }

        // Generate certificates for all IPs
        logger.info("=== Generating TLS certificates ===")
        // Create certificate directory
        runCommand("mkdir -p ${localCertDir}", "Failed to create cert directory")

        // Generate CA private key
        runCommand("openssl genpkey -algorithm RSA -out ${localCertDir}/ca.key -pkeyopt rsa_keygen_bits:2048", "Failed to generate CA private key")

        // Generate CA certificate
        runCommand("openssl req -new -x509 -days 3650 -key ${localCertDir}/ca.key -out ${localCertDir}/ca.crt -subj '/C=CN/ST=Beijing/L=Beijing/O=Doris/OU=Test/CN=DorisCA'", "Failed to generate CA certificate")

        logger.info("Successfully generated certificates:")
        logger.info("  - CA Certificate: ${localCertDir}/ca.crt")

        def current_ip = runCommand('''ip -4 addr show eth0 | grep -oP "(?<=inet\\s)\\d+(\\.\\d+){3}"''', "Failed to get current ip")
        logger.info("Current IP: ${current_ip}")

        // Generate normal certificate - closure function
        def generateNormalCert = { String certDir ->
            // Create certificate directory
            runCommand("mkdir -p ${certDir}", "Failed to create cert directory ${certDir}")

            // Copy CA certificate to cert directory
            runCommand("cp ${localCertDir}/ca.crt ${certDir}/ca.crt", "Failed to copy CA certificate to ${certDir}")
            runCommand("cp ${localCertDir}/ca.key ${certDir}/ca.key", "Failed to copy CA certificate to ${certDir}")

            // Generate server private key
            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/certificate.key -pkeyopt rsa_keygen_bits:2048", "Failed to generate server private key")

            // Create OpenSSL config file with SAN for all IPs
            def sanEntries = allIps.withIndex().collect { ip, idx -> "IP.${idx + 3} = ${ip}" }.join('\n')
            def opensslConf = """
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = CN
ST = Beijing
L = Beijing
O = Doris
OU = Test
CN = doris-cluster

[v3_req]
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = fe-1
DNS.3 = be-1
DNS.4 = ms-1
DNS.5 = recycle-1
DNS.6 = testclient.example.com
IP.1 = 127.0.0.1
IP.2 = ${current_ip}
${sanEntries}
"""
            def confFile = new File("${certDir}/openssl.cnf")
            confFile.text = opensslConf
            logger.info("Created OpenSSL config with SANs for IPs: ${allIps}")

            // Generate Certificate Signing Request (CSR)
            def genCsrCmd = "openssl req -new -key ${certDir}/certificate.key -out ${certDir}/certificate.csr -config ${certDir}/openssl.cnf"
            logger.info("Generating CSR")
            def csrProc = genCsrCmd.execute()
            csrProc.waitFor()
            assert csrProc.exitValue() == 0 : "Failed to generate CSR"

            // Sign the certificate with CA
            def signCertCmd = "openssl x509 -req -days 3650 -in ${certDir}/certificate.csr -CA ${certDir}/ca.crt -CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/certificate.crt -extensions v3_req -extfile ${certDir}/openssl.cnf"
            logger.info("Signing certificate with CA")
            def signProc = signCertCmd.execute()
            signProc.waitFor()
            assert signProc.exitValue() == 0 : "Failed to sign certificate"

            logger.info("Successfully generated certificates:")
            logger.info("  - Server Certificate: ${certDir}/certificate.crt")
            logger.info("  - Server Private Key: ${certDir}/certificate.key")

            // Verify certificate
            def verifyCmd = "openssl x509 -in ${certDir}/certificate.crt -text -noout"
            logger.info("Verifying generated certificate")
            def verifyProc = verifyCmd.execute()
            def verifyOutput = new StringBuilder()
            verifyProc.waitForProcessOutput(verifyOutput, new StringBuilder())
            logger.info("Certificate details:\n${verifyOutput}")

            // Fix permissions to ensure all users can read the certificate files
            runCommand("chmod 644 ${certDir}/*.crt", "Failed to fix permissions for .crt files")
            runCommand("chmod 644 ${certDir}/*.key", "Failed to fix permissions for .key files")

            // Generate Java KeyStore files for JDBC mTLS connection
            logger.info("=== Generating Java KeyStore files for JDBC ===")
            def keystorePassword = "doris123"
            def keystorePath = "${certDir}/keystore.p12"
            def truststorePath = "${certDir}/truststore.p12"

            // Remove old keystore files if they exist
            runCommand("rm -f ${keystorePath}", "Failed to remove old keystore")
            runCommand("rm -f ${truststorePath}", "Failed to remove old truststore")

            // Create PKCS12 keystore from client certificate and private key
            runCommand("openssl pkcs12 -export -in ${certDir}/certificate.crt -inkey ${certDir}/certificate.key -out ${keystorePath} -password pass:${keystorePassword} -name doris-client", "Failed to create client keystore")

            // Create truststore from CA certificate
            runCommand("keytool -import -noprompt -alias ca-cert -file ${certDir}/ca.crt -keystore ${truststorePath} -storepass ${keystorePassword} -storetype PKCS12", "Failed to create truststore")

            logger.info("Successfully generated KeyStore files:")
            logger.info("  - Client KeyStore: ${keystorePath}")
            logger.info("  - TrustStore: ${truststorePath}")
        }

        // Generate client certificates for SAN auth tests
        def generateClientCerts = { String certDir ->
            // Client cert WITH SAN (for SAN matching tests)
            def clientSanOpensslConf = """
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = CN
ST = Beijing
L = Beijing
O = Doris
OU = Test
CN = test-client

[v3_req]
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
subjectAltName = @alt_names

[alt_names]
email.1 = test@example.com
DNS.1 = testclient.example.com
URI.1 = spiffe://example.com/testclient
"""
            new File("${certDir}/client_san_openssl.cnf").text = clientSanOpensslConf

            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/client_san.key -pkeyopt rsa_keygen_bits:2048", "Failed to generate client SAN key")
            runCommand("openssl req -new -key ${certDir}/client_san.key -out ${certDir}/client_san.csr -config ${certDir}/client_san_openssl.cnf", "Failed to generate client SAN CSR")
            runCommand("openssl x509 -req -days 3650 -in ${certDir}/client_san.csr -CA ${certDir}/ca.crt -CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/client_san.crt -extensions v3_req -extfile ${certDir}/client_san_openssl.cnf", "Failed to sign client SAN cert")

            // Client cert WITHOUT SAN (for no-SAN tests)
            def clientNoSanOpensslConf = """
[req]
distinguished_name = req_distinguished_name
prompt = no

[req_distinguished_name]
C = CN
ST = Beijing
L = Beijing
O = Doris
OU = Test
CN = test-client-nosan
"""
            new File("${certDir}/client_nosan_openssl.cnf").text = clientNoSanOpensslConf

            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/client_nosan.key -pkeyopt rsa_keygen_bits:2048", "Failed to generate client no-SAN key")
            runCommand("openssl req -new -key ${certDir}/client_nosan.key -out ${certDir}/client_nosan.csr -config ${certDir}/client_nosan_openssl.cnf", "Failed to generate client no-SAN CSR")
            runCommand("openssl x509 -req -days 3650 -in ${certDir}/client_nosan.csr -CA ${certDir}/ca.crt -CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/client_nosan.crt", "Failed to sign client no-SAN cert")

            runCommand("chmod 644 ${certDir}/client_*.crt ${certDir}/client_*.key", "Failed to fix client cert permissions")

            logger.info("Client SAN cert generated: ${certDir}/client_san.crt")
            logger.info("Client no-SAN cert generated: ${certDir}/client_nosan.crt")
        }

        // Generate certificates
        generateNormalCert("${certFileDir}")
        generateClientCerts("${certFileDir}")

        // Build container names list
        def containerNames = []
        frontends.each { fe -> containerNames.add("doris-${cluster.name}-fe-${fe.index}") }
        backends.each { be -> containerNames.add("doris-${cluster.name}-be-${be.index}") }
        metaservices.each { ms -> containerNames.add("doris-${cluster.name}-ms-${ms.index}") }
        recyclers.each { rc -> containerNames.add("doris-${cluster.name}-recycle-${rc.index}") }

        // Update certificate function
        def updateCertificate = { String certDir, String container ->
            // Create cert directory
            runCommand("docker exec -i ${container} mkdir -p /tmp/certs", "Failed to create cert directory in ${container}")

            // Create temporary directory for atomic update
            runCommand("docker exec -i ${container} rm -rf /tmp/certs_new", "Failed to remove old temp directory in ${container}")
            runCommand("docker exec -i ${container} mkdir -p /tmp/certs_new", "Failed to create temp cert directory in ${container}")

            // Copy all certificate files to temporary directory first
            ['ca.crt', 'certificate.key', 'certificate.crt'].each { fname ->
                def srcFile = new File("${certDir}/${fname}")
                if (srcFile.exists()) {
                    runCommand("docker cp ${srcFile.absolutePath} ${container}:/tmp/certs_new/${fname}", "Failed to copy ${fname} to temp directory in ${container}")
                }
            }

            // Atomically move all files at once using mv command
            runCommand("docker exec -i ${container} bash -c 'mv -f /tmp/certs_new/* /tmp/certs/'", "Failed to move certificates in ${container}")
            runCommand("docker exec -i ${container} rm -rf /tmp/certs_new", "Failed to cleanup temp directory in ${container}")
        }

        def updateAllCertificate = { String certDir ->
            containerNames.each { container ->
                updateCertificate("${certDir}", "${container}")
            }
        }

        // Deploy certificates to all containers
        updateAllCertificate("${certFileDir}")

        // Update config file function
        def updateConfigFile = { confPath ->
            def configFile = new File(confPath)
            if (!configFile.exists()) {
                logger.warn("Config file not found: ${confPath}")
                return
            }

            def lines = configFile.readLines()
            def newLines = lines.collect { line ->
                if (line.trim().startsWith('enable_tls')) {
                    return 'enable_tls=true'
                }
                return line
            }
            configFile.text = newLines.join('\n')
            logger.info("Updated config file: ${confPath} - enable_tls set to true")
        }

        // Step 1: Update configuration files to enable TLS
        logger.info("=== Updating configuration files to enable TLS ===")

        // Update FE configs
        frontends.each { fe ->
            updateConfigFile(fe.getConfFilePath())
        }

        // Update BE configs
        backends.each { be ->
            updateConfigFile(be.getConfFilePath())
        }

        // Update MS configs
        metaservices.each { ms ->
            updateConfigFile(ms.getConfFilePath())
        }

        // Update Recycler configs
        recyclers.each { recycle ->
            updateConfigFile(recycle.getConfFilePath())
        }

        // Step 2: Restart all nodes with TLS enabled
        logger.info("=== Restarting all nodes with TLS enabled ===")

        def dorisComposePath = cluster.config.dorisComposePath
        def clusterName = cluster.name

        // Helper function to restart nodes without JDBC check
        def restartNodesWithoutWait = { String nodeType, String idFlag ->
            def cmd = "python -W ignore ${dorisComposePath} restart ${clusterName} ${idFlag} --wait-timeout 0 -v --output-json"
            logger.info("Executing: ${cmd}")
            def proc = cmd.execute()
            def stdout = new StringBuilder()
            def stderr = new StringBuilder()
            proc.waitForProcessOutput(stdout, stderr)
            if (proc.exitValue() != 0) {
                logger.error("Restart ${nodeType} failed: stdout=${stdout}, stderr=${stderr}")
                throw new Exception("Failed to restart ${nodeType}")
            }
            logger.info("${nodeType} restart initiated successfully")
        }

        // Restart all node types
        def msIds = metaservices.collect { it.index }.join(' ')
        restartNodesWithoutWait('MS', "--ms-id ${msIds}")

        def feIds = frontends.collect { it.index }.join(' ')
        restartNodesWithoutWait('FE', "--fe-id ${feIds}")

        def beIds = backends.collect { it.index }.join(' ')
        restartNodesWithoutWait('BE', "--be-id ${beIds}")

        def recyclerIds = recyclers.collect { it.index }.join(' ')
        restartNodesWithoutWait('Recycler', "--recycle-id ${recyclerIds}")

        logger.info("All nodes restart commands issued, waiting for nodes to initialize...")
        sleep(40000) // Wait for nodes to restart and initialize

        // Step 3: Connect cluster and run TLS SAN auth tests
        logger.info("=== Connecting to cluster with TLS enabled ===")
        def firstFe = frontends[0]
        logger.info("Using FE[${firstFe.index}] at ${firstFe.host}:${firstFe.queryPort} for mTLS connection")

        // First build base JDBC URL
        def baseJdbcUrl = String.format(
            "jdbc:mysql://%s:%s/?useLocalSessionState=true&allowLoadLocalInfile=false",
            firstFe.host, firstFe.queryPort)

        // Test configuration
        def testUserBase = "test_san_auth_user"
        def testPassword = "Test_123456"
        def sanFull = "email:test@example.com, DNS:testclient.example.com, URI:spiffe://example.com/testclient"
        def sanMismatch = "email:wrong@example.com"

        // Run TLS SAN auth tests using the certificate directory
        def runTlsSanAuthTests = { String certDir ->
            def keystorePassword = "doris123"
            def keystorePath = "${certDir}/keystore.p12"
            def truststorePath = "${certDir}/truststore.p12"

            // Client cert paths
            def sanClientCa = "${certDir}/ca.crt"
            def sanClientCert = "${certDir}/client_san.crt"
            def sanClientKey = "${certDir}/client_san.key"
            def noSanClientCert = "${certDir}/client_nosan.crt"
            def noSanClientKey = "${certDir}/client_nosan.key"

            // Get connection info from cluster
            def mysqlHost = firstFe.host
            def mysqlPort = firstFe.queryPort
            def feHostIp = firstFe.host
            def httpPort = firstFe.httpPort

            // Build mTLS JDBC URL
            def tlsJdbcUrl = org.apache.doris.regression.Config.buildUrlWithDb(
                baseJdbcUrl,
                context.dbName,
                keystorePath,
                keystorePassword,
                truststorePath,
                keystorePassword
            )

            logger.info("Reconnecting with mTLS JDBC URL: ${tlsJdbcUrl}")
            logger.info("MySQL host: ${mysqlHost}, port: ${mysqlPort}")
            logger.info("FE HTTPS host IP: ${feHostIp}, port: ${httpPort}")

            context.connect(context.config.jdbcUser, context.config.jdbcPassword, tlsJdbcUrl) {
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

                // Helper: Execute curl command and check result based on JSON response code
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
                            return code != 0
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to parse JSON response: ${e.message}")
                        return !expectSuccess
                    }
                }

                // Helper: Build curl command with SAN certificate
                def sniHostname = "testclient.example.com"
                def httpEndpoint = "/api/bootstrap"

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

                    // ==================================================================================
                    // MySQL Protocol Tests
                    // ==================================================================================

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
                    // ==================================================================================
                    logger.info("=== Starting HTTPS certificate-based auth tests ===")
                    logger.info("Test endpoint: ${httpEndpoint}")

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
            }
        }

        // Run the tests with the generated certificates
        runTlsSanAuthTests("${certFileDir}")
    }
}
