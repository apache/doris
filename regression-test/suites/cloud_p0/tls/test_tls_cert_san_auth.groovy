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
    //   Strict SAN matching tests: subset/superset/case mismatch
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

            // Client cert WITH subset SAN (email only)
            def clientSubsetOpensslConf = """
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
 CN = test-client-subset

[v3_req]
 keyUsage = digitalSignature, keyEncipherment
 extendedKeyUsage = clientAuth
 subjectAltName = @alt_names

[alt_names]
 email.1 = test@example.com
"""
            new File("${certDir}/client_subset_openssl.cnf").text = clientSubsetOpensslConf

            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/client_subset.key -pkeyopt rsa_keygen_bits:2048", "Failed to generate client subset key")
            runCommand("openssl req -new -key ${certDir}/client_subset.key -out ${certDir}/client_subset.csr -config ${certDir}/client_subset_openssl.cnf", "Failed to generate client subset CSR")
            runCommand("openssl x509 -req -days 3650 -in ${certDir}/client_subset.csr -CA ${certDir}/ca.crt -CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/client_subset.crt -extensions v3_req -extfile ${certDir}/client_subset_openssl.cnf", "Failed to sign client subset cert")

            // Client cert WITH case-mismatch SAN
            def clientCaseOpensslConf = """
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
 CN = test-client-case

[v3_req]
 keyUsage = digitalSignature, keyEncipherment
 extendedKeyUsage = clientAuth
 subjectAltName = @alt_names

[alt_names]
 email.1 = TEST@EXAMPLE.COM
 DNS.1 = TESTCLIENT.EXAMPLE.COM
 URI.1 = SPIFFE://EXAMPLE.COM/TESTCLIENT
"""
            new File("${certDir}/client_case_openssl.cnf").text = clientCaseOpensslConf

            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/client_case.key -pkeyopt rsa_keygen_bits:2048", "Failed to generate client case key")
            runCommand("openssl req -new -key ${certDir}/client_case.key -out ${certDir}/client_case.csr -config ${certDir}/client_case_openssl.cnf", "Failed to generate client case CSR")
            runCommand("openssl x509 -req -days 3650 -in ${certDir}/client_case.csr -CA ${certDir}/ca.crt -CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/client_case.crt -extensions v3_req -extfile ${certDir}/client_case_openssl.cnf", "Failed to sign client case cert")

            // Client cert WITH IP SAN (FE IPs)
            def feIps = frontends.collect { it.host }.unique()
            assertTrue(!feIps.isEmpty(), "No FE IPs found for IP SAN cert")
            def ipSanEntries = feIps.withIndex().collect { ip, idx -> " IP.${idx + 1} = ${ip}" }.join('\n')
            def clientIpOpensslConf = """
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
 CN = test-client-ip

[v3_req]
 keyUsage = digitalSignature, keyEncipherment
 extendedKeyUsage = clientAuth
 subjectAltName = @alt_names

[alt_names]
${ipSanEntries}
"""
            new File("${certDir}/client_ip_openssl.cnf").text = clientIpOpensslConf

            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/client_ip.key -pkeyopt rsa_keygen_bits:2048", "Failed to generate client IP key")
            runCommand("openssl req -new -key ${certDir}/client_ip.key -out ${certDir}/client_ip.csr -config ${certDir}/client_ip_openssl.cnf", "Failed to generate client IP CSR")
            runCommand("openssl x509 -req -days 3650 -in ${certDir}/client_ip.csr -CA ${certDir}/ca.crt -CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/client_ip.crt -extensions v3_req -extfile ${certDir}/client_ip_openssl.cnf", "Failed to sign client IP cert")

            runCommand("chmod 644 ${certDir}/client_*.crt ${certDir}/client_*.key", "Failed to fix client cert permissions")

            logger.info("Client SAN cert generated: ${certDir}/client_san.crt")
            logger.info("Client no-SAN cert generated: ${certDir}/client_nosan.crt")
            logger.info("Client subset cert generated: ${certDir}/client_subset.crt")
            logger.info("Client case cert generated: ${certDir}/client_case.crt")
            logger.info("Client IP SAN cert generated: ${certDir}/client_ip.crt")
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
        def sanSubset = "email:test@example.com"
        def privUser = "${testUserBase}_priv"
        def privTable = "test_san_priv_tbl"
        def privWriteTable = "test_san_priv_write_tbl"

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
            def subsetClientCert = "${certDir}/client_subset.crt"
            def subsetClientKey = "${certDir}/client_subset.key"
            def caseClientCert = "${certDir}/client_case.crt"
            def caseClientKey = "${certDir}/client_case.key"
            def ipSanClientCert = "${certDir}/client_ip.crt"
            def ipSanClientKey = "${certDir}/client_ip.key"

            // Get connection info from cluster
            def mysqlHost = firstFe.host
            def mysqlPort = firstFe.queryPort
            def feHostIp = firstFe.host
            def httpPort = firstFe.httpPort

            def feIpParts = mysqlHost.tokenize('.')
            assertTrue(feIpParts.size() >= 2, "Invalid FE host IP: ${mysqlHost}")
            def hostPrefix = "${feIpParts[0]}.${feIpParts[1]}"
            def hostPattern = "${hostPrefix}.%"
            def secondOctet = Integer.parseInt(feIpParts[1])
            def altSecond = secondOctet + 1
            if (altSecond > 255) {
                altSecond = secondOctet - 1
            }
            def otherPattern = "${feIpParts[0]}.${altSecond}.%"

            def ipHostUser = "${testUserBase}_ip_host"
            def ipOtherUser = "${testUserBase}_ip_other"
            def ipAnyUser = "${testUserBase}_ip_any"
            def ipUserPassword = "12345"

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
                    if (password.isEmpty()) {
                        return "mysql -u${user} -h${mysqlHost} -P${mysqlPort} " +
                           "--ssl-mode=VERIFY_CA " +
                           "--tls-version=TLSv1.2 " +
                           "--ssl-ca=${sanClientCa} " +
                           "--ssl-cert=${sanClientCert} --ssl-key=${sanClientKey} " +
                           "-e \"${query}\""
                    }
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

                // Build MySQL command with custom certificate
                def buildMySQLCmdWithCustomCert = { String user, String password, String cert, String key, String query ->
                    return "mysql -u${user} -p'${password}' -h${mysqlHost} -P${mysqlPort} " +
                           "--ssl-mode=VERIFY_CA " +
                           "--tls-version=TLSv1.2 " +
                           "--ssl-ca=${sanClientCa} " +
                           "--ssl-cert=${cert} --ssl-key=${key} " +
                           "-e \"${query}\""
                }

                def extractSanLine = { String certPath ->
                    def output = runCommand("openssl x509 -in ${certPath} -noout -ext subjectAltName",
                            "Failed to extract SAN from ${certPath}")
                    def sanLine = output.readLines().collect { it.trim() }.find { it.startsWith("IP Address") }
                    assertTrue(sanLine != null && !sanLine.isEmpty(),
                            "Failed to parse SAN line from output: ${output}")
                    return sanLine
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

                // Helper: Build curl command with custom certificate
                def buildCurlWithCustomCert = { String user, String password, String cert, String key, String endpoint ->
                    return "curl -s -k " +
                           "--resolve '${sniHostname}:${httpPort}:${feHostIp}' " +
                           "-u '${user}:${password}' " +
                           "--cert ${cert} --key ${key} " +
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
                    try_sql("DROP USER IF EXISTS '${testUserBase}_strict_1'@'%'")
                    try_sql("DROP USER IF EXISTS '${testUserBase}_strict_2'@'%'")
                    try_sql("DROP USER IF EXISTS '${testUserBase}_strict_3'@'%'")
                    try_sql("DROP USER IF EXISTS '${testUserBase}_http_strict_1'@'%'")
                    try_sql("DROP USER IF EXISTS '${testUserBase}_http_strict_2'@'%'")
                    try_sql("DROP USER IF EXISTS '${testUserBase}_http_strict_3'@'%'")
                    try_sql("DROP USER IF EXISTS '${testUserBase}_show_proc'@'%'")
                    try_sql("DROP USER IF EXISTS '${testUserBase}_cycle'@'%'")
                    try_sql("DROP USER IF EXISTS '${ipHostUser}'@'${hostPattern}'")
                    try_sql("DROP USER IF EXISTS '${ipOtherUser}'@'${otherPattern}'")
                    try_sql("DROP USER IF EXISTS '${ipAnyUser}'@'%'")
                    try_sql("DROP USER IF EXISTS '${privUser}'@'%'")
                    try_sql("DROP TABLE IF EXISTS ${privTable}")
                    try_sql("DROP TABLE IF EXISTS ${privWriteTable}")
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

                    def assertSqlFailure = { String stmt, String message ->
                        try {
                            sql stmt
                            assert false : message
                        } catch (Exception e) {
                            logger.info("Expected failure for [${stmt}]: ${e.message}")
                        }
                    }

                    // Resolve current compute group for grants
                    def clusters = sql_return_maparray "show clusters"
                    def currentCluster = clusters.find { it.is_current == "TRUE" }
                    if (currentCluster == null && !clusters.isEmpty()) {
                        currentCluster = clusters.get(0)
                    }
                    assertNotNull(currentCluster, "No compute group found")
                    def computeGroupName = currentCluster.cluster
                    def grantComputeGroupUsage = { String userName ->
                        sql """GRANT USAGE_PRIV ON COMPUTE GROUP '${computeGroupName}' TO '${userName}'@'%'"""
                    }
 
                    // ==================================================================================
                    // TLS SAN semantic validation tests (CREATE/ALTER)
                    // ==================================================================================

                    def invalidSans = [
                        "", " ", ",",
                        "IP", "DNS", "URI", "NONE", "SAN",
                        "ip", "dns", "uri", "none", "san"
                    ]
                    invalidSans.eachWithIndex { sanValue, idx ->
                        def createUser = "${testUserBase}_invalid_create_${idx}"
                        assertSqlFailure(
                                "CREATE USER '${createUser}'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanValue}'",
                                "CREATE USER should fail for SAN '${sanValue}'"
                        )

                        def alterUser = "${testUserBase}_invalid_alter_${idx}"
                        try {
                            sql "CREATE USER '${alterUser}'@'%' IDENTIFIED BY '${testPassword}'"
                            assertSqlFailure(
                                    "ALTER USER '${alterUser}'@'%' REQUIRE SAN '${sanValue}'",
                                    "ALTER USER should fail for SAN '${sanValue}'"
                            )
                        } finally {
                            try_sql("DROP USER IF EXISTS '${alterUser}'@'%'")
                        }
                    }

                    // ==================================================================================
                    // SHOW PROC /auth/ RequireSan checks
                    // ==================================================================================
                    def authUser = "${testUserBase}_show_proc"
                    sql "CREATE USER '${authUser}'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"

                    def authRows1 = sql_return_maparray "show proc '/auth/'"
                    def authRow1 = authRows1.find { it.UserIdentity == "'${authUser}'@'%'" }
                    assertTrue(authRow1 != null, "Should find user ${authUser} in show proc /auth/")
                    assertTrue(authRow1.RequireSan == sanFull, "RequireSan should match SAN after CREATE USER")

                    sql "ALTER USER '${authUser}'@'%' REQUIRE SAN '${sanMismatch}'"
                    def authRows2 = sql_return_maparray "show proc '/auth/'"
                    def authRow2 = authRows2.find { it.UserIdentity == "'${authUser}'@'%'" }
                    assertTrue(authRow2 != null, "Should find user ${authUser} after ALTER USER")
                    assertTrue(authRow2.RequireSan == sanMismatch, "RequireSan should update after ALTER USER")

                    sql "ALTER USER '${authUser}'@'%' REQUIRE NONE"
                    def authRows3 = sql_return_maparray "show proc '/auth/'"
                    def authRow3 = authRows3.find { it.UserIdentity == "'${authUser}'@'%'" }
                    assertTrue(authRow3 != null, "Should find user ${authUser} after REQUIRE NONE")
                    assertTrue(authRow3.RequireSan == null, "RequireSan should clear after REQUIRE NONE")

                    // Multiple SAN updates should keep single RequireSan entry
                    def sanUpdates = [sanFull, sanMismatch, "email:another@example.com"]
                    sanUpdates.each { newSan ->
                        sql "ALTER USER '${authUser}'@'%' REQUIRE SAN '${newSan}'"
                        def authRowsUpdate = sql_return_maparray "show proc '/auth/'"
                        def authMatches = authRowsUpdate.findAll { it.UserIdentity == "'${authUser}'@'%'" }
                        assertTrue(authMatches.size() == 1, "Expect exactly one auth row for ${authUser}")
                        assertTrue(authMatches[0].RequireSan == newSan,
                                "RequireSan should update to ${newSan}")
                    }

                    // ==================================================================================
                    // SAN create/alter/drop loop tests (MySQL)
                    // ==================================================================================
                    def cycleUser = "${testUserBase}_cycle"
                    def cycleUserIdentity = "'${cycleUser}'@'%'"
                    def createSans = [null, sanFull, sanMismatch, sanSubset, sanFull]
                    def wrongSans = [sanMismatch, sanSubset, sanMismatch, sanSubset, sanMismatch]
                    def findAuthRowByIdentity = { String userIdentity ->
                        def authRows = sql_return_maparray "show proc '/auth/'"
                        return authRows.find { it.UserIdentity == userIdentity }
                    }

                    (1..5).each { idx ->
                        logger.info("=== SAN cycle round ${idx} (MySQL) ===")
                        try_sql("DROP USER IF EXISTS '${cycleUser}'@'%'")
 
                        def createSan = createSans[idx - 1]
                        if (createSan == null) {
                            sql "CREATE USER '${cycleUser}'@'%' IDENTIFIED BY '${testPassword}'"
                        } else {
                            sql "CREATE USER '${cycleUser}'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${createSan}'"
                        }
 
                        sql "ALTER USER '${cycleUser}'@'%' REQUIRE SAN '${sanFull}'"
                        def authRowFull = findAuthRowByIdentity(cycleUserIdentity)
                        assertTrue(authRowFull != null, "Should find user ${cycleUser} after REQUIRE SAN")
                        assertTrue(authRowFull.RequireSan == sanFull, "RequireSan should be ${sanFull}")
 
                        def cmdOk = buildMySQLCmdWithCert(cycleUser, testPassword, "SELECT 1")
                        assertTrue(executeMySQLCommand(cmdOk, true),
                                "SAN cycle ${idx} should succeed with matching SAN")
 
                        def wrongSan = wrongSans[idx - 1]
                        sql "ALTER USER '${cycleUser}'@'%' REQUIRE SAN '${wrongSan}'"
                        def authRowWrong = findAuthRowByIdentity(cycleUserIdentity)
                        assertTrue(authRowWrong != null, "Should find user ${cycleUser} after wrong REQUIRE SAN")
                        assertTrue(authRowWrong.RequireSan == wrongSan, "RequireSan should be ${wrongSan}")
 
                        def cmdWrong = buildMySQLCmdWithCert(cycleUser, testPassword, "SELECT 1")
                        assertTrue(executeMySQLCommand(cmdWrong, false),
                                "SAN cycle ${idx} should fail with mismatched SAN")
 
                        sql "ALTER USER '${cycleUser}'@'%' REQUIRE NONE"
                        def authRowNone = findAuthRowByIdentity(cycleUserIdentity)
                        assertTrue(authRowNone != null, "Should find user ${cycleUser} after REQUIRE NONE")
                        assertTrue(authRowNone.RequireSan == null, "RequireSan should be cleared")
                    }

                    // ==================================================================================
                    // Host-based IP SAN user tests (MySQL)
                    // ==================================================================================
                    logger.info("=== Host-based IP SAN user tests (MySQL) ===")
                    def ipSanLine = extractSanLine(ipSanClientCert)
                    logger.info("IP SAN line: ${ipSanLine}")

                    sql "CREATE USER '${ipHostUser}'@'${hostPattern}' IDENTIFIED BY '${ipUserPassword}' REQUIRE SAN '${ipSanLine}'"
                    def ipHostCmd = buildMySQLCmdWithCustomCert(ipHostUser, ipUserPassword, ipSanClientCert, ipSanClientKey, "SELECT 1")
                    assertTrue(executeMySQLCommand(ipHostCmd, true),
                            "IP SAN user should succeed for host pattern ${hostPattern}")

                    sql "CREATE USER '${ipOtherUser}'@'${otherPattern}' IDENTIFIED BY '${ipUserPassword}' REQUIRE SAN '${ipSanLine}'"
                    def ipOtherCmd = buildMySQLCmdWithCustomCert(ipOtherUser, ipUserPassword, ipSanClientCert, ipSanClientKey, "SELECT 1")
                    assertTrue(executeMySQLCommand(ipOtherCmd, false),
                            "IP SAN user should fail for host pattern ${otherPattern}")

                    sql "CREATE USER '${ipAnyUser}'@'%' IDENTIFIED BY '${ipUserPassword}'"
                    sql "ALTER USER '${ipAnyUser}'@'%' REQUIRE SAN '${ipSanLine}'"
                    def ipAnyCmd = buildMySQLCmdWithCustomCert(ipAnyUser, ipUserPassword, ipSanClientCert, ipSanClientKey, "SELECT 1")
                    assertTrue(executeMySQLCommand(ipAnyCmd, true),
                            "IP SAN user should succeed for host pattern %")

                    // ==================================================================================
                    // Privilege enforcement tests (MySQL)
                    // ==================================================================================
                    logger.info("=== Privilege enforcement tests (MySQL) ===")
                    try_sql("DROP TABLE IF EXISTS ${privTable}")
                    try_sql("DROP TABLE IF EXISTS ${privWriteTable}")
                    sql """
                        CREATE TABLE ${privTable} (
                            k1 INT,
                            k2 VARCHAR(50)
                        ) DISTRIBUTED BY HASH(k1) BUCKETS 1
                        PROPERTIES ("replication_num" = "1")
                    """
                    sql "INSERT INTO ${privTable} VALUES (1, 'priv_value')"

                    sql "CREATE USER '${privUser}'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    sql "GRANT SELECT_PRIV ON ${context.dbName}.${privTable} TO '${privUser}'@'%'"
                    grantComputeGroupUsage(privUser)

                    def privSelectCmd = buildMySQLCmdWithCert(privUser, testPassword,
                            "SELECT * FROM ${context.dbName}.${privTable} LIMIT 1")
                    assertTrue(executeMySQLCommand(privSelectCmd, true),
                            "Privilege test should allow SELECT for read-only user")

                    def privCreateCmd = buildMySQLCmdWithCert(privUser, testPassword,
                            "CREATE TABLE ${context.dbName}.${privWriteTable} (k1 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1 " +
                                    "PROPERTIES ('replication_num' = '1')")
                    assertTrue(executeMySQLCommand(privCreateCmd, false),
                            "Privilege test should reject CREATE TABLE without write privilege")

                    sql "GRANT CREATE_PRIV ON ${context.dbName}.* TO '${privUser}'@'%'"
                    sql "GRANT LOAD_PRIV ON ${context.dbName}.* TO '${privUser}'@'%'"

                    def privCreateCmd2 = buildMySQLCmdWithCert(privUser, testPassword,
                            "CREATE TABLE ${context.dbName}.${privWriteTable} (k1 INT, k2 VARCHAR(50)) " +
                                    "DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ('replication_num' = '1')")
                    assertTrue(executeMySQLCommand(privCreateCmd2, true),
                            "Privilege test should allow CREATE TABLE after grant")

                    def privInsertCmd = buildMySQLCmdWithCert(privUser, testPassword,
                            "INSERT INTO ${context.dbName}.${privWriteTable} VALUES (1, 'priv_write')")
                    assertTrue(executeMySQLCommand(privInsertCmd, true),
                            "Privilege test should allow INSERT after grant")
 
                    // ==================================================================================
                    // Root/Admin SAN auth checks
                    // ==================================================================================

                    def rootUser = "root"
                    def adminUser = "admin"
                    def rootPassword = context.config.jdbcPassword
                    def adminPassword = context.config.jdbcPassword
                    def findAuthRow = { String userName ->
                        def authRows = sql_return_maparray "show proc '/auth/'"
                        return authRows.find { it.UserIdentity == "'${userName}'@'%'" }
                    }
                    try {
                        sql "ALTER USER '${rootUser}'@'%' REQUIRE SAN '${sanFull}'"
                        sql "ALTER USER '${adminUser}'@'%' REQUIRE SAN '${sanFull}'"

                        def rootRow = findAuthRow(rootUser)
                        def adminRow = findAuthRow(adminUser)
                        assertTrue(rootRow != null, "Should find root in show proc /auth/")
                        assertTrue(adminRow != null, "Should find admin in show proc /auth/")
                        assertTrue(rootRow.RequireSan == sanFull, "Root RequireSan should match SAN")
                        assertTrue(adminRow.RequireSan == sanFull, "Admin RequireSan should match SAN")

                        def rootCmd = buildMySQLCmdWithCert(rootUser, rootPassword, "SELECT 1")
                        def adminCmd = buildMySQLCmdWithCert(adminUser, adminPassword, "SELECT 1")
                        assertTrue(executeMySQLCommand(rootCmd, true), "Root mTLS login should succeed")
                        assertTrue(executeMySQLCommand(adminCmd, true), "Admin mTLS login should succeed")

                        def rootHttpCmd = buildCurlWithCert(rootUser, rootPassword, httpEndpoint)
                        def adminHttpCmd = buildCurlWithCert(adminUser, adminPassword, httpEndpoint)
                        assertTrue(executeCurlCommand(rootHttpCmd, true), "Root HTTPS auth should succeed")
                        assertTrue(executeCurlCommand(adminHttpCmd, true), "Admin HTTPS auth should succeed")
                    } finally {
                        sql "ALTER USER '${rootUser}'@'%' REQUIRE NONE"
                        sql "ALTER USER '${adminUser}'@'%' REQUIRE NONE"
                    }

                    // ==================================================================================
                    // SAN strict matching tests (MySQL + HTTPS)
                    // ==================================================================================
                    logger.info("=== SAN strict matching tests ===")
                    sql "CREATE USER '${testUserBase}_strict_1'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    def strictCmd1 = buildMySQLCmdWithCustomCert(
                            "${testUserBase}_strict_1",
                            testPassword,
                            subsetClientCert,
                            subsetClientKey,
                            "SELECT 1"
                    )
                    assertTrue(executeMySQLCommand(strictCmd1, false),
                            "Strict Test 1 should fail: REQUIRE SAN full, cert subset")

                    sql "CREATE USER '${testUserBase}_strict_2'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanSubset}'"
                    def strictCmd2 = buildMySQLCmdWithCert("${testUserBase}_strict_2", testPassword, "SELECT 1")
                    assertTrue(executeMySQLCommand(strictCmd2, false),
                            "Strict Test 2 should fail: REQUIRE SAN subset, cert full")

                    sql "CREATE USER '${testUserBase}_strict_3'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    def strictCmd3 = buildMySQLCmdWithCustomCert(
                            "${testUserBase}_strict_3",
                            testPassword,
                            caseClientCert,
                            caseClientKey,
                            "SELECT 1"
                    )
                    assertTrue(executeMySQLCommand(strictCmd3, false),
                            "Strict Test 3 should fail: REQUIRE SAN full, cert case mismatch")

                    sql "CREATE USER '${testUserBase}_http_strict_1'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    sql "GRANT ADMIN_PRIV ON *.*.* TO '${testUserBase}_http_strict_1'@'%'"
                    def httpStrict1 = buildCurlWithCustomCert(
                            "${testUserBase}_http_strict_1",
                            testPassword,
                            subsetClientCert,
                            subsetClientKey,
                            httpEndpoint
                    )
                    assertTrue(executeCurlCommand(httpStrict1, false),
                            "HTTP Strict Test 1 should fail: REQUIRE SAN full, cert subset")

                    sql "CREATE USER '${testUserBase}_http_strict_2'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanSubset}'"
                    sql "GRANT ADMIN_PRIV ON *.*.* TO '${testUserBase}_http_strict_2'@'%'"
                    def httpStrict2 = buildCurlWithCert("${testUserBase}_http_strict_2", testPassword, httpEndpoint)
                    assertTrue(executeCurlCommand(httpStrict2, false),
                            "HTTP Strict Test 2 should fail: REQUIRE SAN subset, cert full")

                    sql "CREATE USER '${testUserBase}_http_strict_3'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    sql "GRANT ADMIN_PRIV ON *.*.* TO '${testUserBase}_http_strict_3'@'%'"
                    def httpStrict3 = buildCurlWithCustomCert(
                            "${testUserBase}_http_strict_3",
                            testPassword,
                            caseClientCert,
                            caseClientKey,
                            httpEndpoint
                    )
                    assertTrue(executeCurlCommand(httpStrict3, false),
                            "HTTP Strict Test 3 should fail: REQUIRE SAN full, cert case mismatch")

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
