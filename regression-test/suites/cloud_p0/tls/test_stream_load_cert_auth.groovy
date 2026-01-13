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

suite('test_stream_load_cert_auth', 'docker, p0') {
    // This test verifies TLS certificate-based authentication for Stream Load via BE.
    // It tests the complete data flow:
    //   Client -> BE HTTPS -> BE extracts cert info -> TCertBasedAuth in Thrift -> FE verifies SAN
    //
    // Test cases:
    //   SL-01: SAN matching + correct password -> success
    //   SL-02: No TLS requirement + password auth -> success
    //   SL-03: ignore_password=true + wrong password -> success (cert only)
    //   SL-04: SAN matching + wrong password + ignore_password=false -> failure (key test!)
    //   SL-05: SAN mismatch -> failure
    //   SL-05a: REQUIRE SAN full, cert subset -> failure
    //   SL-05b: REQUIRE SAN subset, cert full -> failure
    //   SL-05c: REQUIRE SAN full, cert case mismatch -> failure
    //   SL-06: No certificate + REQUIRE SAN -> failure
    //   SL-07: Certificate without SAN extension -> failure
    //   SL-08: Two-phase commit with cert auth -> success
    //   SL-09: ALTER USER add/remove REQUIRE SAN -> dynamic effect
    //   SL-10: Privilege enforcement with SAN -> load failure/success
    //   SL-11: SAN create/alter/drop loop tests

    def testName = "test_stream_load_cert_auth"

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true

    // Add TLS configuration to all node types (exactly same as test_all_node_reload_cert.groovy)
    def tlsConfigs = [
        'enable_tls=false',
        'tls_verify_mode=verify_fail_if_no_peer_cert',
        'tls_certificate_path=/tmp/certs/certificate.crt',
        'tls_private_key_path=/tmp/certs/certificate.key',
        'tls_ca_certificate_path=/tmp/certs/ca.crt',
        'tls_cert_refresh_interval_seconds=5'
    ]

    options.feConfigs += tlsConfigs
    options.beConfigs += tlsConfigs
    options.msConfigs += tlsConfigs
    options.recycleConfigs += tlsConfigs

    def localCertDir = "/tmp/${testName}"
    def certFileDir = "${localCertDir}/certs1"

    docker(options) {
        sql """ CREATE DATABASE IF NOT EXISTS ${context.dbName}; """

        // Get all FE IPs (exactly same as test_all_node_reload_cert.groovy)
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

        // Collect all unique IPs (exactly same as test_all_node_reload_cert.groovy)
        def allIps = []
        frontends.each { fe -> if (!allIps.contains(fe.host)) allIps.add(fe.host) }
        backends.each { be -> if (!allIps.contains(be.host)) allIps.add(be.host) }
        metaservices.each { ms -> if (!allIps.contains(ms.host)) allIps.add(ms.host) }
        recyclers.each { rc -> if (!allIps.contains(rc.host)) allIps.add(rc.host) }
        logger.info("All unique IPs: ${allIps}")

        // Helper: Run command (exactly same as test_all_node_reload_cert.groovy)
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

        // Generate certificates for all IPs (exactly same as test_all_node_reload_cert.groovy)
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

        // Generate normal certificate - closure function (exactly same structure as test_all_node_reload_cert.groovy)
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

        // Generate client certificates for Stream Load SAN auth tests
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

            runCommand("chmod 644 ${certDir}/client_*.crt ${certDir}/client_*.key", "Failed to fix client cert permissions")

            logger.info("Client SAN cert generated: ${certDir}/client_san.crt")
            logger.info("Client no-SAN cert generated: ${certDir}/client_nosan.crt")
            logger.info("Client subset cert generated: ${certDir}/client_subset.crt")
            logger.info("Client case cert generated: ${certDir}/client_case.crt")
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

        // Update certificate function (exactly same as test_all_node_reload_cert.groovy)
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

        // Update config file function (exactly same as test_all_node_reload_cert.groovy)
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

        // Helper function to restart nodes without JDBC check (exactly same as test_all_node_reload_cert.groovy)
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

        // Step 3: Connect cluster and run Stream Load cert auth tests
        logger.info("=== Connecting to cluster with TLS enabled ===")
        def firstFe = frontends[0]
        def firstBe = backends[0]
        logger.info("Using FE[${firstFe.index}] at ${firstFe.host}:${firstFe.queryPort} for mTLS connection")
        logger.info("Using BE[${firstBe.index}] at ${firstBe.host}:${firstBe.httpPort} for Stream Load")

        // First build base JDBC URL (exactly same as test_all_node_reload_cert.groovy)
        def baseJdbcUrl = String.format(
            "jdbc:mysql://%s:%s/?useLocalSessionState=true&allowLoadLocalInfile=false",
            firstFe.host, firstFe.queryPort)

        // Test configuration
        def testUserBase = "test_sl_cert_user"
        def testPassword = "Test_123456"
        def tableName = "test_stream_load_cert_auth_tbl"
        def clientSanValue = "email:test@example.com, DNS:testclient.example.com, URI:spiffe://example.com/testclient"
        def sanFull = clientSanValue
        def sanMismatch = "email:wrong@example.com"
        def sanSubset = "email:test@example.com"
        def privUser = "${testUserBase}_priv"

        // Run Stream Load cert auth tests using the certificate directory
        def runStreamLoadCertAuthTests = { String certDir ->
            def keystorePassword = "doris123"
            def keystorePath = "${certDir}/keystore.p12"
            def truststorePath = "${certDir}/truststore.p12"

            // Client cert paths for curl
            def sanClientCert = "${certDir}/client_san.crt"
            def sanClientKey = "${certDir}/client_san.key"
            def noSanClientCert = "${certDir}/client_nosan.crt"
            def noSanClientKey = "${certDir}/client_nosan.key"
            def subsetClientCert = "${certDir}/client_subset.crt"
            def subsetClientKey = "${certDir}/client_subset.key"
            def caseClientCert = "${certDir}/client_case.crt"
            def caseClientKey = "${certDir}/client_case.key"
            def caCert = "${certDir}/ca.crt"

            def beHost = firstBe.host
            def beHttpPort = firstBe.httpPort

            // Build mTLS JDBC URL (exactly same as test_all_node_reload_cert.groovy)
            def tlsJdbcUrl = org.apache.doris.regression.Config.buildUrlWithDb(
                baseJdbcUrl,
                context.dbName,
                keystorePath,
                keystorePassword,
                truststorePath,
                keystorePassword
            )

            logger.info("Reconnecting with mTLS JDBC URL: ${tlsJdbcUrl}")

            // Helper: Execute Stream Load via curl
            def executeStreamLoadCurl = { Map params ->
                def user = params.user
                def password = params.password
                def db = params.db ?: context.dbName
                def table = params.table
                def data = params.data
                def certPath = params.certPath
                def keyPath = params.keyPath
                def label = params.label ?: "sl_cert_${UUID.randomUUID().toString().replace('-', '')}"
                def twoPhaseCommit = params.twoPhaseCommit ?: false

                def certOpts = (certPath && keyPath) ? "--cert ${certPath} --key ${keyPath}" : ""
                def twoPhaseHeader = twoPhaseCommit ? '-H "two_phase_commit: true"' : ""

                def cmd = """curl -s -k --cacert ${caCert} \\
                    ${certOpts} \\
                    -u '${user}:${password}' \\
                    -H "Expect: 100-continue" \\
                    -H "label: ${label}" \\
                    -H "column_separator: ," \\
                    ${twoPhaseHeader} \\
                    -T - \\
                    'https://${beHost}:${beHttpPort}/api/${db}/${table}/_stream_load' \\
                    <<< '${data}' 2>&1"""

                logger.info("Execute Stream Load for user ${user}")
                logger.debug("Command: ${cmd}")

                def cmds = ["/bin/bash", "-c", cmd]
                Process p = cmds.execute()
                def errMsg = new StringBuilder()
                def msg = new StringBuilder()
                p.waitForProcessOutput(msg, errMsg)

                def output = msg.toString().trim()
                def errorOutput = errMsg.toString().trim()
                logger.info("Stream Load response: ${output}")
                if (errorOutput) logger.info("Stream Load stderr: ${errorOutput}")
                logger.info("Exit value: ${p.exitValue()}")

                if (p.exitValue() != 0) {
                    return [success: false, output: output, error: errorOutput, exitCode: p.exitValue()]
                }

                try {
                    def json = new groovy.json.JsonSlurper().parseText(output)
                    def status = json.Status?.toLowerCase()
                    def isSuccess = (status == "success" || status == "publish timeout")
                    return [success: isSuccess, output: output, json: json, txnId: json.TxnId, label: label, status: status]
                } catch (Exception e) {
                    logger.warn("Failed to parse JSON: ${e.message}")
                    return [success: false, output: output, error: e.message]
                }
            }

            // Helper: Execute 2PC action via curl
            def execute2PCAction = { Map params ->
                def user = params.user
                def password = params.password
                def db = params.db ?: context.dbName
                def txnId = params.txnId
                def action = params.action
                def certPath = params.certPath
                def keyPath = params.keyPath

                def certOpts = (certPath && keyPath) ? "--cert ${certPath} --key ${keyPath}" : ""

                def cmd = """curl -s -k --cacert ${caCert} \\
                    ${certOpts} \\
                    -u '${user}:${password}' \\
                    -X PUT \\
                    -H "txn_id:${txnId}" \\
                    -H "txn_operation:${action}" \\
                    'https://${beHost}:${beHttpPort}/api/${db}/_stream_load_2pc' 2>&1"""

                logger.info("Execute 2PC ${action} for txn ${txnId}")

                def cmds = ["/bin/bash", "-c", cmd]
                Process p = cmds.execute()
                def errMsg = new StringBuilder()
                def msg = new StringBuilder()
                p.waitForProcessOutput(msg, errMsg)

                def output = msg.toString().trim()
                logger.info("2PC ${action} response: ${output}")

                if (p.exitValue() != 0) {
                    return [success: false, output: output, error: errMsg.toString()]
                }

                try {
                    def json = new groovy.json.JsonSlurper().parseText(output)
                    def status = json.status?.toLowerCase()
                    return [success: (status == "success"), output: output, json: json, status: status]
                } catch (Exception e) {
                    return [success: false, output: output, error: e.message]
                }
            }

            // Connect with mTLS and run tests (exactly same pattern as test_all_node_reload_cert.groovy)
            context.connect(context.config.jdbcUser, context.config.jdbcPassword, tlsJdbcUrl) {
                // Cleanup function
                def cleanup = {
                    logger.info("Cleaning up test resources...")
                    try_sql("DROP TABLE IF EXISTS ${tableName}")
                    (1..12).each { i ->
                        try_sql("DROP USER IF EXISTS '${testUserBase}_${i}'@'%'")
                    }
                    try_sql("DROP USER IF EXISTS '${testUserBase}_cycle'@'%'")
                    try_sql("DROP USER IF EXISTS '${privUser}'@'%'")
                }

                // Save original config
                def origIgnorePassword = "false"
                try {
                    def configResult = sql "SHOW FRONTEND CONFIG LIKE 'tls_cert_based_auth_ignore_password'"
                    if (!configResult.isEmpty()) {
                        origIgnorePassword = configResult[0][1]
                    }
                } catch (Exception e) {
                    logger.info("Could not get original config: ${e.message}")
                }

                cleanup()

                // Resolve current compute group and helper to grant usage
                def clusters = sql_return_maparray """show clusters"""
                def currentCluster = clusters.stream().filter { it.is_current == "TRUE" }.findFirst().orElse(
                        clusters.isEmpty() ? null : clusters.get(0))
                assertNotNull(currentCluster, "No compute group found")
                def computeGroupName = currentCluster.cluster
                def grantComputeGroupUsage = { String userName ->
                    sql """GRANT USAGE_PRIV ON COMPUTE GROUP '${computeGroupName}' TO '${userName}'@'%'"""
                }

                try {
                    // Create test table
                    sql """
                        CREATE TABLE ${tableName} (
                            k1 INT,
                            k2 VARCHAR(100)
                        ) DISTRIBUTED BY HASH(k1) BUCKETS 1
                        PROPERTIES ("replication_num" = "1")
                    """
                    logger.info("Created test table: ${tableName}")

                    sql "ADMIN SET FRONTEND CONFIG ('tls_cert_based_auth_ignore_password' = 'false')"

                    // ==================================================================================
                    // SL-01: SAN matching + correct password -> success
                    // ==================================================================================
                    logger.info("=== SL-01: Matching SAN + correct password ===")
                    sql "CREATE USER '${testUserBase}_1'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${testUserBase}_1'@'%'"
                    grantComputeGroupUsage("${testUserBase}_1")

                    def result1 = executeStreamLoadCurl(
                        user: "${testUserBase}_1",
                        password: testPassword,
                        table: tableName,
                        data: "1,value1\\\n2,value2",
                        certPath: sanClientCert,
                        keyPath: sanClientKey
                    )
                    assertTrue(result1.success, "SL-01 should succeed: ${result1.output}")
                    logger.info("SL-01 PASSED")

                    def count1 = sql "SELECT COUNT(*) FROM ${tableName}"
                    assertTrue(count1[0][0] >= 2, "Data should be loaded")
                    sql "TRUNCATE TABLE ${tableName}"

                    // ==================================================================================
                    // SL-01a: Root/Admin REQUIRE SAN + Stream Load -> success
                    // ==================================================================================
                    logger.info("=== SL-01a: Root/Admin REQUIRE SAN Stream Load ===")
                    def rootUser = "root"
                    def adminUser = "admin"
                    def rootPassword = context.config.jdbcPassword
                    def adminPassword = context.config.jdbcPassword
                    try {
                        sql "ALTER USER '${rootUser}'@'%' REQUIRE SAN '${sanFull}'"
                        sql "ALTER USER '${adminUser}'@'%' REQUIRE SAN '${sanFull}'"

                        def rootResult = executeStreamLoadCurl(
                            user: rootUser,
                            password: rootPassword,
                            table: tableName,
                            data: "13,root_value",
                            certPath: sanClientCert,
                            keyPath: sanClientKey
                        )
                        assertTrue(rootResult.success, "SL-01a root should succeed: ${rootResult.output}")

                        def adminResult = executeStreamLoadCurl(
                            user: adminUser,
                            password: adminPassword,
                            table: tableName,
                            data: "14,admin_value",
                            certPath: sanClientCert,
                            keyPath: sanClientKey
                        )
                        assertTrue(adminResult.success, "SL-01a admin should succeed: ${adminResult.output}")

                        def countAdminRoot = sql "SELECT COUNT(*) FROM ${tableName} WHERE k2 IN ('root_value','admin_value')"
                        assertTrue(countAdminRoot[0][0] >= 2, "Root/Admin data should be loaded")
                        sql "TRUNCATE TABLE ${tableName}"
                    } finally {
                        sql "ALTER USER '${rootUser}'@'%' REQUIRE NONE"
                        sql "ALTER USER '${adminUser}'@'%' REQUIRE NONE"
                    }

                    // ==================================================================================
                    // SL-02: No TLS requirement + password auth -> success
                    // ==================================================================================
                    logger.info("=== SL-02: No TLS requirement + password auth ===")
                    sql "CREATE USER '${testUserBase}_2'@'%' IDENTIFIED BY '${testPassword}'"
                    sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${testUserBase}_2'@'%'"
                    grantComputeGroupUsage("${testUserBase}_2")

                    def result2 = executeStreamLoadCurl(
                        user: "${testUserBase}_2",
                        password: testPassword,
                        table: tableName,
                        data: "3,value3",
                        certPath: sanClientCert,
                        keyPath: sanClientKey
                    )
                    assertTrue(result2.success, "SL-02 should succeed: ${result2.output}")
                    logger.info("SL-02 PASSED")
                    sql "TRUNCATE TABLE ${tableName}"

                    // ==================================================================================
                    // SL-03: ignore_password=true + wrong password -> success (cert only)
                    // ==================================================================================
                    logger.info("=== SL-03: ignore_password=true + wrong password ===")
                    sql "ADMIN SET FRONTEND CONFIG ('tls_cert_based_auth_ignore_password' = 'true')"
                    sql "CREATE USER '${testUserBase}_3'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${testUserBase}_3'@'%'"
                    grantComputeGroupUsage("${testUserBase}_3")

                    def result3 = executeStreamLoadCurl(
                        user: "${testUserBase}_3",
                        password: "wrong_password",
                        table: tableName,
                        data: "4,value4",
                        certPath: sanClientCert,
                        keyPath: sanClientKey
                    )
                    assertTrue(result3.success, "SL-03 should succeed (password ignored): ${result3.output}")
                    logger.info("SL-03 PASSED")
                    sql "TRUNCATE TABLE ${tableName}"

                    sql "ADMIN SET FRONTEND CONFIG ('tls_cert_based_auth_ignore_password' = 'false')"

                    // ==================================================================================
                    // SL-04: SAN matching + wrong password + ignore_password=false -> failure
                    // This is the key test: even if cert SAN matches, wrong password should be rejected
                    // when ignore_password is false
                    // ==================================================================================
                    logger.info("=== SL-04: Matching SAN + wrong password + ignore_password=false ===")
                    sql "CREATE USER '${testUserBase}_4'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${testUserBase}_4'@'%'"
                    grantComputeGroupUsage("${testUserBase}_4")

                    def result4 = executeStreamLoadCurl(
                        user: "${testUserBase}_4",
                        password: "wrong_password",
                        table: tableName,
                        data: "5,value5",
                        certPath: sanClientCert,
                        keyPath: sanClientKey
                    )
                    assertFalse(result4.success, "SL-04 should fail: wrong password even with matching SAN when ignore_password=false")
                    logger.info("SL-04 PASSED")

                    // ==================================================================================
                    // SL-05: SAN mismatch -> failure
                    // ==================================================================================
                    logger.info("=== SL-05: SAN mismatch ===")
                    sql "CREATE USER '${testUserBase}_5'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanMismatch}'"
                    sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${testUserBase}_5'@'%'"
                    grantComputeGroupUsage("${testUserBase}_5")

                    def result5 = executeStreamLoadCurl(
                        user: "${testUserBase}_5",
                        password: testPassword,
                        table: tableName,
                        data: "6,value6",
                        certPath: sanClientCert,
                        keyPath: sanClientKey
                    )
                    assertFalse(result5.success, "SL-05 should fail: SAN mismatch")
                    logger.info("SL-05 PASSED")

                    // ==================================================================================
                    // SL-05a: REQUIRE SAN full, cert subset -> failure
                    // ==================================================================================
                    logger.info("=== SL-05a: REQUIRE SAN full, cert subset ===")
                    sql "CREATE USER '${testUserBase}_10'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${testUserBase}_10'@'%'"
                    grantComputeGroupUsage("${testUserBase}_10")

                    def result5a = executeStreamLoadCurl(
                        user: "${testUserBase}_10",
                        password: testPassword,
                        table: tableName,
                        data: "6a,value6a",
                        certPath: subsetClientCert,
                        keyPath: subsetClientKey
                    )
                    assertFalse(result5a.success, "SL-05a should fail: REQUIRE SAN full, cert subset")
                    logger.info("SL-05a PASSED")

                    // ==================================================================================
                    // SL-05b: REQUIRE SAN subset, cert full -> failure
                    // ==================================================================================
                    logger.info("=== SL-05b: REQUIRE SAN subset, cert full ===")
                    sql "CREATE USER '${testUserBase}_11'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanSubset}'"
                    sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${testUserBase}_11'@'%'"
                    grantComputeGroupUsage("${testUserBase}_11")

                    def result5b = executeStreamLoadCurl(
                        user: "${testUserBase}_11",
                        password: testPassword,
                        table: tableName,
                        data: "6b,value6b",
                        certPath: sanClientCert,
                        keyPath: sanClientKey
                    )
                    assertFalse(result5b.success, "SL-05b should fail: REQUIRE SAN subset, cert full")
                    logger.info("SL-05b PASSED")

                    // ==================================================================================
                    // SL-05c: REQUIRE SAN full, cert case mismatch -> failure
                    // ==================================================================================
                    logger.info("=== SL-05c: REQUIRE SAN full, cert case mismatch ===")
                    sql "CREATE USER '${testUserBase}_12'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${testUserBase}_12'@'%'"
                    grantComputeGroupUsage("${testUserBase}_12")

                    def result5c = executeStreamLoadCurl(
                        user: "${testUserBase}_12",
                        password: testPassword,
                        table: tableName,
                        data: "6c,value6c",
                        certPath: caseClientCert,
                        keyPath: caseClientKey
                    )
                    assertFalse(result5c.success, "SL-05c should fail: REQUIRE SAN full, cert case mismatch")
                    logger.info("SL-05c PASSED")

                    // ==================================================================================
                    // SL-06: No certificate + REQUIRE SAN -> failure
                    // ==================================================================================
                    logger.info("=== SL-06: No certificate + REQUIRE SAN ===")
                    sql "CREATE USER '${testUserBase}_6'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${testUserBase}_6'@'%'"
                    grantComputeGroupUsage("${testUserBase}_6")

                    def result6 = executeStreamLoadCurl(
                        user: "${testUserBase}_6",
                        password: testPassword,
                        table: tableName,
                        data: "7,value7"
                        // No certPath/keyPath
                    )
                    assertFalse(result6.success, "SL-06 should fail: no certificate")
                    logger.info("SL-06 PASSED")

                    // ==================================================================================
                    // SL-07: Certificate without SAN extension -> failure
                    // ==================================================================================
                    logger.info("=== SL-07: Certificate without SAN extension ===")
                    sql "CREATE USER '${testUserBase}_7'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${testUserBase}_7'@'%'"
                    grantComputeGroupUsage("${testUserBase}_7")

                    def result7 = executeStreamLoadCurl(
                        user: "${testUserBase}_7",
                        password: testPassword,
                        table: tableName,
                        data: "8,value8",
                        certPath: noSanClientCert,
                        keyPath: noSanClientKey
                    )
                    assertFalse(result7.success, "SL-07 should fail: cert has no SAN")
                    logger.info("SL-07 PASSED")

                    // ==================================================================================
                    // SL-08: Two-phase commit with cert auth -> success
                    // ==================================================================================
                    logger.info("=== SL-08: Two-phase commit ===")
                    sql "CREATE USER '${testUserBase}_8'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${testUserBase}_8'@'%'"
                    grantComputeGroupUsage("${testUserBase}_8")

                    def result8 = executeStreamLoadCurl(
                        user: "${testUserBase}_8",
                        password: testPassword,
                        table: tableName,
                        data: "9,2pc_test",
                        certPath: sanClientCert,
                        keyPath: sanClientKey,
                        twoPhaseCommit: true
                    )
                    assertTrue(result8.success, "SL-08 precommit should succeed: ${result8.output}")
                    assertTrue(result8.txnId != null && result8.txnId > 0, "Should have valid txnId")
                    logger.info("SL-08 precommit succeeded, txnId: ${result8.txnId}")

                    def commitResult = execute2PCAction(
                        user: "${testUserBase}_8",
                        password: testPassword,
                        txnId: result8.txnId,
                        action: "commit",
                        certPath: sanClientCert,
                        keyPath: sanClientKey
                    )
                    assertTrue(commitResult.success, "SL-08 commit should succeed: ${commitResult.output}")
                    logger.info("SL-08 PASSED")

                    def count8 = sql "SELECT COUNT(*) FROM ${tableName} WHERE k2 = '2pc_test'"
                    assertTrue(count8[0][0] >= 1, "2PC data should be committed")
                    sql "TRUNCATE TABLE ${tableName}"

                    // ==================================================================================
                    // SL-09: ALTER USER add/remove REQUIRE SAN -> dynamic effect
                    // ==================================================================================
                    logger.info("=== SL-09: ALTER USER add/remove REQUIRE SAN ===")
                    sql "CREATE USER '${testUserBase}_9'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${testUserBase}_9'@'%'"
                    grantComputeGroupUsage("${testUserBase}_9")

                    // 9a: With SAN requirement - should succeed with matching cert
                    def result9a = executeStreamLoadCurl(
                        user: "${testUserBase}_9",
                        password: testPassword,
                        table: tableName,
                        data: "10,value10",
                        certPath: sanClientCert,
                        keyPath: sanClientKey
                    )
                    assertTrue(result9a.success, "SL-09a should succeed: ${result9a.output}")
                    logger.info("SL-09a PASSED")
                    sql "TRUNCATE TABLE ${tableName}"

                    // Remove SAN requirement
                    sql "ALTER USER '${testUserBase}_9'@'%' REQUIRE NONE"
                    logger.info("Removed REQUIRE SAN")

                    // 9b: After REQUIRE NONE - should succeed with no-SAN certificate
                    def result9b = executeStreamLoadCurl(
                        user: "${testUserBase}_9",
                        password: testPassword,
                        table: tableName,
                        data: "11,value11",
                        certPath: noSanClientCert,
                        keyPath: noSanClientKey
                    )
                    assertTrue(result9b.success, "SL-09b should succeed: ${result9b.output}")
                    logger.info("SL-09b PASSED")

                    // Add back SAN requirement
                    sql "ALTER USER '${testUserBase}_9'@'%' REQUIRE SAN '${sanFull}'"
                    logger.info("Re-added REQUIRE SAN")
                    sql "TRUNCATE TABLE ${tableName}"

                    // 9c: After re-adding REQUIRE SAN - no-SAN cert should fail
                    def result9c = executeStreamLoadCurl(
                        user: "${testUserBase}_9",
                        password: testPassword,
                        table: tableName,
                        data: "12,value12",
                        certPath: noSanClientCert,
                        keyPath: noSanClientKey
                    )
                    assertFalse(result9c.success, "SL-09c should fail: REQUIRE SAN re-added")
                    logger.info("SL-09c PASSED")

                    logger.info("SL-09 PASSED")

                    // ==================================================================================
                    // SL-10: Privilege enforcement with SAN
                    // ==================================================================================
                    logger.info("=== SL-10: Privilege enforcement with SAN ===")
                    sql "CREATE USER '${privUser}'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    sql "GRANT SELECT_PRIV ON ${context.dbName}.${tableName} TO '${privUser}'@'%'"
                    grantComputeGroupUsage(privUser)

                    def privFailResult = executeStreamLoadCurl(
                        user: privUser,
                        password: testPassword,
                        table: tableName,
                        data: "20,priv_load_fail",
                        certPath: sanClientCert,
                        keyPath: sanClientKey
                    )
                    assertFalse(privFailResult.success, "SL-10 should fail without LOAD_PRIV: ${privFailResult.output}")

                    sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${privUser}'@'%'"
                    def privOkResult = executeStreamLoadCurl(
                        user: privUser,
                        password: testPassword,
                        table: tableName,
                        data: "21,priv_load_ok",
                        certPath: sanClientCert,
                        keyPath: sanClientKey
                    )
                    assertTrue(privOkResult.success, "SL-10 should succeed after LOAD_PRIV: ${privOkResult.output}")

                    def privCount = sql "SELECT COUNT(*) FROM ${tableName} WHERE k2 = 'priv_load_ok'"
                    assertTrue(privCount[0][0] >= 1, "SL-10 loaded data should be visible")
                    sql "TRUNCATE TABLE ${tableName}"

                    logger.info("SL-10 PASSED")

                    // ==================================================================================
                    // SL-11: SAN create/alter/drop loop tests
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
                        logger.info("=== SL-11 SAN cycle round ${idx} ===")
                        try_sql("DROP USER IF EXISTS '${cycleUser}'@'%'")

                        def createSan = createSans[idx - 1]
                        if (createSan == null) {
                            sql "CREATE USER '${cycleUser}'@'%' IDENTIFIED BY '${testPassword}'"
                        } else {
                            sql "CREATE USER '${cycleUser}'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${createSan}'"
                        }
                        sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${cycleUser}'@'%'"
                        grantComputeGroupUsage(cycleUser)

                        sql "ALTER USER '${cycleUser}'@'%' REQUIRE SAN '${sanFull}'"
                        def authRowFull = findAuthRowByIdentity(cycleUserIdentity)
                        assertTrue(authRowFull != null, "Should find user ${cycleUser} after REQUIRE SAN")
                        assertTrue(authRowFull.RequireSan == sanFull, "RequireSan should be ${sanFull}")

                        def okResult = executeStreamLoadCurl(
                            user: cycleUser,
                            password: testPassword,
                            table: tableName,
                            data: "100${idx},cycle_ok_${idx}",
                            certPath: sanClientCert,
                            keyPath: sanClientKey
                        )
                        assertTrue(okResult.success, "SL-11 round ${idx} should succeed: ${okResult.output}")
                        sql "TRUNCATE TABLE ${tableName}"

                        def wrongSan = wrongSans[idx - 1]
                        sql "ALTER USER '${cycleUser}'@'%' REQUIRE SAN '${wrongSan}'"
                        def authRowWrong = findAuthRowByIdentity(cycleUserIdentity)
                        assertTrue(authRowWrong != null, "Should find user ${cycleUser} after wrong REQUIRE SAN")
                        assertTrue(authRowWrong.RequireSan == wrongSan, "RequireSan should be ${wrongSan}")

                        def wrongResult = executeStreamLoadCurl(
                            user: cycleUser,
                            password: testPassword,
                            table: tableName,
                            data: "200${idx},cycle_fail_${idx}",
                            certPath: sanClientCert,
                            keyPath: sanClientKey
                        )
                        assertFalse(wrongResult.success, "SL-11 round ${idx} should fail: ${wrongResult.output}")

                        sql "ALTER USER '${cycleUser}'@'%' REQUIRE NONE"
                        def authRowNone = findAuthRowByIdentity(cycleUserIdentity)
                        assertTrue(authRowNone != null, "Should find user ${cycleUser} after REQUIRE NONE")
                        assertTrue(authRowNone.RequireSan == null, "RequireSan should be cleared")
                    }

                    logger.info("=== All Stream Load certificate-based auth tests PASSED ===")


                } finally {
                    try {
                        sql "ADMIN SET FRONTEND CONFIG ('tls_cert_based_auth_ignore_password' = '${origIgnorePassword}')"
                    } catch (Exception e) {
                        logger.warn("Failed to restore config: ${e.message}")
                    }
                    cleanup()
                }
            }
        }

        // Run the tests with the generated certificates
        runStreamLoadCertAuthTests("${certFileDir}")
    }
}
