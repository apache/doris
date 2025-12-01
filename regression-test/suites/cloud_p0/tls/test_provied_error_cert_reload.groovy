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

suite('test_provied_error_cert_reload', 'docker, p0') {
    def testName = "test_provied_error_cert_reload"

    def options = new ClusterOptions()
    options.setFeNum(2)
    options.setBeNum(2)
    options.cloudMode = true

    // Add TLS configuration to all node types
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
    def certFileDir = [ "${localCertDir}/normal",
                        "${localCertDir}/corrupt",
                        "${localCertDir}/partial",
                        "${localCertDir}/missing",
                        "${localCertDir}/ca_changed"]
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
        def certFinger = []
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
DNS.3 = fe-2
DNS.4 = be-1
DNS.5 = be-2
DNS.6 = ms-1
DNS.7 = recycle-1
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
            logger.info("  - Certificate: ${certDir}/certificate.crt")
            logger.info("  - Private Key: ${certDir}/certificate.key")

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

            // Get certificate fingerprint
            def fingerprint = runCommand("openssl x509 -in ${certDir}/certificate.crt -noout -fingerprint -sha256", "Failed to get certificate fingerprint")
            fingerprint = fingerprint.trim()
            certFinger.add(fingerprint)
            logger.info("Certificate fingerprint: ${fingerprint}")

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
            logger.info("  - KeyStore: ${keystorePath}")
            logger.info("  - TrustStore: ${truststorePath}")
        }
        generateNormalCert("${certFileDir[0]}")

        def containerNames = []
        frontends.each { fe -> containerNames.add("doris-${cluster.name}-fe-${fe.index}") }
        backends.each { be -> containerNames.add("doris-${cluster.name}-be-${be.index}") }
        metaservices.each { ms -> containerNames.add("doris-${cluster.name}-ms-${ms.index}") }
        recyclers.each { rc -> containerNames.add("doris-${cluster.name}-recycle-${rc.index}") }

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

        updateAllCertificate("${certFileDir[0]}")

        // update config file
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

        // Check if all nodes are alive by directly checking their HTTP endpoints
        def checkNodesAlive = { String certDir ->
            logger.info("=== Checking if all nodes are alive via HTTP endpoints ===")
            def allAlive = true

            // Check FE nodes via HTTPS with certificates
            frontends.each { fe ->
                def url = "https://${fe.host}:${fe.httpPort}/metrics"
                def curlCmd = "curl --cacert ${certDir}/ca.crt --cert ${certDir}/certificate.crt --key ${certDir}/certificate.key -s -o /dev/null -w '%{http_code}' --connect-timeout 5 ${url}"
                def result = curlCmd.execute()
                result.waitFor()
                def httpCode = result.text.trim()

                if (httpCode.contains("200")) {
                    logger.info("FE[${fe.index}] at ${fe.host}:${fe.httpPort} is alive (HTTP ${httpCode})")
                } else {
                    logger.error("FE[${fe.index}] at ${fe.host}:${fe.httpPort} is NOT responding (HTTP ${httpCode})")
                    allAlive = false
                }
            }

            // Check BE nodes via HTTPS with certificates
            backends.each { be ->
                def url = "https://${be.host}:${be.httpPort}/metrics"
                def curlCmd = "curl --cacert ${certDir}/ca.crt --cert ${certDir}/certificate.crt --key ${certDir}/certificate.key -s -o /dev/null -w '%{http_code}' --connect-timeout 5 ${url}"
                def result = curlCmd.execute()
                result.waitFor()
                def httpCode = result.text.trim()

                if (httpCode.contains("200")) {
                    logger.info("BE[${be.index}] at ${be.host}:${be.httpPort} is alive (HTTP ${httpCode})")
                } else {
                    logger.error("BE[${be.index}] at ${be.host}:${be.httpPort} is NOT responding (HTTP ${httpCode})")
                    allAlive = false
                }
            }

            // Check MS nodes via HTTPS with certificates
            metaservices.each { ms ->
                def url = "https://${ms.host}:${ms.httpPort}"
                def curlCmd = "curl --cacert ${certDir}/ca.crt --cert ${certDir}/certificate.crt --key ${certDir}/certificate.key -s -o /dev/null -w '%{http_code}' --connect-timeout 5 ${url}"
                def result = curlCmd.execute()
                result.waitFor()
                def httpCode = result.text.trim()

                if (httpCode.contains("200")) {
                    logger.info("MS[${ms.index}] at ${ms.host}:${ms.httpPort} is alive (HTTP ${httpCode})")
                } else {
                    logger.error("MS[${ms.index}] at ${ms.host}:${ms.httpPort} is NOT responding (HTTP ${httpCode})")
                    allAlive = false
                }
            }

            // Check Recycler nodes via HTTPS with certificates
            recyclers.each { rc ->
                def url = "https://${rc.host}:${rc.httpPort}"
                def curlCmd = "curl --cacert ${certDir}/ca.crt --cert ${certDir}/certificate.crt --key ${certDir}/certificate.key -s -o /dev/null -w '%{http_code}' --connect-timeout 5 ${url}"
                def result = curlCmd.execute()
                result.waitFor()
                def httpCode = result.text.trim()

                if (httpCode.contains("200")) {
                    logger.info("Recycler[${rc.index}] at ${rc.host}:${rc.httpPort} is alive (HTTP ${httpCode})")
                } else {
                    logger.error("Recycler[${rc.index}] at ${rc.host}:${rc.httpPort} is NOT responding (HTTP ${httpCode})")
                    allAlive = false
                }
            }

            if (!allAlive) {
                throw new Exception("Some nodes are not alive!")
            }
            logger.info("=== All nodes are alive ===")
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


        // Step 3: Connect cluster and do some operator
        logger.info("=== Restarting all nodes with TLS enabled ===")
        def firstFe = frontends[0]
        logger.info("Using FE[${firstFe.index}] at ${firstFe.host}:${firstFe.queryPort} for mTLS connection")

        // First build base JDBC URL
        def baseJdbcUrl = String.format(
            "jdbc:mysql://%s:%s/?useLocalSessionState=true&allowLoadLocalInfile=false",
            firstFe.host, firstFe.queryPort)

        // Generate sample data file for stream load if it doesn't exist
        def generateSampleData = {
            def dataDir = "${context.config.dataPath}/cloud_p0/tls"
            def sampleDataFile = new File("${dataDir}/sample_data2.csv")

            if (sampleDataFile.exists()) {
                logger.info("Sample data file already exists: ${sampleDataFile.absolutePath}, skipping generation")
                return
            }

            logger.info("Generating sample data file: ${sampleDataFile.absolutePath}")
            runCommand("mkdir -p ${dataDir}", "Failed to create data directory")

            // Generate 1000000 rows of data
            sampleDataFile.withWriter { writer ->
                for (int i = 1; i <= 1000000; i++) {
                    writer.write("${i},name_${i},${i % 100}\n")
                }
            }
            logger.info("Successfully generated 1000000 rows of sample data")
        }

        // Generate sample data before running tests
        generateSampleData()

        def basicTest = { String certDir, boolean checkStreamLoad ->
            def keystorePassword = "doris123"
            def keystorePath = "${certDir}/keystore.p12"
            def truststorePath = "${certDir}/truststore.p12"
            // Then add mTLS configuration
            def tlsJdbcUrl = org.apache.doris.regression.Config.buildUrlWithDb(
                baseJdbcUrl,
                context.dbName,
                keystorePath,
                keystorePassword,
                truststorePath,
                keystorePassword
            )

            logger.info("Reconnecting with mTLS JDBC URL: ${tlsJdbcUrl}")
            context.connect(context.config.jdbcUser, context.config.jdbcPassword, tlsJdbcUrl) {
                def tableName = "${testName}"
                // Create table
                sql """ DROP TABLE IF EXISTS ${tableName}; """
                sql """
                    CREATE TABLE ${tableName} (
                        `id` int(11) NULL,
                        `name` varchar(255) NULL,
                        `score` int(11) NULL
                    )
                    DUPLICATE KEY(`id`)
                    DISTRIBUTED BY HASH(`id`) BUCKETS 1;
                """
                // insert into
                sql """ insert into ${tableName} values (1, 'koarz', 10), (2, 'steve', 15), (3, 'alex', 20) """
                def queryData = sql """ select * from ${tableName} """
                assertTrue(queryData.size() == 3)
                sql """ truncate table ${tableName} """
                // stream load
                // approximately 24s
                streamLoad {
                    feHttpAddress firstFe.host, firstFe.httpPort
                    table "${tableName}"

                    // Set TLS configuration dynamically
                    enableTLS true
                    keyStorePath keystorePath
                    keyStorePassword keystorePassword
                    trustStorePath truststorePath
                    trustStorePassword keystorePassword

                    // Set timeout to 60 seconds to prevent indefinite hangs during certificate reload
                    timeout 60000

                    set 'column_separator', ','
                    set 'cloud_cluster', 'compute_cluster'

                    file 'sample_data2.csv'

                    check { loadResult, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        log.info("Stream load result: ${loadResult}".toString())
                        def json = parseJson(loadResult)
                        if (checkStreamLoad) {
                            assertEquals("success", json.Status.toLowerCase())
                            assertEquals(1000000, json.NumberTotalRows)
                            assertEquals(0, json.NumberFilteredRows)
                        }
                        txnId = json.TxnId
                    }
                }
                queryData = sql """ select * from ${tableName} """
                if (checkStreamLoad) {
                    assertTrue(queryData.size() == 1000000)
                }
                sql """ truncate table ${tableName} """
            }
        }
        basicTest("${certFileDir[0]}", true)

        // Test 1: Certificate to be replaced is incorrect/corrupted
        logger.info("=== Test 1: Corrupt Certificate ===")
        def testCorruptCert = {
            runCommand("mkdir -p ${certFileDir[1]}", "Failed to create corrupt cert directory")
            runCommand("cp ${certFileDir[0]}/ca.crt ${certFileDir[1]}/ca.crt", "Failed to copy CA")
            runCommand("cp ${certFileDir[0]}/certificate.key ${certFileDir[1]}/certificate.key", "Failed to copy key")
            // Create a corrupt certificate file
            runCommand("echo 'CORRUPT CERTIFICATE DATA' > ${certFileDir[1]}/certificate.crt", "Failed to create corrupt cert")

            def thread = Thread.start {
                logger.info("Attempting to update with corrupt certificate")
                updateAllCertificate("${certFileDir[1]}")
                sleep(3000)
            }

            // Continue running basicTest while attempting to update
            try {
                basicTest("${certFileDir[0]}", false)
            } catch (Exception e) {
                logger.info("Expected error during corrupt cert test: ${e.message}")
            }
            thread.join()
            logger.info("Restoring normal certificates after error")
            updateAllCertificate("${certFileDir[0]}")
            sleep(10000)

            // Verify that system works with restored cert
            logger.info("Verifying system works after restoring from corrupt cert")
            basicTest("${certFileDir[0]}", true)

            checkNodesAlive("${certFileDir[0]}")
        }
        testCorruptCert()

        // Test 2: Partial certificate files only
        logger.info("=== Test 2: Partial Certificate Files ===")
        def testPartialCert = {
            runCommand("mkdir -p ${certFileDir[2]}", "Failed to create partial cert directory")
            // Only copy CA cert and private key, missing certificate.crt
            runCommand("cp ${certFileDir[0]}/ca.crt ${certFileDir[2]}/ca.crt", "Failed to copy CA")
            runCommand("cp ${certFileDir[0]}/certificate.key ${certFileDir[2]}/certificate.key", "Failed to copy certificate.key")

            def thread = Thread.start {
                logger.info("Attempting to copy partial certificate files directly to containers")
                containerNames.each { container ->
                    logger.info("Copying partial certs to ${container}")
                    // Copy only CA and key, skip certificate.crt
                    runCommand("docker exec -i ${container} rm -rf /tmp/certs/*", "Failed to cleanup cert directory in ${container}")
                    runCommand("docker cp ${certFileDir[2]}/ca.crt ${container}:/tmp/certs/ca.crt", "Failed to copy CA to ${container}")
                    runCommand("docker cp ${certFileDir[2]}/certificate.key ${container}:/tmp/certs/certificate.key", "Failed to copy key to ${container}")
                    sleep(3000)
                }
            }
            // Continue running basicTest while attempting to update
            try {
                basicTest("${certFileDir[0]}", false)
            } catch (Exception e) {
                logger.info("Expected error during partial cert test: ${e.message}")
            }
            thread.join()
            logger.info("Restoring normal certificates after error")
            updateAllCertificate("${certFileDir[0]}")
            sleep(10000)
            logger.info("Verifying system still works after partial cert attempt")
            basicTest("${certFileDir[0]}", true)

            checkNodesAlive("${certFileDir[0]}")
        }
        testPartialCert()

        // Test 3: Replace with the same certificate
        logger.info("=== Test 3: Replace with Same Certificate ===")
        def testSameCert = {
            updateAllCertificate("${certFileDir[0]}")
            sleep(10000)

            def thread = Thread.start {
                logger.info("Replacing with the same certificate")
                updateAllCertificate("${certFileDir[0]}")
            }

            basicTest("${certFileDir[0]}", true)
            thread.join()
            sleep(10000)

            logger.info("Verifying system works after replacing with same cert")
            basicTest("${certFileDir[0]}", true)
        }
        testSameCert()

        // Test 4: Certificate files missing
        logger.info("=== Test 4: Certificate Files Missing ===")
        def testMissingCert = {
            logger.info("Deleting certificate files from containers")
            containerNames.each { container ->
                logger.info("Removing /tmp/certs from ${container}")
                runCommand("docker exec -i ${container} rm -rf /tmp/certs/*", "Failed to remove certs from ${container}")
            }

            sleep(5000) // Wait for some deletions to occur
            // Run basicTest and expect failures
            try {
                basicTest("${certFileDir[0]}", false)
            } catch (Exception e) {
                logger.info("Expected error when certificate files are missing: ${e.message}")
            }
            logger.info("Restoring normal certificates after error")
            updateAllCertificate("${certFileDir[0]}")
            sleep(10000)

            logger.info("Verifying system works after restoring certificates")
            basicTest("${certFileDir[0]}", true)

            checkNodesAlive("${certFileDir[0]}")
        }
        testMissingCert()

        // Test 5: CA certificate change, update node by node, system should recover after all nodes are updated
        logger.info("=== Test 5: CA Certificate Change Node by Node ===")
        def testCAChange = {
            // Backup old CA
            runCommand("cp ${localCertDir}/ca.crt ${localCertDir}/ca_old.crt", "Failed to backup old CA cert")
            runCommand("cp ${localCertDir}/ca.key ${localCertDir}/ca_old.key", "Failed to backup old CA key")

            // Generate new CA
            runCommand("openssl genpkey -algorithm RSA -out ${localCertDir}/ca.key -pkeyopt rsa_keygen_bits:2048", "Failed to generate new CA key")
            runCommand("openssl req -new -x509 -days 3650 -key ${localCertDir}/ca.key -out ${localCertDir}/ca.crt -subj '/C=CN/ST=Beijing/L=Beijing/O=Doris/OU=Test/CN=DorisCA-New'", "Failed to generate new CA cert")

            // Generate new certificates with new CA
            generateNormalCert("${certFileDir[4]}")

            def thread = Thread.start {
                logger.info("Updating CA node by node")
                frontends.each { fe -> updateCertificate("${certFileDir[4]}", "doris-${cluster.name}-fe-${fe.index}") }
                sleep(3000)
                containerNames.each { container ->
                    logger.info("Updating ${container} with new CA certificate")
                    updateCertificate("${certFileDir[4]}", "${container}")
                    sleep(3000)
                }
            }
            // Try to run basicTest with old cert during update
            try {
                basicTest("${certFileDir[4]}", false)
            } catch (Exception e) {
                logger.info("Expected failures during CA transition: ${e.message}")
            }

            // Wait for thread to complete certificate updates
            logger.info("Waiting for all certificate updates to complete")
            thread.join()

            // Wait additional time for certificate reload (monitoring interval is 5 seconds)
            sleep(10000)

            logger.info("All nodes updated with new CA, verifying system works")
            basicTest("${certFileDir[4]}", true)

            checkNodesAlive("${certFileDir[4]}")
        }
        testCAChange()

        // Test 6: Correct certificate, high frequency reload (once per minute, 50 times total, using touch)
        logger.info("=== Test 6: High Frequency Certificate Reload ===")
        def testHighFreqReload = {
            def reloadCount = 50
            def intervalMs = 60000

            def thread = Thread.start {
                logger.info("Starting high frequency certificate reload (${reloadCount} times, every ${intervalMs}ms)")
                for (int i = 0; i < reloadCount; i++) {
                    logger.info("Reload iteration ${i + 1}/${reloadCount}")
                    // Touch certificate files to trigger reload
                    containerNames.each { container ->
                        runCommand("docker exec -i ${container} touch /tmp/certs/ca.crt", "Failed to touch CA in ${container}")
                        runCommand("docker exec -i ${container} touch /tmp/certs/certificate.key", "Failed to touch key in ${container}")
                        runCommand("docker exec -i ${container} touch /tmp/certs/certificate.crt", "Failed to touch cert in ${container}")
                    }
                    if (i < reloadCount - 1) {
                        sleep(intervalMs)
                    }
                }
                logger.info("Completed ${reloadCount} certificate reload iterations")
            }

            // Run basicTest during high frequency reloads
            basicTest("${certFileDir[4]}", true)
            thread.join()
            sleep(10000)

            logger.info("Verifying system still works after high frequency reloads")
            basicTest("${certFileDir[4]}", true)
            checkNodesAlive("${certFileDir[4]}")
        }
        testHighFreqReload()

        // Test 7: Unreadable certificate file permissions
        logger.info("=== Test 7: Unreadable Certificate File Permissions ===")
        def testUnreadablePermissions = {
            logger.info("Changing certificate file permissions to make them unreadable")

            def thread = Thread.start {
                containerNames.each { container ->
                    logger.info("Changing permissions in ${container}")
                    // Change permissions to 000 (no read access)
                    runCommand("docker exec -i ${container} chmod 000 /tmp/certs/certificate.crt", "Failed to change cert permissions in ${container}")
                    runCommand("docker exec -i ${container} chmod 000 /tmp/certs/certificate.key", "Failed to change key permissions in ${container}")
                    runCommand("docker exec -i ${container} chmod 000 /tmp/certs/ca.crt", "Failed to change CA permissions in ${container}")
                }
                sleep(10000) // Wait for monitoring to detect permission issues
            }

            // Try to run basicTest and expect failures due to permission issues
            try {
                basicTest("${certFileDir[4]}", false)
            } catch (Exception e) {
                logger.info("Expected error when certificate files are unreadable: ${e.message}")
            }

            // Wait for permission changes to complete
            thread.join()

            // Try again and should still fail
            try {
                basicTest("${certFileDir[4]}", false)
                logger.warn("Unexpected success - should have failed with unreadable certificates")
            } catch (Exception e) {
                logger.info("Expected continued failure with unreadable certificates: ${e.message}")
            }

            // Restore permissions
            logger.info("Restoring certificate file permissions")
            containerNames.each { container ->
                logger.info("Restoring permissions in ${container}")
                runCommand("docker exec -i ${container} chmod 644 /tmp/certs/certificate.crt", "Failed to restore cert permissions in ${container}")
                runCommand("docker exec -i ${container} chmod 644 /tmp/certs/certificate.key", "Failed to restore key permissions in ${container}")
                runCommand("docker exec -i ${container} chmod 644 /tmp/certs/ca.crt", "Failed to restore CA permissions in ${container}")
            }

            // Wait for monitoring to reload certificates with restored permissions
            sleep(10000)

            logger.info("Verifying system works after restoring permissions")
            basicTest("${certFileDir[4]}", true)

            checkNodesAlive("${certFileDir[4]}")
        }
        testUnreadablePermissions()

        logger.info("=== All Error Certificate Reload Tests Completed ===")
    }
}
