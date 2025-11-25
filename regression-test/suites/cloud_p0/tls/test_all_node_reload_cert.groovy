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

suite('test_all_node_reload_cert', 'docker, p0') {
    def testName = "test_all_node_reload_cert"

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
    def certFileDir = [ "${localCertDir}/certs1",
                        "${localCertDir}/certs2",
                        "${localCertDir}/certs3"]
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

        def generateExpiredCert = { String certDir ->
            logger.info("=== Generating EXPIRED TLS certificate with existing CA ===")

            runCommand("mkdir -p ${certDir}", "Failed to create cert directory ${certDir}")
            runCommand("cp ${localCertDir}/ca.crt ${certDir}/ca.crt", "Failed to copy CA cert")
            runCommand("cp ${localCertDir}/ca.key ${certDir}/ca.key", "Failed to copy CA key")

            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/certificate.key -pkeyopt rsa_keygen_bits:2048",
                    "Failed to generate server private key")

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
            new File("${certDir}/openssl.cnf").text = opensslConf
            logger.info("Created OpenSSL config for EXPIRED cert with SANs for IPs: ${allIps}")

            runCommand("openssl req -new -key ${certDir}/certificate.key -out ${certDir}/certificate.csr -config ${certDir}/openssl.cnf",
                    "Failed to generate CSR for expired certificate")

            def caDb = "${localCertDir}/ca_db"
            new File("${caDb}/newcerts").mkdirs()
            def indexFile = new File("${caDb}/index.txt"); if (!indexFile.exists()) indexFile.text = ""
            def serialFile = new File("${caDb}/serial");   if (!serialFile.exists()) serialFile.text = "1000\n"

            def caConf = """
[ ca ]
default_ca = CA_default

[ CA_default ]
dir             = ${caDb}
database        = ${caDb}/index.txt
new_certs_dir   = ${caDb}/newcerts
serial          = ${caDb}/serial
default_md      = sha256
policy          = policy_any
copy_extensions = copy
unique_subject  = no

[ policy_any ]
commonName              = supplied
stateOrProvinceName     = optional
countryName             = optional
organizationName        = optional
organizationalUnitName  = optional
emailAddress            = optional
"""
            new File("${caDb}/ca.conf").text = caConf

            def fmt = java.time.format.DateTimeFormatter.ofPattern("yyyyMMddHHmmss'Z'").withZone(java.time.ZoneId.of("UTC"))
            def startDate = fmt.format(java.time.ZonedDateTime.now(java.time.ZoneId.of("UTC")).minusDays(40))
            def endDate   = fmt.format(java.time.ZonedDateTime.now(java.time.ZoneId.of("UTC")).minusDays(10))
            logger.info("Signing expired cert with start=${startDate}, end=${endDate}")

            runCommand(
                "openssl ca -batch -config ${caDb}/ca.conf " +
                "-cert ${localCertDir}/ca.crt -keyfile ${localCertDir}/ca.key " +
                "-in ${certDir}/certificate.csr -out ${certDir}/certificate.crt " +
                "-startdate ${startDate} -enddate ${endDate} " +
                "-extensions v3_req -extfile ${certDir}/openssl.cnf",
                "Failed to sign EXPIRED certificate with CA"
            )

            def verifyOutput = runCommand("openssl x509 -in ${certDir}/certificate.crt -noout -dates -text",
                                        "Failed to verify expired certificate")
            logger.info("Expired certificate details:\n${verifyOutput}")

            runCommand("chmod 644 ${certDir}/*.crt", "Failed to chmod .crt")
            runCommand("chmod 644 ${certDir}/*.key", "Failed to chmod .key")

            logger.info("=== Generating (expired) Java KeyStore files for JDBC ===")
            def keystorePassword = "doris123"
            def keystorePath = "${certDir}/keystore.p12"
            def truststorePath = "${certDir}/truststore.p12"
            runCommand("rm -f ${keystorePath} ${truststorePath}", "Failed to remove old keystores")
            runCommand("openssl pkcs12 -export -in ${certDir}/certificate.crt -inkey ${certDir}/certificate.key " +
                    "-out ${keystorePath} -password pass:${keystorePassword} -name doris-client",
                    "Failed to create expired client keystore")
            runCommand("keytool -import -noprompt -alias ca-cert -file ${certDir}/ca.crt " +
                    "-keystore ${truststorePath} -storepass ${keystorePassword} -storetype PKCS12",
                    "Failed to create truststore for expired cert")
            logger.info("Expired KeyStore generated: ${keystorePath}, truststore: ${truststorePath}")
        }

        generateNormalCert("${certFileDir[0]}")
        generateNormalCert("${certFileDir[1]}")
        generateExpiredCert("${certFileDir[2]}")

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

        // Get remote certificate fingerprint from host:port
        def getRemoteFingerprint = { String host, int port ->
            try {
                def cmd = "timeout 5 openssl s_client -connect ${host}:${port} -showcerts < /dev/null 2>/dev/null | openssl x509 -noout -fingerprint -sha256 2>/dev/null"
                def output = runCommand(cmd, "Failed to get remote fingerprint from ${host}:${port}")
                return output.trim()
            } catch (Exception e) {
                logger.warn("Cannot get fingerprint from ${host}:${port}: ${e.message}")
                return null
            }
        }

        // Verify certificate fingerprint for all nodes
        def verifyFingerprints = { String certDir ->
            logger.info("=== Verifying Certificate Fingerprints ===")
            def localFingerprint = runCommand("openssl x509 -in ${certDir}/certificate.crt -noout -fingerprint -sha256",
                                            "Failed to get local fingerprint").trim()
            logger.info("Expected fingerprint: ${localFingerprint}")

            def verifyNode = { String nodeType, String host, List<Integer> ports ->
                ports.each { port ->
                    def remoteFingerprint = getRemoteFingerprint(host, port)
                    if (remoteFingerprint == null) {
                        logger.warn("[WARN] ${nodeType} ${host}:${port} - no response")
                    } else if (remoteFingerprint == localFingerprint) {
                        logger.info("[OK] ${nodeType} ${host}:${port} - fingerprint matches")
                    } else {
                        logger.error("[FAIL] ${nodeType} ${host}:${port} - fingerprint mismatch!")
                        logger.error("  Expected: ${localFingerprint}")
                        logger.error("  Got: ${remoteFingerprint}")
                        throw new Exception("Fingerprint verification failed for ${nodeType} ${host}:${port}")
                    }
                }
            }

            // FE ports: 8030 (http), 9020 (rpc), 9010 (edit_log)
            frontends.each { fe -> verifyNode("FE[${fe.index}]", fe.host, [8030, 9020, 9010]) }

            // BE ports: 9060 (brpc), 8040 (http), 9050 (heartbeat), 8060 (webserver)
            backends.each { be -> verifyNode("BE[${be.index}]", be.host, [9060, 8040, 9050, 8060]) }

            // MS and Recycler port: 5000
            metaservices.each { ms -> verifyNode("MS[${ms.index}]", ms.host, [5000]) }
            recyclers.each { rc -> verifyNode("RC[${rc.index}]", rc.host, [5000]) }

            logger.info("=== All fingerprints verified successfully ===")
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
        logger.info("=== Connecting to cluster with TLS enabled ===")
        def firstFe = frontends[0]
        logger.info("Using FE[${firstFe.index}] at ${firstFe.host}:${firstFe.queryPort} for mTLS connection")

        // First build base JDBC URL
        def baseJdbcUrl = String.format(
            "jdbc:mysql://%s:%s/?useLocalSessionState=true&allowLoadLocalInfile=false",
            firstFe.host, firstFe.queryPort)

        // Generate sample data file for stream load if it doesn't exist
        def generateSampleData = {
            def dataDir = "${context.config.dataPath}/cloud_p0/tls"
            def sampleDataFile = new File("${dataDir}/sample_data.csv")

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

        def basicTest = { String certDir ->
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

                    set 'column_separator', ','
                    set 'cloud_cluster', 'compute_cluster'

                    file 'sample_data.csv'

                    check { loadResult, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        log.info("Stream load result: ${loadResult}".toString())
                        def json = parseJson(loadResult)
                        assertEquals("success", json.Status.toLowerCase())
                        assertEquals(1000000, json.NumberTotalRows)
                        assertEquals(0, json.NumberFilteredRows)
                        txnId = json.TxnId
                    }
                }
                queryData = sql """ select * from ${tableName} """
                assertTrue(queryData.size() == 1000000)
                sql """ truncate table ${tableName} """

                // S3 load test - using standard TPC-H part.tbl data
                // Note: TPC-H is the standard benchmark dataset available in all regression test environments
                def tableNameS3 = "${testName}_s3"
                sql """ DROP TABLE IF EXISTS ${tableNameS3}; """
                sql """
                    CREATE TABLE ${tableNameS3} (
                        P_PARTKEY     INTEGER NOT NULL,
                        P_NAME        VARCHAR(55) NOT NULL,
                        P_MFGR        CHAR(25) NOT NULL,
                        P_BRAND       CHAR(10) NOT NULL,
                        P_TYPE        VARCHAR(25) NOT NULL,
                        P_SIZE        INTEGER NOT NULL,
                        P_CONTAINER   CHAR(10) NOT NULL,
                        P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                        P_COMMENT     VARCHAR(23) NOT NULL
                    )
                    DUPLICATE KEY(P_PARTKEY, P_NAME)
                    DISTRIBUTED BY HASH(P_PARTKEY) BUCKETS 3;
                """

                def s3LoadLabel = "tls_s3_load_" + System.currentTimeMillis()
                def s3LoadSql = """
                    LOAD LABEL ${s3LoadLabel} (
                        DATA INFILE("s3://${getS3BucketName()}/regression/tpch/sf1/part.tbl")
                        INTO TABLE ${tableNameS3}
                        COLUMNS TERMINATED BY "|"
                        (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, temp)
                    )
                    WITH S3 (
                        "AWS_ACCESS_KEY" = "${getS3AK()}",
                        "AWS_SECRET_KEY" = "${getS3SK()}",
                        "AWS_ENDPOINT" = "${getS3Endpoint()}",
                        "AWS_REGION" = "${getS3Region()}",
                        "provider" = "${getS3Provider()}"
                    )
                """
                sql """${s3LoadSql}"""

                // Wait for S3 load to complete
                def maxWaitTime = 300000  // 5 minutes
                def startTime = System.currentTimeMillis()
                while ((System.currentTimeMillis() - startTime) < maxWaitTime) {
                    def result = sql """ show load where label="${s3LoadLabel}" order by createtime desc limit 1 """
                    if (result.size() > 0) {
                        def state = result[0][2]
                        if (state == "FINISHED") {
                            logger.info("S3 load finished successfully")
                            break
                        } else if (state == "CANCELLED") {
                            logger.error("S3 load failed: ${result}")
                            throw new Exception("S3 load failed with state: ${state}")
                        }
                    }
                    Thread.sleep(2000)
                }

                queryData = sql """ select count(*) from ${tableNameS3} """
                logger.info("S3 load result count: ${queryData[0][0]}")
                assertTrue(queryData[0][0] > 0, "S3 load should have loaded data")

            }
        }

        def reloadTest = { String newCertDir, String oldCertDir ->
            // node by node
            logger.info("Node by Node reload New Certificate")
            def thread = Thread.start {
                containerNames.each { container ->
                    logger.info("Reloading ${container}'s Certificate")
                    updateCertificate("${newCertDir}", "${container}")
                    sleep(5000)
                }
            }
            basicTest("${oldCertDir}")
            thread.join()
            sleep(10000)
            verifyFingerprints("${newCertDir}")

            logger.info("Revert to Old Certificate")
            updateAllCertificate("${oldCertDir}")
            sleep(10000)
            verifyFingerprints("${oldCertDir}")

            logger.info("Model by Model reload New Certificate")
            thread = Thread.start {
                String containerName
                frontends.each { fe ->
                    containerName = "doris-${cluster.name}-fe-${fe.index}"
                    logger.info("Reloading ${containerName}'s Certificate")
                    updateCertificate("${newCertDir}", "${containerName}")
                }
                sleep(5000)
                backends.each { be ->
                    containerName = "doris-${cluster.name}-be-${be.index}"
                    logger.info("Reloading ${containerName}'s Certificate")
                    updateCertificate("${newCertDir}", "${containerName}")
                }
                sleep(5000)
                metaservices.each { ms ->
                    containerName = "doris-${cluster.name}-ms-${ms.index}"
                    logger.info("Reloading ${containerName}'s Certificate")
                    updateCertificate("${newCertDir}", "${containerName}")
                }
                sleep(5000)
                recyclers.each { rc ->
                    containerName = "doris-${cluster.name}-recycle-${rc.index}"
                    logger.info("Reloading ${containerName}'s Certificate")
                    updateCertificate("${newCertDir}", "${containerName}")
                }
            }
            basicTest("${oldCertDir}")
            thread.join()
            sleep(10000)
            verifyFingerprints("${newCertDir}")

            logger.info("Revert to Old Certificate")
            updateAllCertificate("${oldCertDir}")
            sleep(10000)
            verifyFingerprints("${oldCertDir}")

            logger.info("All nodes reload New Certificate")
            thread = Thread.start {
                updateAllCertificate("${newCertDir}")
            }
            basicTest("${oldCertDir}")
            thread.join()
            sleep(10000)
            verifyFingerprints("${newCertDir}")
        }

        def expiredTest = { String expiredCertDir, String normalCertDir ->
            logger.info("=== Testing Expired Certificate ===")

            // Test connection to a node port
            def testConnection = { String host, int port ->
                try {
                    def cmd = "timeout 5 curl -k --cert ${expiredCertDir}/certificate.crt --key ${expiredCertDir}/certificate.key --cacert ${expiredCertDir}/ca.crt https://${host}:${port}/ 2>&1"
                    def proc = ["bash", "-lc", cmd].execute()
                    def stdout = new StringBuilder()
                    def stderr = new StringBuilder()
                    proc.waitForProcessOutput(stdout, stderr)
                    def output = stdout.toString() + stderr.toString()
                    // Check if connection failed due to certificate issues
                    return !output.contains("certificate") && proc.exitValue() == 0
                } catch (Exception e) {
                    return false
                }
            }

            // Replace all certificates with expired ones
            logger.info("Replacing all certificates with expired certificate")
            updateAllCertificate(expiredCertDir)
            sleep(10000) // Wait for certificate reload

            // Verify all connections fail
            logger.info("Verifying all connections fail with expired certificate")
            def allFailed = true

            frontends.each { fe ->
                [8030, 9020, 9010].each { port ->
                    if (testConnection(fe.host, port)) {
                        logger.error("FE[${fe.index}] ${fe.host}:${port} should fail but succeeded")
                        allFailed = false
                    } else {
                        logger.info("[Expected] FE[${fe.index}] ${fe.host}:${port} connection failed with expired cert")
                    }
                }
            }

            backends.each { be ->
                [8040, 8060, 9050, 9060].each { port -> // Test HTTP ports
                    if (testConnection(be.host, port)) {
                        logger.error("BE[${be.index}] ${be.host}:${port} should fail but succeeded")
                        allFailed = false
                    } else {
                        logger.info("[Expected] BE[${be.index}] ${be.host}:${port} connection failed with expired cert")
                    }
                }
            }

            metaservices.each { ms ->
                if (testConnection(ms.host, 5000)) {
                    logger.error("MS ${ms.host}:5000 should fail but succeeded")
                    allFailed = false
                } else {
                    logger.info("[Expected] MS ${ms.host}:5000 connection failed with expired cert")
                }
            }

            recyclers.each { rc ->
                if (testConnection(rc.host, 5000)) {
                    logger.error("Recycler[${rc.index}] ${rc.host}:5000 should fail but succeeded")
                    allFailed = false
                } else {
                    logger.info("[Expected] Recycler[${rc.index}] ${rc.host}:5000 connection failed with expired cert")
                }
            }

            if (!allFailed) {
                throw new Exception("Some connections succeeded with expired certificate, expected all to fail")
            }
            logger.info("All connections correctly failed with expired certificate")

            // Replace back to normal certificate
            logger.info("Replacing back to normal certificate")
            updateAllCertificate(normalCertDir)
            sleep(10000) // Wait for certificate reload

            // Verify fingerprints match
            verifyFingerprints(normalCertDir)

            // Verify basic operations work again
            logger.info("Verifying basic operations work after restoring normal certificate")
            basicTest(normalCertDir)

            logger.info("=== Expired Certificate Test Completed Successfully ===")
        }
        // certs are already certs1
        basicTest("${certFileDir[0]}")
        // need reload
        reloadTest("${certFileDir[1]}", "${certFileDir[0]}")
        // use the expired certificate, expected all connect fail
        expiredTest("${certFileDir[2]}", "${certFileDir[1]}")
    }
}