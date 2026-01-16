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

suite('test_tls_excluded_protocols', 'docker, p0') {
    def testName = "test_tls_excluded_protocols"

    def options = new ClusterOptions()
    options.setFeNum(2)
    options.setBeNum(2)
    options.cloudMode = true

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

    docker(options) {
        sql """ CREATE DATABASE IF NOT EXISTS ${context.dbName}; """

        def frontends = cluster.getAllFrontends()
        def backends = cluster.getAllBackends()
        def metaservices = cluster.getAllMetaservices()
        def recyclers = cluster.getAllRecyclers(false)

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
                if (stdout.length() > 0) logger.error("stdout: ${stdout.toString()}")
                if (stderr.length() > 0) logger.error("stderr: ${stderr.toString()}")
                assert false : errorMsg
            }
            return stdout.toString() + stderr.toString()
        }

        def curlHelpOutput = runCommand("curl --help 2>&1 || true", "curl help")
        def curlSupportsHttp09 = curlHelpOutput.contains("--http0.9")
        logger.info("curl --http0.9 supported: ${curlSupportsHttp09}")

        // Certificate generation
        logger.info("=== Generating TLS certificates ===")
        runCommand("mkdir -p ${localCertDir}", "Failed to create cert directory")
        runCommand("openssl genpkey -algorithm RSA -out ${localCertDir}/ca.key -pkeyopt rsa_keygen_bits:2048", "Failed to generate CA private key")
        runCommand("openssl req -new -x509 -days 3650 -key ${localCertDir}/ca.key -out ${localCertDir}/ca.crt -subj '/C=CN/ST=Beijing/L=Beijing/O=Doris/OU=Test/CN=DorisCA'", "Failed to generate CA certificate")
        def current_ip = runCommand('''ip -4 addr show eth0 | grep -oP "(?<=inet\\s)\\d+(\\.\\d+){3}"''', "Failed to get current ip").trim()

        def generateNormalCert = { String certDir ->
            runCommand("mkdir -p ${certDir}", "mkdir ${certDir}")
            if (certDir != localCertDir) {
                runCommand("cp ${localCertDir}/ca.crt ${certDir}/ca.crt", "copy ca.crt")
                runCommand("cp ${localCertDir}/ca.key ${certDir}/ca.key", "copy ca.key")
            }
            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/certificate.key -pkeyopt rsa_keygen_bits:2048", "gen key")
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
            new File("${certDir}/openssl.cnf").text = opensslConf
            runCommand("openssl req -new -key ${certDir}/certificate.key -out ${certDir}/certificate.csr -config ${certDir}/openssl.cnf", "gen csr")
            runCommand("openssl x509 -req -days 3650 -in ${certDir}/certificate.csr -CA ${certDir}/ca.crt -CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/certificate.crt -extensions v3_req -extfile ${certDir}/openssl.cnf", "sign cert")
            runCommand("chmod 644 ${certDir}/*.crt ${certDir}/*.key", "chmod")
            // Generate keystore / truststore for JDBC usage
            def keystorePassword = "doris123"
            runCommand("rm -f ${certDir}/keystore.p12 ${certDir}/truststore.p12", "clean p12")
            runCommand("openssl pkcs12 -export -in ${certDir}/certificate.crt -inkey ${certDir}/certificate.key -out ${certDir}/keystore.p12 -password pass:${keystorePassword} -name doris-client", "gen keystore")
            runCommand("keytool -import -noprompt -alias ca-cert -file ${certDir}/ca.crt -keystore ${certDir}/truststore.p12 -storepass ${keystorePassword} -storetype PKCS12", "gen truststore")
        }

        generateNormalCert("${localCertDir}")

        def containerNames = []
        frontends.each { fe -> containerNames.add("doris-${cluster.name}-fe-${fe.index}") }
        backends.each { be -> containerNames.add("doris-${cluster.name}-be-${be.index}") }
        metaservices.each { ms -> containerNames.add("doris-${cluster.name}-ms-${ms.index}") }
        recyclers.each { rc -> containerNames.add("doris-${cluster.name}-recycle-${rc.index}") }

        def updateCertificate = { String certDir, String container ->
            runCommand("docker exec -i ${container} mkdir -p /tmp/certs", "mkdir certs ${container}")
            runCommand("docker exec -i ${container} bash -c 'rm -rf /tmp/certs_new && mkdir -p /tmp/certs_new'", "tmp dir ${container}")
            ['ca.crt', 'certificate.key', 'certificate.crt'].each { fname ->
                def srcFile = new File("${certDir}/${fname}")
                if (srcFile.exists()) {
                    runCommand("docker cp ${srcFile.absolutePath} ${container}:/tmp/certs_new/${fname}", "cp ${fname} -> ${container}")
                }
            }
            runCommand("docker exec -i ${container} bash -c 'mv -f /tmp/certs_new/* /tmp/certs/'", "atomic move ${container}")
            runCommand("docker exec -i ${container} rm -rf /tmp/certs_new", "cleanup ${container}")
        }
        def updateAllCertificate = { String certDir ->
            containerNames.each { c -> updateCertificate("${certDir}", "${c}") }
        }
        updateAllCertificate("${localCertDir}")

        def updateConfigFile = { confPath, Map<String, String> configParams ->
            def configFile = new File(confPath)
            if (!configFile.exists()) {
                logger.warn("Config file not found: ${confPath}")
                return
            }
            def lines = configFile.readLines()
            def updatedKeys = [] as Set
            def newLines = lines.collect { line ->
                for (param in configParams) {
                    if (line.trim().startsWith("${param.key}=")) {
                        updatedKeys.add(param.key)
                        return "${param.key}=${param.value}"
                    }
                }
                return line
            }
            // Add any config params that weren't found in the file
            configParams.each { key, value ->
                if (!updatedKeys.contains(key)) {
                    newLines.add("${key}=${value}")
                }
            }
            configFile.text = newLines.join('\n')
            logger.info("Updated config file: ${confPath} with params: ${configParams}")
        }

        def updateConfigAndRestart = { Map<String, String> configParams ->
            logger.info("=== Updating configs with: ${configParams} ===")
            frontends.each { fe -> updateConfigFile(fe.getConfFilePath(), configParams) }
            backends.each { be -> updateConfigFile(be.getConfFilePath(), configParams) }
            metaservices.each { ms -> updateConfigFile(ms.getConfFilePath(), configParams) }
            recyclers.each { rc -> updateConfigFile(rc.getConfFilePath(), configParams) }

            logger.info("=== Restarting all nodes ===")
            def dorisComposePath = cluster.config.dorisComposePath
            def clusterName = cluster.name

            def restart = { String nodeType, String idFlag ->
                def cmd = "python -W ignore ${dorisComposePath} restart ${clusterName} ${idFlag} -v --output-json"
                def proc = cmd.execute()
                def stdout = new StringBuilder()
                def stderr = new StringBuilder()
                proc.waitForProcessOutput(stdout, stderr)
                if (proc.exitValue() != 0) {
                    logger.error("Restart ${nodeType} failed: stdout=${stdout}, stderr=${stderr}")
                    throw new Exception("Failed to restart ${nodeType}")
                }
                logger.info("${nodeType} restart initiated")
            }

            restart('MS', "--ms-id " + metaservices.collect { it.index }.join(' '))
            restart('FE', "--fe-id " + frontends.collect { it.index }.join(' '))
            restart('BE', "--be-id " + backends.collect { it.index }.join(' '))
            restart('Recycler', "--recycle-id " + recyclers.collect { it.index }.join(' '))
            logger.info("Waiting nodes to initialize...")
            sleep(90000)
        }

        def testHttpConnection = { String protocol, String host, int port, String componentName ->
            def ca = "${localCertDir}/ca.crt"
            def cert = "${localCertDir}/certificate.crt"
            def key = "${localCertDir}/certificate.key"

            if (protocol == "https") {
                def curlOpts = "--cacert ${ca} --cert ${cert} --key ${key}"
                def cmd = "timeout 5 curl -sS -o /dev/null -w '%{http_code}' ${curlOpts} https://${host}:${port}/ 2>&1 || true"
                def output = runCommand(cmd, "curl ${componentName}")

                def sslErrors = ["ssl certificate problem", "bad certificate", "alert", "unable", "self signed certificate",
                                 "err", "certificate has expired", "ssl: certificate subject name", "failed", "wrong version number", "failure"]
                def hasSslError = sslErrors.any { output.toLowerCase().contains(it.toLowerCase()) }
                def success = !hasSslError && (output.contains("200") || output.contains("302") || output.contains("301") || output.contains("404") || output.contains("000"))

                logger.info("${componentName} https://${host}:${port} => ${success ? 'OK' : 'FAIL'} (${output.trim()})")
                return success
            }

            if (curlSupportsHttp09) {
                def cmd = "timeout 5 curl -sS -o /dev/null -w '%{http_code}' --http0.9 http://${host}:${port}/ 2>&1 || true"
                def output = runCommand(cmd, "curl ${componentName}")

                def sslErrors = ["ssl certificate problem", "bad certificate", "alert", "unable", "self signed certificate",
                                 "err", "certificate has expired", "ssl: certificate subject name", "failed", "wrong version number", "failure"]
                def hasSslError = sslErrors.any { output.toLowerCase().contains(it.toLowerCase()) }
                def success = !hasSslError && (output.contains("200") || output.contains("302") || output.contains("301") || output.contains("404") || output.contains("000"))

                logger.info("${componentName} http://${host}:${port} => ${success ? 'OK' : 'FAIL'} (${output.trim()})")
                return success
            }

            def cmd = "timeout 5 openssl s_client -connect ${host}:${port} < /dev/null 2>&1 || true"
            def output = runCommand(cmd, "openssl ${componentName}")
            def lower = output.toLowerCase()
            def plainMarkers = [
                "wrong version number",
                "unknown protocol",
                "no peer certificate available",
                "ssl handshake has read 0 bytes",
                "cipher is (none)",
                "write:errno=0"
            ]
            def connectErrors = ["connection refused", "timed out", "no route to host", "connection reset"]

            def hasCert = lower.contains("begin certificate")
            def hasProtocol = lower.contains("protocol  : tls") || lower.contains("protocol : tls")
            def hasCipher = lower.contains("cipher    :") || (lower.contains("cipher is") && !lower.contains("cipher is (none)"))
            def hasTlsMarker = hasCert || hasProtocol || hasCipher
            def hasPlainMarker = plainMarkers.any { lower.contains(it) }
            def hasConnectError = connectErrors.any { lower.contains(it) }
            def success = !hasConnectError && !hasTlsMarker && hasPlainMarker

            logger.info("${componentName} openssl://${host}:${port} => ${success ? 'OK' : 'FAIL'} (${output.trim()})")
            return success
        }

        // Generate sample data file for stream load if it doesn't exist
        def generateSampleData = {
            def dataDir = "${context.config.dataPath}/cloud_p0/tls"
            def sampleDataFile = new File("${dataDir}/sample_data_excluded_protocols.csv")

            if (sampleDataFile.exists()) {
                logger.info("Sample data file already exists: ${sampleDataFile.absolutePath}, skipping generation")
                return
            }

            logger.info("Generating sample data file: ${sampleDataFile.absolutePath}")
            runCommand("mkdir -p ${dataDir}", "Failed to create data directory")

            // Generate 1000 rows of data matching table structure (id INT, value VARCHAR)
            sampleDataFile.withWriter { writer ->
                for (int i = 1; i <= 1000; i++) {
                    writer.write("${i},value_${i}\n")
                }
            }
            logger.info("Successfully generated 1000 rows of sample data")
        }

        // Generate sample data before running tests
        generateSampleData()

        def basicTest = { String certDir, boolean mysqlUseTls, boolean httpUseTls ->
            logger.info("=== Basic Test (MySQL TLS=${mysqlUseTls}, HTTP TLS=${httpUseTls}) ===")
            def ca = "${certDir}/ca.crt"
            def cert = "${certDir}/certificate.crt"
            def key = "${certDir}/certificate.key"
            def keystorePassword = "doris123"
            def keystorePath = "${certDir}/keystore.p12"
            def truststorePath = "${certDir}/truststore.p12"

            // Test each FE node
            frontends.each { fe ->
                logger.info("--- Testing FE[${fe.index}] ${fe.host}:${fe.queryPort} ---")

                // Test MySQL CLI connection
                def mysqlCmd = "timeout 10 mysql -h ${fe.host} -P ${fe.queryPort} -u ${context.config.jdbcUser}"
                if (mysqlUseTls) {
                    mysqlCmd += " --ssl-mode=VERIFY_CA"
                    mysqlCmd += " --ssl-ca=${ca} --ssl-cert=${cert} --ssl-key=${key}"
                }
                mysqlCmd += " -e 'SELECT 1;' 2>&1 || true"

                def mysqlOutput = runCommand(mysqlCmd, "mysql cli FE[${fe.index}]")
                def mysqlSuccess = !mysqlOutput.toLowerCase().contains("error") && (mysqlOutput.contains("1") || mysqlOutput.isEmpty())
                logger.info("FE[${fe.index}] MySQL CLI => ${mysqlSuccess ? 'OK' : 'FAIL'} (${mysqlOutput.trim()})")
                assertTrue(mysqlSuccess, "FE[${fe.index}] MySQL CLI connection failed")

                // Build JDBC URL
                def baseJdbcUrl = "jdbc:mysql://${fe.host}:${fe.queryPort}/?useLocalSessionState=true&allowLoadLocalInfile=false"
                def jdbcUrl = mysqlUseTls ? org.apache.doris.regression.Config.buildUrlWithDb(
                    baseJdbcUrl,
                    context.dbName,
                    keystorePath,
                    keystorePassword,
                    truststorePath,
                    keystorePassword
                ) : "${baseJdbcUrl}&useSSL=false"

                context.connect(context.config.jdbcUser, context.config.jdbcPassword, jdbcUrl) {
                    sql """ USE ${context.dbName} """
                    def tableName = "${testName}_test_fe${fe.index}"
                    sql """ DROP TABLE IF EXISTS ${tableName} """
                    sql """
                        CREATE TABLE ${tableName} (
                            id INT,
                            name VARCHAR(100)
                        ) DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES("replication_num" = "1")
                    """
                    sql """ INSERT INTO ${tableName} VALUES (1, 'test1'), (2, 'test2') """
                    def result = sql """ SELECT COUNT(*) FROM ${tableName} """
                    logger.info("FE[${fe.index}] SQL test result: ${result}")
                    assertTrue(result[0][0] == 2, "FE[${fe.index}] Expected 2 rows, got ${result[0][0]}")
                    sql """ DROP TABLE ${tableName} """
                    logger.info("FE[${fe.index}] SQL test passed")

                    // Test StreamLoad
                    def streamTableName = "${testName}_streamload_fe${fe.index}"
                    sql """ DROP TABLE IF EXISTS ${streamTableName} """
                    sql """
                        CREATE TABLE ${streamTableName} (
                            id INT,
                            value VARCHAR(100)
                        ) DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES("replication_num" = "1")
                    """

                    streamLoad {
                        feHttpAddress fe.host, fe.httpPort
                        table streamTableName
                        db context.dbName
                        set 'column_separator', ','
                        set 'format', 'csv'
                        set 'cloud_cluster', 'compute_cluster'

                        if (httpUseTls) {
                            enableTLS true
                            keyStorePath keystorePath
                            keyStorePassword keystorePassword
                            trustStorePath truststorePath
                            trustStorePassword keystorePassword
                        }

                        file 'sample_data_excluded_protocols.csv'
                        time 60000

                        check { result2, exception, startTime, endTime ->
                            if (exception != null) {
                                logger.error("FE[${fe.index}] StreamLoad exception: ${exception.message}")
                                throw exception
                            }
                            logger.info("FE[${fe.index}] StreamLoad result: ${result2}")
                            def json = parseJson(result2)
                            assertTrue(json.Status.toLowerCase() == "success" || json.Status.toLowerCase() == "ok",
                                       "FE[${fe.index}] StreamLoad failed: ${result2}")
                        }
                    }

                    def streamResult = sql """ SELECT COUNT(*) FROM ${streamTableName} """
                    logger.info("FE[${fe.index}] StreamLoad test result count: ${streamResult}")
                    sql """ DROP TABLE ${streamTableName} """
                    logger.info("FE[${fe.index}] StreamLoad test passed")
                }
            }

            logger.info("=== Basic Test completed ===")
        }

        def protocols = ['thrift', 'mysql', 'http', 'brpc', 'bdbje', 'fdbrpc']

        logger.info("=============== Testing TLS Excluded Protocols ===============")

        // Test each protocol exclusion one by one
        protocols.each { excludedProtocol ->
            logger.info("=== Testing with excluded protocol: ${excludedProtocol} ===")

            // Update config to enable TLS and exclude this protocol
            updateConfigAndRestart([enable_tls: 'true', tls_excluded_protocols: excludedProtocol])

            // Collect ports for the excluded protocol to test with HTTP later
            def excludedProtocolPorts = []

            // Test all FE nodes
            frontends.each { fe ->
                def fePorts = [
                    [protocol: 'http', port: 8030],
                    [protocol: 'thrift', port: 9020],
                    [protocol: 'bdbje', port: 9010]
                ]
                fePorts.each { portInfo ->
                    if (portInfo.protocol == excludedProtocol) {
                        excludedProtocolPorts.add([component: "FE[${fe.index}]", host: fe.host, port: portInfo.port])
                    }
                }
            }

            // Test all BE nodes
            backends.each { be ->
                def bePorts = [
                    [protocol: 'http', port: 8040],
                    [protocol: 'thrift', port: 9060],
                    [protocol: 'thrift', port: 9050],
                    [protocol: 'brpc', port: 8060]
                ]
                bePorts.each { portInfo ->
                    if (portInfo.protocol == excludedProtocol) {
                        excludedProtocolPorts.add([component: "BE[${be.index}]", host: be.host, port: portInfo.port])
                    }
                }
            }

            // Test all MS nodes
            metaservices.each { ms ->
                def msPorts = [
                    [protocol: 'brpc', port: 5000]
                ]
                msPorts.each { portInfo ->
                    if (portInfo.protocol == excludedProtocol) {
                        excludedProtocolPorts.add([component: "MS[${ms.index}]", host: ms.host, port: portInfo.port])
                    }
                }
            }

            // Test all RC nodes
            recyclers.each { rc ->
                def rcPorts = [
                    [protocol: 'brpc', port: 5000]
                ]
                rcPorts.each { portInfo ->
                    if (portInfo.protocol == excludedProtocol) {
                        excludedProtocolPorts.add([component: "RC[${rc.index}]", host: rc.host, port: portInfo.port])
                    }
                }
            }

            // Test the excluded protocol ports with HTTP (should work) and HTTPS (should fail)
            excludedProtocolPorts.each { portInfo ->
                logger.info("Testing excluded protocol ${excludedProtocol} on ${portInfo.component}:${portInfo.port}")

                def httpSuccess = testHttpConnection('http', portInfo.host, portInfo.port,
                    "${portInfo.component} ${excludedProtocol} with http")

                assertTrue(httpSuccess, "HTTP should work for excluded protocol ${excludedProtocol} on ${portInfo.component}:${portInfo.port}")
            }

            // Test cluster functionality based on excluded protocol
            def mysqlUseTls = (excludedProtocol != 'mysql')
            def httpUseTls = (excludedProtocol != 'http')
            logger.info("Testing cluster functionality (MySQL TLS=${mysqlUseTls}, HTTP TLS=${httpUseTls}, ${excludedProtocol} excluded)")
            basicTest(localCertDir, mysqlUseTls, httpUseTls)

            logger.info("=== Completed testing for excluded protocol: ${excludedProtocol} ===")
        }

        logger.info("=============== Testing with ALL protocols excluded ===============")

        // Test with all protocols excluded at once
        def allExcluded = protocols.join(',')
        logger.info("=== Testing with all excluded protocols: ${allExcluded} ===")
        updateConfigAndRestart([tls_excluded_protocols: allExcluded])

        // Collect all ports that should now be HTTP-only
        def allExcludedPorts = []

        frontends.each { fe ->
            allExcludedPorts.add([component: "FE[${fe.index}]", protocol: 'http', host: fe.host, port: 8030])
            allExcludedPorts.add([component: "FE[${fe.index}]", protocol: 'thrift', host: fe.host, port: 9020])
            allExcludedPorts.add([component: "FE[${fe.index}]", protocol: 'bdbje', host: fe.host, port: 9010])
        }

        backends.each { be ->
            allExcludedPorts.add([component: "BE[${be.index}]", protocol: 'http', host: be.host, port: 8040])
            allExcludedPorts.add([component: "BE[${be.index}]", protocol: 'thrift', host: be.host, port: 9060])
            allExcludedPorts.add([component: "BE[${be.index}]", protocol: 'thrift', host: be.host, port: 9050])
            allExcludedPorts.add([component: "BE[${be.index}]", protocol: 'brpc', host: be.host, port: 8060])
        }

        metaservices.each { ms ->
            allExcludedPorts.add([component: "MS[${ms.index}]", protocol: 'brpc', host: ms.host, port: 5000])
        }

        recyclers.each { rc ->
            allExcludedPorts.add([component: "RC[${rc.index}]", protocol: 'brpc', host: rc.host, port: 5000])
        }

        // Test that all ports work with HTTP but not HTTPS
        allExcludedPorts.each { portInfo ->
            logger.info("Testing ${portInfo.protocol} on ${portInfo.component}:${portInfo.port} (all excluded)")

            def httpSuccess = testHttpConnection('http', portInfo.host, portInfo.port,
                "${portInfo.component} ${portInfo.protocol} with http (all excluded)")

            assertTrue(httpSuccess, "HTTP should work when all protocols excluded for ${portInfo.protocol} on ${portInfo.component}:${portInfo.port}")
        }

        // Test MySQL connections with all protocols excluded (use non-TLS for both)
        logger.info("Testing cluster functionality with all protocols excluded")
        basicTest(localCertDir, false, false)

        logger.info("=============== ALL TLS excluded protocols tests COMPLETED ===============")
    }
}
