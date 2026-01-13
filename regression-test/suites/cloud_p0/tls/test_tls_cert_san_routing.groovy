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

suite('test_tls_cert_san_routing', 'docker, p0') {
    // This test verifies certificate-based auth routing across FE master/follower and BE.
    // A. FE master + follower: MySQL and Stream Load both succeed.
    // B. Stream Load directly to BE succeeds.

    def testName = "test_tls_cert_san_routing"

    def options = new ClusterOptions()
    options.setFeNum(2)
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

        def frontends = cluster.getAllFrontends()
        def backends = cluster.getAllBackends()
        def metaservices = cluster.getAllMetaservices()
        def recyclers = cluster.getAllRecyclers(false)

        def allIps = []
        frontends.each { fe -> if (!allIps.contains(fe.host)) allIps.add(fe.host) }
        backends.each { be -> if (!allIps.contains(be.host)) allIps.add(be.host) }
        metaservices.each { ms -> if (!allIps.contains(ms.host)) allIps.add(ms.host) }
        recyclers.each { rc -> if (!allIps.contains(rc.host)) allIps.add(rc.host) }

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

        logger.info("=== Generating TLS certificates ===")
        runCommand("mkdir -p ${localCertDir}", "Failed to create cert directory")
        runCommand("openssl genpkey -algorithm RSA -out ${localCertDir}/ca.key -pkeyopt rsa_keygen_bits:2048",
                "Failed to generate CA private key")
        runCommand("openssl req -new -x509 -days 3650 -key ${localCertDir}/ca.key -out ${localCertDir}/ca.crt " +
                "-subj '/C=CN/ST=Beijing/L=Beijing/O=Doris/OU=Test/CN=DorisCA'",
                "Failed to generate CA certificate")

        def current_ip = runCommand('''ip -4 addr show eth0 | grep -oP "(?<=inet\\s)\\d+(\\.\\d+){3}"''', "Failed to get current ip")

        def generateNormalCert = { String certDir ->
            runCommand("mkdir -p ${certDir}", "Failed to create cert directory ${certDir}")
            runCommand("cp ${localCertDir}/ca.crt ${certDir}/ca.crt", "Failed to copy CA certificate")
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
DNS.5 = ms-1
DNS.6 = recycle-1
DNS.7 = testclient.example.com
IP.1 = 127.0.0.1
IP.2 = ${current_ip}
${sanEntries}
"""
            def confFile = new File("${certDir}/openssl.cnf")
            confFile.text = opensslConf

            def genCsrCmd = "openssl req -new -key ${certDir}/certificate.key -out ${certDir}/certificate.csr -config ${certDir}/openssl.cnf"
            def csrProc = genCsrCmd.execute()
            csrProc.waitFor()
            assert csrProc.exitValue() == 0 : "Failed to generate CSR"

            def signCertCmd = "openssl x509 -req -days 3650 -in ${certDir}/certificate.csr -CA ${certDir}/ca.crt " +
                    "-CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/certificate.crt -extensions v3_req " +
                    "-extfile ${certDir}/openssl.cnf"
            def signProc = signCertCmd.execute()
            signProc.waitFor()
            assert signProc.exitValue() == 0 : "Failed to sign certificate"

            runCommand("chmod 644 ${certDir}/*.crt", "Failed to fix permissions for .crt files")
            runCommand("chmod 644 ${certDir}/*.key", "Failed to fix permissions for .key files")

            def keystorePassword = "doris123"
            def keystorePath = "${certDir}/keystore.p12"
            def truststorePath = "${certDir}/truststore.p12"
            runCommand("rm -f ${keystorePath}", "Failed to remove old keystore")
            runCommand("rm -f ${truststorePath}", "Failed to remove old truststore")
            runCommand("openssl pkcs12 -export -in ${certDir}/certificate.crt -inkey ${certDir}/certificate.key " +
                    "-out ${keystorePath} -password pass:${keystorePassword} -name doris-client",
                    "Failed to create client keystore")
            runCommand("keytool -import -noprompt -alias ca-cert -file ${certDir}/ca.crt -keystore ${truststorePath} " +
                    "-storepass ${keystorePassword} -storetype PKCS12",
                    "Failed to create truststore")
        }

        def generateClientCerts = { String certDir ->
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

            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/client_san.key -pkeyopt rsa_keygen_bits:2048",
                    "Failed to generate client SAN key")
            runCommand("openssl req -new -key ${certDir}/client_san.key -out ${certDir}/client_san.csr " +
                    "-config ${certDir}/client_san_openssl.cnf",
                    "Failed to generate client SAN CSR")
            runCommand("openssl x509 -req -days 3650 -in ${certDir}/client_san.csr -CA ${certDir}/ca.crt " +
                    "-CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/client_san.crt -extensions v3_req " +
                    "-extfile ${certDir}/client_san_openssl.cnf",
                    "Failed to sign client SAN cert")

            runCommand("chmod 644 ${certDir}/client_san.crt ${certDir}/client_san.key",
                    "Failed to fix client cert permissions")
        }

        generateNormalCert(certFileDir)
        generateClientCerts(certFileDir)

        def containerNames = []
        frontends.each { fe -> containerNames.add("doris-${cluster.name}-fe-${fe.index}") }
        backends.each { be -> containerNames.add("doris-${cluster.name}-be-${be.index}") }
        metaservices.each { ms -> containerNames.add("doris-${cluster.name}-ms-${ms.index}") }
        recyclers.each { rc -> containerNames.add("doris-${cluster.name}-recycle-${rc.index}") }

        def updateCertificate = { String certDir, String container ->
            runCommand("docker exec -i ${container} mkdir -p /tmp/certs", "Failed to create cert directory in ${container}")
            runCommand("docker exec -i ${container} rm -rf /tmp/certs_new", "Failed to remove old temp directory in ${container}")
            runCommand("docker exec -i ${container} mkdir -p /tmp/certs_new", "Failed to create temp cert directory in ${container}")

            ['ca.crt', 'certificate.key', 'certificate.crt'].each { fname ->
                def srcFile = new File("${certDir}/${fname}")
                if (srcFile.exists()) {
                    runCommand("docker cp ${srcFile.absolutePath} ${container}:/tmp/certs_new/${fname}",
                            "Failed to copy ${fname} to temp directory in ${container}")
                }
            }

            runCommand("docker exec -i ${container} bash -c 'mv -f /tmp/certs_new/* /tmp/certs/'",
                    "Failed to move certificates in ${container}")
            runCommand("docker exec -i ${container} rm -rf /tmp/certs_new", "Failed to cleanup temp directory in ${container}")
        }

        def updateAllCertificate = { String certDir ->
            containerNames.each { container ->
                updateCertificate(certDir, container)
            }
        }

        updateAllCertificate(certFileDir)

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

        frontends.each { fe -> updateConfigFile(fe.getConfFilePath()) }
        backends.each { be -> updateConfigFile(be.getConfFilePath()) }
        metaservices.each { ms -> updateConfigFile(ms.getConfFilePath()) }
        recyclers.each { rc -> updateConfigFile(rc.getConfFilePath()) }

        logger.info("=== Restarting all nodes with TLS enabled ===")
        def dorisComposePath = cluster.config.dorisComposePath
        def clusterName = cluster.name
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

        def msIds = metaservices.collect { it.index }.join(' ')
        def feIds = frontends.collect { it.index }.join(' ')
        def beIds = backends.collect { it.index }.join(' ')
        def recyclerIds = recyclers.collect { it.index }.join(' ')

        restartNodesWithoutWait('MS', "--ms-id ${msIds}")
        restartNodesWithoutWait('FE', "--fe-id ${feIds}")
        restartNodesWithoutWait('BE', "--be-id ${beIds}")
        restartNodesWithoutWait('Recycler', "--recycle-id ${recyclerIds}")

        sleep(40000)

        def firstFe = frontends[0]
        def baseJdbcUrl = String.format(
            "jdbc:mysql://%s:%s/?useLocalSessionState=true&allowLoadLocalInfile=false",
            firstFe.host, firstFe.queryPort)

        def testUser = "tls_route_user"
        def testPassword = "Test_123456"
        def tableName = "test_tls_route_tbl"
        def masterTable = "test_tls_route_tbl_master"
        def observerTable = "test_tls_route_tbl_observer"
        def clientSanValue = "email:test@example.com, DNS:testclient.example.com, URI:spiffe://example.com/testclient"
        def sanFull = clientSanValue

        def runRoutingTests = { String certDir ->
            def keystorePassword = "doris123"
            def keystorePath = "${certDir}/keystore.p12"
            def truststorePath = "${certDir}/truststore.p12"

            def sanClientCa = "${certDir}/ca.crt"
            def sanClientCert = "${certDir}/client_san.crt"
            def sanClientKey = "${certDir}/client_san.key"

            def tlsJdbcUrl = org.apache.doris.regression.Config.buildUrlWithDb(
                baseJdbcUrl,
                context.dbName,
                keystorePath,
                keystorePassword,
                truststorePath,
                keystorePassword
            )

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

            def buildMySQLCmdWithCert = { String host, String port, String user, String password, String query ->
                return "mysql -u${user} -p'${password}' -h${host} -P${port} " +
                        "--ssl-mode=VERIFY_CA --tls-version=TLSv1.2 " +
                        "--ssl-ca=${sanClientCa} --ssl-cert=${sanClientCert} --ssl-key=${sanClientKey} " +
                        "-e \"${query}\""
            }

            def executeStreamLoadCurl = { Map params ->
                def host = params.host
                def port = params.port
                def user = params.user
                def password = params.password
                def db = params.db ?: context.dbName
                def table = params.table
                def data = params.data
                def label = params.label ?: "sl_route_${UUID.randomUUID().toString().replace('-', '')}"
                def resolveHost = params.resolveHost
                def sniHostname = params.sniHostname ?: "testclient.example.com"
                def resolveOpt = resolveHost ? "--resolve '${sniHostname}:${port}:${resolveHost}'" : ""
                def targetHost = resolveHost ? sniHostname : host
                def followRedirect = params.followRedirect ?: false
                def redirectOpt = followRedirect ? "--location-trusted --max-redirs 3" : ""

                def cmd = """curl -s -k --cacert ${sanClientCa} \\
                    --cert ${sanClientCert} --key ${sanClientKey} \\
                    ${resolveOpt} \\
                    ${redirectOpt} \\
                    -u '${user}:${password}' \\
                    -H "Expect: 100-continue" \\
                    -H "label: ${label}" \\
                    -H "column_separator: ," \\
                    -T - \\
                    'https://${targetHost}:${port}/api/${db}/${table}/_stream_load' \\
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
                if (errorOutput) {
                    logger.info("Stream Load stderr: ${errorOutput}")
                }
                logger.info("Exit value: ${p.exitValue()}")

                if (p.exitValue() != 0) {
                    return [success: false, output: output, error: errorOutput, exitCode: p.exitValue()]
                }

                try {
                    def json = new groovy.json.JsonSlurper().parseText(output)
                    def status = json.Status?.toLowerCase()
                    def isSuccess = (status == "success" || status == "publish timeout")
                    return [success: isSuccess, output: output, json: json, status: status]
                } catch (Exception e) {
                    return [success: false, output: output, error: e.message]
                }
            }

            context.connect(context.config.jdbcUser, context.config.jdbcPassword, tlsJdbcUrl) {
                def cleanup = {
                    try_sql("DROP TABLE IF EXISTS ${tableName}")
                    try_sql("DROP TABLE IF EXISTS ${masterTable}")
                    try_sql("DROP TABLE IF EXISTS ${observerTable}")
                    try_sql("DROP USER IF EXISTS '${testUser}'@'%'")
                }

                cleanup()

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

                try {
                    sql """
                        CREATE TABLE ${tableName} (
                            k1 INT,
                            k2 VARCHAR(50)
                        ) DISTRIBUTED BY HASH(k1) BUCKETS 1
                        PROPERTIES ("replication_num" = "1")
                    """
                    sql "INSERT INTO ${tableName} VALUES (1, 'route_value')"

                    sql "CREATE USER '${testUser}'@'%' IDENTIFIED BY '${testPassword}' REQUIRE SAN '${sanFull}'"
                    sql "GRANT SELECT_PRIV ON ${context.dbName}.${tableName} TO '${testUser}'@'%'"
                    sql "GRANT CREATE_PRIV ON ${context.dbName}.* TO '${testUser}'@'%'"
                    sql "GRANT LOAD_PRIV ON ${context.dbName}.${tableName} TO '${testUser}'@'%'"
                    grantComputeGroupUsage(testUser)

                    def feRows = sql_return_maparray "show frontends"
                    def masterFe = feRows.find { it.IsMaster == "true" }
                    def observerFe = feRows.find { it.IsMaster == "false" }
                    assertNotNull(masterFe, "Master FE not found")
                    assertNotNull(observerFe, "Observer FE not found")

                    def selectMasterCmd = buildMySQLCmdWithCert(
                        masterFe.Host,
                        masterFe.QueryPort,
                        testUser,
                        testPassword,
                        "SELECT * FROM ${context.dbName}.${tableName} LIMIT 1"
                    )
                    assertTrue(executeMySQLCommand(selectMasterCmd, true),
                            "Master FE should allow SELECT via MySQL")

                    def createMasterCmd = buildMySQLCmdWithCert(
                        masterFe.Host,
                        masterFe.QueryPort,
                        testUser,
                        testPassword,
                        "CREATE TABLE ${context.dbName}.${masterTable} (k1 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1 " +
                                "PROPERTIES ('replication_num' = '1')"
                    )
                    assertTrue(executeMySQLCommand(createMasterCmd, true),
                            "Master FE should allow CREATE TABLE via MySQL")

                    def selectObserverCmd = buildMySQLCmdWithCert(
                        observerFe.Host,
                        observerFe.QueryPort,
                        testUser,
                        testPassword,
                        "SELECT * FROM ${context.dbName}.${tableName} LIMIT 1"
                    )
                    assertTrue(executeMySQLCommand(selectObserverCmd, true),
                            "Observer FE should allow SELECT via MySQL")

                    def createObserverCmd = buildMySQLCmdWithCert(
                        observerFe.Host,
                        observerFe.QueryPort,
                        testUser,
                        testPassword,
                        "CREATE TABLE ${context.dbName}.${observerTable} (k1 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1 " +
                                "PROPERTIES ('replication_num' = '1')"
                    )
                    assertTrue(executeMySQLCommand(createObserverCmd, true),
                            "Observer FE should allow CREATE TABLE via MySQL")

                    def feMasterResult = executeStreamLoadCurl(
                        host: masterFe.Host,
                        port: masterFe.HttpPort,
                        resolveHost: masterFe.Host,
                        sniHostname: "testclient.example.com",
                        followRedirect: true,
                        user: testUser,
                        password: testPassword,
                        table: tableName,
                        data: "10,fe_master"
                    )
                    assertTrue(feMasterResult.success, "Stream Load via master FE should succeed: ${feMasterResult.output}")

                    def feObserverResult = executeStreamLoadCurl(
                        host: observerFe.Host,
                        port: observerFe.HttpPort,
                        resolveHost: observerFe.Host,
                        sniHostname: "testclient.example.com",
                        followRedirect: true,
                        user: testUser,
                        password: testPassword,
                        table: tableName,
                        data: "11,fe_observer"
                    )
                    assertTrue(feObserverResult.success, "Stream Load via observer FE should succeed: ${feObserverResult.output}")

                    def beNode = backends[0]
                    def beResult = executeStreamLoadCurl(
                        host: beNode.host,
                        port: beNode.httpPort,
                        user: testUser,
                        password: testPassword,
                        table: tableName,
                        data: "12,be_direct"
                    )
                    assertTrue(beResult.success, "Stream Load via BE should succeed: ${beResult.output}")

                    def countRows = sql "SELECT COUNT(*) FROM ${tableName} WHERE k2 IN ('fe_master','fe_observer','be_direct')"
                    assertTrue(countRows[0][0] >= 3, "Stream Load rows should be visible")
                } finally {
                    cleanup()
                }
            }
        }

        runRoutingTests(certFileDir)
    }
}
