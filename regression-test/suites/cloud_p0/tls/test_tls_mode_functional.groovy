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

suite('test_tls_mode_functional', 'docker, p0') {
    def testName = "test_tls_mode_functional"

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
        def frontends = cluster.getAllFrontends()
        def backends = cluster.getAllBackends()
        def metaservices = cluster.getAllMetaservices()
        def recyclers = cluster.getAllRecyclers(false)

        logger.info("=== Frontend nodes ===")
        frontends.each { fe ->
            logger.info("FE[${fe.index}] - Host: ${fe.host}, HTTP Port: ${fe.httpPort}, Query Port: ${fe.queryPort}")
        }
        logger.info("=== Backend nodes ===")
        backends.each { be ->
            logger.info("BE[${be.index}] - Host: ${be.host}, HTTP Port: ${be.httpPort}, Heartbeat Port: ${be.heartbeatPort}")
        }
        logger.info("=== MetaService nodes ===")
        metaservices.each { ms ->
            logger.info("MS - Host: ${ms.host}, HTTP Port: ${ms.httpPort}")
        }
        logger.info("=== Recycler nodes ===")
        recyclers.each { rc ->
            logger.info("Recycler[${rc.index}] - Host: ${rc.host}, HTTP Port: ${rc.httpPort}")
        }

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
            // Return merged stdout + stderr result (for capturing all output from commands like curl)
            return stdout.toString() + stderr.toString()
        }

        // Certificate generation: root CA, normal, expired, wrong CA
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
            runCommand("openssl req -new -key ${certDir}/certificate.key -out ${certDir}/certificate.csr -config ${certDir}/openssl.cnf", "gen csr")
            runCommand("openssl x509 -req -days 3650 -in ${certDir}/certificate.csr -CA ${certDir}/ca.crt -CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/certificate.crt -extensions v3_req -extfile ${certDir}/openssl.cnf", "sign cert")
            runCommand("chmod 644 ${certDir}/*.crt ${certDir}/*.key", "chmod")
            // Generate keystore / truststore for JDBC usage
            def keystorePassword = "doris123"
            runCommand("rm -f ${certDir}/keystore.p12 ${certDir}/truststore.p12", "clean p12")
            runCommand("openssl pkcs12 -export -in ${certDir}/certificate.crt -inkey ${certDir}/certificate.key -out ${certDir}/keystore.p12 -password pass:${keystorePassword} -name doris-client", "gen keystore")
            runCommand("keytool -import -noprompt -alias ca-cert -file ${certDir}/ca.crt -keystore ${certDir}/truststore.p12 -storepass ${keystorePassword} -storetype PKCS12", "gen truststore")
        }
        def generateExpiredCert = { String certDir ->
            runCommand("mkdir -p ${certDir}", "mkdir expired")
            if (certDir != localCertDir) {
                runCommand("cp ${localCertDir}/ca.crt ${certDir}/ca.crt", "copy ca.crt")
                runCommand("cp ${localCertDir}/ca.key ${certDir}/ca.key", "copy ca.key")
            }
            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/certificate.key -pkeyopt rsa_keygen_bits:2048", "gen expired key")
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
CN = doris-cluster-expired

[v3_req]
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
IP.2 = ${current_ip}
${sanEntries}
"""
            new File("${certDir}/openssl.cnf").text = opensslConf
            runCommand("openssl req -new -key ${certDir}/certificate.key -out ${certDir}/certificate.csr -config ${certDir}/openssl.cnf", "gen expired csr")
            runCommand("openssl x509 -req -days -1 -in ${certDir}/certificate.csr -CA ${certDir}/ca.crt -CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/certificate.crt -extensions v3_req -extfile ${certDir}/openssl.cnf", "sign expired")
            runCommand("chmod 644 ${certDir}/*.crt ${certDir}/*.key", "chmod expired")
            // JDBC p12
            def pwd = "doris123"
            runCommand("rm -f ${certDir}/keystore.p12 ${certDir}/truststore.p12", "clean p12 expired")
            runCommand("openssl pkcs12 -export -in ${certDir}/certificate.crt -inkey ${certDir}/certificate.key -out ${certDir}/keystore.p12 -password pass:${pwd} -name doris-client", "gen ks expired")
            runCommand("keytool -import -noprompt -alias ca-cert -file ${certDir}/ca.crt -keystore ${certDir}/truststore.p12 -storepass ${pwd} -storetype PKCS12", "gen ts expired")
        }
        def generateWrongCA = { String certDir ->
            runCommand("mkdir -p ${certDir}", "mkdir wrongca")
            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/ca.key -pkeyopt rsa_keygen_bits:2048", "gen wrong ca key")
            runCommand("openssl req -new -x509 -days 3650 -key ${certDir}/ca.key -out ${certDir}/ca.crt -subj '/C=CN/ST=Beijing/L=Beijing/O=WrongCA/OU=Test/CN=WrongCA'", "gen wrong ca crt")
            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/certificate.key -pkeyopt rsa_keygen_bits:2048", "gen wrong key")
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
O = WrongCA
OU = Test
CN = wrong-cluster

[v3_req]
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
IP.2 = ${current_ip}
${sanEntries}
"""
            new File("${certDir}/openssl.cnf").text = opensslConf
            runCommand("openssl req -new -key ${certDir}/certificate.key -out ${certDir}/certificate.csr -config ${certDir}/openssl.cnf", "gen wrong csr")
            runCommand("openssl x509 -req -days 3650 -in ${certDir}/certificate.csr -CA ${certDir}/ca.crt -CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/certificate.crt -extensions v3_req -extfile ${certDir}/openssl.cnf", "sign wrong")
            runCommand("chmod 644 ${certDir}/*.crt ${certDir}/*.key", "chmod wrongca")
            // JDBC p12
            def pwd = "doris123"
            runCommand("rm -f ${certDir}/keystore.p12 ${certDir}/truststore.p12", "clean p12 wrong")
            runCommand("openssl pkcs12 -export -in ${certDir}/certificate.crt -inkey ${certDir}/certificate.key -out ${certDir}/keystore.p12 -password pass:${pwd} -name doris-client", "gen ks wrong")
            runCommand("keytool -import -noprompt -alias ca-cert -file ${certDir}/ca.crt -keystore ${certDir}/truststore.p12 -storepass ${pwd} -storetype PKCS12", "gen ts wrong")
        }

        generateNormalCert("${localCertDir}")
        def expiredCertDir = "${localCertDir}_expired";  generateExpiredCert(expiredCertDir)
        def wrongCACertDir = "${localCertDir}_wrongca";  generateWrongCA(wrongCACertDir)

        def mismatchedCertDir = "${localCertDir}_mismatch_ck"  // correct cert + wrong key
        runCommand("mkdir -p ${mismatchedCertDir}", "mkdir mismatch_ck")
        runCommand("cp ${localCertDir}/ca.crt ${mismatchedCertDir}/ca.crt", "cp ca")
        runCommand("cp ${localCertDir}/ca.key ${mismatchedCertDir}/ca.key", "cp ca.key")
        runCommand("openssl genpkey -algorithm RSA -out ${mismatchedCertDir}/certificate.key -pkeyopt rsa_keygen_bits:2048", "gen new key")
        runCommand("cp ${localCertDir}/certificate.crt ${mismatchedCertDir}/certificate.crt", "cp old cert")
        runCommand("chmod 644 ${mismatchedCertDir}/*.crt ${mismatchedCertDir}/*.key", "chmod mismatch_ck")
        // Generate keystore (cert+key mismatch will cause errors)
        def pwd = "doris123"
        runCommand("rm -f ${mismatchedCertDir}/keystore.p12 ${mismatchedCertDir}/truststore.p12", "clean p12 mismatch_ck")
        // Note: cert and key don't match here, generating keystore may fail itself or fail during usage
        runCommand("openssl pkcs12 -export -in ${mismatchedCertDir}/certificate.crt -inkey ${mismatchedCertDir}/certificate.key -out ${mismatchedCertDir}/keystore.p12 -password pass:${pwd} -name doris-client 2>&1 || true", "gen ks mismatch_ck")
        runCommand("keytool -import -noprompt -alias ca-cert -file ${mismatchedCertDir}/ca.crt -keystore ${mismatchedCertDir}/truststore.p12 -storepass ${pwd} -storetype PKCS12", "gen ts mismatch_ck")

        def mismatchedCertDir2 = "${localCertDir}_mismatch_kc" // correct key + wrong cert
        runCommand("mkdir -p ${mismatchedCertDir2}", "mkdir mismatch_kc")
        runCommand("cp ${localCertDir}/ca.crt ${mismatchedCertDir2}/ca.crt", "cp ca2")
        runCommand("cp ${localCertDir}/ca.key ${mismatchedCertDir2}/ca.key", "cp ca.key2")
        runCommand("cp ${localCertDir}/certificate.key ${mismatchedCertDir2}/certificate.key", "cp key ok")
        runCommand("cp ${wrongCACertDir}/certificate.crt ${mismatchedCertDir2}/certificate.crt", "cp wrong cert")
        runCommand("chmod 644 ${mismatchedCertDir2}/*.crt ${mismatchedCertDir2}/*.key", "chmod mismatch_kc")
        // Generate keystore (cert+key mismatch will cause errors)
        runCommand("rm -f ${mismatchedCertDir2}/keystore.p12 ${mismatchedCertDir2}/truststore.p12", "clean p12 mismatch_kc")
        runCommand("openssl pkcs12 -export -in ${mismatchedCertDir2}/certificate.crt -inkey ${mismatchedCertDir2}/certificate.key -out ${mismatchedCertDir2}/keystore.p12 -password pass:${pwd} -name doris-client 2>&1 || true", "gen ks mismatch_kc")
        runCommand("keytool -import -noprompt -alias ca-cert -file ${mismatchedCertDir2}/ca.crt -keystore ${mismatchedCertDir2}/truststore.p12 -storepass ${pwd} -storetype PKCS12", "gen ts mismatch_kc")

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
            def newLines = lines.collect { line ->
                for (param in configParams) {
                    if (line.trim().startsWith("${param.key}=")) {
                        return "${param.key}=${param.value}"
                    }
                }
                return line
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
                sleep(30000)
            }

            restart('MS', "--ms-id " + metaservices.collect { it.index }.join(' '))
            restart('FE', "--fe-id " + frontends.collect { it.index }.join(' '))
            restart('BE', "--be-id " + backends.collect { it.index }.join(' '))
            restart('Recycler', "--recycle-id " + recyclers.collect { it.index }.join(' '))
            logger.info("Waiting nodes to initialize...")
        }

        def testAllConnections = { boolean expectSuccess, Map certOptions, String description ->
            logger.info("=== Testing Connections (expect ${expectSuccess ? 'SUCCESS' : 'FAILURE'}) ===")
            logger.info("Test case: ${description}")
            logger.info("Certificate options: ${certOptions}")

            def certDir = certOptions.certDir ?: localCertDir
            def ca = certOptions.useCa ? "${certDir}/ca.crt" : null
            def cert = certOptions.useCert ? "${certDir}/certificate.crt" : null
            def key = certOptions.useKey ? "${certDir}/certificate.key" : null
            def useTls = certOptions.tlsEnabled == null ? true : certOptions.tlsEnabled
            def checkProtocolMismatch = certOptions.checkProtocolMismatch ?: false

            def curlOpts = ""
            if (ca) curlOpts += " --cacert ${ca}"
            if (cert) curlOpts += " --cert ${cert}"
            if (key) curlOpts += " --key ${key}"

            def protocol = useTls ? "https" : "http"

            def sslErrors = ["ssl certificate problem", "bad certificate", "alert bad certificate", "tlsv13 alert",
                             "ssl connect error", "unable to get local issuer certificate", "self signed certificate",
                             "certificate has expired", "ssl: certificate subject name", "tlsv1 alert", "sslv3 alert",
                             "failed", "wrong version number", "failure", "ssl_error", "unable to set"]

            def checkSuccess = { String output, String host, int port ->
                // If checkProtocolMismatch is enabled, compare responses from both protocols
                if (checkProtocolMismatch) {
                    def oppositeProtocol = useTls ? "http" : "https"
                    def oppositeCmd = "timeout 5 curl -sS -o /dev/null -w '%{http_code}' ${curlOpts} ${oppositeProtocol}://${host}:${port}/ 2>&1 || true"
                    def oppositeOutput = runCommand(oppositeCmd, "curl ${oppositeProtocol} ${host}:${port}")

                    // If both protocols give different response, it's a failure (protocol mismatch detected)
                    if (output.trim() != oppositeOutput.trim()) {
                        logger.info("  -> Different responses for protocols: ${protocol}=(${output.trim()}) vs ${oppositeProtocol}=(${oppositeOutput.trim()})")
                        return false
                    }
                }

                def hasSslError = sslErrors.any { output.toLowerCase().contains(it.toLowerCase()) }
                return !hasSslError
            }

            // HTTP - FE(8030/9020/9010), BE(9060/8040/9050/8060), MS/Recycler(5000)
            def fePortProtocols = [8030: "HTTP", 9020: "Thrift", 9010: "BDB"]
            def bePortProtocols = [9060: "Brpc", 8040: "Http", 9050: "Thrift", 8060: "Thrift"]

            frontends.each { fe ->
                [8030, 9020, 9010].each { port ->
                    def cmd = "timeout 5 curl -sS -o /dev/null -w '%{http_code}' ${curlOpts} ${protocol}://${fe.host}:${port}/ 2>&1 || true"
                    def output = runCommand(cmd, "curl FE[${fe.index}]:${port}")
                    def success = checkSuccess(output, fe.host, port)
                    logger.info("FE[${fe.index}] ${fe.host}:${port} (${fePortProtocols[port]}) => ${success ? 'OK' : 'FAIL'} (${output})")
                    assertEquals(expectSuccess, success)
                }
            }
            backends.each { be ->
                [9060, 8040, 9050, 8060].each { port ->
                    def cmd = "timeout 5 curl -sS -o /dev/null -w '%{http_code}' ${curlOpts} ${protocol}://${be.host}:${port}/ 2>&1 || true"
                    def output = runCommand(cmd, "curl BE[${be.index}]:${port}")
                    def success = checkSuccess(output, be.host, port)
                    logger.info("BE[${be.index}] ${be.host}:${port} (${bePortProtocols[port]}) => ${success ? 'OK' : 'FAIL'} (${output})")
                    assertEquals(expectSuccess, success)
                }
            }
            metaservices.each { ms ->
                def cmd = "timeout 5 curl -sS -o /dev/null -w '%{http_code}' ${curlOpts} ${protocol}://${ms.host}:5000/ 2>&1 || true"
                def output = runCommand(cmd, "curl MS[${ms.index}]:5000")
                def success = checkSuccess(output, ms.host, 5000)
                logger.info("MS[${ms.index}] ${ms.host}:5000 (HTTP) => ${success ? 'OK' : 'FAIL'} (${output})")
                assertEquals(expectSuccess, success)
            }
            recyclers.each { rc ->
                def cmd = "timeout 5 curl -sS -o /dev/null -w '%{http_code}' ${curlOpts} ${protocol}://${rc.host}:5000/ 2>&1 || true"
                def output = runCommand(cmd, "curl Recycler[${rc.index}]:5000")
                def success = checkSuccess(output, rc.host, 5000)
                logger.info("Recycler[${rc.index}] ${rc.host}:5000 (HTTP) => ${success ? 'OK' : 'FAIL'} (${output})")
                assertEquals(expectSuccess, success)
            }

            def fe0 = frontends[0]
            // JDBC (FE queryPort)
            if (certOptions.skipJDBC == null) {
                def jdbcUrl = "jdbc:mysql://${fe0.host}:${fe0.queryPort}/${context.dbName}?useLocalSessionState=true&allowLoadLocalInfile=false&useSSL=${useTls}"
                def keystorePassword = "doris123"
                if (certOptions.useCa) {
                    def truststorePath = "${certDir}/truststore.p12"
                    if (!new File(truststorePath).exists()) truststorePath = "${localCertDir}/truststore.p12"
                    jdbcUrl += "&trustCertificateKeyStoreUrl=file:${truststorePath}&trustCertificateKeyStorePassword=${keystorePassword}"
                }
                if (certOptions.useCert && certOptions.useKey) {
                    def keystorePath = "${certDir}/keystore.p12"
                    if (!new File(keystorePath).exists()) keystorePath = "${localCertDir}/keystore.p12"
                    jdbcUrl += "&clientCertificateKeyStoreUrl=file:${keystorePath}&clientCertificateKeyStorePassword=${keystorePassword}"
                }
                if (useTls) jdbcUrl += "&sslMode=VERIFY_CA"
                def jdbcSuccess = false
                try {
                    context.connect(context.config.jdbcUser, context.config.jdbcPassword, jdbcUrl) { sql "SELECT 1" }
                    jdbcSuccess = true
                } catch (Exception e) {
                    logger.info("JDBC exception: ${e.message}")
                    jdbcSuccess = false
                }
                logger.info("JDBC => ${jdbcSuccess ? 'OK' : 'FAIL'}")
                assertEquals(expectSuccess, jdbcSuccess)
            }

            // MySQL CLI
            if (certOptions.skipMysql == null) {
                def mysqlCmd = "timeout 10 mysql -h ${fe0.host} -P ${fe0.queryPort} -u ${context.config.jdbcUser}"
                if (useTls) mysqlCmd += " --ssl-mode=VERIFY_CA"
                else mysqlCmd += " --ssl-mode=PREFERRED"
                if (ca)   mysqlCmd += " --ssl-ca=${ca}"
                if (cert) mysqlCmd += " --ssl-cert=${cert}"
                if (key)  mysqlCmd += " --ssl-key=${key}"
                def statusCmd = mysqlCmd + " -e 'STATUS;' 2>&1 || true"
                mysqlCmd += " -e 'SELECT 1;' 2>&1 || true"
                def mysqlOutput = runCommand(mysqlCmd, "mysql cli")
                def mysqlSuccess = !mysqlOutput.toLowerCase().contains("error") && (mysqlOutput.contains("1") || mysqlOutput.isEmpty())
                logger.info("MySQL CLI => ${mysqlSuccess ? 'OK' : 'FAIL'} (${mysqlOutput})")
                assertEquals(expectSuccess, mysqlSuccess)
                runCommand(statusCmd, "mysql cli")
            }
        }

        // ===============================================================
        // SUITE 1: verify_fail_if_no_peer_cert
        // ===============================================================
        logger.info("=============== Suite 1: verify_fail_if_no_peer_cert ===============")

        // 1) enable_tls=false + (CA+cert+key) => success
        updateConfigAndRestart([enable_tls: 'false', tls_verify_mode: 'verify_fail_if_no_peer_cert'])
        testAllConnections(true, [useCa: true, useCert: true, useKey: true, tlsEnabled: false],
            "Suite1-Test1 : enable_tls=false, use HTTP with certs")

        // Switch back to enable_tls=true
        updateConfigAndRestart([enable_tls: 'true', tls_verify_mode: 'verify_fail_if_no_peer_cert'])

        // 2) wrong CA => failure
        testAllConnections(false, [useCa: true, useCert: true, useKey: true, certDir: wrongCACertDir],
            "Suite1-Test2 : verify_fail_if_no_peer_cert, wrong CA cert")

        // 3) CA + expired cert/key => failure
        testAllConnections(false, [useCa: true, useCert: true, useKey: true, certDir: expiredCertDir],
            "Suite1-Test3 : verify_fail_if_no_peer_cert, expired cert")

        // 4) only CA (no cert/key) => failure
        testAllConnections(false, [useCa: true, useCert: false, useKey: false],
            "Suite1-Test4 : verify_fail_if_no_peer_cert, only CA no client cert")

        // 5) CA + key (no cert) => failure
        testAllConnections(false, [useCa: true, useCert: false, useKey: true],
            "Suite1-Test5 : verify_fail_if_no_peer_cert, CA+key no cert")

        // 6) CA + cert (no key) => failure
        testAllConnections(false, [useCa: true, useCert: true, useKey: false],
            "Suite1-Test6 : verify_fail_if_no_peer_cert, CA+cert no key")

        // 7) positive case: CA + cert + key => success
        testAllConnections(true, [useCa: true, useCert: true, useKey: true],
            "Suite1-Test7 : verify_fail_if_no_peer_cert, positive test with full certs")

        // 8) no CA (but with cert/key) => failure
        testAllConnections(false, [useCa: false, useCert: true, useKey: true],
            "Suite1-Test8 : verify_fail_if_no_peer_cert, no CA only cert+key")

        // 9a) mismatch: correct cert + wrong key => failure
        testAllConnections(false, [useCa: true, useCert: true, useKey: true, certDir: mismatchedCertDir, skipJDBC: true],
            "Suite1-Test9a : verify_fail_if_no_peer_cert, cert+key mismatch")
        // 9b) mismatch: correct key + wrong cert => failure
        testAllConnections(false, [useCa: true, useCert: true, useKey: true, certDir: mismatchedCertDir2, skipJDBC: true],
            "Suite1-Test9b : verify_fail_if_no_peer_cert, key+cert mismatch")

        testAllConnections(false, [useCa: true, useCert: false, useKey: false, tlsEnabled: false, checkProtocolMismatch: true],
            "Suite1-Test10 : verify_fail_if_no_peer_cert, http try to connect")

        // ===============================================================
        // SUITE 2: verify_none
        // ===============================================================
        logger.info("=============== Suite 2: verify_none ===============")

        // 1) enable_tls=true + (CA+cert+key) => success (positive case)
        updateConfigAndRestart([enable_tls: 'true', tls_verify_mode: 'verify_none'])
        testAllConnections(true, [useCa: true, useCert: true, useKey: true],
            "Suite2-Test1 : verify_none, positive test with full certs")

        // 2) wrong CA (client with CA triggers validation) => failure
        testAllConnections(false, [useCa: true, useCert: true, useKey: true, certDir: wrongCACertDir],
            "Suite2-Test2 : verify_none, wrong CA cert")

        // 3) CA + expired cert/key => success
        testAllConnections(true, [useCa: true, useCert: true, useKey: true, certDir: expiredCertDir],
            "Suite2-Test3 : verify_none, expired cert (should succeed)")

        // 4) no CA/cert/key => failure (positive case), client will try to verify server certificate using system defaults which won't work
        testAllConnections(false, [useCa: false, useCert: false, useKey: false],
            "Suite2-Test4 : verify_none, no CA/cert/key at all")
        // 5) extended: only CA => success (positive case)
        testAllConnections(true, [useCa: true, useCert: false, useKey: false],
            "Suite2-Test5 : verify_none, only CA no client cert")

        // 6) partial: CA+cert => only JDBC supports, others require private key
        testAllConnections(false, [useCa: true, useCert: true, useKey: false, skipJDBC: true],
            "Suite2-Test6 : verify_none, CA+cert no key")
        // 7) partial: CA+key => mysql fails with ERROR 2026 (HY000): SSL connection error: Unable to get certificate
        testAllConnections(true, [useCa: true, useCert: false, useKey: true, skipMysql: true],
            "Suite2-Test7 : verify_none, CA+key no cert")

        // ===============================================================
        // SUITE 3: verify_peer
        // ===============================================================
        logger.info("=============== Suite 3: verify_peer ===============")
        // Temporarily not testing

        // updateConfigAndRestart([enable_tls: 'true', tls_verify_mode: 'verify_peer'])
        // testAllConnections(true, [useCa: true, useCert: true, useKey: true])

        // // 1) wrong CA => failure
        // testAllConnections(false, [useCa: true, useCert: true, useKey: true, certDir: wrongCACertDir])

        // // 2) no certificate => success
        // testAllConnections(true, [useCa: true, useCert: false, useKey: false])

        // // 3) no CA => failure
        // testAllConnections(false, [useCa: false, useCert: true, useKey: true])

        // // 4) CA+key+wrong cert (mismatch_kc) => failure
        // testAllConnections(false, [useCa: true, useCert: true, useKey: true, certDir: mismatchedCertDir2, skipJDBC: true])

        // // 5) CA+wrong key+cert (mismatch_ck) => failure
        // testAllConnections(false, [useCa: true, useCert: true, useKey: true, certDir: mismatchedCertDir, skipJDBC: true])

        // // 6) partial: CA+cert => JDBC can connect because it doesn't use keystore
        // testAllConnections(false, [useCa: true, useCert: true, useKey: false, skipJDBC: true])

        // // 7) partial: CA+key => mysql fails with ERROR 2026 (HY000): SSL connection error: Unable to get certificate
        // testAllConnections(true, [useCa: true, useCert: false, useKey: true, skipMysql: true])

        // // 8) CA + expired cert/key => failure
        // testAllConnections(false, [useCa: true, useCert: true, useKey: true, certDir: expiredCertDir])

        logger.info("=============== ALL TLS functional tests COMPLETED ===============")
    }
}
