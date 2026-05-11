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

suite('test_tls_peer_cert_required_san_dns_thrift_multi_module', 'docker, p0') {
    def testName = "test_tls_peer_cert_required_san_dns_thrift_multi_module"

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
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

        logger.info("=== Generating TLS certificates ===")
        runCommand("mkdir -p ${localCertDir}", "Failed to create cert directory")
        runCommand("openssl genpkey -algorithm RSA -out ${localCertDir}/ca.key -pkeyopt rsa_keygen_bits:2048", "Failed to generate CA key")
        runCommand("openssl req -new -x509 -days 3650 -key ${localCertDir}/ca.key -out ${localCertDir}/ca.crt " +
                "-subj '/C=CN/ST=Beijing/L=Beijing/O=Doris/OU=Test/CN=DorisCA'", "Failed to generate CA cert")
        def currentIp = runCommand('''ip -4 addr show eth0 | grep -oP "(?<=inet\\s)\\d+(\\.\\d+){3}"''', "Failed to get current ip").trim()

        def generateServerCert = { String certDir ->
            runCommand("mkdir -p ${certDir}", "mkdir ${certDir}")
            if (certDir != localCertDir) {
                runCommand("cp ${localCertDir}/ca.crt ${certDir}/ca.crt", "copy ca.crt")
                runCommand("cp ${localCertDir}/ca.key ${certDir}/ca.key", "copy ca.key")
            }
            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/certificate.key -pkeyopt rsa_keygen_bits:2048", "gen server key")
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
DNS.1 = internal.com
DNS.2 = localhost
DNS.3 = fe-1
DNS.4 = be-1
DNS.5 = ms-1
DNS.6 = recycle-1
IP.1 = 127.0.0.1
IP.2 = ${currentIp}
${sanEntries}
"""
            new File("${certDir}/openssl.cnf").text = opensslConf
            runCommand("openssl req -new -key ${certDir}/certificate.key -out ${certDir}/certificate.csr -config ${certDir}/openssl.cnf", "gen server csr")
            runCommand("openssl x509 -req -days 3650 -in ${certDir}/certificate.csr -CA ${certDir}/ca.crt -CAkey ${certDir}/ca.key -CAcreateserial " +
                    "-out ${certDir}/certificate.crt -extensions v3_req -extfile ${certDir}/openssl.cnf", "sign server cert")
            runCommand("chmod 644 ${certDir}/*.crt ${certDir}/*.key", "chmod server certs")
        }

        def generateClientCert = { String certDir, String name, List<String> dnsNames,
                                   String signerCaCrtPath, String signerCaKeyPath ->
            def clientSanEntries = dnsNames.withIndex().collect { dns, idx ->
                "DNS.${idx + 1} = ${dns}"
            }.join('\n')
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
CN = ${name}

[v3_req]
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
subjectAltName = @alt_names

[alt_names]
${clientSanEntries}
"""
            new File("${certDir}/${name}_openssl.cnf").text = opensslConf
            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/${name}.key -pkeyopt rsa_keygen_bits:2048", "gen client key ${name}")
            runCommand("openssl req -new -key ${certDir}/${name}.key -out ${certDir}/${name}.csr -config ${certDir}/${name}_openssl.cnf", "gen client csr ${name}")
            runCommand("openssl x509 -req -days 3650 -in ${certDir}/${name}.csr -CA ${signerCaCrtPath} -CAkey ${signerCaKeyPath} -CAcreateserial " +
                    "-out ${certDir}/${name}.crt -extensions v3_req -extfile ${certDir}/${name}_openssl.cnf", "sign client cert ${name}")
            runCommand("chmod 644 ${certDir}/${name}.crt ${certDir}/${name}.key", "chmod client cert ${name}")
        }

        runCommand("openssl genpkey -algorithm RSA -out ${localCertDir}/external_ca.key -pkeyopt rsa_keygen_bits:2048", "Failed to generate external CA key")
        runCommand("openssl req -new -x509 -days 3650 -key ${localCertDir}/external_ca.key -out ${localCertDir}/external_ca.crt " +
                "-subj '/C=CN/ST=Beijing/L=Beijing/O=Doris/OU=Test/CN=ExternalDorisCA'", "Failed to generate external CA cert")

        generateServerCert("${localCertDir}")
        generateClientCert("${localCertDir}", "client_allowed", ["other.example.com", "internal.com"],
                "${localCertDir}/ca.crt", "${localCertDir}/ca.key")
        generateClientCert("${localCertDir}", "client_denied", ["external.com"],
                "${localCertDir}/ca.crt", "${localCertDir}/ca.key")
        generateClientCert("${localCertDir}", "client_untrusted", ["other-untrusted.example.com", "internal.com"],
                "${localCertDir}/external_ca.crt", "${localCertDir}/external_ca.key")

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
        containerNames.each { c -> updateCertificate("${localCertDir}", "${c}") }

        def updateConfigFile = { String confPath, Map<String, String> configParams ->
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
            configParams.each { key, value ->
                if (!updatedKeys.contains(key)) {
                    newLines.add("${key}=${value}")
                }
            }
            configFile.text = newLines.join('\n')
            logger.info("Updated config file: ${confPath} with params: ${configParams}")
        }

        def dorisComposePath = cluster.config.dorisComposePath
        def clusterName = cluster.name
        def restartNodesWithoutWait = { String nodeType, String idFlag ->
            def cmd = "python -W ignore ${dorisComposePath} restart ${clusterName} ${idFlag} --wait-timeout 0 -v --output-json"
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

        def configureForTarget = { String target ->
            if (target == "FE") {
                frontends.each { fe ->
                    updateConfigFile(fe.getConfFilePath(), [
                        "enable_tls": "true",
                        "tls_peer_cert_required_san_dns": "thrift=internal.com",
                        "tls_verify_mode": "verify_fail_if_no_peer_cert"
                    ])
                }
                backends.each { be -> updateConfigFile(be.getConfFilePath(), ["enable_tls": "true"]) }
            } else if (target == "BE") {
                frontends.each { fe ->
                    updateConfigFile(fe.getConfFilePath(), [
                        "enable_tls": "true",
                        "tls_verify_mode": "verify_peer",
                        "tls_peer_cert_required_san_dns": ""
                    ])
                }
                backends.each { be ->
                    updateConfigFile(be.getConfFilePath(), [
                        "enable_tls": "true",
                        "tls_verify_mode": "verify_fail_if_no_peer_cert",
                        "tls_peer_cert_required_san_dns": "thrift=internal.com"
                    ])
                }
            } else if (target == "BRPC_ONLY") {
                frontends.each { fe ->
                    updateConfigFile(fe.getConfFilePath(), [
                        "enable_tls": "true",
                        "tls_verify_mode": "verify_peer",
                        "tls_peer_cert_required_san_dns": ""
                    ])
                }
                backends.each { be ->
                    updateConfigFile(be.getConfFilePath(), [
                        "enable_tls": "true",
                        "tls_verify_mode": "verify_fail_if_no_peer_cert",
                        "tls_peer_cert_required_san_dns": "brpc=internal.com"
                    ])
                }
            } else {
                throw new IllegalArgumentException("Unknown target: ${target}")
            }
            metaservices.each { ms -> updateConfigFile(ms.getConfFilePath(), ["enable_tls": "true"]) }
            recyclers.each { rc -> updateConfigFile(rc.getConfFilePath(), ["enable_tls": "true"]) }
        }

        def restartAll = {
            restartNodesWithoutWait('MS', "--ms-id ${msIds}")
            restartNodesWithoutWait('FE', "--fe-id ${feIds}")
            restartNodesWithoutWait('BE', "--be-id ${beIds}")
            restartNodesWithoutWait('Recycler', "--recycle-id ${recyclerIds}")
            sleep(40000)
        }

        def ca = "${localCertDir}/ca.crt"
        def allowedCert = "${localCertDir}/client_allowed.crt"
        def allowedKey = "${localCertDir}/client_allowed.key"
        def deniedCert = "${localCertDir}/client_denied.crt"
        def deniedKey = "${localCertDir}/client_denied.key"
        def untrustedCert = "${localCertDir}/client_untrusted.crt"
        def untrustedKey = "${localCertDir}/client_untrusted.key"

        def runSClient = { String host, int port, String certPath, String keyPath, String alpn ->
            def certArgs = certPath ? "-cert ${certPath} -key ${keyPath}" : ""
            def alpnArg = alpn ? "-alpn ${alpn}" : ""
            def cmd = "timeout 5 openssl s_client -connect ${host}:${port} -servername internal.com -CAfile ${ca} ${alpnArg} ${certArgs} < /dev/null 2>&1 || true"
            return runCommand(cmd, "openssl s_client ${host}:${port}")
        }

        def hasConnectError = { String output ->
            def lower = output.toLowerCase()
            def connectErrors = ["connection refused", "timed out", "no route to host", "connection reset", "could not connect"]
            return connectErrors.any { lower.contains(it) }
        }

        def hasHandshakeError = { String output ->
            def lower = output.toLowerCase()
            def errors = [
                "handshake failure",
                "bad certificate",
                "no peer certificate",
                "peer did not return a certificate",
                "certificate required",
                "tlsv1 alert",
                "ssl3 alert",
                "sslv3 alert",
                "alert",
                "unknown ca",
                "verify error"
            ]
            return errors.any { lower.contains(it) }
        }

        def isHandshakeSuccess = { String output ->
            def lower = output.toLowerCase()
            def okMarkers = ["verify return code: 0 (ok)", "ssl-session:"]
            return okMarkers.any { lower.contains(it) } && !hasHandshakeError(output) && !hasConnectError(output)
        }

        def assertHandshakeSuccess = { String output, String label ->
            assertTrue(isHandshakeSuccess(output), "${label} expected handshake success, output: ${output}")
        }

        def assertHandshakeFailure = { String output, String label ->
            assertTrue(!hasConnectError(output) && hasHandshakeError(output), "${label} expected handshake failure, output: ${output}")
        }

        logger.info("=== Phase 1: FE thrift SAN gate ===")
        configureForTarget("FE")
        restartAll()
        frontends.each { fe ->
            def label = "FE[${fe.index}] thrift 9020"
            assertHandshakeFailure(runSClient(fe.host, 9020, null, null, null), "${label} no cert")
            assertHandshakeSuccess(runSClient(fe.host, 9020, allowedCert, allowedKey, null), "${label} allowed cert")
            assertHandshakeFailure(runSClient(fe.host, 9020, untrustedCert, untrustedKey, null),
                    "${label} untrusted ca with allowed san")
            assertHandshakeFailure(runSClient(fe.host, 9020, deniedCert, deniedKey, null), "${label} denied cert")
        }

        logger.info("=== Phase 2: BE thrift SAN gate ===")
        configureForTarget("BE")
        restartAll()
        context.reconnectFe()
        backends.each { be ->
            [9060, 9050].each { port ->
                def label = "BE[${be.index}] thrift ${port}"
                assertHandshakeFailure(runSClient(be.host, port, null, null, null), "${label} no cert")
                assertHandshakeSuccess(runSClient(be.host, port, allowedCert, allowedKey, null), "${label} allowed cert")
                assertHandshakeFailure(runSClient(be.host, port, untrustedCert, untrustedKey, null),
                        "${label} untrusted ca with allowed san")
                assertHandshakeFailure(runSClient(be.host, port, deniedCert, deniedKey, null), "${label} denied cert")
            }
        }

        logger.info("=== Phase 3: brpc SAN gate should not affect thrift ===")
        configureForTarget("BRPC_ONLY")
        restartAll()
        context.reconnectFe()
        backends.each { be ->
            [9060, 9050].each { port ->
                def label = "BE[${be.index}] thrift ${port} brpc-only"
                assertHandshakeFailure(runSClient(be.host, port, null, null, null), "${label} no cert")
                assertHandshakeSuccess(runSClient(be.host, port, allowedCert, allowedKey, null), "${label} allowed cert")
                assertHandshakeSuccess(runSClient(be.host, port, deniedCert, deniedKey, null),
                        "${label} denied SAN cert should pass")
                assertHandshakeFailure(runSClient(be.host, port, untrustedCert, untrustedKey, null),
                        "${label} untrusted cert")
            }
        }

        sql "select 1"
        sql "show backends"
    }
}
