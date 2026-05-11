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

suite('test_tls_excluded_protocols_san_dns_interaction', 'docker, p0') {
    def testName = "test_tls_excluded_protocols_san_dns_interaction"

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
        def frontends = cluster.getAllFrontends()
        def backends = cluster.getAllBackends()
        def metaservices = cluster.getAllMetaservices()
        def recyclers = cluster.getAllRecyclers(false)

        assertTrue(frontends.size() > 0, "expected at least one FE")
        assertTrue(backends.size() > 0, "expected at least one BE")
        assertTrue(metaservices.size() > 0, "expected at least one MS")
        assertTrue(recyclers.size() > 0, "expected at least one recycler")

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

        def runCommandAllowFail = { String cmd ->
            logger.info("Executing command (allow fail): ${cmd}")
            def proc = ["bash", "-lc", cmd].execute()
            def stdout = new StringBuilder()
            def stderr = new StringBuilder()
            proc.waitForProcessOutput(stdout, stderr)
            return [
                exitCode: proc.exitValue(),
                output: stdout.toString() + stderr.toString()
            ]
        }

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
        def restartWithWait = { String idFlag, int waitTimeoutSec ->
            def cmd = "python -W ignore ${dorisComposePath} restart ${clusterName} ${idFlag} --wait-timeout ${waitTimeoutSec} -v --output-json"
            return runCommandAllowFail(cmd)
        }

        def waitContainersState = { List<String> containers, boolean expectRunning, int timeoutSeconds ->
            long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L
            while (System.currentTimeMillis() < deadline) {
                def allMatch = containers.every { container ->
                    def inspect = runCommandAllowFail("docker inspect -f '{{.State.Running}}' ${container}")
                    if (inspect.exitCode != 0) {
                        return false
                    }
                    def running = inspect.output.trim().equalsIgnoreCase("true")
                    return running == expectRunning
                }
                if (allMatch) {
                    return true
                }
                sleep(1000)
            }
            return false
        }

        def waitContainersRunning = { List<String> containers, int timeoutSeconds ->
            return waitContainersState(containers, true, timeoutSeconds)
        }

        def waitContainersExit = { List<String> containers, int timeoutSeconds ->
            return waitContainersState(containers, false, timeoutSeconds)
        }

        def assertRestartFailure = { String idFlag, List<String> containers, String label ->
            def ret = restartWithWait(idFlag, 25)
            def exited = waitContainersExit(containers, 30)
            assertTrue(ret.exitCode != 0 || exited,
                    "${label} expected startup failure, exitCode=${ret.exitCode}, output=${ret.output}")
        }

        def assertRestartSuccess = { String idFlag, List<String> containers, String label ->
            def ret = restartWithWait(idFlag, 25)
            assertTrue(ret.exitCode == 0, "${label} expected restart success, output=${ret.output}")
            assertTrue(waitContainersRunning(containers, 45),
                    "${label} expected all containers running")
        }

        def allIps = []
        frontends.each { fe -> if (!allIps.contains(fe.host)) allIps.add(fe.host) }
        backends.each { be -> if (!allIps.contains(be.host)) allIps.add(be.host) }
        metaservices.each { ms -> if (!allIps.contains(ms.host)) allIps.add(ms.host) }
        recyclers.each { rc -> if (!allIps.contains(rc.host)) allIps.add(rc.host) }

        logger.info("=== Generating TLS certificates ===")
        runCommand("mkdir -p ${localCertDir}", "Failed to create cert directory")
        runCommand("openssl genpkey -algorithm RSA -out ${localCertDir}/ca.key -pkeyopt rsa_keygen_bits:2048",
                "Failed to generate CA key")
        runCommand("openssl req -new -x509 -days 3650 -key ${localCertDir}/ca.key -out ${localCertDir}/ca.crt "
                + "-subj '/C=CN/ST=Beijing/L=Beijing/O=Doris/OU=Test/CN=DorisCA'",
                "Failed to generate CA cert")
        def currentIp = runCommand("""ip -4 addr show eth0 | grep -oP \"(?<=inet\\s)\\d+(\\.\\d+){3}\"""",
                "Failed to get current ip").trim()

        def generateServerCert = { String certDir ->
            runCommand("mkdir -p ${certDir}", "mkdir ${certDir}")
            if (certDir != localCertDir) {
                runCommand("cp ${localCertDir}/ca.crt ${certDir}/ca.crt", "copy ca.crt")
                runCommand("cp ${localCertDir}/ca.key ${certDir}/ca.key", "copy ca.key")
            }
            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/certificate.key -pkeyopt rsa_keygen_bits:2048",
                    "gen server key")
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
DNS.2 = internal2.com
DNS.3 = localhost
DNS.4 = fe-1
DNS.5 = be-1
DNS.6 = ms-1
DNS.7 = recycle-1
IP.1 = 127.0.0.1
IP.2 = ${currentIp}
${sanEntries}
"""
            new File("${certDir}/openssl.cnf").text = opensslConf
            runCommand("openssl req -new -key ${certDir}/certificate.key -out ${certDir}/certificate.csr "
                    + "-config ${certDir}/openssl.cnf", "gen server csr")
            runCommand("openssl x509 -req -days 3650 -in ${certDir}/certificate.csr -CA ${certDir}/ca.crt "
                    + "-CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/certificate.crt "
                    + "-extensions v3_req -extfile ${certDir}/openssl.cnf", "sign server cert")
            runCommand("chmod 644 ${certDir}/*.crt ${certDir}/*.key", "chmod server certs")
        }

        def generateClientCert = { String certDir, String name, List<String> dnsNames ->
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
            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/${name}.key -pkeyopt rsa_keygen_bits:2048",
                    "gen client key ${name}")
            runCommand("openssl req -new -key ${certDir}/${name}.key -out ${certDir}/${name}.csr "
                    + "-config ${certDir}/${name}_openssl.cnf", "gen client csr ${name}")
            runCommand("openssl x509 -req -days 3650 -in ${certDir}/${name}.csr -CA ${certDir}/ca.crt "
                    + "-CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/${name}.crt "
                    + "-extensions v3_req -extfile ${certDir}/${name}_openssl.cnf", "sign client cert ${name}")
            runCommand("chmod 644 ${certDir}/${name}.crt ${certDir}/${name}.key", "chmod client cert ${name}")
        }

        generateServerCert("${localCertDir}")
        generateClientCert("${localCertDir}", "client_allowed", ["internal.com"])

        def feContainers = frontends.collect { fe -> "doris-${cluster.name}-fe-${fe.index}" }
        def beContainers = backends.collect { be -> "doris-${cluster.name}-be-${be.index}" }
        def msContainers = metaservices.collect { ms -> "doris-${cluster.name}-ms-${ms.index}" }
        def rcContainers = recyclers.collect { rc -> "doris-${cluster.name}-recycle-${rc.index}" }
        def allContainers = feContainers + beContainers + msContainers + rcContainers

        def updateCertificate = { String certDir, String container ->
            runCommand("docker exec -i ${container} mkdir -p /tmp/certs", "mkdir certs ${container}")
            runCommand("docker exec -i ${container} bash -c 'rm -rf /tmp/certs_new && mkdir -p /tmp/certs_new'",
                    "tmp dir ${container}")
            ['ca.crt', 'certificate.key', 'certificate.crt'].each { fname ->
                def srcFile = new File("${certDir}/${fname}")
                if (srcFile.exists()) {
                    runCommand("docker cp ${srcFile.absolutePath} ${container}:/tmp/certs_new/${fname}",
                            "cp ${fname} -> ${container}")
                }
            }
            runCommand("docker exec -i ${container} bash -c 'mv -f /tmp/certs_new/* /tmp/certs/'",
                    "atomic move ${container}")
            runCommand("docker exec -i ${container} rm -rf /tmp/certs_new", "cleanup ${container}")
        }
        allContainers.each { c -> updateCertificate("${localCertDir}", "${c}") }

        def feIds = frontends.collect { it.index }.join(' ')
        def beIds = backends.collect { it.index }.join(' ')
        def msIds = metaservices.collect { it.index }.join(' ')
        def rcIds = recyclers.collect { it.index }.join(' ')

        def sanDnsConfig = "brpc=internal.com,internal2.com;thrift=internal.com"
        def baselineExcluded = "mysql"

        def applyConfigs = {
            Map<String, String> feCfg,
            Map<String, String> beCfg,
            Map<String, String> msCfg,
            Map<String, String> rcCfg ->
            frontends.each { fe -> updateConfigFile(fe.getConfFilePath(), feCfg) }
            backends.each { be -> updateConfigFile(be.getConfFilePath(), beCfg) }
            metaservices.each { ms -> updateConfigFile(ms.getConfFilePath(), msCfg) }
            recyclers.each { rc -> updateConfigFile(rc.getConfFilePath(), rcCfg) }
        }

        def baselineFeCfg = [
            "enable_tls": "true",
            "tls_verify_mode": "verify_fail_if_no_peer_cert",
            "tls_peer_cert_required_san_dns": sanDnsConfig,
            "tls_excluded_protocols": baselineExcluded
        ]
        def baselineBeCfg = [
            "enable_tls": "true",
            "tls_verify_mode": "verify_fail_if_no_peer_cert",
            "tls_peer_cert_required_san_dns": sanDnsConfig,
            "tls_excluded_protocols": baselineExcluded
        ]
        def baselineMsCfg = [
            "enable_tls": "true",
            "tls_verify_mode": "verify_fail_if_no_peer_cert",
            "tls_peer_cert_required_san_dns": sanDnsConfig,
            "tls_excluded_protocols": baselineExcluded
        ]
        def baselineRcCfg = [
            "enable_tls": "true",
            "tls_verify_mode": "verify_fail_if_no_peer_cert",
            "tls_peer_cert_required_san_dns": sanDnsConfig,
            "tls_excluded_protocols": baselineExcluded
        ]

        def restartAllAndReconnect = {
            assertRestartSuccess("--ms-id ${msIds}", msContainers, "restart MS")
            assertRestartSuccess("--fe-id ${feIds}", feContainers, "restart FE")
            assertRestartSuccess("--be-id ${beIds}", beContainers, "restart BE")
            assertRestartSuccess("--recycle-id ${rcIds}", rcContainers, "restart recycler")
            context.reconnectFe()
        }

        logger.info("=== Apply baseline non-overlap config and ensure cluster starts ===")
        applyConfigs(baselineFeCfg, baselineBeCfg, baselineMsCfg, baselineRcCfg)
        restartAllAndReconnect()

        logger.info("=== Case 1: tls_excluded_protocols overlaps with SAN gate, startup must fail ===")
        frontends.each { fe ->
            updateConfigFile(fe.getConfFilePath(), [
                "enable_tls": "true",
                "tls_verify_mode": "verify_fail_if_no_peer_cert",
                "tls_peer_cert_required_san_dns": sanDnsConfig,
                "tls_excluded_protocols": "thrift"
            ])
        }
        assertRestartFailure("--fe-id ${feIds}", feContainers, "FE overlap excluded+SAN")
        frontends.each { fe -> updateConfigFile(fe.getConfFilePath(), baselineFeCfg) }
        assertRestartSuccess("--fe-id ${feIds}", feContainers, "restore FE baseline")
        context.reconnectFe()

        backends.each { be ->
            updateConfigFile(be.getConfFilePath(), [
                "enable_tls": "true",
                "tls_verify_mode": "verify_fail_if_no_peer_cert",
                "tls_peer_cert_required_san_dns": sanDnsConfig,
                "tls_excluded_protocols": "brpc,thrift"
            ])
        }
        assertRestartFailure("--be-id ${beIds}", beContainers, "BE overlap excluded+SAN")
        backends.each { be -> updateConfigFile(be.getConfFilePath(), baselineBeCfg) }
        assertRestartSuccess("--be-id ${beIds}", beContainers, "restore BE baseline")

        metaservices.each { ms ->
            updateConfigFile(ms.getConfFilePath(), [
                "enable_tls": "true",
                "tls_verify_mode": "verify_fail_if_no_peer_cert",
                "tls_peer_cert_required_san_dns": sanDnsConfig,
                "tls_excluded_protocols": "brpc"
            ])
        }
        assertRestartFailure("--ms-id ${msIds}", msContainers, "MS overlap excluded+SAN")
        metaservices.each { ms -> updateConfigFile(ms.getConfFilePath(), baselineMsCfg) }
        assertRestartSuccess("--ms-id ${msIds}", msContainers, "restore MS baseline")

        recyclers.each { rc ->
            updateConfigFile(rc.getConfFilePath(), [
                "enable_tls": "true",
                "tls_verify_mode": "verify_fail_if_no_peer_cert",
                "tls_peer_cert_required_san_dns": sanDnsConfig,
                "tls_excluded_protocols": "brpc"
            ])
        }
        assertRestartFailure("--recycle-id ${rcIds}", rcContainers, "recycler overlap excluded+SAN")
        recyclers.each { rc -> updateConfigFile(rc.getConfFilePath(), baselineRcCfg) }
        assertRestartSuccess("--recycle-id ${rcIds}", rcContainers, "restore recycler baseline")

        logger.info("=== Case 2: tls_excluded_protocols does not overlap with SAN gate, valid cert should pass ===")
        applyConfigs(baselineFeCfg, baselineBeCfg, baselineMsCfg, baselineRcCfg)
        restartAllAndReconnect()

        def caPath = "${localCertDir}/ca.crt"
        def allowedCert = "${localCertDir}/client_allowed.crt"
        def allowedKey = "${localCertDir}/client_allowed.key"

        def runSClient = { String host, int port, String certPath, String keyPath, String alpn ->
            def certArgs = certPath ? "-cert ${certPath} -key ${keyPath}" : ""
            def alpnArg = alpn ? "-alpn ${alpn}" : ""
            def cmd = "timeout 5 openssl s_client -connect ${host}:${port} -servername internal.com -CAfile ${caPath} -ign_eof ${alpnArg} ${certArgs} < /dev/null 2>&1 || true"
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

        frontends.each { fe ->
            assertHandshakeSuccess(runSClient(fe.host, 9020, allowedCert, allowedKey, null),
                    "FE[${fe.index}] thrift with allowed cert")
        }
        backends.each { be ->
            assertHandshakeSuccess(runSClient(be.host, 8060, allowedCert, allowedKey, "h2"),
                    "BE[${be.index}] brpc with allowed cert")
        }
        metaservices.each { ms ->
            assertHandshakeSuccess(runSClient(ms.host, 5000, allowedCert, allowedKey, "h2"),
                    "MS[${ms.index}] brpc with allowed cert")
        }
        recyclers.each { rc ->
            assertHandshakeSuccess(runSClient(rc.host, 5000, allowedCert, allowedKey, "h2"),
                    "recycler[${rc.index}] brpc with allowed cert")
        }

        def runSqlWithRetry = { String stmt ->
            int attempts = 10
            for (int i = 0; i < attempts; ++i) {
                try {
                    sql stmt
                    return
                } catch (Throwable t) {
                    if (i == attempts - 1) {
                        throw t
                    }
                    Thread.sleep(3000)
                }
            }
        }

        runSqlWithRetry("select 1")
        runSqlWithRetry("show backends")

        logger.info("=== Case 3: SAN gate + verify_none should fail startup on FE/BE/MS/recycler ===")
        frontends.each { fe ->
            updateConfigFile(fe.getConfFilePath(), [
                "enable_tls": "true",
                "tls_verify_mode": "verify_none",
                "tls_peer_cert_required_san_dns": sanDnsConfig,
                "tls_excluded_protocols": baselineExcluded
            ])
        }
        assertRestartFailure("--fe-id ${feIds}", feContainers, "FE verify_none + SAN gate")
        frontends.each { fe -> updateConfigFile(fe.getConfFilePath(), baselineFeCfg) }
        assertRestartSuccess("--fe-id ${feIds}", feContainers, "restore FE after verify_none")
        context.reconnectFe()

        backends.each { be ->
            updateConfigFile(be.getConfFilePath(), [
                "enable_tls": "true",
                "tls_verify_mode": "verify_none",
                "tls_peer_cert_required_san_dns": sanDnsConfig,
                "tls_excluded_protocols": baselineExcluded
            ])
        }
        assertRestartFailure("--be-id ${beIds}", beContainers, "BE verify_none + SAN gate")
        backends.each { be -> updateConfigFile(be.getConfFilePath(), baselineBeCfg) }
        assertRestartSuccess("--be-id ${beIds}", beContainers, "restore BE after verify_none")

        metaservices.each { ms ->
            updateConfigFile(ms.getConfFilePath(), [
                "enable_tls": "true",
                "tls_verify_mode": "verify_none",
                "tls_peer_cert_required_san_dns": sanDnsConfig,
                "tls_excluded_protocols": baselineExcluded
            ])
        }
        assertRestartFailure("--ms-id ${msIds}", msContainers, "MS verify_none + SAN gate")
        metaservices.each { ms -> updateConfigFile(ms.getConfFilePath(), baselineMsCfg) }
        assertRestartSuccess("--ms-id ${msIds}", msContainers, "restore MS after verify_none")

        recyclers.each { rc ->
            updateConfigFile(rc.getConfFilePath(), [
                "enable_tls": "true",
                "tls_verify_mode": "verify_none",
                "tls_peer_cert_required_san_dns": sanDnsConfig,
                "tls_excluded_protocols": baselineExcluded
            ])
        }
        assertRestartFailure("--recycle-id ${rcIds}", rcContainers, "recycler verify_none + SAN gate")
        recyclers.each { rc -> updateConfigFile(rc.getConfFilePath(), baselineRcCfg) }
        assertRestartSuccess("--recycle-id ${rcIds}", rcContainers, "restore recycler after verify_none")

        logger.info("=== Case 4: SAN gate + verify_peer should fail startup on BE/MS ===")
        backends.each { be ->
            updateConfigFile(be.getConfFilePath(), [
                "enable_tls": "true",
                "tls_verify_mode": "verify_peer",
                "tls_peer_cert_required_san_dns": sanDnsConfig,
                "tls_excluded_protocols": baselineExcluded
            ])
        }
        assertRestartFailure("--be-id ${beIds}", beContainers, "BE verify_peer + SAN gate")
        backends.each { be -> updateConfigFile(be.getConfFilePath(), baselineBeCfg) }
        assertRestartSuccess("--be-id ${beIds}", beContainers, "restore BE after verify_peer")

        metaservices.each { ms ->
            updateConfigFile(ms.getConfFilePath(), [
                "enable_tls": "true",
                "tls_verify_mode": "verify_peer",
                "tls_peer_cert_required_san_dns": sanDnsConfig,
                "tls_excluded_protocols": baselineExcluded
            ])
        }
        assertRestartFailure("--ms-id ${msIds}", msContainers, "MS verify_peer + SAN gate")
        metaservices.each { ms -> updateConfigFile(ms.getConfFilePath(), baselineMsCfg) }
        assertRestartSuccess("--ms-id ${msIds}", msContainers, "restore MS after verify_peer")

        logger.info("=== Final health check ===")
        applyConfigs(baselineFeCfg, baselineBeCfg, baselineMsCfg, baselineRcCfg)
        restartAllAndReconnect()
        runSqlWithRetry("select 1")
        runSqlWithRetry("show backends")
    }
}
