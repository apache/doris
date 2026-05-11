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

suite('test_tls_peer_cert_required_san_dns_fe_be_register_heartbeat', 'docker, p0') {
    def testName = "test_tls_peer_cert_required_san_dns_fe_be_register_heartbeat"

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
    def feCertDir = "${localCertDir}/fe"
    def beMismatchCertDir = "${localCertDir}/be_mismatch"
    def beMatchCertDir = "${localCertDir}/be_match"

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
        runCommand("openssl req -new -x509 -days 3650 -key ${localCertDir}/ca.key -out ${localCertDir}/ca.crt -subj '/C=CN/ST=Beijing/L=Beijing/O=Doris/OU=Test/CN=DorisCA'", "Failed to generate CA cert")
        def currentIp = runCommand('''ip -4 addr show eth0 | grep -oP "(?<=inet\\s)\\d+(\\.\\d+){3}"''', "Failed to get current ip").trim()

        def generateNodeCert = { String certDir, String commonName, List<String> extraDns ->
            runCommand("mkdir -p ${certDir}", "mkdir ${certDir}")
            runCommand("cp ${localCertDir}/ca.crt ${certDir}/ca.crt", "copy ca.crt")
            runCommand("cp ${localCertDir}/ca.key ${certDir}/ca.key", "copy ca.key")
            runCommand("openssl genpkey -algorithm RSA -out ${certDir}/certificate.key -pkeyopt rsa_keygen_bits:2048", "gen server key")

            def dnsSet = new LinkedHashSet<String>()
            ["localhost", "fe-1", "be-1", "ms-1", "recycle-1"].each { dnsSet.add(it) }
            extraDns.each { dnsSet.add(it) }
            def dnsEntries = dnsSet.toList().withIndex().collect { dns, idx ->
                "DNS.${idx + 1} = ${dns}"
            }.join('\n')
            def ipEntries = allIps.withIndex().collect { ip, idx ->
                "IP.${idx + 3} = ${ip}"
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
CN = ${commonName}

[v3_req]
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
${dnsEntries}
IP.1 = 127.0.0.1
IP.2 = ${currentIp}
${ipEntries}
"""
            new File("${certDir}/openssl.cnf").text = opensslConf
            runCommand("openssl req -new -key ${certDir}/certificate.key -out ${certDir}/certificate.csr -config ${certDir}/openssl.cnf", "gen csr ${commonName}")
            runCommand("openssl x509 -req -days 3650 -in ${certDir}/certificate.csr -CA ${certDir}/ca.crt -CAkey ${certDir}/ca.key -CAcreateserial -out ${certDir}/certificate.crt -extensions v3_req -extfile ${certDir}/openssl.cnf", "sign cert ${commonName}")
            runCommand("chmod 644 ${certDir}/*.crt ${certDir}/*.key", "chmod certs ${commonName}")
        }

        generateNodeCert(feCertDir, "fe-node", ["internal.com"])
        generateNodeCert(beMismatchCertDir, "be-node-mismatch", ["mismatch.example.com"])
        generateNodeCert(beMatchCertDir, "be-node-match", ["internal.com"])

        def updateCertificate = { String certDir, String container ->
            runCommand("docker exec -i ${container} mkdir -p /tmp/certs", "mkdir certs ${container}")
            runCommand("docker exec -i ${container} bash -c 'rm -rf /tmp/certs_new && mkdir -p /tmp/certs_new'", "tmp dir ${container}")
            ['ca.crt', 'certificate.key', 'certificate.crt'].each { fname ->
                runCommand("docker cp ${certDir}/${fname} ${container}:/tmp/certs_new/${fname}", "cp ${fname} -> ${container}")
            }
            runCommand("docker exec -i ${container} bash -c 'mv -f /tmp/certs_new/* /tmp/certs/'", "atomic move ${container}")
            runCommand("docker exec -i ${container} rm -rf /tmp/certs_new", "cleanup ${container}")
        }

        frontends.each { fe ->
            updateCertificate(feCertDir, "doris-${cluster.name}-fe-${fe.index}")
        }
        backends.each { be ->
            updateCertificate(beMismatchCertDir, "doris-${cluster.name}-be-${be.index}")
        }
        metaservices.each { ms ->
            updateCertificate(feCertDir, "doris-${cluster.name}-ms-${ms.index}")
        }
        recyclers.each { rc ->
            updateCertificate(feCertDir, "doris-${cluster.name}-recycle-${rc.index}")
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

        frontends.each { fe ->
            updateConfigFile(fe.getConfFilePath(), [
                "enable_tls": "true",
                "tls_verify_mode": "verify_fail_if_no_peer_cert",
                "tls_peer_cert_required_san_dns": "thrift=internal.com"
            ])
        }
        backends.each { be ->
            updateConfigFile(be.getConfFilePath(), [
                "enable_tls": "true",
                "tls_verify_mode": "verify_fail_if_no_peer_cert"
            ])
        }
        metaservices.each { ms -> updateConfigFile(ms.getConfFilePath(), ["enable_tls": "true"]) }
        recyclers.each { rc -> updateConfigFile(rc.getConfFilePath(), ["enable_tls": "true"]) }

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
        restartNodesWithoutWait('MS', "--ms-id ${msIds}")
        restartNodesWithoutWait('FE', "--fe-id ${feIds}")
        restartNodesWithoutWait('BE', "--be-id ${beIds}")
        restartNodesWithoutWait('Recycler', "--recycle-id ${recyclerIds}")
        sleep(45000)

        def countFixedString = { String fixedString, String filePattern ->
            def cmd = "grep -h -F '${fixedString}' ${filePattern} 2>/dev/null | wc -l"
            def output = runCommand(cmd, "count '${fixedString}' in logs").trim()
            if (output.isEmpty()) {
                return 0
            }
            return Integer.parseInt(output)
        }

        def waitForCondition = { String label, int timeoutSeconds, int intervalMs, Closure<Boolean> condition ->
            long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L
            while (System.currentTimeMillis() < deadline) {
                if (condition()) {
                    return
                }
                sleep(intervalMs)
            }
            assert false : "Timeout waiting for ${label}"
        }

        def targetBe = backends[0]
        def beWarnPattern = "${targetBe.getBasePath()}/log/be.WARNING*"
        def beInfoPattern = "${targetBe.getBasePath()}/log/be.INFO*"
        def feLogPath = frontends[0].getLogFilePath()

        logger.info("=== Scenario 1: BE cert SAN not in FE allowlist, register/heartbeat should fail ===")
        waitForCondition("FE SAN gate reject log", 180, 5000) {
            countFixedString("TLS SAN DNS gate reject peer certificate", feLogPath) > 0
        }
        waitForCondition("BE report failure with certificate unknown", 180, 5000) {
            countFixedString("SSL_connect: sslv3 alert certificate unknown", beWarnPattern) > 0
        }
        waitForCondition("BE report-to-master failures", 180, 5000) {
            countFixedString("fail to report to master", beWarnPattern) > 0
        }

        logger.info("=== Scenario 2: Replace BE cert in-place (no restart), register/heartbeat should auto recover ===")
        updateCertificate(beMatchCertDir, "doris-${cluster.name}-be-${targetBe.index}")
        // Wait for auto refresh to observe the new cert without restarting BE.
        sleep(20000)

        waitForCondition("BE receives FE heartbeat", 120, 5000) {
            countFixedString("get heartbeat from FE.host:", beInfoPattern) > 0
        }

        int unknownCountPhase2A = countFixedString("SSL_connect: sslv3 alert certificate unknown", beWarnPattern)
        int feRejectCountPhase2A = countFixedString("TLS SAN DNS gate reject peer certificate", feLogPath)
        sleep(25000)
        int unknownCountPhase2B = countFixedString("SSL_connect: sslv3 alert certificate unknown", beWarnPattern)
        int feRejectCountPhase2B = countFixedString("TLS SAN DNS gate reject peer certificate", feLogPath)

        assertTrue(unknownCountPhase2B == unknownCountPhase2A,
                "Expected BE certificate-unknown report failures to stop growing after in-place cert replacement without restart, before=${unknownCountPhase2A}, after=${unknownCountPhase2B}")
        assertTrue(feRejectCountPhase2B == feRejectCountPhase2A,
                "Expected FE SAN gate reject logs to stop growing after in-place cert replacement without restart, before=${feRejectCountPhase2A}, after=${feRejectCountPhase2B}")
    }
}
