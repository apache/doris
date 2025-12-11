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

suite('test_tls_mysql_thread_leak', 'docker, p0') {
    def testName = "test_tls_mysql_thread_leak"

    def options = new ClusterOptions()
    options.setFeNum(2)
    options.setBeNum(2)
    options.cloudMode = true

    def tlsConfigs = [
        'enable_tls=false',
        'tls_verify_mode=verify_peer',
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

        def runCommand = { String cmd, String errorMsg ->
            logger.info("Executing command: ${cmd}")
            def proc = ["bash", "-lc", cmd].execute()
            def stdout = new StringBuilder()
            def stderr = new StringBuilder()
            proc.waitForProcessOutput(stdout, stderr)
            if (proc.exitValue() != 0) {
                logger.error("Command failed: ${cmd}")
                if (stdout.length() > 0) logger.error("stdout: ${stdout.toString()}")
                if (stderr.length() > 0) logger.error("stderr: ${stderr.toString()}")
                assert false : errorMsg
            }
            return stdout.toString()
        }

        def allIps = []
        frontends.each { fe -> if (!allIps.contains(fe.host)) allIps.add(fe.host) }
        backends.each { be -> if (!allIps.contains(be.host)) allIps.add(be.host) }
        metaservices.each { ms -> if (!allIps.contains(ms.host)) allIps.add(ms.host) }
        recyclers.each { rc -> if (!allIps.contains(rc.host)) allIps.add(rc.host) }

        def currentIp = runCommand('''ip -4 addr show eth0 | grep -oP "(?<=inet\\s)\\d+(\\.\\d+){3}"''',
                "Failed to get current ip").trim()

        runCommand("mkdir -p ${localCertDir}", "Failed to create cert directory")
        runCommand("openssl genpkey -algorithm RSA -out ${localCertDir}/ca.key -pkeyopt rsa_keygen_bits:2048",
                "Failed to generate CA key")
        runCommand("openssl req -new -x509 -days 3650 -key ${localCertDir}/ca.key -out ${localCertDir}/ca.crt "
                + "-subj '/C=CN/ST=Beijing/L=Beijing/O=Doris/OU=Test/CN=DorisCA'",
                "Failed to generate CA certificate")

        runCommand("openssl genpkey -algorithm RSA -out ${localCertDir}/certificate.key -pkeyopt rsa_keygen_bits:2048",
                "Failed to generate server key")
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
IP.1 = 127.0.0.1
IP.2 = ${currentIp}
${sanEntries}
"""
        new File("${localCertDir}/openssl.cnf").text = opensslConf
        runCommand("openssl req -new -key ${localCertDir}/certificate.key -out ${localCertDir}/certificate.csr "
                + "-config ${localCertDir}/openssl.cnf", "Failed to generate CSR")
        runCommand("openssl x509 -req -days 3650 -in ${localCertDir}/certificate.csr "
                + "-CA ${localCertDir}/ca.crt -CAkey ${localCertDir}/ca.key -CAcreateserial "
                + "-out ${localCertDir}/certificate.crt -extensions v3_req -extfile ${localCertDir}/openssl.cnf",
                "Failed to sign server certificate")
        runCommand("chmod 644 ${localCertDir}/*.crt ${localCertDir}/*.key", "Failed to chmod cert files")

        def keystorePassword = "doris123"
        runCommand("rm -f ${localCertDir}/keystore.p12 ${localCertDir}/truststore.p12", "Clean keystore")
        runCommand("openssl pkcs12 -export -in ${localCertDir}/certificate.crt -inkey ${localCertDir}/certificate.key "
                + "-out ${localCertDir}/keystore.p12 -password pass:${keystorePassword} -name doris-client",
                "Failed to generate keystore")
        runCommand("keytool -import -noprompt -alias ca-cert -file ${localCertDir}/ca.crt "
                + "-keystore ${localCertDir}/truststore.p12 -storepass ${keystorePassword} -storetype PKCS12",
                "Failed to generate truststore")

        def containerNames = []
        frontends.each { fe -> containerNames.add("doris-${cluster.name}-fe-${fe.index}") }
        backends.each { be -> containerNames.add("doris-${cluster.name}-be-${be.index}") }
        metaservices.each { ms -> containerNames.add("doris-${cluster.name}-ms-${ms.index}") }
        recyclers.each { rc -> containerNames.add("doris-${cluster.name}-recycle-${rc.index}") }

        def updateCertificate = { String certDir, String container ->
            runCommand("docker exec -i ${container} mkdir -p /tmp/certs", "Failed to create cert dir in ${container}")
            runCommand("docker exec -i ${container} bash -c 'rm -rf /tmp/certs_new && mkdir -p /tmp/certs_new'",
                    "Failed to init temp cert dir in ${container}")
            ['ca.crt', 'certificate.key', 'certificate.crt'].each { fname ->
                def srcFile = new File("${certDir}/${fname}")
                if (srcFile.exists()) {
                    runCommand("docker cp ${srcFile.absolutePath} ${container}:/tmp/certs_new/${fname}",
                            "Failed to copy ${fname} to ${container}")
                }
            }
            runCommand("docker exec -i ${container} bash -c 'mv -f /tmp/certs_new/* /tmp/certs/'",
                    "Failed to move certs in ${container}")
            runCommand("docker exec -i ${container} rm -rf /tmp/certs_new",
                    "Failed to clean temp cert dir in ${container}")
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
            logger.info("Updating configs: ${configParams}")
            frontends.each { fe -> updateConfigFile(fe.getConfFilePath(), configParams) }
            backends.each { be -> updateConfigFile(be.getConfFilePath(), configParams) }
            metaservices.each { ms -> updateConfigFile(ms.getConfFilePath(), configParams) }
            recyclers.each { rc -> updateConfigFile(rc.getConfFilePath(), configParams) }

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
            logger.info("Restart done, waiting for cluster to be ready")
        }

        updateConfigAndRestart([enable_tls: 'true'])
        sleep(90000)

        def fe0 = frontends[0]
        def jdbcUrl = "jdbc:mysql://${fe0.host}:${fe0.queryPort}/${context.dbName}"
        jdbcUrl += "?useLocalSessionState=true&allowLoadLocalInfile=false&useSSL=true&sslMode=VERIFY_CA"
        jdbcUrl += "&trustCertificateKeyStoreUrl=file:${localCertDir}/truststore.p12"
        jdbcUrl += "&trustCertificateKeyStorePassword=${keystorePassword}"

        logger.info("Creating 1000 TLS JDBC connections to ${fe0.host}:${fe0.queryPort}")
        (1..1000).each { idx ->
            context.connect(context.config.jdbcUser, context.config.jdbcPassword, jdbcUrl) { sql "SELECT 1" }
            if (idx % 200 == 0) {
                logger.info("Finished ${idx} connections")
            }
        }

def countThreadByKeyword = { String container, String keyword ->
            def script = '''
pid=$(ps -ef | grep "org.apache.doris.DorisFE" | grep "java" | grep -v grep | awk '{print $2}' | head -n 1)
if [ -z "$pid" ]; then echo 0; exit 0; fi
cat /proc/${pid}/task/*/comm 2>/dev/null | grep -c "__KEYWORD__" || true
'''.stripIndent().trim()

            script = script.replace("__KEYWORD__", keyword)
            
            def escaped = script.replace("'", "'\"'\"'")
            def cmd = "docker exec -i ${container} bash -lc '${escaped}'"
            
            def output = runCommand(cmd, "Failed to count ${keyword} in ${container}").trim()
            return output.isInteger() ? Integer.parseInt(output) : 0
        }

        frontends.each { fe ->
            def container = "doris-${cluster.name}-fe-${fe.index}"
            def total = countThreadByKeyword(container, "MysqlSSL")
            logger.info("Container ${container} MysqlSSL threads => total: ${total}")
            assertTrue(total < 5, "MysqlSSL thread leak detected in ${container}, total=${total}")
        }
    }
}
