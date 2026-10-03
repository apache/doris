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
import org.apache.doris.regression.util.MySqlClient

suite("test_mysql_connection") { suite ->
    // NOTE: this suite needs mysql client 5.7+ to support --ssl-mode.
    if (!((context.config.otherConfigs.get('enableTLS')?.toString()?.equalsIgnoreCase('true')) ?: false)) {
        URI endpoint = new URI(context.config.jdbcUrl.substring('jdbc:'.length()))
        def executeMySQLCommand = { List<String> tlsOptions ->
            def options = ['-h', endpoint.host, '-P', endpoint.port.toString()] + tlsOptions
            logger.info("Execute mysql with options: ${options}")
            def result = MySqlClient.execute('root', context.config.getRootPassword(), options, 'show variables;')
            assert result.stderr.isEmpty(): "error occurred!" + result.stderr
            assert result.stdout.contains('version'): "error occurred!" + result.stderr
            assert result.exitCode == 0: "mysql exited with ${result.exitCode}: ${result.stderr}"
        }
        String certPath = context.config.sslCertificatePath
        executeMySQLCommand([])
        executeMySQLCommand(['--ssl-mode=DISABLED'])
        executeMySQLCommand(['--ssl-mode=REQUIRED', '--tls-version=TLSv1.2'])
        // Client verifies the server certificate.
        executeMySQLCommand(['--ssl-mode=VERIFY_CA', "--ssl-ca=${certPath}/ca.pem", '--tls-version=TLSv1.2'])
        // Both server and client authenticate with certificates.
        executeMySQLCommand(['--ssl-mode=VERIFY_CA', "--ssl-ca=${certPath}/ca.pem",
                             "--ssl-cert=${certPath}/client-cert.pem", "--ssl-key=${certPath}/client-key.pem",
                             '--tls-version=TLSv1.2'])
        // mysql-client 5.7.32 does not support TLSv1.3.
    }
}
