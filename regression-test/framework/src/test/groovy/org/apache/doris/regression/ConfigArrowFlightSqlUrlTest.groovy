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

package org.apache.doris.regression

import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertFalse
import static org.junit.jupiter.api.Assertions.assertTrue

class ConfigArrowFlightSqlUrlTest {
    @Test
    void plaintextFlightUrlRemainsAvailableWhenTlsIsDisabled() {
        Config config = new Config()
        assertEquals("jdbc:arrow-flight-sql://localhost:8070/?useServerPrepStmts=false&useEncryption=false",
                config.getArrowFlightSqlJdbcUrl("localhost", "8070"))
    }

    @Test
    void mtlsFlightUrlUsesCaAndClientPemFiles() {
        Config config = new Config()
        config.otherConfigs.put("enableTLS", "true")
        config.otherConfigs.put("trustCACert", "/test/ca.crt")
        config.otherConfigs.put("trustCert", "/test/client.crt")
        config.otherConfigs.put("trustCAKey", "/test/client.key")

        String url = config.getArrowFlightSqlJdbcUrl("localhost", "8070", "test_db")
        assertTrue(url.startsWith("jdbc:arrow-flight-sql://localhost:8070/catalog=test_db?"))
        assertTrue(url.contains("useEncryption=true"))
        assertTrue(url.contains("tlsRootCerts=/test/ca.crt"))
        assertTrue(url.contains("clientCertificate=/test/client.crt"))
        assertTrue(url.contains("clientKey=/test/client.key"))
        assertFalse(url.contains("disableCertificateVerification=true"))
    }

    @Test
    void flightUrlHonorsDisabledCertificateVerification() {
        Config config = new Config()
        config.otherConfigs.put("enableTLS", "true")
        config.otherConfigs.put("tlsVerifyMode", "none")

        String url = config.getArrowFlightSqlJdbcUrl("localhost", "8070")
        assertTrue(url.contains("useEncryption=true"))
        assertTrue(url.contains("disableCertificateVerification=true"))
    }
}
