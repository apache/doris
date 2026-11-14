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

package org.apache.doris.regression.util

import com.amazonaws.auth.AWSSessionCredentials
import com.amazonaws.auth.BasicAWSCredentials
import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.*

class S3TestCredentialsTest {
    private static Map credentials(String token = "syntheticSessionToken0001+/=") {
        return [ak: "syntheticAccessKey0001", sk: "syntheticSecretKey0001", sessionToken: token]
    }

    @Test
    void sdkRetainsTheConfiguredSessionToken() {
        def config = credentials()
        def sdk = new S3TestCredentials(config).getCredentials()
        assertTrue(sdk instanceof AWSSessionCredentials)
        assertEquals(config.ak, sdk.AWSAccessKeyId)
        assertEquals(config.sk, sdk.AWSSecretKey)
        assertEquals(config.sessionToken, sdk.sessionToken)
    }

    @Test
    void longTermCredentialsRemainCompatibleWithoutAToken() {
        for (String token : [null, ""]) {
            def sdk = new S3TestCredentials(credentials(token)).getCredentials()
            assertTrue(sdk instanceof BasicAWSCredentials)
            assertFalse(sdk instanceof AWSSessionCredentials)
        }
    }

    @Test
    void everySupportedSqlNamespaceCarriesAllThreeValues() {
        def config = credentials()
        def provider = new S3TestCredentials(config)
        assertEquals(["s3.access_key": config.ak, "s3.secret_key": config.sk,
                      "s3.session_token": config.sessionToken], provider.properties())
        assertEquals([AWS_ACCESS_KEY: config.ak, AWS_SECRET_KEY: config.sk,
                      AWS_TOKEN: config.sessionToken], provider.properties("AWS"))
        assertEquals([ACCESS_KEY: config.ak, SECRET_KEY: config.sk,
                      session_token: config.sessionToken], provider.properties("tvf"))
        assertEquals(2, new S3TestCredentials(credentials(null)).properties().size())
    }

    @Test
    void sdkAndNewStatementsReadTheUpdatedCredentialSnapshot() {
        Map config = new Properties()
        config.putAll(credentials())
        def provider = new S3TestCredentials(config)
        def oldCredentials = provider.getCredentials()
        config.putAll([ak: "rotatedAccessKey0002", sk: "rotatedSecretKey0002", sessionToken: "rotatedSession0002"])
        provider.refresh()
        assertEquals("rotatedAccessKey0002", provider.getCredentials().AWSAccessKeyId)
        assertEquals("rotatedSession0002", provider.getCredentials().sessionToken)
        assertEquals("rotatedSession0002", provider.properties()["s3.session_token"])
        assertEquals("syntheticAccessKey0001", oldCredentials.AWSAccessKeyId)
    }

    @Test
    void missingCredentialsAndUnknownNamespacesFailWithoutSecretValues() {
        def error = assertThrows(IllegalArgumentException) {
            new S3TestCredentials([sessionToken: "privateSessionValue0003"]).getCredentials()
        }
        assertFalse(error.message.contains("privateSessionValue0003"))
        assertThrows(IllegalArgumentException) { new S3TestCredentials(credentials()).properties("unknown") }
    }

    @Test
    void renderingEscapesQuotesAndBackslashesWithoutChangingTheCredential() {
        def config = credentials('quoted"Session\\Token0004')
        def provider = new S3TestCredentials(config)
        assertEquals(config.sessionToken, provider.getCredentials().sessionToken)
        assertTrue(provider.sqlProperties().contains('"s3.session_token" = "quoted\\"Session\\\\Token0004"'))
    }

    @Test
    void redactionPreservesTheErrorAndDoesNotMutateSqlOrCredentials() {
        def config = credentials()
        def provider = new S3TestCredentials(config)
        String sql = provider.sqlProperties()
        String diagnostic = "InvalidAccessKeyId: " + sql
        String safe = SecretRedactor.redact(diagnostic)
        assertTrue(safe.startsWith("InvalidAccessKeyId:"))
        assertFalse(safe.contains(config.ak))
        assertFalse(safe.contains(config.sk))
        assertFalse(safe.contains(config.sessionToken))
        assertTrue(sql.contains(config.sessionToken))
        assertEquals(config.sessionToken, provider.getCredentials().sessionToken)
        assertFalse(provider.toString().contains(config.sessionToken))
    }
}
