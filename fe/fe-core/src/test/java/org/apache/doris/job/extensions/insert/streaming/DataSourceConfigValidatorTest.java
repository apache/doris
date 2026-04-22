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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.job.cdc.DataSourceConfigKeys;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DataSourceConfigValidatorTest {

    private static Map<String, String> sslModeInput(String value) {
        Map<String, String> input = new HashMap<>();
        input.put(DataSourceConfigKeys.SSL_MODE, value);
        return input;
    }

    @Test
    public void testSslModeLegalValues() {
        DataSourceConfigValidator.validateSource(sslModeInput(DataSourceConfigKeys.SSL_MODE_DISABLE));
        DataSourceConfigValidator.validateSource(sslModeInput(DataSourceConfigKeys.SSL_MODE_REQUIRE));
        DataSourceConfigValidator.validateSource(sslModeInput(DataSourceConfigKeys.SSL_MODE_VERIFY_CA));
    }

    @Test
    public void testSslModeRejectsMysqlUnderscoreSpelling() {
        assertReject(sslModeInput("verify_ca"));
    }

    @Test
    public void testSslModeRejectsVerifyFull() {
        assertReject(sslModeInput("verify-full"));
    }

    @Test
    public void testSslModeRejectsPreferredAndAllow() {
        assertReject(sslModeInput("preferred"));
        assertReject(sslModeInput("prefer"));
        assertReject(sslModeInput("allow"));
    }

    @Test
    public void testSslModeRejectsUppercaseVariants() {
        assertReject(sslModeInput("DISABLE"));
        assertReject(sslModeInput("Verify-CA"));
    }

    @Test
    public void testSslModeRejectsEmpty() {
        assertReject(sslModeInput(""));
    }

    @Test
    public void testSslModeOptional() {
        // ssl_mode is not required; validateSource should pass when absent
        Map<String, String> input = new HashMap<>();
        input.put(DataSourceConfigKeys.JDBC_URL, "jdbc:mysql://host/db");
        DataSourceConfigValidator.validateSource(input);
    }

    @Test
    public void testVerifyCaRequiresRootcert() {
        Map<String, String> input = sslModeInput(DataSourceConfigKeys.SSL_MODE_VERIFY_CA);
        assertReject(input);
    }

    @Test
    public void testVerifyCaWithRootcertPasses() {
        Map<String, String> input = sslModeInput(DataSourceConfigKeys.SSL_MODE_VERIFY_CA);
        input.put(DataSourceConfigKeys.SSL_ROOTCERT, "FILE:ca.pem");
        DataSourceConfigValidator.validateSource(input);
    }

    @Test
    public void testDisableWithoutRootcertPasses() {
        DataSourceConfigValidator.validateSource(sslModeInput(DataSourceConfigKeys.SSL_MODE_DISABLE));
        DataSourceConfigValidator.validateSource(sslModeInput(DataSourceConfigKeys.SSL_MODE_REQUIRE));
    }

    private static void assertReject(Map<String, String> input) {
        try {
            DataSourceConfigValidator.validateSource(input);
            Assert.fail("expected IllegalArgumentException for input: " + input);
        } catch (IllegalArgumentException ignored) {
            // expected
        }
    }
}
