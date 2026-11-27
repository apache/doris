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

package org.apache.doris.connector.iceberg;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class IcebergGoogleCredentialValidationTest {
    private static final String JSON = "gcp.auth.credentials-json";
    private static final String MARKER = "fixture-private-key-marker";

    @ParameterizedTest
    @ValueSource(strings = {"client_id", "client_email", "private_key", "private_key_id", "project_id",
            "token_uri", "quota_project_id", "universe_domain"})
    void rejectsNonStringSdkFieldsAtCreateUpdateAndRuntime(String field) {
        for (String value : List.of("null", "1", "true", "[]", "{}")) {
            Map<String, String> invalid = properties("{\"type\":\"service_account\",\"" + field + "\":" + value
                    + ",\"fixture_marker\":\"" + MARKER + "\"}");
            assertRejected(() -> new IcebergConnectorProvider().validateProperties(invalid));
            assertRejected(() -> new IcebergConnectorProvider().validatePropertiesForUpdate(
                    properties("{\"type\":\"service_account\"}"), Map.of(JSON, invalid.get(JSON))));
            assertRejected(() -> runtimeOptions(invalid));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "[]", "{", " ", "\t", "\n", "not-json-fixture-private-key-marker",
            "{\"type\":{}}", "{\"type\":null}", "{\"type\":1}"})
    void parserErrorsNeverEchoCredentialInputOrAttachParserCause(String json) {
        assertRejected(() -> new IcebergConnectorProvider().validateProperties(properties(json)));
        assertRejected(() -> runtimeOptions(properties(json)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"service_account", "authorized_user", "external_account",
            "impersonated_service_account", "external_account_authorized_user", "gdch_service_account"})
    void retainsSdkCredentialTypesAndEndpointSelection(String type) {
        String json = "{\"type\":\"" + type + "\",\"client_id\":\"fixture-client\","
                + "\"token_uri\":\"https://auth.example.com/token\",\"universe_domain\":\"example.com\"}";
        Map<String, String> raw = properties(json);
        Assertions.assertDoesNotThrow(() -> new IcebergConnectorProvider().validateProperties(raw));
        Assertions.assertEquals(json, runtimeOptions(raw).get(JSON));
    }

    @Test
    void retainsOperatorPathAndAdcConfigurationWithoutOpeningFiles() {
        Map<String, String> raw = properties(null);
        raw.put("gcp.auth.credentials-path", "/fixture/credential.json");
        Assertions.assertDoesNotThrow(() -> new IcebergConnectorProvider().validateProperties(raw));
        Assertions.assertEquals("/fixture/credential.json", runtimeOptions(raw).get("gcp.auth.credentials-path"));
        raw.remove("gcp.auth.credentials-path");
        Assertions.assertDoesNotThrow(() -> runtimeOptions(raw));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void acceptsEmptyInlineCredentialsAtCreate(boolean usePath) {
        Map<String, String> raw = emptyInlineCredentials(usePath);
        Assertions.assertDoesNotThrow(() -> new IcebergConnectorProvider().validateProperties(raw));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void allowsAlterToClearInlineCredentialsForAdcOrAPath(boolean usePath) {
        Map<String, String> updates = new HashMap<>(Map.of(JSON, ""));
        if (usePath) {
            updates.put("gcp.auth.credentials-path", "/fixture/credential.json");
        }
        Map<String, String> existing = properties("{\"type\":\"service_account\"}");
        Assertions.assertDoesNotThrow(() -> new IcebergConnectorProvider()
                .validatePropertiesForUpdate(existing, updates));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void preservesEmptyInlineCredentialFallbackAtRuntime(boolean usePath) {
        Map<String, String> options = runtimeOptions(emptyInlineCredentials(usePath));
        Assertions.assertEquals("", options.get(JSON));
        Assertions.assertEquals(usePath ? "/fixture/credential.json" : null,
                options.get("gcp.auth.credentials-path"));
    }

    private static Map<String, String> emptyInlineCredentials(boolean usePath) {
        Map<String, String> raw = properties("");
        if (usePath) {
            raw.put("gcp.auth.credentials-path", "/fixture/credential.json");
        }
        return raw;
    }

    @Test
    void duplicateFieldsUseTheSameFinalTypeAsTheSdkParser() {
        Map<String, String> valid = properties("{\"type\":\"service_account\","
                + "\"client_id\":{},\"client_id\":\"fixture-client\"}");
        Assertions.assertDoesNotThrow(() -> new IcebergConnectorProvider().validateProperties(valid));
        Map<String, String> invalid = properties("{\"type\":\"service_account\","
                + "\"client_id\":\"fixture-client\",\"client_id\":{}}");
        assertRejected(() -> runtimeOptions(invalid));
    }

    @Test
    void declaresInlineGoogleCredentialsSensitiveIndependentlyOfTheNamePattern() {
        List<String> keys = new IcebergConnectorProvider().securitySurface().sensitiveKeys();
        Assertions.assertTrue(keys.contains(JSON));
        Assertions.assertTrue(keys.contains("gcs.oauth2.token"));
    }

    private static void assertRejected(Runnable action) {
        IllegalArgumentException failure = Assertions.assertThrows(IllegalArgumentException.class, action::run);
        Assertions.assertTrue(failure.getMessage().contains(JSON), failure.getMessage());
        Assertions.assertFalse(failure.getMessage().contains(MARKER), failure.getMessage());
        Assertions.assertNull(failure.getCause());
    }

    private static Map<String, String> properties(String json) {
        Map<String, String> raw = new HashMap<>(Map.of("iceberg.catalog.type", "rest",
                "iceberg.rest.uri", "https://catalog.example.com", "iceberg.rest.security.type", "google"));
        if (json != null) {
            raw.put(JSON, json);
        }
        return raw;
    }

    private static Map<String, String> runtimeOptions(Map<String, String> raw) {
        return IcebergCatalogFactory.buildCatalogProperties(IcebergCatalogProperties.of(raw), Optional.empty(), false);
    }
}
