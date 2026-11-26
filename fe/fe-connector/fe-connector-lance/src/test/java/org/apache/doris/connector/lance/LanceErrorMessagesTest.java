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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.DorisConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Provider errors reach the user and the log only with every credential and the dataset URI removed. */
class LanceErrorMessagesTest {

    @Test
    void secretsAndTheDatasetUriNeverReachTheMessageTheCauseOrTheStackTrace() {
        String bearerToken = "sentinel-bearer-token";
        String apiKey = "sentinel-api-key";
        String accessKey = "sentinel-access-key";
        String secretKey = "sentinel-secret-key";
        String sessionToken = "sentinel-session-token";
        String ossAccessKey = "sentinel-oss-access-key";
        String ossSecretKey = "sentinel-oss-secret-key";
        String ossSecurityToken = "sentinel-oss-security-token";
        String datasetUri = "s3://sentinel-user:sentinel-password@bucket/private/table.lance";

        Map<String, String> runtimeStorageOptions = new HashMap<>();
        runtimeStorageOptions.put("aws_access_key_id", accessKey);
        runtimeStorageOptions.put("aws_secret_access_key", secretKey);
        runtimeStorageOptions.put("aws_session_token", sessionToken);
        runtimeStorageOptions.put("oss_access_key_id", ossAccessKey);
        runtimeStorageOptions.put("oss_secret_access_key", ossSecretKey);
        runtimeStorageOptions.put("oss_security_token", ossSecurityToken);
        String providerMessage = "provider failure\nuri=" + datasetUri
                + " bearer=" + bearerToken + " api-key=" + apiKey
                + " access=" + accessKey + " secret=" + secretKey + " session=" + sessionToken
                + " oss-access=" + ossAccessKey + " oss-secret=" + ossSecretKey
                + " oss-token=" + ossSecurityToken;

        RuntimeException providerFailure = new RuntimeException(providerMessage);
        DorisConnectorException exposed = LanceErrorMessages.failure(
                "Failed to load Lance table metadata for db.table", providerFailure, datasetUri,
                runtimeStorageOptions, Arrays.asList(bearerToken, apiKey));
        StringWriter stackTrace = new StringWriter();
        exposed.printStackTrace(new PrintWriter(stackTrace));

        for (String sentinel : Arrays.asList(bearerToken, apiKey, accessKey, secretKey,
                sessionToken, ossAccessKey, ossSecretKey, ossSecurityToken, datasetUri)) {
            Assertions.assertFalse(exposed.getMessage().contains(sentinel), sentinel);
            Assertions.assertFalse(exposed.getCause().getMessage().contains(sentinel), sentinel);
            Assertions.assertFalse(stackTrace.toString().contains(sentinel), sentinel);
        }
        Assertions.assertNotSame(providerFailure, exposed.getCause());
        Assertions.assertTrue(exposed.getMessage().startsWith("Failed to load Lance table metadata for db.table: "));
        Assertions.assertTrue(exposed.getCause().getMessage().contains("***"));
        Assertions.assertFalse(exposed.getCause().getMessage().contains("\n"));
        Assertions.assertTrue(exposed.getCause().getMessage().getBytes(StandardCharsets.UTF_8).length <= 1024);
    }

    @Test
    void passThroughCredentialsNeverReachTheMessageTheCauseOrTheStackTrace() {
        for (String key : Arrays.asList("token", "azure_storage_sas_token", "sas_key",
                "AZURE_STORAGE_ACCESS_KEY", "google_service_account", "google_service_account_key",
                "google_application_credentials", "client_secret", "password", "authorization", "cookie")) {
            String secret = "vended-sensitive-value-for-" + key;
            Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(
                    "az://container/table.lance", Collections.emptyList(),
                    Map.of(key, secret, "endpoint", "https://storage.example.com"));
            DorisConnectorException exposed = LanceErrorMessages.failure("Failed to open dataset",
                    new RuntimeException("provider failure: " + secret + " at https://storage.example.com"),
                    "az://container/table.lance", options, Collections.emptyList());
            StringWriter stackTrace = new StringWriter();
            exposed.printStackTrace(new PrintWriter(stackTrace));

            Assertions.assertFalse(exposed.getMessage().contains(secret), key);
            Assertions.assertFalse(exposed.getCause().getMessage().contains(secret), key);
            Assertions.assertFalse(stackTrace.toString().contains(secret), key);
            Assertions.assertTrue(exposed.getMessage().contains("https://storage.example.com"), key);
            Assertions.assertEquals(secret, options.get(key), "sanitization must not change the options");
        }
    }

    @Test
    void theLimitIsInUtf8Bytes() {
        char[] multibyteCharacters = new char[1024];
        Arrays.fill(multibyteCharacters, '界');

        String sanitized = LanceErrorMessages.sanitize(
                new RuntimeException(new String(multibyteCharacters)), null, Collections.emptyMap(),
                Collections.emptyList());

        Assertions.assertTrue(sanitized.getBytes(StandardCharsets.UTF_8).length <= 1024);
    }

    /** The longer secret goes first, so a shorter one it contains cannot leave a suffix behind. */
    @Test
    void overlappingSecretsAreReplacedWhole() {
        Map<String, String> runtimeStorageOptions = Collections.singletonMap(
                "aws_secret_access_key", "overlapping-secret-with-suffix");

        String sanitized = LanceErrorMessages.sanitize(
                new RuntimeException("overlapping-secret-with-suffix"), null, runtimeStorageOptions,
                Collections.singletonList("overlapping-secret"));

        Assertions.assertEquals("RuntimeException: ***", sanitized);
    }

    @Test
    void theSanitizedCauseKeepsTheProviderErrorType() {
        IllegalArgumentException metadataFailure = new IllegalArgumentException("invalid metadata");

        DorisConnectorException exposed = LanceErrorMessages.failure(
                "Failed to load Lance table metadata for db.table", metadataFailure, null, null,
                Collections.emptyList());

        Assertions.assertTrue(exposed.getCause() instanceof IllegalArgumentException);
        Assertions.assertNotSame(metadataFailure, exposed.getCause());
        Assertions.assertEquals("IllegalArgumentException: invalid metadata", exposed.getCause().getMessage());
    }
}
