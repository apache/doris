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

package org.apache.doris.filesystem.azure;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class AzureFileSystemProviderTest {

    private final AzureFileSystemProvider provider = new AzureFileSystemProvider();

    @Test
    void supports_recognizesAccountNameProperty() {
        Map<String, String> props = new HashMap<>();
        props.put("AZURE_ACCOUNT_NAME", "myaccount");
        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_returnsFalseWhenNoAccountAndNoEndpoint() {
        Assertions.assertFalse(provider.supports(Collections.emptyMap()));
    }

    @Test
    void supports_returnsFalseForPlainAwsAccessKeyWithoutAzureSignal() {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ACCESS_KEY", "aws-account");
        Assertions.assertFalse(provider.supports(props));
    }

    @Test
    void supports_recognizesProviderOwnedSasToken() {
        Map<String, String> props = new HashMap<>();
        props.put("AZURE_SAS_TOKEN", "sv=2024-01-01&sig=temporary");
        Assertions.assertTrue(provider.supports(props));
    }

    @Test
    void supports_returnsFalseForUnknownEndpointSuffix() {
        Map<String, String> props = new HashMap<>();
        props.put("AZURE_ENDPOINT", "https://myaccount.s3.amazonaws.com");
        Assertions.assertFalse(provider.supports(props));
    }

    @Test
    void supportsExplicit_requiresRawAzureSupportFlag() {
        Assertions.assertTrue(provider.supportsExplicit(Map.of("fs.azure.support", "true")));
        Assertions.assertFalse(provider.supportsExplicit(Map.of("fs.azure.support", "false")));
        Assertions.assertFalse(provider.supportsExplicit(Map.of("provider", "azure")));
        Assertions.assertFalse(provider.supportsExplicit(Map.of("_STORAGE_TYPE_", "AZURE")));
        Assertions.assertFalse(provider.supportsExplicit(Map.of("AZURE_ACCOUNT_NAME", "account")));
        Assertions.assertFalse(provider.supportsExplicit(Map.of("fs.s3.support", "true")));
    }

    @Test
    void supportsGuess_doesNotTreatConvertedStorageMarkerAsExplicitInput() {
        Assertions.assertFalse(provider.supportsGuess(Map.of("_STORAGE_TYPE_", "AZURE")));
        Assertions.assertTrue(provider.supportsGuess(Map.of("provider", "azure")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://ACCOUNT.DFS.CORE.WINDOWS.NET",
            "https://account.dfs.core.chinacloudapi.cn", "https://ACCOUNT.BLOB.CORE.USGOVCLOUDAPI.NET"})
    void supportsGuess_recognizesDfsAndCaseInsensitiveAzureHosts(String endpoint) {
        Assertions.assertTrue(provider.supportsGuess(Map.of("azure.endpoint", endpoint)));
    }

    // F21 — provider must recognise all four Azure sovereign-cloud blob host suffixes.
    @ParameterizedTest
    @ValueSource(strings = {
            "https://myaccount.blob.core.windows.net",
            "https://myaccount.blob.core.chinacloudapi.cn",
            "https://myaccount.blob.core.usgovcloudapi.net",
            "https://myaccount.blob.core.cloudapi.de"
    })
    void supports_recognizesSovereignCloudEndpoints(String endpoint) {
        Map<String, String> props = new HashMap<>();
        props.put("AZURE_ENDPOINT", endpoint);
        Assertions.assertTrue(provider.supports(props),
                "endpoint should be recognised as Azure: " + endpoint);
    }
}
