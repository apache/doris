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

import org.apache.doris.foundation.property.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

class AzureIcebergFileIOPropertiesTest {
    @Test
    void legacySharedKeyEmitsOnlyOfficialFileIOCredentialsAndEndpoints() {
        AzureFileSystemProperties storage = AzureFileSystemProperties.of(Map.of(
                "AZURE_ACCOUNT_NAME", "account", "AZURE_ACCOUNT_KEY", "test-key",
                "azure.endpoint", "http://account.dfs.core.windows.net:10000/proxy%2Fpath"));

        Map<String, String> output = storage.toIcebergFileIOProperties();

        Assertions.assertEquals(Map.of(
                "adls.auth.shared-key.account.name", "account",
                "adls.auth.shared-key.account.key", "test-key",
                "adls.connection-string.account.dfs.core.windows.net",
                "http://account.blob.core.windows.net:10000/proxy%2Fpath",
                "adls.connection-string.account.blob.core.windows.net",
                "http://account.blob.core.windows.net:10000/proxy%2Fpath"), output);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> output.put("io-impl", "other"));
    }

    @ParameterizedTest
    @CsvSource({"4102444800000,4102444800000", "4133980800000,4102531200000"})
    void sasEmitsTheEarlierExpiryWithoutReencodingTheToken(String explicitExpiry, String expectedExpiry) {
        String token = "se=2100-01-02T00%3A00%3A00Z&sig=a+b%2Fc";
        AzureFileSystemProperties storage = AzureFileSystemProperties.of(Map.of(
                "azure.account_name", "account", "azure.sas_token", "?" + token,
                "azure.sas_expiry_ms", explicitExpiry));

        Map<String, String> output = storage.toIcebergFileIOProperties();

        Assertions.assertEquals(Map.of(
                "adls.sas-token.account.dfs.core.windows.net", token,
                "adls.sas-token.account.blob.core.windows.net", token,
                "adls.sas-token-expires-at-ms.account.dfs.core.windows.net", expectedExpiry,
                "adls.sas-token-expires-at-ms.account.blob.core.windows.net", expectedExpiry,
                "adls.connection-string.account.dfs.core.windows.net", "https://account.blob.core.windows.net",
                "adls.connection-string.account.blob.core.windows.net", "https://account.blob.core.windows.net"),
                output);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> output.put("adls.token", "other"));
    }

    @Test
    void sasWithUnknownExpiryDoesNotInventAnExpiryProperty() {
        String token = "si=stored-access-policy&sig=test-signature";
        AzureFileSystemProperties storage = AzureFileSystemProperties.of(Map.of(
                "azure.account_name", "account", "azure.sas_token", token));

        Assertions.assertEquals(Map.of(
                "adls.sas-token.account.dfs.core.windows.net", token,
                "adls.sas-token.account.blob.core.windows.net", token,
                "adls.connection-string.account.dfs.core.windows.net", "https://account.blob.core.windows.net",
                "adls.connection-string.account.blob.core.windows.net", "https://account.blob.core.windows.net"),
                storage.toIcebergFileIOProperties());
    }

    @Test
    void cachedBindingRejectsFileIOCredentialOutputAtExpiry() {
        Clock clock = Mockito.mock(Clock.class);
        Instant expiry = Instant.parse("2026-01-02T00:00:00Z");
        Mockito.when(clock.instant()).thenReturn(expiry.minusMillis(1));
        AzureFileSystemProperties storage = AzureFileSystemProperties.of(Map.of(
                "azure.account_name", "account",
                "azure.sas_token", "se=2026-01-02T00:00:00Z&sig=private-material"), clock);
        Assertions.assertDoesNotThrow(storage::toIcebergFileIOProperties);

        Mockito.when(clock.instant()).thenReturn(expiry);

        StoragePropertiesException error = Assertions.assertThrows(StoragePropertiesException.class,
                storage::toIcebergFileIOProperties);
        Assertions.assertEquals("Azure SAS credential is expired", error.getMessage());
        Assertions.assertNull(error.getCause());
        Assertions.assertDoesNotThrow(storage::validate);
    }

    @ParameterizedTest
    @ValueSource(strings = {"core.windows.net", "core.chinacloudapi.cn", "core.usgovcloudapi.net"})
    void fileIOSasAliasesRebindToTheSameNativeCredentials(String cloudSuffix) {
        Map<String, String> input = Map.of(
                "azure.account_name", "account",
                "azure.endpoint", "https://account.blob." + cloudSuffix,
                "azure.sas_token", "sig=test-signature&se=2100-01-01T00:00:00Z",
                "azure.sas_expiry_ms", "4070908800000");
        AzureFileSystemProvider provider = new AzureFileSystemProvider();
        AzureFileSystemProperties storage = provider.bind(input);
        Map<String, String> fileIOProperties = storage.toIcebergFileIOProperties();

        AzureFileSystemProperties rebound = provider.bindVended(fileIOProperties, input).orElseThrow();

        Assertions.assertEquals(storage.toMap(), rebound.toMap());
        Assertions.assertEquals(fileIOProperties, rebound.toIcebergFileIOProperties());
        Assertions.assertEquals("4070908800000", rebound.toMap().get("AZURE_SAS_EXPIRY_MS"));
    }

    @ParameterizedTest
    @MethodSource("conflictingFileIOAliases")
    void rebindRejectsConflictingFileIOAliasesWithoutStaticFallback(Map<String, String> credentials) {
        AzureFileSystemProvider provider = new AzureFileSystemProvider();
        Map<String, String> catalog = Map.of("azure.account_name", "account", "azure.account_key", "test-key");
        List<Map.Entry<String, String>> entries = new ArrayList<>(credentials.entrySet());
        for (boolean reverse : List.of(false, true)) {
            if (reverse) {
                Collections.reverse(entries);
            }
            Map<String, String> ordered = new LinkedHashMap<>();
            entries.forEach(entry -> ordered.put(entry.getKey(), entry.getValue()));
            StoragePropertiesException error = Assertions.assertThrows(StoragePropertiesException.class,
                    () -> provider.bindVended(ordered, catalog));
            Assertions.assertFalse(error.getMessage().contains("private-material"));
            Assertions.assertNull(error.getCause());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"account.dfs.core.windows.net", "onelake.dfs.fabric.microsoft.com"})
    void clientSecretOAuthRequiresHadoopFileIOWithoutChangingOtherCredentialViews(String host) {
        AzureFileSystemProperties storage = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "OAuth2",
                "azure.oauth2_account_host", host,
                "azure.oauth2_client_id", "test-client",
                "azure.oauth2_client_secret", "test-client-secret",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/test-tenant/oauth2/token"));
        Map<String, String> nativeProperties = storage.toMap();
        Map<String, String> hadoopProperties = storage.toHadoopConfigurationMap();

        Map<String, String> output = storage.toIcebergFileIOProperties();
        Assertions.assertEquals(Map.of("io-impl", "org.apache.iceberg.hadoop.HadoopFileIO"), output);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> output.put("adls.token", "other"));

        Assertions.assertEquals(nativeProperties, storage.toMap());
        Assertions.assertEquals("OAUTH2", nativeProperties.get("AZURE_AUTH_TYPE"));
        Assertions.assertEquals(hadoopProperties, storage.toHadoopConfigurationMap());
        Assertions.assertEquals("test-client-secret",
                hadoopProperties.get("fs.azure.account.oauth2.client.secret." + host));
    }

    private static Stream<Map<String, String>> conflictingFileIOAliases() {
        String dfsToken = "adls.sas-token.account.dfs.core.windows.net";
        String blobToken = "adls.sas-token.account.blob.core.windows.net";
        String token = "sig=private-material";
        return Stream.of(
                Map.of(dfsToken, token, blobToken, "sig=other-private-material"),
                Map.of(dfsToken, token, blobToken, token,
                        "adls.sas-token-expires-at-ms.account.dfs.core.windows.net", "4102444800000",
                        "adls.sas-token-expires-at-ms.account.blob.core.windows.net", "4133980800000"),
                Map.of(dfsToken, token, "adls.sas-token.other.blob.core.windows.net", token),
                Map.of(dfsToken, token, "adls.sas-token.account.blob.core.chinacloudapi.cn", token));
    }

    @ParameterizedTest
    @MethodSource("invalidFileIOConnections")
    void fileIOOutputRejectsInvalidConnectionsWithoutChangingGeneralBinding(Map<String, String> input) {
        AzureFileSystemProperties storage = AzureFileSystemProperties.of(input);
        Assertions.assertDoesNotThrow(storage::validate);

        StoragePropertiesException error = Assertions.assertThrows(StoragePropertiesException.class,
                storage::toIcebergFileIOProperties);

        Assertions.assertNull(error.getCause());
        Assertions.assertFalse(error.getMessage().contains("private-material"));
        Assertions.assertFalse(error.getMessage().contains("https://"));
    }

    private static Stream<Map<String, String>> invalidFileIOConnections() {
        Stream<Map<String, String>> embeddedCredentials = Stream.of(
                "https://account.blob.core.windows.net?sig=private-material",
                "https://private-material@account.blob.core.windows.net",
                "https://account.blob.core.windows.net#private-material").flatMap(endpoint -> Stream.of(
                        Map.of("azure.account_name", "account", "azure.account_key", "test-key",
                                "azure.endpoint", endpoint),
                        Map.of("azure.account_name", "account", "azure.sas_token", "sig=bound-sas",
                                "azure.endpoint", endpoint)));
        return Stream.concat(embeddedCredentials, Stream.of(Map.of("azure.sas_token", "sig=private-material")));
    }
}
