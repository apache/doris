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
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class AzureFileIOSharedKeyTest {
    private static final String NAME = "adls.auth.shared-key.account.name";
    private static final String KEY = "adls.auth.shared-key.account.key";
    private static final String KEY_A = "a2V5LWE=";
    private static final String KEY_B = "a2V5LWI=";
    private static final Map<String, String> SHARED_KEY = Map.of(NAME, "account", KEY, KEY_B);

    private final AzureFileSystemProvider provider = new AzureFileSystemProvider();

    @Test
    void staticSharedKeySurvivesTheOfficialFileIODialectRoundTrip() {
        Map<String, String> catalog = Map.of("azure.account_name", "account", "azure.account_key", KEY_A);
        AzureFileSystemProperties original = provider.bind(catalog);
        Map<String, String> fileIOProperties = new HashMap<>(catalog);
        fileIOProperties.putAll(original.toIcebergFileIOProperties());

        AzureFileSystemProperties rebound = provider.bindVended(fileIOProperties, catalog).orElseThrow();

        Assertions.assertEquals(original.toMap(), rebound.toMap());
        Assertions.assertEquals(KEY_A, rebound.getAccountKey());
        Assertions.assertEquals(AzureAuthType.SHARED_KEY, rebound.authType());
    }

    @Test
    void officialSharedKeyDoesNotRequireAStaticAzureBinding() {
        AzureFileSystemProperties properties = provider.bindVended(SHARED_KEY, Map.of()).orElseThrow();

        Assertions.assertEquals(Map.of(
                "provider", "azure", "AZURE_AUTH_TYPE", "SHARED_KEY", "AZURE_ACCOUNT_NAME", "account",
                "AZURE_ENDPOINT", "https://account.blob.core.windows.net", "AZURE_ACCOUNT_KEY", KEY_B),
                properties.toMap());
        Assertions.assertEquals(SHARED_KEY, properties.rawProperties());
        Assertions.assertFalse(properties.toString().contains(KEY_B));
    }

    @ParameterizedTest
    @ValueSource(strings = {"SharedKey", "SAS", "OAuth2", "obsolete-auth-type"})
    void officialSharedKeyReplacesEveryOldNativeAuthenticationField(String oldAuthType) {
        Map<String, String> catalog = new HashMap<>(Map.of(
                "azure.account_name", "account", "azure.auth_type", oldAuthType,
                "azure.endpoint", "https://account.dfs.core.windows.net",
                "azure.account_key", "old-shared-key", "AZURE_ACCOUNT_KEY", "old-uppercase-key",
                "azure.sas_token", "sig=old-expired-signature", "azure.sas_expiry_ms", "1"));
        catalog.put("azure.oauth2_account_host", "old-account.dfs.core.chinacloudapi.cn");
        catalog.put("azure.oauth2_client_id", "old-client-id");
        catalog.put("azure.oauth2_client_secret", "old-client-secret");
        catalog.put("azure.oauth2_server_uri", "https://login.microsoftonline.com/old-tenant/oauth2/token");
        catalog.put("azure.oauth2_client_tenant_id", "old-tenant");
        catalog.put("fs.azure.sas.fixed.token.account.dfs.core.windows.net", "old-hadoop-token");
        Map<String, String> fileIOProperties = new HashMap<>(catalog);
        fileIOProperties.putAll(SHARED_KEY);

        AzureFileSystemProperties properties = provider.bindVended(fileIOProperties, catalog).orElseThrow();

        Assertions.assertDoesNotThrow(properties::validateForAccess);
        Assertions.assertEquals(AzureAuthType.SHARED_KEY, properties.authType());
        Assertions.assertEquals(KEY_B, properties.getAccountKey());
        Assertions.assertEquals("", properties.getSasToken());
        Assertions.assertEquals("", properties.getSasExpiryMs());
        Assertions.assertEquals("", properties.getClientId());
        Assertions.assertEquals("", properties.getClientSecret());
        Assertions.assertEquals("", properties.getOauthAccountHost());
        Assertions.assertEquals("", properties.getOauthServerUri());
        Assertions.assertEquals("", properties.getTenantId());
        Assertions.assertFalse(properties.rawProperties().values().stream().anyMatch(value -> value.contains("old-")));
        Assertions.assertEquals(Map.of(
                "provider", "azure", "AZURE_AUTH_TYPE", "SHARED_KEY", "AZURE_ACCOUNT_NAME", "account",
                "AZURE_ENDPOINT", "https://account.blob.core.windows.net", "AZURE_ACCOUNT_KEY", KEY_B),
                properties.toMap());
        Assertions.assertEquals(KEY_B, properties.toIcebergFileIOProperties().get(KEY));
    }

    @ParameterizedTest
    @MethodSource("incompleteCredentials")
    void incompleteOfficialSharedKeyNeverFallsBackToTheCatalog(Map<String, String> credentials) {
        StoragePropertiesException failure = Assertions.assertThrows(StoragePropertiesException.class,
                () -> provider.bindVended(credentials,
                        Map.of("azure.account_name", "account", "azure.account_key", KEY_A)));

        Assertions.assertTrue(failure.getMessage().contains("SharedKey"));
        Assertions.assertFalse(failure.getMessage().contains(KEY_A));
        Assertions.assertFalse(failure.getMessage().contains(KEY_B));
        Assertions.assertNull(failure.getCause());
    }

    private static List<Map<String, String>> incompleteCredentials() {
        return List.of(Map.of(NAME, "account"), Map.of(KEY, KEY_B),
                Map.of(NAME, "account", KEY, ""), Map.of(NAME, " ", KEY, KEY_B));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void conflictingCaseAliasesAreRejectedIndependentOfMapOrder(boolean reversed) {
        Map<String, String> credentials = new LinkedHashMap<>();
        String alias = "ADLS.AUTH.SHARED-KEY.ACCOUNT.KEY";
        credentials.put(NAME, "account");
        credentials.put(reversed ? alias : KEY, reversed ? KEY_A : KEY_B);
        credentials.put(reversed ? KEY : alias, reversed ? KEY_B : KEY_A);

        StoragePropertiesException failure = Assertions.assertThrows(StoragePropertiesException.class,
                () -> provider.bindVended(credentials, Map.of()));

        Assertions.assertTrue(failure.getMessage().contains("Conflicting"));
        Assertions.assertFalse(failure.getMessage().contains(KEY_A));
        Assertions.assertFalse(failure.getMessage().contains(KEY_B));
    }

    @ParameterizedTest
    @MethodSource("conflictingCatalogs")
    void sharedKeyAccountCannotOverrideTheCatalogLocationScope(Map<String, String> catalog) {
        StoragePropertiesException failure = Assertions.assertThrows(StoragePropertiesException.class,
                () -> provider.bindVended(SHARED_KEY, catalog));

        Assertions.assertTrue(failure.getMessage().contains("does not match"));
        Assertions.assertFalse(failure.getMessage().contains(KEY_B));
    }

    private static List<Map<String, String>> conflictingCatalogs() {
        return List.of(Map.of("azure.account_name", "other", "azure.account_key", KEY_A),
                Map.of("azure.account_name", "account", "azure.endpoint", "https://other.blob.core.windows.net"));
    }

    @Test
    void sharedKeyReusesSovereignEndpointAndUriScopeValidation() {
        AzureFileSystemProperties properties = provider.bindVended(SHARED_KEY, Map.of(
                "azure.account_name", "account", "azure.endpoint", "https://account.dfs.core.chinacloudapi.cn",
                "container", "container", "use_path_style", "true", "force_parsing_by_standard_uri", "true"))
                .orElseThrow();
        String path = "abfss://container@account.dfs.core.chinacloudapi.cn/path/file.parquet";

        Assertions.assertEquals("https://account.blob.core.chinacloudapi.cn", properties.getEndpoint());
        Assertions.assertEquals("true", properties.getUsePathStyle());
        Assertions.assertEquals("true", properties.getForceParsingByStandardUrl());
        Assertions.assertEquals(path, properties.validateAndNormalizeUri(path));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> properties.validateAndNormalizeUri(path.replace("container@", "other@")));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> properties.validateAndNormalizeUri(path.replace("account.dfs", "other.dfs")));
    }

    @Test
    void sharedKeyPreservesAnExplicitCustomEndpointWithoutTreatingItsHostAsAnAccount() {
        String endpoint = "http://127.0.0.1:10000/proxy%2Fpath";
        AzureFileSystemProperties properties = provider.bindVended(SHARED_KEY, Map.of(
                "azure.account_name", "account", "azure.endpoint", endpoint)).orElseThrow();

        Assertions.assertEquals(endpoint, properties.getEndpoint());
        Assertions.assertEquals("account", properties.getAccountName());
        Assertions.assertEquals(endpoint, properties.toIcebergFileIOConnectionProperties()
                .get("adls.connection-string.127.0.0.1"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void accessTokenRemainsExplicitlyUnsupportedForNativeBinding(boolean withSharedKey) {
        Map<String, String> credentials = new HashMap<>(withSharedKey ? SHARED_KEY : Map.of());
        credentials.put("adls.token", "private-access-token");

        StoragePropertiesException failure = Assertions.assertThrows(StoragePropertiesException.class,
                () -> provider.bindVended(credentials,
                        Map.of("azure.account_name", "account", "azure.account_key", KEY_A)));

        Assertions.assertTrue(failure.getMessage().contains("not supported"));
        Assertions.assertFalse(failure.getMessage().contains("private-access-token"));
    }

    @Test
    void bindingDoesNotMutateInputsOrRetainMutableCredentialMaps() {
        Map<String, String> credentials = new HashMap<>(SHARED_KEY);
        Map<String, String> catalog = new HashMap<>(Map.of(
                "azure.account_name", "account", "azure.account_key", KEY_A, "container", "container"));
        Map<String, String> beforeCatalog = new HashMap<>(catalog);

        AzureFileSystemProperties properties = provider.bindVended(credentials, catalog).orElseThrow();

        Assertions.assertEquals(SHARED_KEY, credentials);
        Assertions.assertEquals(beforeCatalog, catalog);
        credentials.put(KEY, KEY_A);
        catalog.put("container", "other");
        Assertions.assertEquals(KEY_B, properties.getAccountKey());
        Assertions.assertEquals("container", properties.getContainer());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> properties.rawProperties().put(KEY, KEY_A));
    }
}
