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
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

class AzureVendedCredentialsTest {

    private static final String HOST = "account.dfs.core.windows.net";
    private static final String TOKEN_KEY = "adls.sas-token." + HOST;
    private static final String EXPIRY_KEY = "adls.sas-token-expires-at-ms." + HOST;
    private static final String TOKEN = "sv=2024-01-01&sig=secret-signature%2B%2F%3D";
    private static final String EXPIRING_TOKEN = TOKEN + "&se=2100-01-01T00%3A00%3A00Z";

    private final AzureFileSystemProvider provider = new AzureFileSystemProvider();

    @Test
    void bindVended_bindsAccountScopedSasDirectlyToTypedProperties() {
        AzureFileSystemProperties properties = provider.bindVended(Map.of(TOKEN_KEY, EXPIRING_TOKEN),
                Map.of()).orElseThrow();

        Assertions.assertEquals(AzureAuthType.SAS, properties.authType());
        Assertions.assertEquals("account", properties.getAccountName());
        Assertions.assertEquals("https://account.blob.core.windows.net", properties.getEndpoint());
        Assertions.assertEquals(Map.of(TOKEN_KEY, EXPIRING_TOKEN), properties.rawProperties());
        Assertions.assertEquals(properties.rawProperties(), properties.matchedProperties());
        Assertions.assertEquals(Map.of(
                "provider", "azure",
                "AZURE_AUTH_TYPE", "SAS",
                "AZURE_ACCOUNT_NAME", "account",
                "AZURE_ENDPOINT", "https://account.blob.core.windows.net",
                "AZURE_SAS_TOKEN", EXPIRING_TOKEN,
                "AZURE_SAS_EXPIRY_MS", "4102444800000"), properties.toMap());
        Assertions.assertFalse(properties.toString().contains("secret-signature"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"SharedKey", "OAuth2", "SAS", "obsolete-auth-type"})
    void bindVended_replacesTheWholeOldCredentialGroup(String oldAuthType) {
        Map<String, String> catalog = new HashMap<>();
        catalog.put("provider", "azure");
        catalog.put("azure.account_name", "account");
        catalog.put("azure.endpoint", "https://account.dfs.core.windows.net");
        catalog.put("azure.auth_type", oldAuthType);
        catalog.put("azure.account_key", "old-shared-key");
        catalog.put("AZURE_ACCOUNT_KEY", "old-uppercase-key");
        catalog.put("azure.oauth2_account_host", "old-account.dfs.core.chinacloudapi.cn");
        catalog.put("azure.oauth2_client_id", "old-client-id");
        catalog.put("azure.oauth2_client_secret", "old-client-secret");
        catalog.put("azure.oauth2_server_uri", "https://login.microsoftonline.com/old-tenant/oauth2/token");
        catalog.put("azure.oauth2_client_tenant_id", "old-tenant");
        catalog.put("azure.sas_token", "sig=old-signature");
        catalog.put("azure.sas_expiry_ms", "1");
        catalog.put("fs.azure.sas.fixed.token." + HOST, "sig=old-hadoop-signature");
        Map<String, String> credentials = new HashMap<>(catalog);
        credentials.put(TOKEN_KEY, TOKEN);

        AzureFileSystemProperties properties = provider.bindVended(credentials, catalog).orElseThrow();

        Assertions.assertEquals(AzureAuthType.SAS, properties.authType());
        Assertions.assertEquals("", properties.getAccountKey());
        Assertions.assertEquals("", properties.getClientId());
        Assertions.assertEquals("", properties.getClientSecret());
        Assertions.assertEquals("", properties.getOauthAccountHost());
        Assertions.assertEquals("", properties.getOauthServerUri());
        Assertions.assertEquals("", properties.getTenantId());
        Assertions.assertEquals("", properties.getSasExpiryMs());
        Assertions.assertEquals(Map.of(
                "provider", "azure",
                "AZURE_AUTH_TYPE", "SAS",
                "AZURE_ACCOUNT_NAME", "account",
                "AZURE_ENDPOINT", "https://account.blob.core.windows.net",
                "AZURE_SAS_TOKEN", TOKEN), properties.toMap());
        Assertions.assertFalse(properties.rawProperties().values().stream().anyMatch(value -> value.contains("old-")));
        Assertions.assertEquals(TOKEN,
                properties.toHadoopConfigurationMap().get("fs.azure.sas.fixed.token." + HOST));
    }

    @ParameterizedTest
    @ValueSource(strings = {"?", "&", "?&&"})
    void bindVended_normalizesTransportPrefixWithoutReencodingSignature(String prefix) {
        AzureFileSystemProperties properties = provider.bindVended(Map.of(TOKEN_KEY, prefix + EXPIRING_TOKEN),
                Map.of()).orElseThrow();

        Assertions.assertEquals(EXPIRING_TOKEN, properties.getSasToken());
        Assertions.assertEquals(EXPIRING_TOKEN, properties.toMap().get("AZURE_SAS_TOKEN"));
    }

    @ParameterizedTest
    @CsvSource({"4102444700000, 4102444700000", "4200000000000, 4102444800000"})
    void bindVended_usesTheEarlierKnownExpiry(String explicitExpiry, String expectedExpiry) {
        AzureFileSystemProperties properties = provider.bindVended(
                Map.of(TOKEN_KEY, EXPIRING_TOKEN, EXPIRY_KEY, explicitExpiry), Map.of()).orElseThrow();

        Assertions.assertEquals(expectedExpiry, properties.getSasExpiryMs());
        Assertions.assertEquals(expectedExpiry, properties.toMap().get("AZURE_SAS_EXPIRY_MS"));
    }

    @Test
    void bindVended_doesNotInventExpiryWhenNeitherSourceProvidesOne() {
        AzureFileSystemProperties properties = provider.bindVended(Map.of(TOKEN_KEY, TOKEN), Map.of()).orElseThrow();

        Assertions.assertEquals("", properties.getSasExpiryMs());
        Assertions.assertFalse(properties.toMap().containsKey("AZURE_SAS_EXPIRY_MS"));
        Assertions.assertDoesNotThrow(properties::validateForAccess);
    }

    @Test
    void bindVended_defersKnownExpiryUntilAccessAndNeverFallsBack() {
        AzureFileSystemProperties properties = provider.bindVended(Map.of(TOKEN_KEY, TOKEN, EXPIRY_KEY, "1"),
                Map.of("azure.account_name", "account", "azure.account_key", "old-shared-key")).orElseThrow();

        Assertions.assertEquals(AzureAuthType.SAS, properties.authType());
        Assertions.assertEquals("", properties.getAccountKey());
        Assertions.assertDoesNotThrow(properties::validate);
        StoragePropertiesException error = Assertions.assertThrows(StoragePropertiesException.class,
                properties::validateForAccess);
        Assertions.assertEquals("Azure SAS credential is expired", error.getMessage());
        Assertions.assertThrows(StoragePropertiesException.class, properties::toMap);
        Assertions.assertThrows(StoragePropertiesException.class, properties::toHadoopConfigurationMap);
    }

    @Test
    void bindVended_doesNotMutateInputsOrRetainMutableViews() {
        Map<String, String> credentials = new HashMap<>(Map.of(TOKEN_KEY, TOKEN));
        Map<String, String> catalog = new HashMap<>(Map.of(
                "azure.account_name", "account", "azure.account_key", "old-shared-key", "container", "container"));
        Map<String, String> originalCredentials = new HashMap<>(credentials);
        Map<String, String> originalCatalog = new HashMap<>(catalog);
        AzureFileSystemProperties properties = provider.bindVended(credentials, catalog).orElseThrow();

        Assertions.assertEquals(originalCredentials, credentials);
        Assertions.assertEquals(originalCatalog, catalog);
        credentials.put(TOKEN_KEY, "sig=replacement");
        catalog.put("container", "replacement-container");
        Assertions.assertEquals(TOKEN, properties.getSasToken());
        Assertions.assertEquals(TOKEN, properties.rawProperties().get(TOKEN_KEY));
        Assertions.assertEquals("container", properties.getContainer());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> properties.rawProperties().put(TOKEN_KEY, "sig=replacement"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void bindVended_rejectsMultipleAccountsIndependentOfMapOrder(boolean reverseOrder) {
        Map<String, String> credentials = new LinkedHashMap<>();
        String otherKey = "adls.sas-token.other.dfs.core.windows.net";
        credentials.put(reverseOrder ? otherKey : TOKEN_KEY, TOKEN);
        credentials.put(reverseOrder ? TOKEN_KEY : otherKey, TOKEN);

        assertRejected(credentials);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void bindVended_rejectsExpiryForAnotherHostIndependentOfMapOrder(boolean expiryFirst) {
        Map<String, String> credentials = new LinkedHashMap<>();
        String otherExpiry = "adls.sas-token-expires-at-ms.account.dfs.core.chinacloudapi.cn";
        credentials.put(expiryFirst ? otherExpiry : TOKEN_KEY, expiryFirst ? "4102444800000" : TOKEN);
        credentials.put(expiryFirst ? TOKEN_KEY : otherExpiry, expiryFirst ? TOKEN : "4102444800000");

        assertRejected(credentials);
    }

    @Test
    void bindVended_acceptsEquivalentCaseVariantsOfOneAccount() {
        AzureFileSystemProperties properties = provider.bindVended(Map.of(
                TOKEN_KEY, TOKEN,
                "ADLS.SAS-TOKEN.ACCOUNT.DFS.CORE.WINDOWS.NET", TOKEN,
                "ADLS.SAS-TOKEN-EXPIRES-AT-MS.ACCOUNT.DFS.CORE.WINDOWS.NET", "4102444800000"),
                Map.of()).orElseThrow();

        Assertions.assertEquals("account", properties.getAccountName());
        Assertions.assertEquals(TOKEN, properties.getSasToken());
        Assertions.assertEquals("4102444800000", properties.getSasExpiryMs());
    }

    @Test
    void bindVended_rejectsConflictingTokensWithDifferentKeyCase() {
        assertRejected(Map.of(TOKEN_KEY, TOKEN,
                "ADLS.SAS-TOKEN.ACCOUNT.DFS.CORE.WINDOWS.NET", "sig=other-secret-signature"));
    }

    @Test
    void bindVended_rejectsConflictingExpiryAliases() {
        assertRejected(Map.of(TOKEN_KEY, TOKEN, EXPIRY_KEY, "4102444800000",
                "ADLS.SAS-TOKEN-EXPIRES-AT-MS.ACCOUNT.DFS.CORE.WINDOWS.NET", "4200000000000"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t", "?&&"})
    void bindVended_rejectsMissingTokenWithoutStaticFallback(String token) {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(TOKEN_KEY, token);

        assertRejected(credentials);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "0", "-1", "9223372036854775808", "secret-signature"})
    void bindVended_rejectsMalformedExplicitExpiryWithoutLeakingValue(String expiry) {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(TOKEN_KEY, TOKEN);
        credentials.put(EXPIRY_KEY, expiry);

        assertRejected(credentials);
    }

    @Test
    void bindVended_rejectsExpiryWithoutToken() {
        assertRejected(Map.of(EXPIRY_KEY, "4102444800000"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"se=", "se=secret-signature", "se=%GG", "sig=secret-signature\r\n"})
    void bindVended_rejectsMalformedSasWithoutLeakingInput(String token) {
        assertRejected(Map.of(TOKEN_KEY, token));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "account.dfs.core.windows.net/path", "user@account.dfs.core.windows.net",
            "account.dfs.core.windows.net:443", "account.dfs.core.windows.net?sig=secret-signature",
            "https://account.dfs.core.windows.net"})
    void bindVended_rejectsNonHostPropertySuffix(String host) {
        assertRejected(Map.of("adls.sas-token." + host, TOKEN));
    }

    @ParameterizedTest
    @ValueSource(strings = {"adls.token", "ADLS.TOKEN"})
    void bindVended_rejectsUnsupportedVendedAccessToken(String key) {
        StoragePropertiesException error = assertRejected(Map.of(key, "secret-signature"));

        Assertions.assertTrue(error.getMessage().contains("access tokens are not supported"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"adls.container", "ADLS.CONTAINER", "adls.container-name",
            "azure.container", "AZURE.BUCKET"})
    void bindVended_preservesAndValidatesContainerScope(String containerKey) {
        AzureFileSystemProperties properties = provider.bindVended(
                Map.of(TOKEN_KEY, TOKEN, containerKey, " container "), Map.of()).orElseThrow();

        Assertions.assertEquals("container", properties.getContainer());
        Assertions.assertFalse(properties.toMap().containsKey("AZURE_CONTAINER"));
        Assertions.assertEquals("abfss://container@" + HOST + "/file",
                properties.validateAndNormalizeUri("abfss://container@" + HOST + "/file"));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> properties.validateAndNormalizeUri("abfss://other@" + HOST + "/file"));
    }

    @Test
    void bindVended_rejectsConflictingContainerScopesAcrossAliases() {
        assertRejected(Map.of(TOKEN_KEY, TOKEN, "ADLS.CONTAINER", "first", "azure.bucket", "second"));
        Assertions.assertThrows(StoragePropertiesException.class, () -> provider.bindVended(
                Map.of(TOKEN_KEY, TOKEN, "ADLS.CONTAINER", "other"),
                Map.of("provider", "azure", "azure.bucket", "container")));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" "})
    void bindVended_rejectsEmptyContainerScope(String container) {
        Map<String, String> credentials = new HashMap<>(Map.of(TOKEN_KEY, TOKEN));
        credentials.put("adls.container", container);

        assertRejected(credentials);
    }

    @ParameterizedTest
    @CsvSource({"azure.account_name, other", "azure.endpoint, https://other.blob.core.windows.net",
            "azure.endpoint, https://account.blob.core.chinacloudapi.cn"})
    void bindVended_rejectsCatalogAccountOrCloudMismatch(String key, String value) {
        StoragePropertiesException error = Assertions.assertThrows(StoragePropertiesException.class,
                () -> provider.bindVended(Map.of(TOKEN_KEY, TOKEN), Map.of("provider", "azure", key, value)));

        Assertions.assertTrue(error.getMessage().contains("does not match"));
        Assertions.assertNull(error.getCause());
    }

    @Test
    void bindVended_rejectsLegacyAccountConflictWhenReplacingCredentials() {
        StoragePropertiesException error = Assertions.assertThrows(StoragePropertiesException.class,
                () -> provider.bindVended(Map.of(TOKEN_KEY, TOKEN), Map.of(
                        "provider", "azure", "s3.access_key", "old-account")));

        Assertions.assertEquals("Azure vended credential account does not match the legacy account",
                error.getMessage());
    }

    @Test
    void bindVended_rejectsLegacyEndpointAccountConflictWhenReplacingCredentials() {
        StoragePropertiesException error = Assertions.assertThrows(StoragePropertiesException.class,
                () -> provider.bindVended(Map.of(TOKEN_KEY, TOKEN), Map.of(
                        "s3.endpoint", "https://old-account.blob.core.windows.net")));

        Assertions.assertEquals("Azure vended credential account does not match the legacy endpoint",
                error.getMessage());
    }

    @Test
    void bindVended_rejectsCanonicalEndpointAccountConflictWhenReplacingCredentials() {
        StoragePropertiesException error = Assertions.assertThrows(StoragePropertiesException.class,
                () -> provider.bindVended(Map.of(TOKEN_KEY, TOKEN), Map.of(
                        "azure.endpoint", "https://old-account.blob.core.windows.net")));

        Assertions.assertEquals("Azure vended credential account does not match the legacy endpoint",
                error.getMessage());
    }

    @Test
    void bindVended_rejectsUppercaseLegacyEndpointAccountConflictWhenReplacingCredentials() {
        StoragePropertiesException error = Assertions.assertThrows(StoragePropertiesException.class,
                () -> provider.bindVended(Map.of(TOKEN_KEY, TOKEN), Map.of(
                        "AZURE_ENDPOINT", "https://old-account.blob.core.windows.net")));

        Assertions.assertEquals("Azure vended credential account does not match the legacy endpoint",
                error.getMessage());
    }

    @Test
    void bindVended_inheritsHistoricalUppercaseAzureCustomEndpoint() {
        AzureFileSystemProperties properties = provider.bindVended(Map.of(TOKEN_KEY, TOKEN), Map.of(
                "provider", "azure", "AZURE_ENDPOINT", "http://localhost:10000/devstoreaccount1",
                "container", "container")).orElseThrow();

        Assertions.assertEquals("http://localhost:10000/devstoreaccount1", properties.getEndpoint());
    }

    @Test
    void bindVended_preservesHistoricalUppercaseEndpointWithAccountAlias() {
        AzureFileSystemProperties properties = provider.bindVended(Map.of(TOKEN_KEY, TOKEN), Map.of(
                "AZURE_ENDPOINT", "https://proxy.example.test:8443",
                "AZURE_ACCOUNT_NAME", "account")).orElseThrow();

        Assertions.assertEquals("https://proxy.example.test:8443", properties.getEndpoint());
    }

    @Test
    void bindVendedSharedKey_rejectsLegacyAccountConflictWhenReplacingCredentials() {
        StoragePropertiesException error = Assertions.assertThrows(StoragePropertiesException.class,
                () -> provider.bindVended(Map.of(
                        "adls.auth.shared-key.account.name", "new-account",
                        "adls.auth.shared-key.account.key", "new-key"), Map.of(
                        "provider", "azure", "s3.access_key", "old-account")));

        Assertions.assertEquals("Azure vended credential account does not match the legacy account",
                error.getMessage());
    }

    @Test
    void bindVended_doesNotInheritSiblingS3Endpoint() {
        AzureFileSystemProperties properties = provider.bindVended(Map.of(TOKEN_KEY, TOKEN), Map.of(
                "provider", "azure", "azure.account_name", "account",
                "s3.endpoint", "https://s3.example.test")).orElseThrow();

        Assertions.assertEquals("https://account.blob.core.windows.net", properties.getEndpoint());
    }

    @ParameterizedTest
    @ValueSource(strings = {"http://localhost:10000/devstoreaccount1",
            "https://storage.example.test:8443/proxy%2Fpath"})
    void bindVended_inheritsExplicitAzureCustomEndpointAndConnectionOptions(String endpoint) {
        AzureFileSystemProperties properties = provider.bindVended(Map.of(TOKEN_KEY, TOKEN), Map.of(
                "fs.azure.support", "true", "azure.endpoint", endpoint,
                "container", "container", "use_path_style", "true",
                "force_parsing_by_standard_uri", "true")).orElseThrow();

        Assertions.assertEquals(endpoint, properties.getEndpoint());
        Assertions.assertEquals("container", properties.getContainer());
        Assertions.assertEquals("true", properties.getUsePathStyle());
        Assertions.assertEquals("true", properties.getForceParsingByStandardUrl());
    }

    @Test
    void bindVended_inheritsDfsEndpointWithUppercaseHost() {
        AzureFileSystemProperties properties = provider.bindVended(Map.of(TOKEN_KEY, TOKEN), Map.of(
                "azure.endpoint", "http://ACCOUNT.DFS.CORE.WINDOWS.NET:8443/proxy%2Fpath",
                "container", "container")).orElseThrow();

        Assertions.assertEquals("http://ACCOUNT.blob.CORE.WINDOWS.NET:8443/proxy%2Fpath", properties.getEndpoint());
        Assertions.assertEquals("container", properties.getContainer());
    }

    @ParameterizedTest
    @CsvSource({"use_path_style, s3.path-style-access", "s3.path-style-access, use_path_style"})
    void bindVended_overridesConnectionOptionByFieldInsteadOfAlias(String catalogKey, String vendedKey) {
        AzureFileSystemProperties properties = provider.bindVended(Map.of(
                TOKEN_KEY, TOKEN, vendedKey, "true", "force_parsing_by_standard_uri", "true"), Map.of(
                        "provider", "azure", catalogKey, "false", "force_parsing_by_standard_uri", "false"))
                .orElseThrow();

        Assertions.assertEquals("true", properties.getUsePathStyle());
        Assertions.assertEquals("true", properties.getForceParsingByStandardUrl());
        Assertions.assertFalse(properties.toMap().containsKey("use_path_style"));
        Assertions.assertFalse(properties.rawProperties().containsKey(catalogKey));
    }

    @Test
    void bindVended_doesNotRecognizeS3Credentials() {
        Assertions.assertTrue(provider.bindVended(Map.of(
                "s3.access-key-id", "aws-key", "s3.secret-access-key", "aws-secret", "s3.session-token", "aws-token"),
                Map.of("provider", "azure")).isEmpty());
    }

    @Test
    void bindVended_doesNotInheritOtherStoresLocationOrAuthentication() {
        Map<String, String> otherStore = Map.of(
                "provider", "s3", "fs.s3.support", "true", "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.access_key", "aws-key", "s3.secret_key", "aws-secret", "s3.bucket", "aws-bucket");
        Map<String, String> credentials = new HashMap<>(otherStore);
        credentials.put(TOKEN_KEY, TOKEN);
        AzureFileSystemProperties properties = provider.bindVended(credentials, otherStore).orElseThrow();

        Assertions.assertEquals("account", properties.getAccountName());
        Assertions.assertEquals("https://account.blob.core.windows.net", properties.getEndpoint());
        Assertions.assertEquals("", properties.getContainer());
        Assertions.assertEquals("", properties.getAccountKey());
        Assertions.assertEquals(Map.of(TOKEN_KEY, TOKEN), properties.rawProperties());
    }

    private StoragePropertiesException assertRejected(Map<String, String> credentials) {
        StoragePropertiesException error = Assertions.assertThrows(StoragePropertiesException.class,
                () -> provider.bindVended(credentials,
                        Map.of("azure.account_name", "account", "azure.account_key", "old-shared-key")));
        Assertions.assertFalse(error.getMessage().contains("secret-signature"));
        Assertions.assertFalse(error.getMessage().contains("old-shared-key"));
        Assertions.assertNull(error.getCause());
        return error;
    }
}
