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

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.FsCacheKeys;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.foundation.property.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class AzureFileSystemPropertiesTest {

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"SharedKey", "SHARED_KEY"})
    void bind_preservesSharedKeyWithUnusedLegacyOAuth2Fields(String authType) {
        Map<String, String> input = new HashMap<>(Map.of(
                "azure.account_name", "account",
                "azure.account_key", "shared-key",
                "azure.oauth2_account_host", "not a valid Azure host",
                "azure.oauth2_client_id", "legacy-client",
                "azure.oauth2_client_secret", "legacy-secret",
                "azure.oauth2_client_tenant_id", "legacy-tenant",
                "azure.oauth2_server_uri", "not a valid token endpoint"));
        if (authType != null) {
            input.put("azure.auth_type", authType);
        }
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(input);

        Assertions.assertEquals(AzureAuthType.SHARED_KEY, properties.authType());
        Assertions.assertEquals("not a valid Azure host", properties.getOauthAccountHost());
        Assertions.assertEquals("legacy-client", properties.getClientId());
        Assertions.assertEquals("legacy-secret", properties.getClientSecret());
        Assertions.assertEquals("https://account.blob.core.windows.net", properties.getEndpoint());
        Assertions.assertEquals(Map.of(
                "provider", "azure",
                "AZURE_AUTH_TYPE", "SHARED_KEY",
                "AZURE_ACCOUNT_NAME", "account",
                "AZURE_ENDPOINT", "https://account.blob.core.windows.net",
                "AZURE_ACCOUNT_KEY", "shared-key"), properties.toMap());
        Map<String, String> hadoop = properties.toHadoopConfigurationMap();
        Assertions.assertEquals("shared-key", hadoop.get("fs.azure.account.key.account.blob.core.windows.net"));
        Assertions.assertFalse(hadoop.keySet().stream().anyMatch(key -> key.contains("oauth")));
        Assertions.assertFalse(hadoop.values().stream().anyMatch(value -> value.contains("legacy-")));
        Assertions.assertFalse(properties.toString().contains("legacy-secret"));
    }

    @Test
    void validateAndNormalizeUri_preservesCloudAndOriginalObjectPath() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SAS", "azure.sas_token", "sig=temporary",
                "azure.endpoint", "https://account.dfs.core.chinacloudapi.cn", "container", "container"));
        String path = "abfss://container@account.dfs.core.chinacloudapi.cn//dir/http://example/a+b%252Ffile";

        Assertions.assertEquals(path, properties.validateAndNormalizeUri(path));
        Assertions.assertEquals("https://account.blob.core.chinacloudapi.cn/container/dir/file",
                properties.validateAndNormalizeUri("https://account.blob.core.chinacloudapi.cn/container/dir/file"));
        StoragePropertiesException mismatch = Assertions.assertThrows(StoragePropertiesException.class,
                () -> properties.validateAndNormalizeUri(
                        "abfss://container@account.dfs.core.windows.net/dir/file"));
        Assertions.assertEquals("Azure URI account host does not match the binding", mismatch.getMessage());
        // Only legacy SharedKey access supports S3-style locations without an account authority.
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> properties.validateAndNormalizeUri("s3://container/dir/file"));
    }

    @Test
    void validateAndNormalizeUri_doesNotIncludeSasInErrors() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.account_name", "account", "azure.account_key", "key"));
        StoragePropertiesException error = Assertions.assertThrows(StoragePropertiesException.class,
                () -> properties.validateAndNormalizeUri(
                        "https://account.blob.core.windows.net/container/%GG?sig=secret-signature"));

        Assertions.assertEquals("Invalid Azure URI", error.getMessage());
        Assertions.assertEquals("Invalid percent encoding in Azure object path", error.getCause().getMessage());
        Assertions.assertNull(error.getCause().getCause());
    }

    @Test
    void bind_usesFeCoreAzureAliasOrder() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.endpoint", "account.blob.core.windows.net",
                "azure.account_name", "azure-account",
                "AWS_ACCESS_KEY", "aws-account",
                "azure.account_key", "azure-key",
                "AWS_SECRET_KEY", "aws-key"));

        Assertions.assertEquals("https://account.blob.core.windows.net", properties.getEndpoint());
        Assertions.assertEquals("azure-account", properties.getAccountName());
        Assertions.assertEquals("azure-key", properties.getAccountKey());
    }

    @ParameterizedTest
    @ValueSource(strings = {"SAS", "OAuth2"})
    void nativeAuthentication_doesNotConsumeOtherProvidersStaticCredentials(String authType) {
        Map<String, String> input = new HashMap<>(Map.of(
                "azure.auth_type", authType,
                "azure.endpoint", "https://account.blob.core.windows.net",
                "s3.access_key", "s3-account", "s3.secret_key", "s3-secret",
                "AWS_ACCESS_KEY", "aws-account", "AWS_SECRET_KEY", "aws-secret",
                "access_key", "generic-account", "secret_key", "generic-secret"));
        if (authType.equals("SAS")) {
            input.put("azure.sas_token", "sig=azure-token");
        } else {
            input.putAll(Map.of(
                    "azure.oauth2_account_host", "account.dfs.core.windows.net",
                    "azure.oauth2_client_id", "client-id",
                    "azure.oauth2_client_secret", "client-secret",
                    "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token"));
        }

        AzureFileSystemProperties properties = new AzureFileSystemProvider().bind(input);

        Assertions.assertEquals("", properties.getAccountKey());
        Assertions.assertEquals("account", properties.toMap().get("AZURE_ACCOUNT_NAME"));
        Assertions.assertFalse(properties.toMap().containsKey("AZURE_ACCOUNT_KEY"));
        Assertions.assertFalse(properties.matchedProperties().containsKey("s3.secret_key"));
        Assertions.assertFalse(properties.matchedProperties().containsKey("AWS_SECRET_KEY"));
    }

    @Test
    void sharedKey_doesNotSilentlyFillMissingAzureKeyFromS3() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SharedKey", "azure.account_name", "account",
                "azure.endpoint", "https://account.blob.core.windows.net",
                "s3.access_key", "s3-account", "s3.secret_key", "s3-secret")));
    }

    @Test
    void sharedKey_doesNotUseS3SecretWhenProviderMarkerIsPresent() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> AzureFileSystemProperties.of(Map.of(
                "provider", "azure", "azure.auth_type", "SharedKey", "azure.account_name", "account",
                "s3.endpoint", "https://s3.example.test", "s3.access_key", "s3-account",
                "s3.secret_key", "s3-secret")));
    }

    @Test
    void sas_doesNotUseS3EndpointWhenOnlyAzureAccountIsConfigured() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "provider", "azure", "azure.auth_type", "SAS", "azure.account_name", "account",
                "azure.sas_token", "sig=temporary", "s3.endpoint", "https://s3.example.test"));

        Assertions.assertEquals("https://account.blob.core.windows.net", properties.getEndpoint());
        Assertions.assertEquals("https://account.blob.core.windows.net",
                properties.toMap().get("AZURE_ENDPOINT"));
    }

    @Test
    void sharedKey_doesNotUseS3EndpointWhenAzureCredentialsAreTyped() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "provider", "azure", "azure.account_name", "account", "azure.account_key", "key",
                "s3.endpoint", "https://s3.example.test"));

        Assertions.assertEquals("https://account.blob.core.windows.net", properties.getEndpoint());
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://account.blob.core.windows.net", "https://account.dfs.core.usgovcloudapi.net"})
    void sharedKey_preservesProviderQualifiedLegacyS3Credentials(String endpoint) {
        Map<String, String> input = Map.of("s3.endpoint", endpoint,
                "s3.access_key", "account", "s3.secret_key", "legacy-key");
        AzureFileSystemProperties properties = new AzureFileSystemProvider().bind(input);

        Assertions.assertEquals("account", properties.getAccountName());
        Assertions.assertEquals("legacy-key", properties.toMap().get("AZURE_ACCOUNT_KEY"));
        Assertions.assertEquals(input, properties.rawProperties());
        Assertions.assertEquals(input, properties.matchedProperties());
    }

    @Test
    void sharedKey_preservesExplicitAzureLegacyCredentialsForCustomEndpoints() {
        AzureFileSystemProperties properties = new AzureFileSystemProvider().bind(Map.of(
                "provider", "azure", "s3.endpoint", "https://proxy.example.test",
                "s3.access_key", "account", "s3.secret_key", "legacy-key"));

        Assertions.assertEquals("account", properties.getAccountName());
        Assertions.assertEquals("legacy-key", properties.getAccountKey());
        Assertions.assertEquals("https://proxy.example.test", properties.getEndpoint());
        Assertions.assertTrue(new AzureFileSystemProvider().sensitivePropertyKeys().containsAll(
                Set.of("s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY")));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "https://other.blob.core.windows.net",
            "https://account.blob.core.chinacloudapi.cn"
    })
    void oauth2_rejectsAccountHostAndEndpointConflictsAtBinding(String endpoint) {
        StoragePropertiesException error = Assertions.assertThrows(StoragePropertiesException.class,
                () -> AzureFileSystemProperties.of(oauth2Properties(endpoint)));

        Assertions.assertEquals("Azure OAuth2 account host does not match the storage endpoint", error.getMessage());
        Assertions.assertNull(error.getCause());
    }

    @Test
    void oauth2_rejectsAccountNameAndAccountHostConflictsAtBinding() {
        Map<String, String> input = oauth2Properties("https://account.blob.core.windows.net");
        input.put("azure.account_name", "other");

        Assertions.assertThrows(StoragePropertiesException.class, () -> AzureFileSystemProperties.of(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "https://account.blob.core.windows.net",
            "https://account.dfs.core.windows.net"
    })
    void oauth2_acceptsMatchingAccountOrStandardTransport(String endpoint) {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(oauth2Properties(endpoint));
        String path = "abfss://container@account.dfs.core.windows.net/dir/file.parquet";

        Assertions.assertEquals(path, properties.validateAndNormalizeUri(path));
        Assertions.assertEquals("account", properties.toMap().get("AZURE_ACCOUNT_NAME"));
        Assertions.assertEquals(endpoint.replace(".dfs.", ".blob."), properties.getEndpoint());
    }

    @Test
    void oauth2_acceptsCustomEndpointForStandardAbfsUri() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(
                oauth2Properties("https://proxy.example.test:8443"));

        Assertions.assertEquals("abfss://container@account.dfs.core.windows.net/dir/file.parquet",
                properties.validateAndNormalizeUri(
                        "abfss://container@account.dfs.core.windows.net/dir/file.parquet"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://login.example.test/tenant/custom/token",
            "ftp://login.example.test/tenant/oauth2/token",
            "http://login.example.test/tenant/oauth2/token",
            "https://private-material@login.example.test/tenant/oauth2/token",
            "https://login.example.test/tenant/oauth2/token?sig=private-material",
            "https://login.example.test/tenant/oauth2/token#private-material"})
    void oauth2_rejectsUnsupportedTokenEndpointsWithoutEchoingCredentials(String tokenEndpoint) {
        Map<String, String> input = oauth2Properties("https://account.blob.core.windows.net");
        input.put("azure.oauth2_server_uri", tokenEndpoint);
        StoragePropertiesException error = Assertions.assertThrows(StoragePropertiesException.class,
                () -> AzureFileSystemProperties.of(input));
        Assertions.assertNull(error.getCause());
        Assertions.assertFalse(error.getMessage().contains("private-material"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://login.microsoftonline.com/tenant/oauth2/token",
            "https://login.microsoftonline.us/tenant/oauth2/v2.0/token"})
    void oauth2_acceptsSupportedEntraTokenEndpoints(String tokenEndpoint) {
        Map<String, String> input = oauth2Properties("https://account.blob.core.windows.net");
        input.put("azure.oauth2_server_uri", tokenEndpoint);
        Assertions.assertEquals(tokenEndpoint,
                AzureFileSystemProperties.of(input).toMap().get("AZURE_OAUTH_SERVER_URI"));
    }

    @Test
    void sharedKeyKeepsLegacyS3UriWithCustomEndpoint() {
        AzureFileSystemProperties properties = new AzureFileSystemProvider().bind(Map.of(
                "provider", "azure", "s3.endpoint", "https://proxy.example.test",
                "s3.access_key", "account", "s3.secret_key", "legacy-key"));

        Assertions.assertEquals("s3://container/dir/file",
                properties.validateAndNormalizeUri("s3://container/dir/file"));
    }

    private static Map<String, String> oauth2Properties(String endpoint) {
        return new HashMap<>(Map.of(
                "azure.auth_type", "OAuth2", "azure.endpoint", endpoint,
                "azure.oauth2_account_host", "account.dfs.core.windows.net",
                "azure.oauth2_client_id", "client-id", "azure.oauth2_client_secret", "client-secret",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token"));
    }

    @ParameterizedTest
    @CsvSource({
            "AZURE_AUTH_TYPE, SAS",
            "AZURE_SAS_TOKEN, sig=wire-only",
            "AZURE_SAS_EXPIRY_MS, 1",
            "AZURE_OAUTH_SERVER_URI, https://login.microsoftonline.com/wire-tenant/oauth2/token",
            "AZURE_OAUTH_ACCOUNT_HOST, wire-account.dfs.core.windows.net"
    })
    void bind_doesNotAcceptNewBackendFieldsAsInputAliases(String key, String value) {
        Map<String, String> input = new HashMap<>(Map.of(
                "azure.account_name", "account", "azure.account_key", "catalog-key"));
        input.put(key, value);

        AzureFileSystemProperties properties = AzureFileSystemProperties.of(input);

        Assertions.assertEquals(input, properties.rawProperties());
        Assertions.assertEquals(Map.of("azure.account_name", "account", "azure.account_key", "catalog-key"),
                properties.matchedProperties());
        Assertions.assertEquals(AzureAuthType.SHARED_KEY, properties.authType());
        Assertions.assertEquals("", properties.getSasToken());
        Assertions.assertEquals("", properties.getSasExpiryMs());
        Assertions.assertEquals("", properties.getOauthServerUri());
        Assertions.assertEquals("", properties.getOauthAccountHost());
        Assertions.assertEquals(Map.of(
                "provider", "azure", "AZURE_AUTH_TYPE", "SHARED_KEY",
                "AZURE_ACCOUNT_NAME", "account", "AZURE_ACCOUNT_KEY", "catalog-key",
                "AZURE_ENDPOINT", "https://account.blob.core.windows.net"), properties.toMap());
    }

    @Test
    void toString_masksCredentialsAndNeverLeaksPlaintext() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.endpoint", "account.blob.core.windows.net",
                "azure.account_name", "azure-account",
                "azure.account_key", "azure-key-plain",
                "AZURE_CLIENT_SECRET", "azure-clientsecret-plain"));

        String rendered = properties.toString();

        Assertions.assertFalse(rendered.contains("azure-key-plain"), rendered);
        Assertions.assertFalse(rendered.contains("azure-clientsecret-plain"), rendered);
        Assertions.assertTrue(rendered.contains("accountKey=***"), rendered);
        Assertions.assertTrue(rendered.contains("clientSecret=***"), rendered);
        // accountName is the storage account identifier (also appears in the endpoint), not a secret.
        Assertions.assertTrue(rendered.contains("accountName=azure-account"), rendered);
    }

    @Test
    void toString_masksSasToken() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SAS",
                "azure.endpoint", "account.blob.core.windows.net",
                "azure.account_name", "account",
                "azure.sas_token", "sas-token-plain"));

        String rendered = properties.toString();

        Assertions.assertFalse(rendered.contains("sas-token-plain"), rendered);
        Assertions.assertTrue(rendered.contains("sasToken=***"), rendered);
    }

    @Test
    void provider_sensitivePropertyKeysCoverSecretsButNotAccountName() {
        Set<String> keys = new AzureFileSystemProvider().sensitivePropertyKeys();

        Assertions.assertTrue(keys.contains("azure.secret_key"), keys.toString());
        Assertions.assertTrue(keys.contains("AZURE_ACCOUNT_KEY"), keys.toString());
        Assertions.assertTrue(keys.contains("AZURE_CLIENT_SECRET"), keys.toString());
        Assertions.assertTrue(keys.contains("AZURE_SAS_TOKEN"), keys.toString());
        Assertions.assertFalse(keys.contains("AZURE_ACCOUNT_NAME"), keys.toString());
        Assertions.assertFalse(keys.contains("azure.access_key"), keys.toString());
    }

    @Test
    void bind_formatsEndpointFromAccountNameWhenEndpointMissing() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.account_name", "myaccount",
                "azure.account_key", "key"));

        Assertions.assertEquals("https://myaccount.blob.core.windows.net", properties.getEndpoint());
        Assertions.assertEquals(AzureAuthType.SHARED_KEY, properties.authType());
        Assertions.assertEquals("SharedKey", properties.getAzureAuthType());
    }

    @ParameterizedTest
    @ValueSource(strings = {"core.windows.net", "core.chinacloudapi.cn", "core.usgovcloudapi.net", "core.cloudapi.de"})
    void bind_convertsDfsEndpointToBlobEndpointForNativeClient(String suffix) {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.endpoint", "account.dfs." + suffix,
                "azure.account_name", "account",
                "azure.account_key", "key"));

        Assertions.assertEquals("https://account.blob." + suffix, properties.getEndpoint());
        Assertions.assertEquals("key", properties.toHadoopConfigurationMap()
                .get("fs.azure.account.key.account.blob." + suffix));
    }

    @ParameterizedTest
    @CsvSource({
            "http://account.dfs.core.windows.net:10000/proxy%2Fpath, http://account.blob.core.windows.net:10000/proxy%2Fpath",
            "http://127.0.0.1:10000/account, http://127.0.0.1:10000/account",
            "https://storage.example.test:8443, https://storage.example.test:8443"
    })
    void bind_preservesExplicitEndpointTransport(String input, String expected) {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.endpoint", input,
                "azure.account_name", "account",
                "azure.account_key", "key"));

        Assertions.assertEquals(expected, properties.getEndpoint());
        Assertions.assertEquals(expected, properties.toMap().get("AZURE_ENDPOINT"));
    }

    @Test
    void bind_usesEndpointAccountAndCloudSuffixForSas() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.endpoint", "account.dfs.core.usgovcloudapi.net",
                "azure.sas_token", "sig=temporary"));

        Assertions.assertEquals("account", properties.toMap().get("AZURE_ACCOUNT_NAME"));
        Assertions.assertEquals("https://account.blob.core.usgovcloudapi.net",
                properties.toMap().get("AZURE_ENDPOINT"));
        Assertions.assertEquals("sig=temporary", properties.toHadoopConfigurationMap()
                .get("fs.azure.sas.fixed.token.account.dfs.core.usgovcloudapi.net"));
    }

    @Test
    void bind_acceptsLegacyUppercaseKeysForExistingAzureCallers() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "AZURE_ACCOUNT_NAME", "legacy-account",
                "AZURE_ACCOUNT_KEY", "legacy-key",
                "AZURE_CONTAINER", "legacy-container"));

        Assertions.assertEquals("legacy-account", properties.getAccountName());
        Assertions.assertEquals("legacy-key", properties.getAccountKey());
        Assertions.assertEquals("legacy-container", properties.getContainer());
        Assertions.assertEquals("https://legacy-account.blob.core.windows.net", properties.getEndpoint());
    }

    @Test
    void bind_preservesHistoricalUppercaseAzureEndpointAlias() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "AZURE_ENDPOINT", "https://proxy.example.test:8443",
                "AZURE_ACCOUNT_NAME", "account",
                "AZURE_ACCOUNT_KEY", "key"));

        Assertions.assertEquals("https://proxy.example.test:8443", properties.getEndpoint());
        Assertions.assertEquals("https://proxy.example.test:8443",
                properties.toMap().get("AZURE_ENDPOINT"));
    }

    @Test
    void bind_preservesLegacyAccountAliasFallbackWithoutProviderMarker() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "AZURE_ACCOUNT_NAME", "account", "s3.secret_key", "legacy-key"));

        Assertions.assertEquals("account", properties.getAccountName());
        Assertions.assertEquals("legacy-key", properties.getAccountKey());
    }

    @ParameterizedTest
    @ValueSource(strings = {"SharedKey", "sharedkey", "SHARED_KEY", "shared_key", " SHARED_KEY "})
    void bind_normalizesSharedKeyAuthType(String authType) {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", authType,
                "azure.account_name", "account",
                "azure.account_key", "key"));

        Assertions.assertEquals(AzureAuthType.SHARED_KEY, properties.authType());
        Assertions.assertEquals("SharedKey", properties.getAzureAuthType());
        Assertions.assertEquals("SHARED_KEY", properties.toMap().get("AZURE_AUTH_TYPE"));
        Assertions.assertEquals("key", properties.toHadoopConfigurationMap()
                .get("fs.azure.account.key.account.blob.core.windows.net"));
    }

    @Test
    void bind_retainsLegacySharedKeyAliasesWithoutAcceptingTheBackendAuthType() {
        AzureFileSystemProperties rebound = AzureFileSystemProperties.of(Map.of(
                "AZURE_ACCOUNT_NAME", "account", "AZURE_ACCOUNT_KEY", "key", "AZURE_AUTH_TYPE", "SAS"));

        Assertions.assertEquals(AzureAuthType.SHARED_KEY, rebound.authType());
        Assertions.assertEquals("key", rebound.toMap().get("AZURE_ACCOUNT_KEY"));
        Assertions.assertFalse(rebound.matchedProperties().containsKey("AZURE_AUTH_TYPE"));
    }

    @Test
    void bind_infersSasWhenAuthTypeIsAbsent() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.account_name", "account",
                "azure.sas_token", "?sig=temporary"));

        Assertions.assertEquals(AzureAuthType.SAS, properties.authType());
        Assertions.assertEquals("SAS", properties.getAzureAuthType());
        Assertions.assertEquals("sig=temporary", properties.toHadoopConfigurationMap()
                .get("fs.azure.sas.fixed.token.account.dfs.core.windows.net"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"azure.auth_type", "AZURE_AUTH_TYPE"})
    void bind_infersSasWhenAuthTypeIsBlank(String authTypeKey) {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                authTypeKey, " ",
                "azure.sas_token", "sig=temporary"));

        Assertions.assertEquals(AzureAuthType.SAS, properties.authType());
        Assertions.assertFalse(properties.matchedProperties().containsKey(authTypeKey));
    }

    @Test
    void bind_backendAuthTypeDoesNotOverrideCanonicalInput() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SAS",
                "AZURE_AUTH_TYPE", "SharedKey",
                "azure.sas_token", "sig=temporary"));

        Assertions.assertEquals(AzureAuthType.SAS, properties.authType());
        Assertions.assertTrue(properties.matchedProperties().containsKey("azure.auth_type"));
        Assertions.assertFalse(properties.matchedProperties().containsKey("AZURE_AUTH_TYPE"));
    }

    @Test
    void bind_blankCanonicalAuthTypeDoesNotFallBackToBackendInput() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", " ", "AZURE_AUTH_TYPE", "SharedKey", "azure.sas_token", "sig=temporary"));

        Assertions.assertEquals(AzureAuthType.SAS, properties.authType());
        Assertions.assertEquals(Map.of("azure.sas_token", "sig=temporary"), properties.matchedProperties());
    }

    @Test
    void bind_matchesAuthTypeKeysCaseSensitivelyLikeOtherProperties() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "Azure.Auth_Type", "SharedKey",
                "azure.sas_token", "sig=temporary"));

        Assertions.assertEquals(AzureAuthType.SAS, properties.authType());
        Assertions.assertFalse(properties.matchedProperties().containsKey("Azure.Auth_Type"));
    }

    @Test
    void bind_rejectsUnknownExplicitAuthTypeWithoutSasFallback() {
        StoragePropertiesException exception = Assertions.assertThrows(StoragePropertiesException.class,
                () -> AzureFileSystemProperties.of(Map.of(
                        "azure.auth_type", "unknown",
                        "azure.sas_token", "sig=temporary")));

        Assertions.assertTrue(exception.getMessage().contains("Unsupported Azure auth_type"));
    }

    @Test
    void bind_rejectsExplicitSharedKeyWithSasToken() {
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AzureFileSystemProperties.of(Map.of(
                        "azure.auth_type", "SHARED_KEY",
                        "azure.account_name", "account",
                        "azure.account_key", "key",
                        "azure.sas_token", "sig=temporary")));

        Assertions.assertTrue(exception.getMessage().contains("sas_token must not be set"));
    }

    @Test
    void bind_rejectsExplicitSasWithoutTokenInsteadOfUsingSharedKey() {
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AzureFileSystemProperties.of(Map.of(
                        "azure.auth_type", "SAS",
                        "azure.account_name", "account",
                        "azure.account_key", "key")));

        Assertions.assertTrue(exception.getMessage().contains("When auth_type is SAS, sas_token is required"));
    }

    @Test
    void bind_rejectsIncompleteOAuth2InsteadOfUsingSharedKeyOrSas() {
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AzureFileSystemProperties.of(Map.of(
                        "azure.auth_type", "OAUTH2",
                        "azure.account_name", "account",
                        "azure.account_key", "key",
                        "azure.sas_token", "sig=temporary",
                        "azure.oauth2_client_id", "client-id")));

        Assertions.assertTrue(exception.getMessage().contains("When auth_type is OAuth2"));
    }

    @Test
    void toBackendProperties_matchesFeCoreAzureSharedKeyMap() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.endpoint", "account.blob.core.windows.net",
                "azure.account_name", "account",
                "azure.account_key", "key",
                "use_path_style", "true"));

        BackendStorageProperties backend = properties.toBackendProperties().orElseThrow();
        Map<String, String> backendMap = backend.toMap();

        Assertions.assertEquals(BackendStorageKind.NATIVE, backend.backendKind());
        Assertions.assertEquals(Map.of(
                "provider", "azure",
                "AZURE_AUTH_TYPE", "SHARED_KEY",
                "AZURE_ENDPOINT", "https://account.blob.core.windows.net",
                "AZURE_ACCOUNT_NAME", "account",
                "AZURE_ACCOUNT_KEY", "key"), backendMap);
    }

    @Test
    void toBackendProperties_emitsNativeSasCredentialsAndExpiry() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SAS",
                "AZURE_ENDPOINT", "account.blob.core.windows.net",
                "AZURE_ACCOUNT_NAME", "account",
                "AZURE_CONTAINER", "container",
                "azure.sas_token", "?sv=2024-01-01&sig=temporary",
                "azure.sas_expiry_ms", "4102444800000"));

        Map<String, String> backendMap = properties.toBackendProperties().orElseThrow().toMap();

        Assertions.assertEquals(Map.of(
                "provider", "azure",
                "AZURE_AUTH_TYPE", "SAS",
                "AZURE_ENDPOINT", "https://account.blob.core.windows.net",
                "AZURE_ACCOUNT_NAME", "account",
                "AZURE_SAS_TOKEN", "sv=2024-01-01&sig=temporary",
                "AZURE_SAS_EXPIRY_MS", "4102444800000"), backendMap);
    }

    @Test
    void bind_normalizesSasTokenAndReadsTokenExpiry() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SAS",
                "azure.account_name", "account",
                "azure.sas_token", "?sv=2024-01-01&se=2100-01-01T00:00:00Z&sig=temporary"));

        Assertions.assertEquals("sv=2024-01-01&se=2100-01-01T00:00:00Z&sig=temporary",
                properties.getSasToken());
        Assertions.assertEquals(properties.getSasToken(), properties.toMap().get("AZURE_SAS_TOKEN"));
        Assertions.assertEquals("4102444800000", properties.getSasExpiryMs());
        Assertions.assertEquals("4102444800000", properties.toMap().get("AZURE_SAS_EXPIRY_MS"));
    }

    @ParameterizedTest
    @CsvSource({"4102444800000, 4102444800000", "4133980800000, 4102531200000"})
    void toMap_emitsEarlierExplicitOrTokenExpiry(String explicitExpiry, String expectedExpiry) {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.sas_token", "se=2100-01-02T00%3A00%3A00Z&sig=a+b%2Fc",
                "azure.sas_expiry_ms", explicitExpiry));

        Assertions.assertEquals(expectedExpiry, properties.getSasExpiryMs());
        Assertions.assertEquals(expectedExpiry, properties.toMap().get("AZURE_SAS_EXPIRY_MS"));
        Assertions.assertEquals("se=2100-01-02T00%3A00%3A00Z&sig=a+b%2Fc",
                properties.toMap().get("AZURE_SAS_TOKEN"));
    }

    @Test
    void toMap_doesNotInventUnknownSasExpiry() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.sas_token", "si=stored-access-policy&sig=temporary"));

        Assertions.assertEquals("", properties.getSasExpiryMs());
        Assertions.assertDoesNotThrow(properties::validateForAccess);
        Assertions.assertFalse(properties.toMap().containsKey("AZURE_SAS_EXPIRY_MS"));
    }

    @Test
    void validateAndNormalizeUri_usesConfiguredLocationOnlyAsConsistencyCheck() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.account_name", "account",
                "azure.account_key", "key",
                "azure.container", "container"));

        String path = "abfss://container@account.dfs.core.windows.net/path/file.parquet";
        Assertions.assertEquals(path, properties.validateAndNormalizeUri(path));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> properties.validateAndNormalizeUri(
                        "abfss://other@account.dfs.core.windows.net/path/file.parquet"));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> properties.validateAndNormalizeUri(
                        "abfss://container@other.dfs.core.windows.net/path/file.parquet"));
    }

    @Test
    void bind_defersExpiredSasUntilCredentialOutput() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SAS",
                "AZURE_ENDPOINT", "account.blob.core.windows.net",
                "azure.sas_token", "sv=2024-01-01&sig=expired",
                "azure.sas_expiry_ms", "1"));

        StorageProperties bound = properties;
        Assertions.assertDoesNotThrow(bound::validate);
        StoragePropertiesException accessException = Assertions.assertThrows(StoragePropertiesException.class,
                bound::validateForAccess);
        Assertions.assertEquals("Azure SAS credential is expired", accessException.getMessage());
        Assertions.assertNull(accessException.getCause());

        StoragePropertiesException exception = Assertions.assertThrows(StoragePropertiesException.class,
                () -> properties.toMap());

        Assertions.assertTrue(exception.getMessage().contains("expired"), exception.getMessage());
        Assertions.assertThrows(StoragePropertiesException.class, properties::toHadoopConfigurationMap);
    }

    @Test
    void validateForAccess_acceptsUnexpiredSas() {
        StorageProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.sas_token", "sig=temporary",
                "azure.sas_expiry_ms", "4102444800000"));

        Assertions.assertDoesNotThrow(properties::validateForAccess);
    }

    @Test
    void validateForAccess_doesNotApplySasExpiryToSharedKeyOrOAuth2() {
        StorageProperties sharedKey = AzureFileSystemProperties.of(Map.of(
                "azure.account_name", "account",
                "azure.account_key", "key",
                "azure.sas_expiry_ms", "1"));
        StorageProperties oauth2 = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "OAuth2",
                "azure.oauth2_account_host", "account.dfs.core.windows.net",
                "azure.oauth2_client_id", "client-id",
                "azure.oauth2_client_secret", "client-secret",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token",
                "azure.sas_expiry_ms", "1"));

        Assertions.assertDoesNotThrow(sharedKey::validateForAccess);
        Assertions.assertDoesNotThrow(oauth2::validateForAccess);
    }

    @Test
    void validateSasExpiry_acceptsInjectedClockBeforeExpiry() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SAS",
                "azure.sas_token", "sv=2024-01-01&se=2026-01-02T00:00:00Z&sig=x"));

        properties.validateSasExpiry(Clock.fixed(
                Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC));
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> properties.validateSasExpiry(Clock.fixed(
                        Instant.parse("2026-01-02T00:00:00Z"), ZoneOffset.UTC)));
    }

    @Test
    void bind_rejectsConflictingExplicitCredentialMaterials() {
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AzureFileSystemProperties.of(Map.of(
                        "azure.auth_type", "OAuth2",
                        "azure.oauth2_account_host", "account.dfs.core.windows.net",
                        "azure.oauth2_client_id", "client-id",
                        "azure.oauth2_client_secret", "client-secret",
                        "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token",
                        "azure.sas_token", "sig=temporary")));

        Assertions.assertTrue(exception.getMessage().contains("sas_token must not be set"), exception.getMessage());
    }

    @Test
    void bind_rejectsAmbiguousImplicitCredentialMaterials() {
        StoragePropertiesException exception = Assertions.assertThrows(StoragePropertiesException.class,
                () -> AzureFileSystemProperties.of(Map.of(
                        "azure.account_name", "account",
                        "azure.account_key", "key",
                        "azure.sas_token", "sig=temporary")));

        Assertions.assertTrue(exception.getMessage().contains("cannot infer"), exception.getMessage());
    }

    /**
     * The full Hadoop configuration belongs only to the URI-selected OneLake backend view.
     */
    @Test
    void resolveBackendProperties_oneLakeDumpsHadoopResolvedConfig() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "OAuth2",
                "azure.oauth2_account_host", "onelake.dfs.fabric.microsoft.com",
                "azure.oauth2_client_id", "client-id",
                "azure.oauth2_client_secret", "client-secret",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token"));

        BackendStorageProperties backend = properties.resolveBackendProperties(
                "abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/file").orElseThrow();
        Map<String, String> backendMap = backend.toMap();
        Assertions.assertEquals(BackendStorageKind.HDFS, backend.backendKind());

        // 1. The OAuth config the ABFS connector actually authenticates with.
        Assertions.assertEquals("OAuth",
                backendMap.get("fs.azure.account.auth.type.onelake.dfs.fabric.microsoft.com"));
        Assertions.assertEquals("org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                backendMap.get("fs.azure.account.oauth.provider.type.onelake.dfs.fabric.microsoft.com"));
        Assertions.assertEquals("client-id",
                backendMap.get("fs.azure.account.oauth2.client.id.onelake.dfs.fabric.microsoft.com"));
        Assertions.assertEquals("client-secret",
                backendMap.get("fs.azure.account.oauth2.client.secret.onelake.dfs.fabric.microsoft.com"));
        Assertions.assertEquals("https://login.microsoftonline.com/tenant/oauth2/token",
                backendMap.get("fs.azure.account.oauth2.client.endpoint.onelake.dfs.fabric.microsoft.com"));

        // 2. hadoop core-default.xml is merged in. This is the whole reason the module compiles
        //    against hadoop-common: a plain key-value map would carry the OAuth keys above but none
        //    of these, and the change would be invisible to every other assertion.
        Assertions.assertEquals("file:///", backendMap.get("fs.defaultFS"));
        Assertions.assertTrue(backendMap.containsKey("hadoop.security.authentication"), backendMap.toString());
        Assertions.assertTrue(backendMap.size() > 100,
                "expected a resolved hadoop config, got " + backendMap.size() + " keys");

        // 3. The selected Hadoop view never carries native Azure authentication fields.
        Assertions.assertFalse(backendMap.containsKey("provider"));
        Assertions.assertFalse(backendMap.keySet().stream().anyMatch(key -> key.startsWith("AZURE_")));
    }

    @Test
    void toHadoopConfigurationMap_preservesOneLakeOAuth2Host() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "OAuth2",
                "azure.oauth2_account_host", "onelake.dfs.fabric.microsoft.com",
                "azure.oauth2_client_id", "client-id",
                "azure.oauth2_client_secret", "client-secret",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token"));

        Map<String, String> hadoop = properties.toHadoopConfigurationMap();
        Assertions.assertEquals("OAuth", hadoop.get("fs.azure.account.auth.type.onelake.dfs.fabric.microsoft.com"));
        Assertions.assertEquals("client-id",
                hadoop.get("fs.azure.account.oauth2.client.id.onelake.dfs.fabric.microsoft.com"));
        Assertions.assertEquals("client-secret",
                hadoop.get("fs.azure.account.oauth2.client.secret.onelake.dfs.fabric.microsoft.com"));
        Assertions.assertEquals("OAuth",
                properties.resolveBackendProperties("abfss://workspace@onelake.dfs.fabric.microsoft.com/file")
                        .orElseThrow().toMap().get("fs.azure.account.auth.type.onelake.dfs.fabric.microsoft.com"));
        Assertions.assertFalse(properties.toMap().keySet().stream().anyMatch(key -> key.startsWith("fs.")));
        Assertions.assertFalse(hadoop.keySet().stream().anyMatch(key -> key.contains(".blob.fabric.microsoft.com")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"OAuth2", "OAUTH2", "oauth2"})
    void bind_preservesLegacyOAuth2ClientAliases(String authType) {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", authType,
                "azure.oauth2_account_host", "account.dfs.core.windows.net",
                "AZURE_CLIENT_ID", "client-id",
                "AZURE_CLIENT_SECRET", "client-secret",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token"));

        Assertions.assertTrue(properties.isOauth2Auth());
        Assertions.assertEquals(AzureAuthType.OAUTH2, properties.authType());
        Assertions.assertEquals("OAuth2", properties.getAzureAuthType());
        Assertions.assertEquals("account.dfs.core.windows.net", properties.getOauthAccountHost());
        Assertions.assertEquals("https://login.microsoftonline.com/tenant/oauth2/token",
                properties.getOauthServerUri());
    }

    @Test
    void resolveBackendProperties_oneLakePassesUserFsKeysThroughAndNormalizesCacheFlags() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "OAuth2",
                "azure.oauth2_account_host", "onelake.dfs.fabric.microsoft.com",
                "azure.oauth2_client_id", "client-id",
                "azure.oauth2_client_secret", "client-secret",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token",
                // arbitrary user fs.* key, not azure-scoped: legacy passed the whole fs.* family through
                "fs.azure.readaheadqueue.depth", "8",
                // explicit cache flag in a spelling only BooleanUtils understands
                "fs.abfss.impl.disable.cache", "no"));

        Map<String, String> backendMap = properties.resolveBackendProperties(
                "abfss://workspace@onelake.dfs.fabric.microsoft.com/file").orElseThrow().toMap();

        Assertions.assertEquals("8", backendMap.get("fs.azure.readaheadqueue.depth"));
        // Explicit user value wins, but normalized to true/false — "no" must not reach BE verbatim.
        Assertions.assertEquals("false", backendMap.get("fs.abfss.impl.disable.cache"));
        // The other three legacy schemes are no longer force-disabled: the patched FileSystem keys
        // its cache by the per-scheme credential fingerprint instead.
        Assertions.assertNull(backendMap.get("fs.abfs.impl.disable.cache"));
        Assertions.assertNull(backendMap.get("fs.wasb.impl.disable.cache"));
        Assertions.assertNull(backendMap.get("fs.wasbs.impl.disable.cache"));
        for (String scheme : List.of("abfs", "abfss", "wasb", "wasbs")) {
            Assertions.assertEquals(properties.fsCacheFingerprint(),
                    backendMap.get(FsCacheKeys.fsCacheKeyProperty(scheme)));
        }
    }

    @Test
    void bind_rejectsMissingSharedKeyLikeFeCore() {
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AzureFileSystemProperties.of(Map.of(
                        "azure.account_name", "account")));

        Assertions.assertTrue(exception.getMessage().contains(
                "When auth_type is SharedKey, account_name and account_key are required"));
    }

    @Test
    void provider_bindReturnsAzureTypedProperties() throws IOException {
        AzureFileSystemProvider provider = new AzureFileSystemProvider();
        AzureFileSystemProperties properties = provider.bind(Map.of(
                "azure.account_name", "account",
                "azure.account_key", "key"));
        FileSystem fileSystem = provider.create(properties);

        Assertions.assertEquals("AZURE", properties.providerName());
        Assertions.assertEquals(StorageKind.OBJECT_STORAGE, properties.kind());
        Assertions.assertEquals(FileSystemType.AZURE, properties.type());
        Assertions.assertInstanceOf(AzureFileSystem.class, fileSystem);
    }

    @Test
    void provider_supportsExplicitAzureProvider() {
        AzureFileSystemProvider provider = new AzureFileSystemProvider();

        Assertions.assertTrue(provider.supports(Map.of(
                "provider", "azure")));
    }
}
