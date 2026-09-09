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
import org.apache.doris.foundation.property.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

class AzureFileSystemPropertiesTest {

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

    @Test
    void bind_convertsDfsEndpointToBlobEndpointForNativeClient() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.endpoint", "account.dfs.core.chinacloudapi.cn",
                "azure.account_key", "key"));

        Assertions.assertEquals("https://account.blob.core.chinacloudapi.cn", properties.getEndpoint());
        Assertions.assertEquals("key", properties.toHadoopConfigurationMap()
                .get("fs.azure.account.key.account.blob.core.chinacloudapi.cn"));
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
    void bind_acceptsSharedKeyBackendMapRoundTrip() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.account_name", "account",
                "azure.account_key", "key"));
        Map<String, String> backendMap = properties.toMap();

        AzureFileSystemProperties rebound = AzureFileSystemProperties.of(backendMap);

        Assertions.assertEquals(AzureAuthType.SHARED_KEY, rebound.authType());
        Assertions.assertEquals(backendMap, rebound.toMap());
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
    void bind_usesAuthTypeAliasPrecedence() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SAS",
                "AZURE_AUTH_TYPE", "SharedKey",
                "azure.sas_token", "sig=temporary"));

        Assertions.assertEquals(AzureAuthType.SAS, properties.authType());
        Assertions.assertTrue(properties.matchedProperties().containsKey("azure.auth_type"));
        Assertions.assertFalse(properties.matchedProperties().containsKey("AZURE_AUTH_TYPE"));
    }

    @Test
    void bind_usesNonblankAuthTypeAlias() {
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AzureFileSystemProperties.of(Map.of(
                        "azure.auth_type", " ",
                        "AZURE_AUTH_TYPE", "SharedKey",
                        "azure.sas_token", "sig=temporary")));

        Assertions.assertTrue(exception.getMessage().contains("When auth_type is SharedKey"));
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

        Assertions.assertEquals(BackendStorageKind.S3_COMPATIBLE, backend.backendKind());
        Assertions.assertEquals(Map.of(
                "provider", "azure",
                "AZURE_AUTH_TYPE", "SHARED_KEY",
                "AZURE_ENDPOINT", "https://account.blob.core.windows.net",
                "AZURE_ACCOUNT_NAME", "account",
                "AZURE_ACCOUNT_KEY", "key",
                "use_path_style", "true"), backendMap);
    }

    @Test
    void toBackendProperties_emitsNativeSasCredentialsAndExpiry() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "AZURE_AUTH_TYPE", "SAS",
                "AZURE_ENDPOINT", "account.blob.core.windows.net",
                "AZURE_ACCOUNT_NAME", "account",
                "AZURE_CONTAINER", "container",
                "AZURE_SAS_TOKEN", "?sv=2024-01-01&sig=temporary",
                "AZURE_SAS_EXPIRY_MS", "4102444800000"));

        Map<String, String> backendMap = properties.toBackendProperties().orElseThrow().toMap();

        Assertions.assertEquals(Map.of(
                "provider", "azure",
                "AZURE_AUTH_TYPE", "SAS",
                "AZURE_ENDPOINT", "https://account.blob.core.windows.net",
                "AZURE_ACCOUNT_NAME", "account",
                "AZURE_CONTAINER", "container",
                "AZURE_SAS_TOKEN", "sv=2024-01-01&sig=temporary",
                "AZURE_SAS_EXPIRY_MS", "4102444800000",
                "use_path_style", "false"), backendMap);
    }

    @Test
    void bind_normalizesSasTokenAndReadsTokenExpiry() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SAS",
                "azure.account_name", "account",
                "azure.sas_token", "?sv=2024-01-01&se=4102444800000&sig=temporary"));

        Assertions.assertEquals("sv=2024-01-01&se=4102444800000&sig=temporary", properties.getSasToken());
        Assertions.assertEquals(properties.getSasToken(), properties.toMap().get("AZURE_SAS_TOKEN"));
    }

    @Test
    void bind_rejectsExpiredSasBeforeClientCreation() {
        StoragePropertiesException exception = Assertions.assertThrows(StoragePropertiesException.class,
                () -> AzureFileSystemProperties.of(Map.of(
                        "AZURE_AUTH_TYPE", "SAS",
                        "AZURE_ENDPOINT", "account.blob.core.windows.net",
                        "AZURE_SAS_TOKEN", "sv=2024-01-01&sig=expired",
                        "AZURE_SAS_EXPIRY_MS", "1")));

        Assertions.assertTrue(exception.getMessage().contains("expired"), exception.getMessage());
    }

    /**
     * Pins both the compatibility OAuth2 map used by genuine Microsoft Fabric OneLake locations
     * and the native service-principal fields used by ordinary Azure ABFS paths.
     */
    @Test
    void toBackendProperties_oauth2DumpsHadoopResolvedConfig() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.endpoint", "account.blob.core.windows.net",
                "azure.auth_type", "OAuth2",
                "azure.oauth2_account_host", "myaccount.dfs.core.windows.net",
                "azure.oauth2_client_id", "client-id",
                "azure.oauth2_client_secret", "client-secret",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token"));

        Map<String, String> backendMap = properties.toBackendProperties().orElseThrow().toMap();

        // 1. The OAuth config the ABFS connector actually authenticates with.
        Assertions.assertEquals("OAuth",
                backendMap.get("fs.azure.account.auth.type.myaccount.dfs.core.windows.net"));
        Assertions.assertEquals("org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                backendMap.get("fs.azure.account.oauth.provider.type.myaccount.dfs.core.windows.net"));
        Assertions.assertEquals("client-id",
                backendMap.get("fs.azure.account.oauth2.client.id.myaccount.dfs.core.windows.net"));
        Assertions.assertEquals("client-secret",
                backendMap.get("fs.azure.account.oauth2.client.secret.myaccount.dfs.core.windows.net"));
        Assertions.assertEquals("https://login.microsoftonline.com/tenant/oauth2/token",
                backendMap.get("fs.azure.account.oauth2.client.endpoint.myaccount.dfs.core.windows.net"));

        // 2. hadoop core-default.xml is merged in. This is the whole reason the module compiles
        //    against hadoop-common: a plain key-value map would carry the OAuth keys above but none
        //    of these, and the change would be invisible to every other assertion.
        Assertions.assertEquals("file:///", backendMap.get("fs.defaultFS"));
        Assertions.assertTrue(backendMap.containsKey("hadoop.security.authentication"), backendMap.toString());
        Assertions.assertTrue(backendMap.size() > 100,
                "expected a resolved hadoop config, got " + backendMap.size() + " keys");

        // 3. Native attempts carry an explicit OAuth2 marker and service-principal fields, never
        // an AK/SK or SAS fallback.
        Assertions.assertEquals("OAUTH2", backendMap.get("AZURE_AUTH_TYPE"), backendMap.toString());
        Assertions.assertEquals("client-id", backendMap.get("AZURE_CLIENT_ID"));
        Assertions.assertEquals("client-secret", backendMap.get("AZURE_CLIENT_SECRET"));
        Assertions.assertEquals("tenant", backendMap.get("AZURE_TENANT_ID"));
        Assertions.assertEquals("https://login.microsoftonline.com/tenant/oauth2/token",
                backendMap.get("AZURE_OAUTH_SERVER_URI"));
        Assertions.assertFalse(backendMap.containsKey("AZURE_ACCOUNT_KEY"), backendMap.toString());
        Assertions.assertFalse(backendMap.containsKey("AZURE_SAS_TOKEN"), backendMap.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"OAuth2", "OAUTH2", "oauth2"})
    void bind_acceptsNativeOAuth2ServerAliases(String authType) {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "AZURE_AUTH_TYPE", authType,
                "AZURE_OAUTH_ACCOUNT_HOST", "account.dfs.core.windows.net",
                "AZURE_CLIENT_ID", "client-id",
                "AZURE_CLIENT_SECRET", "client-secret",
                "AZURE_OAUTH_SERVER_URI", "https://login.microsoftonline.com/tenant/oauth2/token"));

        Assertions.assertTrue(properties.isOauth2Auth());
        Assertions.assertEquals(AzureAuthType.OAUTH2, properties.authType());
        Assertions.assertEquals("OAuth2", properties.getAzureAuthType());
        Assertions.assertEquals("account.dfs.core.windows.net", properties.getOauthAccountHost());
        Assertions.assertEquals("https://login.microsoftonline.com/tenant/oauth2/token",
                properties.getOauthServerUri());
    }

    @Test
    void toBackendProperties_oauth2PassesUserFsKeysThroughAndNormalizesCacheFlags() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.endpoint", "account.blob.core.windows.net",
                "azure.auth_type", "OAuth2",
                "azure.oauth2_account_host", "myaccount.dfs.core.windows.net",
                "azure.oauth2_client_id", "client-id",
                "azure.oauth2_client_secret", "client-secret",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token",
                // arbitrary user fs.* key, not azure-scoped: legacy passed the whole fs.* family through
                "fs.azure.readaheadqueue.depth", "8",
                // explicit cache flag in a spelling only BooleanUtils understands
                "fs.abfss.impl.disable.cache", "no"));

        Map<String, String> backendMap = properties.toBackendProperties().orElseThrow().toMap();

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
