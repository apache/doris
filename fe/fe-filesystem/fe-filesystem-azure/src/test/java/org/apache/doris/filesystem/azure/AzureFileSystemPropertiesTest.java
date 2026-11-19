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
import org.apache.doris.filesystem.properties.AzureBackendPropertyKeys;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.FsCacheKeys;
import org.apache.doris.filesystem.properties.StorageKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    void oauth2_derivesBlobEndpointFromDfsAccountHost() {
        // Only the ADLS Gen2 host is given: the Blob endpoint of the same account and cloud is used.
        for (String[] hostAndEndpoint : new String[][] {
                {"acct.dfs.core.windows.net", "https://acct.blob.core.windows.net"},
                {"acct.dfs.core.chinacloudapi.cn", "https://acct.blob.core.chinacloudapi.cn"},
                {"https://acct.dfs.core.windows.net", "https://acct.blob.core.windows.net"},
                {"acct.blob.core.windows.net", "https://acct.blob.core.windows.net"},
                {"storage.example.com", "https://storage.example.com"}}) {
            AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                    "azure.auth_type", "OAuth2",
                    "azure.oauth2_account_host", hostAndEndpoint[0],
                    "azure.oauth2_client_id", "client",
                    "azure.oauth2_client_secret", "secret",
                    "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token"));
            Assertions.assertEquals(hostAndEndpoint[1], properties.getEndpoint(), hostAndEndpoint[0]);
        }

        // An explicit endpoint wins over the account host.
        AzureFileSystemProperties explicit = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "OAuth2",
                "azure.endpoint", "https://custom.example.com",
                "azure.oauth2_account_host", "acct.dfs.core.windows.net",
                "azure.oauth2_client_id", "client",
                "azure.oauth2_client_secret", "secret",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token"));
        Assertions.assertEquals("https://custom.example.com", explicit.getEndpoint());
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
    void toBackendProperties_matchesFeCoreAzureSharedKeyMap() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.endpoint", "account.blob.core.windows.net",
                "azure.account_name", "account",
                "azure.account_key", "key",
                "use_path_style", "true"));

        BackendStorageProperties backend = properties.toBackendProperties().orElseThrow();
        Map<String, String> backendMap = backend.toMap();

        Assertions.assertEquals(BackendStorageKind.S3_COMPATIBLE, backend.backendKind());
        Assertions.assertEquals("https://account.blob.core.windows.net", backendMap.get("AWS_ENDPOINT"));
        Assertions.assertEquals("dummy_region", backendMap.get("AWS_REGION"));
        Assertions.assertEquals("account", backendMap.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("key", backendMap.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("true", backendMap.get("AWS_NEED_OVERRIDE_ENDPOINT"));
        Assertions.assertEquals("azure", backendMap.get("provider"));
        Assertions.assertEquals("true", backendMap.get("use_path_style"));
        // Native access keys for the typed BE request, next to the S3-style keys old BEs read.
        Assertions.assertEquals("SharedKey", backendMap.get(AzureBackendPropertyKeys.AUTH_TYPE));
        Assertions.assertEquals("https://account.blob.core.windows.net",
                backendMap.get(AzureBackendPropertyKeys.ENDPOINT));
        Assertions.assertEquals("account", backendMap.get(AzureBackendPropertyKeys.ACCOUNT_NAME));
        Assertions.assertEquals("key", backendMap.get(AzureBackendPropertyKeys.ACCOUNT_KEY));
        // The hadoop Configuration dump is the OAuth2 arm ONLY: SharedKey must stay the exact
        // 7-key S3-style map plus the 4 native keys, so a regression that applies the dump
        // unconditionally shows up here.
        Assertions.assertEquals(11, backendMap.size(), backendMap.toString());
    }

    @Test
    void toBackendProperties_sasCarriesNativeKeysButNoLegacyCredential() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "sas",
                "azure.account_name", "account",
                "azure.sas_token", "?sv=2024&se=2026-10-15&sig=abc"));

        Assertions.assertTrue(properties.isSasAuth());
        Map<String, String> backendMap = properties.toBackendProperties().orElseThrow().toMap();

        Assertions.assertEquals("SAS", backendMap.get(AzureBackendPropertyKeys.AUTH_TYPE));
        // The leading '?' copied from a URL is dropped.
        Assertions.assertEquals("sv=2024&se=2026-10-15&sig=abc",
                backendMap.get(AzureBackendPropertyKeys.SAS_TOKEN));
        Assertions.assertEquals("https://account.blob.core.windows.net",
                backendMap.get(AzureBackendPropertyKeys.ENDPOINT));
        Assertions.assertEquals("azure", backendMap.get("provider"));
        // SAS has no S3-style credential, so an old BE is never handed a wrong one.
        Assertions.assertFalse(backendMap.containsKey("AWS_ACCESS_KEY"), backendMap.toString());
        Assertions.assertFalse(backendMap.containsKey("AWS_SECRET_KEY"), backendMap.toString());

        Map<String, String> hadoop = properties.toHadoopConfigurationMap();
        Assertions.assertEquals("SAS", hadoop.get("fs.azure.account.auth.type"));
        Assertions.assertEquals("sv=2024&se=2026-10-15&sig=abc", hadoop.get("fs.azure.sas.fixed.token"));
    }

    @Test
    void bind_rejectsSasWithoutTokenOrEndpoint() {
        IllegalArgumentException noToken = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AzureFileSystemProperties.of(Map.of(
                        "azure.auth_type", "SAS",
                        "azure.account_name", "account")));
        Assertions.assertTrue(noToken.getMessage().contains("When auth_type is SAS"), noToken.getMessage());
        Assertions.assertThrows(IllegalArgumentException.class, () -> AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SAS",
                "azure.sas_token", "sig=abc")));
    }

    @Test
    void toBackendProperties_sasCarriesKnownExpiry() {
        Map<String, String> backendMap = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SAS",
                "azure.account_name", "account",
                "azure.sas_token", "sv=2024&sig=abc",
                "azure.sas_token_expires_at_ms", "1792400400000")).toBackendProperties().orElseThrow().toMap();
        Assertions.assertEquals("1792400400000", backendMap.get(AzureBackendPropertyKeys.SAS_EXPIRES_AT_MS));

        Map<String, String> noExpiry = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "SAS",
                "azure.account_name", "account",
                "azure.sas_token", "sv=2024&sig=abc")).toBackendProperties().orElseThrow().toMap();
        Assertions.assertFalse(noExpiry.containsKey(AzureBackendPropertyKeys.SAS_EXPIRES_AT_MS));

        IllegalArgumentException notMillis = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AzureFileSystemProperties.of(Map.of(
                        "azure.auth_type", "SAS",
                        "azure.account_name", "account",
                        "azure.sas_token", "sv=2024&sig=abc",
                        "azure.sas_token_expires_at_ms", "2030-01-01")));
        Assertions.assertTrue(notMillis.getMessage().contains("sas_token_expires_at_ms"), notMillis.getMessage());
    }

    @Test
    void toBackendProperties_oauth2CarriesNativeClientSecretKeys() {
        AzureFileSystemProperties properties = AzureFileSystemProperties.of(Map.of(
                "azure.auth_type", "OAuth2",
                "azure.oauth2_account_host", "account.dfs.core.windows.net",
                "azure.oauth2_client_id", "client",
                "azure.oauth2_client_secret", "secret",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant-1/oauth2/token"));

        Map<String, String> backendMap = properties.toBackendProperties().orElseThrow().toMap();

        Assertions.assertEquals("OAuth2", backendMap.get(AzureBackendPropertyKeys.AUTH_TYPE));
        Assertions.assertEquals("tenant-1", backendMap.get(AzureBackendPropertyKeys.TENANT_ID));
        Assertions.assertEquals("client", backendMap.get(AzureBackendPropertyKeys.CLIENT_ID));
        Assertions.assertEquals("secret", backendMap.get(AzureBackendPropertyKeys.CLIENT_SECRET));
        Assertions.assertEquals("https://login.microsoftonline.com/",
                backendMap.get(AzureBackendPropertyKeys.AUTHORITY_HOST));
        Assertions.assertEquals("https://account.blob.core.windows.net",
                backendMap.get(AzureBackendPropertyKeys.ENDPOINT));
        // The OneLake FILE_HDFS path still gets its hadoop OAuth config.
        Assertions.assertEquals("client",
                backendMap.get("fs.azure.account.oauth2.client.id.account.dfs.core.windows.net"));
    }

    @Test
    void provider_sensitivePropertyKeysCoverSasToken() {
        Set<String> keys = new AzureFileSystemProvider().sensitivePropertyKeys();

        Assertions.assertTrue(keys.contains("azure.sas_token"), keys.toString());
        Assertions.assertTrue(keys.contains(AzureBackendPropertyKeys.SAS_TOKEN), keys.toString());
    }

    /**
     * Pins the OAuth2 backend map that moved here from fe-core {@code StorageAdapter} (which built a
     * hadoop {@code Configuration} and dumped it). BE's live consumer is Microsoft Fabric OneLake:
     * {@code abfs[s]://...dfs.fabric.microsoft.com} routes to {@code FILE_HDFS} and every entry is
     * fed into BE's JNI hadoop builder, so dropping the hadoop-resolved keys silently changes what
     * the ABFS connector is configured with.
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

        // 3. Never S3-style: OAuth2 has no AK/SK the BE S3 adapter could consume.
        Assertions.assertFalse(backendMap.containsKey("AWS_ACCESS_KEY"), backendMap.toString());
        Assertions.assertFalse(backendMap.containsKey("provider"), backendMap.toString());
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
