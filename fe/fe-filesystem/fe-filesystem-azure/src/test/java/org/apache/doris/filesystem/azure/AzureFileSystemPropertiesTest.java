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
                "AZURE_SAS_TOKEN", "sas-token-plain",
                "AZURE_CLIENT_SECRET", "azure-clientsecret-plain"));

        String rendered = properties.toString();

        Assertions.assertFalse(rendered.contains("azure-key-plain"), rendered);
        Assertions.assertFalse(rendered.contains("azure-clientsecret-plain"), rendered);
        Assertions.assertFalse(rendered.contains("sas-token-plain"), rendered);
        Assertions.assertTrue(rendered.contains("accountKey=***"), rendered);
        Assertions.assertTrue(rendered.contains("sasToken=***"), rendered);
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
        Assertions.assertEquals("SHARED_KEY", backendMap.get("AZURE_AUTH_TYPE"));
        Assertions.assertEquals("https://account.blob.core.windows.net", backendMap.get("AZURE_ENDPOINT"));
        Assertions.assertEquals("account", backendMap.get("AZURE_ACCOUNT_NAME"));
        Assertions.assertEquals("key", backendMap.get("AZURE_ACCOUNT_KEY"));
        Assertions.assertEquals("azure", backendMap.get("provider"));
        Assertions.assertEquals("true", backendMap.get("use_path_style"));
        Assertions.assertFalse(backendMap.keySet().stream().anyMatch(keyName -> keyName.startsWith("AWS_")));
        Assertions.assertFalse(backendMap.containsKey("AZURE_SAS_TOKEN"));
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

        Assertions.assertEquals("SAS", backendMap.get("AZURE_AUTH_TYPE"));
        Assertions.assertEquals("https://account.blob.core.windows.net", backendMap.get("AZURE_ENDPOINT"));
        Assertions.assertEquals("account", backendMap.get("AZURE_ACCOUNT_NAME"));
        Assertions.assertEquals("container", backendMap.get("AZURE_CONTAINER"));
        Assertions.assertEquals("sv=2024-01-01&sig=temporary", backendMap.get("AZURE_SAS_TOKEN"));
        Assertions.assertEquals("4102444800000", backendMap.get("AZURE_SAS_EXPIRY_MS"));
        Assertions.assertFalse(backendMap.keySet().stream().anyMatch(keyName -> keyName.startsWith("AWS_")));
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
     * Pins the compatibility OAuth2 map used by genuine Microsoft Fabric OneLake locations. Native
     * FILE_S3 Azure OAuth2 is marked explicitly and rejected by BE until a complete Entra-ID path is
     * available; OneLake remains FILE_HDFS and consumes the Hadoop settings below.
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

        // 3. Native attempts carry an explicit unsupported marker, never an AK/SK or SAS fallback.
        Assertions.assertEquals("OAUTH2", backendMap.get("AZURE_AUTH_TYPE"), backendMap.toString());
        Assertions.assertFalse(backendMap.containsKey("AZURE_ACCOUNT_KEY"), backendMap.toString());
        Assertions.assertFalse(backendMap.containsKey("AZURE_SAS_TOKEN"), backendMap.toString());
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
