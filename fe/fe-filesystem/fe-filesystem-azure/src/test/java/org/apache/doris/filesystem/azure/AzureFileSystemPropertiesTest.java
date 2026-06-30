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
import org.apache.doris.filesystem.properties.StorageKind;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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
        Assertions.assertFalse(backendMap.keySet().stream().anyMatch(keyName -> keyName.startsWith("AZURE_")));
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
