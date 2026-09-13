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

package org.apache.doris.datasource;

import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.StorageDesc;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TFileType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

class AzureCatalogPropertyPersistTest {
    @ParameterizedTest
    @MethodSource("authenticationProperties")
    void authenticationRebindsAfterCatalogSerialization(Map<String, String> input, Map<String, String> expected) {
        CatalogProperty original = new CatalogProperty(null, input);
        original.setPluginDerivedStorageDefaultsSupplier(Collections::emptyMap);
        Map<String, String> before = original.getStorageAdaptersMap().get(StorageTypeId.AZURE)
                .getBackendConfigProperties();

        CatalogProperty restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(original), CatalogProperty.class);
        restored.setPluginDerivedStorageDefaultsSupplier(Collections::emptyMap);
        Map<String, String> after = restored.getStorageAdaptersMap().get(StorageTypeId.AZURE)
                .getBackendConfigProperties();

        Assertions.assertEquals(input, restored.getProperties());
        Assertions.assertEquals(before, after);
        Assertions.assertEquals(expected, after);
        String location = "abfss://container@account.dfs.core.windows.net/path/file.parquet";
        LocationPath path = LocationPath.ofAdapters(location, restored.getStorageAdaptersMap());
        Assertions.assertEquals(location, path.getNormalizedLocation());
        Assertions.assertEquals(TFileType.FILE_S3, path.getTFileTypeForBE());
    }

    @ParameterizedTest
    @MethodSource("objectStoreProperties")
    void storageDescriptorRebindsRawPropertiesDuringGsonPostProcessing(
            Map<String, String> input, Map<String, String> expected) {
        StorageDesc original = new StorageDesc("azure", StorageBackend.StorageType.AZURE, input);
        Assertions.assertEquals(expected, original.getBackendConfigProperties());

        StorageDesc restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(original), StorageDesc.class);

        Assertions.assertEquals(input, restored.getProperties());
        Assertions.assertEquals(StorageTypeId.AZURE, restored.getStorageAdapter().getType());
        Assertions.assertEquals(expected, restored.getBackendConfigProperties());
    }

    @Test
    void expiredSasCanBeRestoredButCannotSupplyExecutionCredentials() {
        Map<String, String> input = Map.of("azure.account_name", "account",
                "azure.sas_token", "sig=expired-test-token", "azure.sas_expiry_ms", "1");
        CatalogProperty original = new CatalogProperty(null, input);
        CatalogProperty restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(original), CatalogProperty.class);
        restored.setPluginDerivedStorageDefaultsSupplier(Collections::emptyMap);
        StorageAdapter adapter = restored.getStorageAdaptersMap().get(StorageTypeId.AZURE);
        Assertions.assertEquals(input, restored.getProperties());
        Assertions.assertEquals(StorageTypeId.AZURE, adapter.getType());
        StoragePropertiesException catalogError = Assertions.assertThrows(StoragePropertiesException.class,
                adapter::getBackendConfigProperties);
        Assertions.assertEquals("Azure SAS credential is expired", catalogError.getMessage());

        StorageDesc descriptor = new StorageDesc("azure", StorageBackend.StorageType.AZURE, input);
        StorageDesc restoredDescriptor = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(descriptor), StorageDesc.class);
        Assertions.assertEquals(input, restoredDescriptor.getProperties());
        Assertions.assertEquals(StorageTypeId.AZURE, restoredDescriptor.getStorageAdapter().getType());
        StoragePropertiesException descriptorError = Assertions.assertThrows(StoragePropertiesException.class,
                restoredDescriptor::getBackendConfigProperties);
        Assertions.assertEquals("Azure SAS credential is expired", descriptorError.getMessage());
    }

    @Test
    void backendAuthTypeDoesNotApplyTheOAuthCatalogRestrictionToSharedKeyInput() {
        StorageAdapter adapter = StorageAdapter.ofProvider("AZURE", Map.of(
                "type", "hms", "azure.account_name", "account", "azure.account_key", "key",
                "AZURE_AUTH_TYPE", "OAuth2"));

        Assertions.assertEquals("SHARED_KEY", adapter.getBackendConfigProperties().get("AZURE_AUTH_TYPE"));
        Assertions.assertFalse(adapter.getSpiProperties().matchedProperties().containsKey("AZURE_AUTH_TYPE"));
    }

    @Test
    void canonicalOAuthInputRetainsTheIcebergRestRestriction() {
        Map<String, String> input = new HashMap<>(oauthProperties());
        input.put("type", "hms");

        UnsupportedOperationException error = Assertions.assertThrows(UnsupportedOperationException.class,
                () -> StorageAdapter.ofProvider("AZURE", input));

        Assertions.assertEquals("OAuth2 auth type is only supported for iceberg rest catalog", error.getMessage());
    }

    private static Stream<Arguments> objectStoreProperties() {
        return Stream.of(
                Arguments.of(Map.of(
                        "AZURE_ENDPOINT", "https://account.blob.core.windows.net",
                        "AZURE_ACCOUNT_NAME", "account", "AZURE_ACCOUNT_KEY", "legacy-key",
                        "AZURE_CONTAINER", "container"), Map.of(
                        "provider", "azure", "AZURE_AUTH_TYPE", "SHARED_KEY",
                        "AZURE_ENDPOINT", "https://account.blob.core.windows.net",
                        "AZURE_ACCOUNT_NAME", "account", "AZURE_ACCOUNT_KEY", "legacy-key")),
                Arguments.of(Map.of(
                        "azure.account_name", "account", "azure.auth_type", "SAS",
                        "azure.sas_token", "sig=test-signature", "azure.sas_expiry_ms", "4102444800000"), Map.of(
                        "provider", "azure", "AZURE_AUTH_TYPE", "SAS",
                        "AZURE_ENDPOINT", "https://account.blob.core.windows.net",
                        "AZURE_ACCOUNT_NAME", "account", "AZURE_SAS_TOKEN", "sig=test-signature",
                        "AZURE_SAS_EXPIRY_MS", "4102444800000")));
    }

    private static Stream<Arguments> authenticationProperties() {
        return Stream.concat(objectStoreProperties(), Stream.of(Arguments.of(oauthProperties(), Map.of(
                "provider", "azure", "AZURE_AUTH_TYPE", "OAUTH2",
                "AZURE_ENDPOINT", "https://account.blob.core.windows.net", "AZURE_ACCOUNT_NAME", "account",
                "AZURE_CLIENT_ID", "client-id", "AZURE_CLIENT_SECRET", "client-secret", "AZURE_TENANT_ID", "tenant",
                "AZURE_OAUTH_SERVER_URI", "https://login.microsoftonline.com/tenant/oauth2/token"))));
    }

    private static Map<String, String> oauthProperties() {
        return Map.of("type", "iceberg", "iceberg.catalog.type", "rest", "fs.azure.support", "true",
                "azure.auth_type", "OAuth2", "azure.oauth2_account_host", "account.dfs.core.windows.net",
                "AZURE_CLIENT_ID", "client-id", "AZURE_CLIENT_SECRET", "client-secret", "AZURE_TENANT_ID", "tenant",
                "azure.oauth2_server_uri", "https://login.microsoftonline.com/tenant/oauth2/token");
    }
}
